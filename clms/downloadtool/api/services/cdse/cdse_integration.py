# -*- coding: utf-8 -*-
"""
CDSE: CDSE integration scripts
"""
import io
import geopandas as gpd
from shapely.geometry import box, shape
import boto3
import requests
from plone import api
import json

from clms.downloadtool.api.services.cdse.polygons import get_polygon


def get_portal_config():
    """Get CDSE and S3 bucket configuration from the portal catalog"""
    return {
        'token_url': api.portal.get_registry_record(
            "clms.downloadtool.cdse_config_controlpanel.token_url"
        ),
        's3_bucket_name': api.portal.get_registry_record(
            "clms.downloadtool.cdse_config_controlpanel.s3_bucket_name"
        ),
        's3_access_key': api.portal.get_registry_record(
            "clms.downloadtool.cdse_config_controlpanel.s3_bucket_accesskey"
        ),
        's3_secret_key': api.portal.get_registry_record(
            # pylint: disable=line-too-long
            "clms.downloadtool.cdse_config_controlpanel.s3_bucket_secretaccesskey"  # noqa: E501
        ),
        'client_id': api.portal.get_registry_record(
            "clms.downloadtool.cdse_config_controlpanel.client_id"
        ),
        'client_secret': api.portal.get_registry_record(
            "clms.downloadtool.cdse_config_controlpanel.client_secret"
        ),
        'batch_url': api.portal.get_registry_record(
            "clms.downloadtool.cdse_config_controlpanel.batch_url"
        ),
        's3_endpoint_url': api.portal.get_registry_record(
            "clms.downloadtool.cdse_config_controlpanel.s3_endpoint_url"
        ),
        'layers_collection_url': api.portal.get_registry_record(
            "clms.downloadtool.cdse_config_controlpanel.layers_collection_url"
        )
    }


def get_token():
    """Get token for CDSE"""
    config = get_portal_config()
    token_response = requests.post(config['token_url'], data={
        "grant_type": "client_credentials",
        "client_id": config['client_id'],
        "client_secret": config['client_secret']
    })

    token = token_response.json().get("access_token")
    if not token:
        raise RuntimeError("Failed to obtain token.")
    print("Token acquired successfully.")

    return token


def generate_evalscript(layer_ids):
    """Generate evalscript dynamically based on layer IDs"""
    # Create input array with layer IDs plus dataMask
    input_array = json.dumps(layer_ids + ["dataMask"])

    # Create output array with all layer IDs
    output_items = []
    for layer_id in layer_ids:
        output_items.append(f'      {{ id: "{layer_id}", bands: 1}}')
    output_array = ",\n".join(output_items)

    # Create return object for evaluatePixel
    return_items = []
    for layer_id in layer_ids:
        return_items.append(f'    {layer_id}: [samples.{layer_id}]')
    return_object = ",\n".join(return_items)

    # Generate JavaScript evalscript for Sentinel Hub
    evalscript = f"""//VERSION=3
const factor = 1;
const offset = 0;

function setup() {{
  return {{
    input: {input_array},
    output: [
{output_array}
    ],
  }};
}}

function evaluatePixel(samples) {{
  return {{
{return_object}
  }};
}}
"""
    return evalscript


def _generate_crs_url(crs_code):
    """Generate CRS URL from CRS code"""
    return "http://www.opengis.net/def/crs/" + \
        crs_code.replace(":", "/0/")


def create_batch(geopackage_file, cdse_dataset):
    """Create batch process and return batch ID"""
    config = get_portal_config()

    target_crs = cdse_dataset["OutputGCS"]
    datasource = cdse_dataset["ByocCollection"]
    service_endpoint = cdse_dataset["ViewService"].split('/')[-1]

    # WIP: check if they exist first
    time_range_start = cdse_dataset["TemporalFilter"]["StartDate"]
    time_range_end = cdse_dataset["TemporalFilter"]["EndDate"]

    geom = None
    geometry = None
    gdf_identifier = None
    geometry_source = None
    payload_bounds = None
    crs_url = None

    has_bbox = cdse_dataset.get('BoundingBox', None) is not None
    has_nutsid = cdse_dataset.get('NUTSID', None) is not None

    if has_bbox:
        t_bbox = cdse_dataset["BoundingBox"]
        geom = box(t_bbox[0], t_bbox[1], t_bbox[2], t_bbox[3])
        gdf_identifier = "full_tile"
        geometry_source = "bbox"
    elif has_nutsid:
        geometry_source = "nuts"
        polygon_data = get_polygon(cdse_dataset["NUTSID"])
        geometry = polygon_data["geometry"]
        geom = shape(geometry)
        gdf_identifier = "tile_" + cdse_dataset["NUTSID"]

    gdf = gpd.GeoDataFrame({
        "id": [1],
        "identifier": [gdf_identifier],
        "width": [1000],
        "height": [1000],
        "resolution": [0.0001],
    }, geometry=[geom], crs="EPSG:4326")

    if target_crs and target_crs.upper() != "EPSG:4326":
        gdf_processed = gdf.to_crs(target_crs)
        crs_url = _generate_crs_url(target_crs)

        if geometry_source == "bbox":
            minx, miny, maxx, maxy = gdf_processed.total_bounds
            payload_bounds = {
                "bbox": [minx, miny, maxx, maxy],
                "properties": {"crs": crs_url}
            }
        else:
            payload_bounds = {
                "geometry": gdf_processed.geometry.iloc[0].__geo_interface__,
                "properties": {"crs": crs_url}
            }
    else:
        gdf_processed = gdf
        crs_url = _generate_crs_url("EPSG:4326")

        if geometry_source == "bbox":
            payload_bounds = {
                "bbox": cdse_dataset["BoundingBox"],
                "properties": {"crs": crs_url}
            }
        else:
            payload_bounds = {
                "geometry": geometry,
                "properties": {"crs": crs_url}
            }

    buffer = io.BytesIO()
    gdf_processed.to_file(buffer, driver="GPKG")
    buffer.seek(0)

    s3 = boto3.client(
        "s3",
        endpoint_url=config['s3_endpoint_url'],
        aws_access_key_id=config['s3_access_key'],
        aws_secret_access_key=config['s3_secret_key']
    )
    s3.upload_fileobj(buffer, config['s3_bucket_name'], geopackage_file)
    gpkg_url = f"s3://{config['s3_bucket_name']}/{geopackage_file}"

    description = f"ndvi_{geometry_source}"
    if geometry_source == "nuts":
        description += "_" + cdse_dataset["NUTSID"]
    if target_crs and target_crs.upper() != "EPSG:4326":
        description += f"_{target_crs.lower().replace(':', '')}"

    token = get_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    layers_url = config['layers_collection_url'] + \
        service_endpoint + "/layers"
    response_layers = requests.get(layers_url, headers=headers)

    if response_layers.status_code == 200:
        data = response_layers.json()
        layer_ids = [d["id"] for d in data]
        evalscript = generate_evalscript(layer_ids)
    else:
        print(f"Error {response_layers.status_code}: {response_layers.text}")

    # Build responses array for each layer
    responses = []
    for layer_id in layer_ids:
        responses.append({
            "identifier": layer_id,
            "format": {"type": "image/tiff"}
        })

    payload = {
        "processRequest": {
            "input": {
                "bounds": payload_bounds,
                "data": [
                    {
                        "type": "byoc-" + datasource,
                        "dataFilter": {
                            "timeRange": {
                                "from": time_range_start,
                                "to": time_range_end
                            }
                        }
                    }
                ]
            },
            "output": {
                "responses": responses
            },
            "evalscript": evalscript
        },
        "input": {
            "type": "geopackage",
            "features": {
                "s3": {
                    "url": gpkg_url,
                    "accessKey": config['s3_access_key'],
                    "secretAccessKey": config['s3_secret_key']
                }
            }
        },
        "output": {
            "type": "raster",
            "delivery": {
                "s3": {
                    "url": f"s3://{config['s3_bucket_name']}/output",
                    "accessKey": config['s3_access_key'],
                    "secretAccessKey": config['s3_secret_key']
                }
            }
        },
        "description": description
    }

    response = requests.post(
        config['batch_url'], headers=headers, json=payload)

    if response.status_code != 201:
        print(f"Batch failed: {response.status_code} - {response.text}")
        return {
            'batch_id': None,
            'error': response.text
        }

    response_json = response.json()
    batch_id = response_json['id']

    print(f"Batch created successfully with ID: {batch_id}")
    return {
        'batch_id': batch_id
    }


def start_batch(batch_id):
    """Start the batch process"""
    config = get_portal_config()
    url = f"{config['batch_url']}/{batch_id}/start"
    print(url)

    token = get_token()
    # Token
    headers = {
        "Authorization": f"Bearer {token}"
    }

    # POST request
    response = requests.post(url, headers=headers)
    print(response.status_code)


# pylint: disable=line-too-long
# CREATED is not queued; it's a batch that waits for the start and that should NOT be a "normal" status  # noqa: E501
# a batch in CREATED should probably be sent a start call or further checked
status_map = {
    "CREATED": "QUEUED",
    "ANALYSIS": "QUEUED",
    "ANALYSIS_DONE": "QUEUED",
    "PROCESSING": "IN PROGRESS",
    "DONE": "FINISHED_OK",
    "FAILED": "REJECTED",
    "STOPPED": "CANCELLED"
}


def get_status(token, batch_url, batch_id=None):
    """Get status for all batch processes"""
    if batch_id:
        url = f"{batch_url}/{batch_id}"
    else:
        url = batch_url

    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(url, headers=headers)
    data = response.json()
    # print(data)

    result = {}
    if 'data' in data:
        for batch in data['data']:
            batch_id = batch['id']
            status = batch['status']
            error = batch.get('error', '')  # in case of FAIL we will know why
            result[batch_id] = {'original_status': status,
                                'status': status_map[status],
                                'error': error}

    return result


# Example usage:
# batch_id = create_batch("test_file.gpkg")
# # time.sleep(1)
# start_batch(batch_id)

# Example to get status of all batches:
# pylint: disable=line-too-long
# token = get_token()
# all_batches_status = get_status(token, config['batch_url'])
# print('All batches:', all_batches_status)
