# -*- coding: utf-8 -*-
"""
CDSE: CDSE integration scripts
"""
import re
import io
import json
import uuid
from datetime import datetime, timedelta, timezone
from logging import getLogger
from zoneinfo import ZoneInfo
from shapely.geometry import box
import geopandas as gpd
import boto3
import requests
from plone import api

from clms.downloadtool.api.services.cdse.cdse_helpers import (
    plan_tiles, to_multipolygon, reproject_geom, request_Catalog_API
)
from clms.downloadtool.api.services.cdse.s3_cleanup import (
    list_files, delete_file, delete_directory
)

from clms.downloadtool.api.services.cdse.polygons import get_polygon

log = getLogger(__name__)

TZ = ZoneInfo("Europe/Madrid")
POLL_INTERVAL = 10
LOCAL_GPKG_FILE = "area_of_interest.gpkg"
GPKG_S3_KEY = "custom_grid/area_of_interest.gpkg"
# pylint: disable=line-too-long
CATALOG_API_URL = "https://sh.dataspace.copernicus.eu/api/v1/catalog/1.0.0/search"  # noqa: E501
RESOLUTION_M = 1000  # default
MAX_PX = 3500
MAX_POINTS = 1500
LIMIT = 100


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


def get_s3():
    """s3 client"""
    config = get_portal_config()
    s3 = boto3.client(
        "s3",
        endpoint_url=config['s3_endpoint_url'],
        aws_access_key_id=config['s3_access_key'],
        aws_secret_access_key=config['s3_secret_key']
    )
    return s3


def get_s3_bucket():
    """Bucket name from our config"""
    config = get_portal_config()
    return config['s3_bucket_name']


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


def generate_evalscript(layer_ids, dt_forName):
    """Generate evalscript dynamically based on layer IDs"""
    # Create input array with layer IDs plus dataMask
    input_array = json.dumps(layer_ids + ["dataMask"])

    # Create output array with all layer IDs
    output_items = []
    for layer_id in layer_ids:
        output_items.append(
            f"""{{ id: "{layer_id}_{dt_forName}", bands: 1, sampleType: "FLOAT32"}}""")    # noqa: E501
    output_array = ",\n".join(output_items)

    # Create return object for evaluatePixel
    return_items = []
    band_algebra = ""
    for layer_id in layer_ids:
        band_algebra = band_algebra + f"""\nvar {layer_id}_outputVal = samples.dataMask === 1 ? samples.{layer_id} : NaN;"""    # noqa: E501
        return_items.append(
            f'    {layer_id}_{dt_forName}: [samples.{layer_id}]')
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
  {band_algebra}
  return {{
{return_object}
  }};
}}
"""
    return evalscript


def create_batches(cdse_dataset):
    """Create batches"""
    match = re.search(r"raster\s+(\d+)\s*(km|m)", cdse_dataset["DatasetTitle"])

    if match:
        value, unit = match.groups()
        value = int(value.strip())
        if unit == "km":
            value *= 1000
        # RESOLUTION_M = value
        resolution_value = value

        MAX_SIDE_M = resolution_value * MAX_PX
    else:
        raise ValueError("Missing Resolution in Dataset Title in m/km")

    token = get_token()
    if cdse_dataset.get("BoundingBox"):
        t_bbox = cdse_dataset["BoundingBox"]
        geom_wgs84 = box(t_bbox[0], t_bbox[1], t_bbox[2], t_bbox[3])
        tiles = plan_tiles(geom_wgs84, 3035, MAX_SIDE_M, resolution_value)
    elif cdse_dataset.get("NUTSID"):
        polygon_data = get_polygon(cdse_dataset["NUTSID"])
        geom_wgs84 = polygon_data["geometry"].iloc[0]
        tiles = plan_tiles(geom_wgs84, 3035, MAX_SIDE_M,
                           resolution_value, MAX_POINTS)
    else:
        raise ValueError("Dataset must contain either BoundingBox or NUTSID")

    geoms_out = [to_multipolygon(reproject_geom(
        t["clip_geom"], 3035, 4326)) for t in tiles]

    gdf = gpd.GeoDataFrame(
        {
            "id": list(range(1, len(tiles) + 1)),
            "identifier": [f"tile_{i}" for i in range(1, len(tiles) + 1)],
            "width": [t["width_px"] for t in tiles],
            "height": [t["height_px"] for t in tiles]
        },
        geometry=geoms_out,
        crs="EPSG:4326"
    )

    buffer = io.BytesIO()
    gdf.to_file(buffer, driver="GPKG")
    buffer.seek(0)

    config = get_portal_config()
    datasource = cdse_dataset["ByocCollection"]

    brains = api.content.find(UID=cdse_dataset["DatasetID"])
    service_endpoint = brains[0].getObject().mapviewer_service_id

    time_range_start = cdse_dataset["TemporalFilter"]["StartDate"]
    time_range_end = cdse_dataset["TemporalFilter"]["EndDate"]

    s3 = boto3.client(
        "s3",
        endpoint_url=config['s3_endpoint_url'],
        aws_access_key_id=config['s3_access_key'],
        aws_secret_access_key=config['s3_secret_key']
    )

    unique_geopackage_id = str(uuid.uuid4())
    gpkg_name = f"{unique_geopackage_id}.gpkg"

    s3.upload_fileobj(buffer, config['s3_bucket_name'], gpkg_name)
    gpkg_url = f"s3://{config['s3_bucket_name']}/{gpkg_name}"

    #  Catalog API to get available dates
    bbox_array = [geom_wgs84.bounds[0], geom_wgs84.bounds[1],
                  geom_wgs84.bounds[2], geom_wgs84.bounds[3]]

    catalog_data = request_Catalog_API(
        token, "byoc-" + datasource, bbox_array, time_range_start,
        time_range_end, CATALOG_API_URL, limit=LIMIT)
    if not catalog_data or "features" not in catalog_data:
        raise RuntimeError("No data returned from Catalog API")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    layers_url = config['layers_collection_url'] + service_endpoint + "/layers"
    response_layers = requests.get(layers_url, headers=headers)

    if response_layers.status_code == 200:
        data = response_layers.json()
        layer_ids = [d["id"] for d in data]
    else:
        print(f"Error {response_layers.status_code}: {response_layers.text}")
        return {"batch_id": None, "error": response_layers.text}

    all_results = []
    for feature in catalog_data["features"]:
        dt_str = feature["properties"]["datetime"]
        dt = datetime.strptime(
            dt_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        start = (dt - timedelta(seconds=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
        end = (dt + timedelta(seconds=10)).strftime("%Y-%m-%dT%H:%M:%SZ")

        dt_forName = dt.strftime("%Y%m%dT%H%M%SZ")

        evalscript = generate_evalscript(layer_ids, dt_forName)
        responses = []

        for layer_id in layer_ids:
            responses.append({
                "identifier": f"{layer_id}_{dt_forName}",
                "format": {"type": "image/tiff"}
            })
        
        token = get_token()
        headers = {"Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"}

        payload = {
            "processRequest": {
                "input": {
                    "data": [
                        {
                            "type": "byoc-" + datasource,
                            "dataFilter": {
                                "timeRange": {
                                    "from": start, "to": end
                                }
                            }
                        }
                    ]
                },
                "output": {"responses": responses},
                "evalscript": evalscript
            },
            "input": {
                "type": "geopackage",
                "features": {
                    "s3": {
                        "url": gpkg_url,
                        "accessKey": config['s3_access_key'],
                        "secretAccessKey": config['s3_secret_key'],                        
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
            "description": f"{dt_forName}"
        }
        response = requests.post(
            config['batch_url'], headers=headers, json=payload)

        if response.status_code != 201:
            print(
                f"Batch failed: {response.status_code} - {response.text}")
            return {'batch_id': None, 'error': response.text}

        response_json = response.json()
        batch_id = response_json['id']

        start_batch_response = start_batch(batch_id)
        if start_batch_response.status_code not in [200, 204]:
            print("Error starting batch:", start_batch_response.text)
            continue
        print(f"Batch {batch_id} started for date {dt_str}")

        all_results.append({
            "batch_id": batch_id,
            "gpkg_name": gpkg_name,
        })

    return all_results


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
    return response


# CREATED is not queued; it's a batch that waits for the start
# and that should NOT be a "normal" status
# a batch in CREATED should probably be sent a start call or further checked
status_map = {
    "CREATED": "QUEUED",
    "ANALYSIS": "QUEUED",
    "ANALYSING": "QUEUED",
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
    else:
        # we requested status for a single batch_id
        batch = data
        batch_id = batch['id']
        status = batch['status']
        error = batch.get('error', '')  # in case of FAIL we will know why
        result[batch_id] = {'original_status': status,
                            'status': status_map[status],
                            'error': error}

    return result


def stop_batch_and_remove_s3_directory(s3, bucket, batch_id):
    """Stop the batch process and remove directory from s3"""
    config = get_portal_config()
    url = f"{config['batch_url']}/{batch_id}/stop"
    print(url)

    token = get_token()
    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = requests.post(url, headers=headers)
    print(response.status_code)

    # WIP return the status and block the cancelling in case of error
    # Example:
    # '{"error":{"status":400,"reason":"Bad Request",
    # "message":"Illegal to change userAction from START to STOP with task
    # status CREATED","code":"COMMON_BAD_PAYLOAD"}}'

    delete_directory(s3, bucket, "output/" + batch_id)


def stop_batch_ids_and_remove_s3_directory(batch_ids):
    """Stop list of batch_ids and remove directories from s3"""
    s3 = get_s3()
    bucket = get_s3_bucket()
    for batch_id in batch_ids:
        stop_batch_and_remove_s3_directory(s3, bucket, batch_id)


def clean_s3_bucket_files(filenames):
    """Clean s3 bucket files"""
    s3 = get_s3()
    bucket = get_s3_bucket()
    root_files = list_files(s3, bucket)

    for filename in filenames:
        if filename in root_files:
            delete_file(s3, bucket, filename)


# Example usage:
# batch_id = create_batch("test_file.gpkg")
# # time.sleep(1)
# start_batch(batch_id)

# Example to get status of all batches:
# pylint: disable=line-too-long
# token = get_token()
# all_batches_status = get_status(token, config['batch_url'])
# print('All batches:', all_batches_status)
