# -*- coding: utf-8 -*-
"""
CDSE: CDSE integration scripts
"""
import io
import geopandas as gpd
from shapely.geometry import box
import boto3
import requests
from plone import api


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
            "clms.downloadtool.cdse_config_controlpanel.s3_endpoint_url")
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


def create_batch(geopackage_file, geom=None):
    """Create batch process and return batch ID"""
    # GPKG EPSG:4326
    # geometry must come as a parameter from the API call
    if geom is None:
        geom = box(6.89, 51.01, 7.11, 51.10)

    gdf = gpd.GeoDataFrame({
        "id": [1],
        "identifier": ["full_tile"],
        "width": [1000],
        "height": [1000],
        "resolution": [0.0001],
    }, geometry=[geom], crs="EPSG:4326")

    target_crs = 'EPSG:3857'
    datasource = 'byoc-61caacdf-8a23-471c-b3d3-e5a8a537c44d'
    time_range_start = "2006-12-21T00:00:00Z"
    time_range_end = "2006-12-21T23:59:59Z"

    # REPROJECT
    gdf_reprojected = gdf.to_crs(target_crs)

    # EXPORT GPKG
    buffer = io.BytesIO()
    gdf_reprojected.to_file(buffer, driver="GPKG")
    buffer.seek(0)

    # UPLOAD TO S3
    s3 = boto3.client(
        "s3",
        endpoint_url=config['s3_endpoint_url'],
        aws_access_key_id=config['s3_access_key'],
        aws_secret_access_key=config['s3_secret_key']
    )
    s3.upload_fileobj(buffer, config['s3_bucket_name'], geopackage_file)
    gpkg_url = f"s3://{config['s3_bucket_name']}/{geopackage_file}"
    print(f"GPKG uploaded to {gpkg_url}")

    # EVALSCRIPT
    evalscript = """
    //VERSION=3
    const factor = 1 / 250;
    const offset = -0.08;

    function setup() {
      return {
        input: ["NDVI", "dataMask"],
        output: {
          bands: 1,
          sampleType: "FLOAT32"
        }
      };
    }

    function evaluatePixel(sample) {
      if (sample.NDVI === 254 || isNaN(sample.NDVI)) {
        return [0];
      }
      let val = sample.NDVI * factor + offset;
      val = Math.max(-0.08, Math.min(val, 0.93));
      return [val];
    }
    """
    # BBOX IN EPSG:3857
    minx, miny, maxx, maxy = gdf_reprojected.total_bounds

    # create URL from target_crs (ticket / Ghita)
    crs_url = "http://www.opengis.net/def/crs/EPSG/0/3857"

    payload = {
        "processRequest": {
            "input": {
                "bounds": {
                    "bbox": [minx, miny, maxx, maxy],
                    "properties": {"crs": crs_url}
                },
                "data": [
                    {
                        "type": datasource,
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
                "responses": [
                    {"identifier": "default", "format": {"type": "image/tiff"}}
                ]
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
        "description": "ndvi_singlelayer_bbox_epsg3857"
    }

    token = get_token()

    headers = {"Authorization": f"Bearer {token}",
               "Content-Type": "application/json"}
    response = requests.post(
        config['batch_url'], headers=headers, json=payload)

    print("Status:", response.status_code)
    print("Response:", response.text)

    response_json = response.json()

    print("Batch id:", response_json['id'])
    return response_json['id']


def start_batch(batch_id):
    """Start the batch process"""
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
    print(data)

    result = {}
    if 'data' in data:
        for batch in data['data']:
            batch_id = batch['id']
            status = batch['status']
            result[batch_id] = {'original_status': status,
                                'status': status_map[status]}

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
