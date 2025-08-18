# -*- coding: utf-8 -*-
"""
CDSE process
"""

import json
from logging import getLogger
from clms.downloadtool.api.services.cdse.polygons import get_polygon
from clms.downloadtool.api.services.cdse.cdse_integration import get_token, create_batch

log = getLogger(__name__)


def cdse_response(dataset_json, response_json):
    """ CDSE dataset download request case
    """
    # pol = get_polygons()
    nuts_id = response_json.get('NUTSID', None)
    polygon = "placeholder"
    if nuts_id is not None:
        log.info("CDSE: NUTS ID %s", nuts_id)
        polygon = get_polygon(response_json['NUTSID'])

    polygon_json_str = json.dumps(polygon)
    polygon_len = len(polygon_json_str)
    polygon_preview = polygon_json_str[0:200]

    log.info("Polygon %s chars | %s...",
             polygon_len, polygon_preview)

    cdse_output_gcs = "http://www.opengis.net/def/crs/" + \
        dataset_json['OutputGCS'].replace(":", "/0/")
    response_json.update(
        {"OutputGCS": cdse_output_gcs}
    )

    token = get_token()
    log.info(token)
    batch_id = create_batch("test_file.gpkg")
    log.info(batch_id)
    # import pdb
    # pdb.set_trace()
    return {
        "status": "error",
        "msg": "WIP CDSE. Output GCS: " +
        cdse_output_gcs + "Polygon: " +
        polygon_preview,
    }
