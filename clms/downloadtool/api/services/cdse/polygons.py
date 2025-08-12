# -*- coding: utf-8 -*-
"""
CDSE: NUTS to Polygons based on geojson file
"""
import json
from plone import api


POLYGONS_CACHE = None

# pylint: disable=global-statement


def get_polygons():
    """ Cache polygons from geojson file
    """
    print("START POLYGONS PREPARE")
    global POLYGONS_CACHE  # pylint: disable=global-statement
    if POLYGONS_CACHE is None:
        portal = api.portal.get()
        file_obj = portal.unrestrictedTraverse(
            "Plone/en/cdse/nutsgauls_geometry4326-geojson")
        POLYGONS_CACHE = json.loads(file_obj.file.data)
    print("END POLYGONS PREPARE")
    return POLYGONS_CACHE


def get_polygon(nuts_id):
    """ Get polygon by NUTS ID
    """
    polygons = get_polygons()
    for feature in polygons['features']:
        if feature['properties']['NUTS_ID'] == nuts_id:
            return feature
    return "POLYGON NOT FOUND"
