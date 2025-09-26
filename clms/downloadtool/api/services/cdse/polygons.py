# -*- coding: utf-8 -*-
"""
CDSE: NUTS to Polygons - simple in-memory cache
"""
import json
from plone import api
import geopandas as gpd

_POLYGONS_INDEX = None
MAX_POINTS = 1500

# pylint: disable=global-statement


def _load_polygons():
    """Load and index polygons once per worker."""
    global _POLYGONS_INDEX
    if _POLYGONS_INDEX is None:
        print("LOADING polygons into memory...")
        portal = api.portal.get()
        file_obj = portal.unrestrictedTraverse(
            "Plone/en/cdse/nutsgauls_geometry4326-geojson"
        )
        _POLYGONS_INDEX = gpd.read_file(file_obj.file.data)
    return _POLYGONS_INDEX


def get_polygon(nuts_id):
    """Return polygon by NUTS_ID in O(1) time."""
    gdf = _load_polygons()
    return gdf[gdf["NUTS_ID"] == nuts_id]
