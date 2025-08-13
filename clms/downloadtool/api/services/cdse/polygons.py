# -*- coding: utf-8 -*-
"""
CDSE: NUTS to Polygons - simple in-memory cache
"""
import json
from plone import api

_POLYGONS_INDEX = None

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
        data = json.loads(file_obj.file.data)
        _POLYGONS_INDEX = {
            f["properties"]["NUTS_ID"]: f
            for f in data["features"]
        }
        print(f"LOADED {len(_POLYGONS_INDEX)} polygons.")
    return _POLYGONS_INDEX


def get_polygon(nuts_id):
    """Return polygon by NUTS_ID in O(1) time."""
    polygons = _load_polygons()
    return polygons.get(nuts_id, "POLYGON NOT FOUND")
