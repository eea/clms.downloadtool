# -*- coding: utf-8 -*-
"""
CDSE: NUTS to Polygons - simple in-memory cache
"""
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
    """Return polygon rows matching NUTS_ID or fallback ISO_2DIGIT.

    Attempts exact match on column 'NUTS_ID'. If no rows found, tries
    'ISO_2DIGIT' (country-level fallback). Raises ValueError if neither
    yields a result. Returns a GeoDataFrame subset (may contain multiple
    rows if identifier is not unique)."""
    gdf = _load_polygons()

    if "NUTS_ID" in gdf.columns:
        subset = gdf[gdf["NUTS_ID"] == nuts_id]
        if not subset.empty:
            return subset

    if "ISO_2DIGIT" in gdf.columns:
        subset = gdf[gdf["ISO_2DIGIT"] == nuts_id]
        if not subset.empty:
            return subset

    raise ValueError(f"Geometry Not Found for identifier '{nuts_id}'")
            
