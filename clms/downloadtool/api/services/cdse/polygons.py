# -*- coding: utf-8 -*-
"""
CDSE: NUTS to Polygons - simple in-memory cache
"""
import json
from plone import api
from shapely.geometry import Polygon, MultiPolygon

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


def split_large_polygon(polygon, max_points=MAX_POINTS):
    """Split large polygon in multiple subpolygons"""
    coords = list(polygon.exterior.coords)
    if len(coords) <= max_points:
        return [polygon]
    subpolygons = []
    for i in range(0, len(coords)-1, max_points-1):
        sub_coords = coords[i:i+max_points]
        if sub_coords[0] != sub_coords[-1]:
            sub_coords.append(sub_coords[0])
        subpolygons.append(Polygon(sub_coords))
    return subpolygons


def process_geometry(geometry, max_points=MAX_POINTS):
    """Process geometry of Polygon or MultiPolygon"""
    polygons_to_use = []
    if isinstance(geometry, Polygon):
        polygons_to_use.extend(split_large_polygon(geometry, max_points))
    elif isinstance(geometry, MultiPolygon):
        for poly in geometry.geoms:
            polygons_to_use.extend(split_large_polygon(poly, max_points))
    else:
        raise TypeError("Geometry must be Polygon or MultiPolygon")
    return polygons_to_use
