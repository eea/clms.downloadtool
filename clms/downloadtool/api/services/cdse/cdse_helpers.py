"""CDSE helpers"""
import math
import requests
from shapely.geometry import box, Polygon, MultiPolygon
from shapely.ops import transform
import pyproj
import numpy as np

MAX_PX = 3500


def reproject_geom(geom, src_epsg, dst_epsg):
    """Reproject"""
    project = pyproj.Transformer.from_crs(
        f"EPSG:{src_epsg}", f"EPSG:{dst_epsg}", always_xy=True).transform
    return transform(project, geom)


def extract_polygons(geom):
    """Extract polygons"""
    if geom.is_empty:
        return MultiPolygon()
    if isinstance(geom, Polygon):
        return MultiPolygon([geom])
    if isinstance(geom, MultiPolygon):
        return geom
    raise ValueError(f"Unsupported geometry type: {geom.geom_type}")


def count_vertices(geom):
    """Count vertices"""
    if isinstance(geom, Polygon):
        return len(geom.exterior.coords)
    if isinstance(geom, MultiPolygon):
        return sum(len(poly.exterior.coords) for poly in geom.geoms)
    return 0


def make_initial_grid(bounds, cell_size):
    """Make grid"""
    minx, miny, maxx, maxy = bounds
    xs = np.arange(minx, maxx, cell_size)
    ys = np.arange(miny, maxy, cell_size)
    tiles = []
    for x in xs:
        for y in ys:
            x2 = min(x + cell_size, maxx)
            y2 = min(y + cell_size, maxy)
            tiles.append(box(x, y, x2, y2))
    return tiles


def split_tile(tile):
    """Split tile"""
    minx, miny, maxx, maxy = tile.bounds
    w = maxx - minx
    h = maxy - miny
    if w >= h:
        xm = (minx + maxx) / 2.0
        return [box(minx, miny, xm, maxy), box(xm, miny, maxx, maxy)]
    ym = (miny + maxy) / 2.0
    return [box(minx, miny, maxx, ym), box(minx, ym, maxx, maxy)]


def plan_tiles(aoi_wgs84, target_epsg, max_side_m, resolution, points_limit=0):
    """Plan tiles
    points_limit = 0 when we have bbox
    """
    aoi_m = reproject_geom(aoi_wgs84, 4326, target_epsg)
    initial_tiles = make_initial_grid(aoi_m.bounds, max_side_m)
    accepted = []
    queue = list(initial_tiles)

    while queue:
        tile = queue.pop()
        clip = aoi_m.intersection(tile)
        clip = extract_polygons(clip)
        if clip.is_empty:
            continue

        minx, miny, maxx, maxy = tile.bounds
        w_m = maxx - minx
        h_m = maxy - miny
        w_px = int(math.ceil(w_m / resolution))
        h_px = int(math.ceil(h_m / resolution))

        if points_limit == 0:
            if w_px <= MAX_PX and h_px <= MAX_PX:
                accepted.append({
                    "bbox": [minx, miny, maxx, maxy],
                    "bbox_epsg": target_epsg,
                    "width_px": w_px,
                    "height_px": h_px,
                    "clip_geom": clip
                })
                continue
        else:
            n_points = count_vertices(clip)
            if n_points <= points_limit and w_m <= max_side_m and \
                    h_m <= max_side_m:
                if w_px <= MAX_PX and h_px <= MAX_PX:
                    accepted.append({
                        "bbox": [minx, miny, maxx, maxy],
                        "bbox_epsg": target_epsg,
                        "width_px": w_px,
                        "height_px": h_px,
                        "clip_geom": clip
                    })
                    continue
        queue.extend(split_tile(tile))
    return accepted


def to_multipolygon(geom):
    """Transform to MultiPolygon"""
    if geom.is_empty:
        return MultiPolygon()
    if isinstance(geom, Polygon):
        return MultiPolygon([geom])
    if isinstance(geom, MultiPolygon):
        return geom
    raise ValueError(f"Unsupported geometry type: {geom.geom_type}")


def request_Catalog_API(token, byoc_id, bbox_array, date_from, date_to,
                        url_catalog_api, limit=10):
    """Request Catalog API"""
    headers = {
        'Content-type': 'application/json',
        'Authorization': f'Bearer {token}',
        "Accept": "application/geo+json"
    }
    data = {
        "bbox": bbox_array,
        "datetime": "{date_from}/{date_to}".format(
            date_from=date_from, date_to=date_to),
        "collections": [byoc_id],
        "limit": limit
    }
    response = requests.post(url_catalog_api, headers=headers, json=data)
    if response.status_code == 200:
        print("ok")
        return response.json()
    print(f"Error {response.status_code}: {response.text}")
    return False
