"""CDSE helpers"""
import ast
import json
import math
import operator
import re

import numpy as np
import pyproj
import requests
from shapely.geometry import MultiPolygon, Polygon, box
from shapely.ops import transform


MAX_PX = 3500


def reproject_geom(geom, src_epsg, dst_epsg):
    """Reproject"""
    project = pyproj.Transformer.from_crs(
        f"EPSG:{src_epsg}",
        f"EPSG:{dst_epsg}",
        always_xy=True,
    ).transform
    return transform(project, geom)


def extract_polygons(geom):
    """Extract polygons"""
    if geom.is_empty:
        return MultiPolygon()
    if isinstance(geom, Polygon):
        return MultiPolygon([geom])
    if isinstance(geom, MultiPolygon):
        return geom
    raise ValueError(
        f"Unsupported geometry type: {geom.geom_type}"
    )


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
    raise ValueError(
        f"Unsupported geometry type: {geom.geom_type}"
    )


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
    response = requests.post(
        url_catalog_api,
        headers=headers,
        json=data,
    )
    if response.status_code == 200:
        print("ok")
        return response.json()
    print(f"Error {response.status_code}: {response.text}")
    return False


def _safe_eval_expr(expr):
    """
    Parses string and turns them into number and unary op (+/-), or
    binary op (+, -, *, /, //).
    Evaluate left and right, then apply the operator.
    """
    def _eval(node):
        if isinstance(node, ast.Expression):
            return _eval(node.body)
        if isinstance(node, ast.Num):
            return node.n
        # pylint: disable=line-too-long        
        if (isinstance(node, ast.Constant) and isinstance(node.value, (int, float))):    # noqa: E501
            return node.value
        # pylint: disable=line-too-long
        
        if (isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub))):    # noqa: E501
            val = _eval(node.operand)
            return +val if isinstance(node.op, ast.UAdd) else -val
        # pylint: disable=line-too-long
        if (isinstance(node, ast.BinOp) and isinstance(node.op, (ast.Add, ast.Sub, ast.Mult, ast.Div, ast.FloorDiv),)):    # noqa: E501
            left = _eval(node.left)
            right = _eval(node.right)
            ops = {
                ast.Add: operator.add,
                ast.Sub: operator.sub,
                ast.Mult: operator.mul,
                ast.Div: operator.truediv,
                ast.FloorDiv: operator.floordiv,
            }
            return ops[type(node.op)](left, right)
        raise ValueError("Unsupported expression")
    tree = ast.parse(expr, mode="eval")
    return float(_eval(tree))


def parse_factor_offset(evalscript):
    """Parse factor and offset constants from an evalscript string."""
    factor = None
    offset = None
    m_factor = re.search(
        r"\bconst\s+factor\s*=\s*([^;]+);",
        evalscript,
        re.IGNORECASE,
    )
    m_offset = re.search(
        r"\bconst\s+offset\s*=\s*([^;]+);",
        evalscript,
        re.IGNORECASE,
    )
    if m_factor:
        factor = _safe_eval_expr(m_factor.group(1).strip())
    if m_offset:
        offset = _safe_eval_expr(m_offset.group(1).strip())
    return factor, offset


def extract_layer_params_map(layers):
    """Build a map of layer id to factor/offset parsed from styles."""
    results = {}
    for layer in layers:
        layer_id = layer.get("id")
        styles = layer.get("styles", [])
        preferred = layer.get("defaultStyleName") or "default"
        evalscript = None
        for s in styles:
            if s.get("name") == preferred and "evalScript" in s:
                evalscript = s["evalScript"]
                break
        if evalscript is None:
            for s in styles:
                if "evalScript" in s:
                    evalscript = s["evalScript"]
                    break
        factor, offset = (
            (None, None) if evalscript is None
            else parse_factor_offset(evalscript)
        )
        results[layer_id] = {"offset": offset, "factor": factor}
    return results


def to_json(data):
    """Serialize data to pretty-printed JSON with UTF-8."""
    return json.dumps(data, ensure_ascii=False, indent=2)
