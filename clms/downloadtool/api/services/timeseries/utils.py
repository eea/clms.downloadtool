"""
util functions to extract time series information from a WM(T)S service
"""

# -*- coding: utf-8 -*-
import itertools
from logging import getLogger

import requests
from lxml import etree

NAMESPACES = {
    "wms_default": "http://www.opengis.net/wms",
    "wmts_default": "http://www.opengis.net/wmts/1.0",
    "ows": "http://www.opengis.net/ows/1.1",
}


log = getLogger(__name__)


def get_metadata_from_service(url, layers=None):
    """extract information"""
    if url:
        if url.find("wmts") != -1:
            try:
                if layers is not None:
                    return parse_wmts_service(
                        url, layers=layers
                    )
                return parse_wmts_service(url)
            except Exception as e:
                log.info(e)
                return {}
        try:
            return parse_wms_service(url)
        except Exception as e:
            log.info(e)
            return {}

    return {}


def parse_wmts_service(url, layers=None):
    """Parse a WTMS service"""
    sock = requests.get(url, timeout=10)
    if not sock.ok:
        return {
            "status_code": sock.status_code,
            "text": sock.text,
            "url": sock.url,
            "status": "error",
            "msg": "WTMS service request response is not ok",
        }
    tree = etree.fromstring(sock.content)
    data = {}

    results_are_filtered = False
    if layers is not None:
        results_are_filtered = True
    data = extract_dimensions_from_wmts_layers(tree)
    if data:
        if not results_are_filtered:
            arrays = sorted(
                itertools.chain(
                    *[x.get(
                        "array", []) for x in data.values() if x.get("array")]
                )
            )
        else:
            # Refs #276844 - use mapviewer_layers as filter
            arrays = []

            for layer_key, layer_value in data.items():
                if layer_key in layers:
                    array = layer_value.get("array", [])
                    arrays.append(array)

            arrays = list(itertools.chain(*arrays))
            arrays.sort()

        if arrays:
            start = arrays[0]
            end = arrays[-1]
            period = "P1D"
            return {
                "start": start, "end": end, "period": period,
                "data_arrays": arrays
            }

        start = min({x.get("start") for x in data.values()})
        end = max({x.get("end") for x in data.values()})
        period = {x.get("period") for x in data.values()}
        return {"start": start, "end": end, "period": period[0]}

    dimension = extract_dimension_from_global(tree)
    data = extract_data_from_dimension(dimension)

    return data


def parse_wms_service(url):
    """parse a WMS service"""

    sock = requests.get(url, timeout=10)
    tree = etree.fromstring(sock.content)
    data = {}

    data = extract_dimensions_from_wms_layers(tree)
    if data:
        start = min(
            map(lambda x: x.get("start", "ZZZZZZZZZZZZ"), data.values()))
        end = max(map(lambda x: x.get("end", "ZZZZZZZZZZZZ"), data.values()))
        period = set(map(lambda x: x.get("period", "ZZZZZ"), data.values()))
        return {"start": start, "end": end, "period": period}

    dimension = extract_dimension_from_global(tree)
    data = extract_data_from_dimension(dimension)

    return data


def extract_dimensions_from_wms_layers(tree):
    """parse layer information to extract dimension data, if available,
    from them"""
    data = {}
    layers = tree.xpath("//wms_default:Layer", namespaces=NAMESPACES)
    for layer in layers:
        name = layer.find("wms_default:Name", namespaces=NAMESPACES)
        if name is not None:
            value = extract_dimension_wms(layer)
            if value:
                data[name.text] = value
            else:
                values = layer.xpath("wms_default:Value",
                                     namespaces=NAMESPACES)
                if values:
                    data[name.text] = {"array": values}

    return data


def extract_dimensions_from_wmts_layers(tree):
    """parse layer information to extract dimension data, if available,
    from them"""
    data = {}
    layers = tree.xpath("//wmts_default:Layer", namespaces=NAMESPACES)
    for layer in layers:
        name = layer.find("ows:Title", namespaces=NAMESPACES)
        if name is not None:
            value = extract_dimension_wtms(layer)
            if value:
                data[name.text] = value
            else:
                dimensions = layer.xpath(
                    "wmts_default:Dimension", namespaces=NAMESPACES
                )
                for dimension in dimensions:
                    values = dimension.xpath(
                        "wmts_default:Value", namespaces=NAMESPACES
                    )
                    if values:
                        data[name.text] = {"array": map(
                            lambda x: x.text, values)}

    return data


def extract_data_from_dimension(dimension):
    """extract start/end/period data from a dimension string"""
    value = {}
    if dimension:
        if "/P" in dimension:
            start, end, period = dimension.split("/")
            value = {
                "start": start,
                "end": end,
                "period": period,
            }
        else:
            value = {"array": dimension.split(",")}

    return value


def extract_dimension_wms(tree):
    """extract the dimension from a given xml tree of a WMS service"""
    value = {}
    dimension = tree.find("wms_default:Dimension", namespaces=NAMESPACES)
    if dimension:
        value = extract_data_from_dimension(dimension.text)

    return value


def extract_dimension_wtms(tree):
    """extract the dimension from a given xml tree of a WMTS service"""
    value = {}
    dimension = tree.find("wmts_default:Dimension", namespaces=NAMESPACES)
    if dimension and dimension.text:
        value = extract_data_from_dimension(dimension.text)

    return value


def extract_dimension_from_global(tree):
    """extract dimension string parsing the global XML tree"""
    dimensions = tree.xpath("//wms_default:Dimension", namespaces=NAMESPACES)
    if dimensions:
        values = tree.xpath("//wms_default:Extent", namespaces=NAMESPACES)
        if values:
            return values[0].text

    return ""


if __name__ == "__main__":
    # print(
    #     get_metadata_from_service(
    #         "https://cryo.land.copernicus.eu/wms/FSC/?REQUEST=GETCAPABILITIES"
    #     )
    # )

    print(
        get_metadata_from_service(
            "https://phenology.vgt.vito.be/wmts?REQUEST=GETCAPABILITIES"
        )
    )
