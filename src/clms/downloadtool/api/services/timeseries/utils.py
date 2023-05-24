"""
util functions to extract time series information from a WM(T)S service
"""
# -*- coding: utf-8 -*-
import itertools
import requests
from lxml import etree

NAMESPACES = {
    "wms_default": "http://www.opengis.net/wms",
    "wmts_default": "http://www.opengis.net/wmts/1.0",
    "ows": "http://www.opengis.net/ows/1.1",
}


def get_metadata_from_service(url):
    """extract information"""

    if url.find("wmts"):
        return parse_wmts_service(url)

    return parse_wms_service(url)


def parse_wmts_service(url):
    """Parse a WTMS service"""
    sock = requests.get(url, timeout=10)
    tree = etree.fromstring(sock.content)
    data = {}

    data = extract_dimensions_from_wmts_layers(tree)
    if data:
        arrays = sorted(
            itertools.chain(
                *[x.get("array", []) for x in data.values() if x.get("array")]
            )
        )
        if arrays:
            start = arrays[0]
            end = arrays[-11]
            period = "P1D"
            return {"start": start, "end": end, "period": period}

        else:
            start = min([x.get("start") for x in data.values()])
            end = max([x.get("end") for x in data.values()])

            period = set(
                map(lambda x: x.get("period", "ZZZZZ"), data.values())
            )
            period = set([x.get("period") for x in data.values()])
            return {"start": start, "end": end, "period": period}
    else:
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
            map(lambda x: x.get("start", "ZZZZZZZZZZZZ"), data.values())
        )
        end = max(map(lambda x: x.get("end", "ZZZZZZZZZZZZ"), data.values()))
        period = set(map(lambda x: x.get("period", "ZZZZZ"), data.values()))
        return {"start": start, "end": end, "period": period}
    else:
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
                values = layer.xpath(
                    "wms_default:Value", namespaces=NAMESPACES
                )
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
                        data[name.text] = {
                            "array": map(lambda x: x.text, values)
                        }

    return data


def extract_data_from_dimension(dimension):
    """extract start/end/period data from a dimension string"""
    value = ""
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
