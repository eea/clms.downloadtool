"""
Specific endpoint to get catalog api dates of CDSE dataset
"""

# -*- coding: utf-8 -*-
from datetime import datetime, timezone, timedelta
import requests
from clms.downloadtool.api.services.cdse.cdse_integration import (
    get_token,
    CATALOG_API_URL,
)
from clms.downloadtool.api.services.cdse.cdse_helpers import (
    request_Catalog_API_dates
)
from plone.restapi.services import Service

# cache the results as it can take a lot of requests to get all data
_local_dates_cache = {}


def get_dates(byoc, token):
    """
    Get all dates
    """
    return request_Catalog_API_dates(token, byoc, CATALOG_API_URL)


def get_geometry(byoc, token):
    """
    Get geometry from the first search entry
    """
    headers = {"Authorization": f"Bearer {token}",
               "Content-Type": "application/json"}

    now = datetime.now(timezone.utc)
    now_formatted = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    search_one = {
        "collections": [f"byoc-{byoc}"],
        "datetime": f"1970-01-01T00:00:00Z/{now_formatted}",
        "bbox": [-180, -90, 180, 90],
        "limit": 1,
    }

    search_response = requests.post(
        CATALOG_API_URL, headers=headers, json=search_one)

    # print(search_response)
    if search_response.status_code == 200:
        # print(search_response.text)
        catalog_entries = search_response.json()
        if "features" in catalog_entries and len(
                catalog_entries["features"]) > 0:
            entry = {}
            f = catalog_entries["features"][0]
            if "bbox" in f:
                entry["bbox"] = f["bbox"]
            if "geometry" in f:
                entry["geometry"] = f["geometry"]

            return entry
    else:
        print(
            "Error calling catalog API:",
            search_response.status_code,
            search_response.text,
        )
        # WIP send error response
        return None
    return None


def get_full_response(byoc, token):
    """
    Get full response from Catalog API
    """
    all_dates = get_dates(byoc, token)
    first_geometry = get_geometry(byoc, token)
    return {"metadata": first_geometry, "dates": all_dates}


def current_cache_key():
    """
    creates a date cache key that is not at midnight
    as the CDSE data is usually updated around noon
    """
    now = datetime.now(timezone.utc)

    # define cutoff = today at 15:00 UTC
    cutoff = now.replace(hour=15, minute=0, second=0, microsecond=0)

    if now < cutoff:
        period_start = cutoff - timedelta(days=1)
    else:
        period_start = cutoff

    # use start time as cache key
    return period_start.strftime("%Y-%m-%d-%H")


def get_cached_response(byoc, force_refresh=False):
    """
    Get cached response
    """
    cache_key = current_cache_key()
    if byoc in _local_dates_cache and not force_refresh:
        if _local_dates_cache[byoc]["cached"] == cache_key:
            return _local_dates_cache[byoc]
    # not cached
    token = get_token()
    result = get_full_response(byoc, token)

    # cache only if it has actual data
    if "dates" in result and len(
        result["dates"]) > 0 and "metadata" in result and result[
            "metadata"] is not None:
        result["cached"] = cache_key
        _local_dates_cache[byoc] = result
    return result


class GetCatalogApiDates(Service):
    """
        Endpoint to get the catalog api dates of a given dataset via byoc
        '/@get_catalogapi_dates?byoc=' + byoc + '&' +
        'force_refresh=' + force_refresh
    """

    def reply(self):
        """endpoint response"""
        byoc = self.request.get("byoc", None)
        force_refresh = self.request.get("force_refresh", False)
        if byoc is None:
            self.request.response.setStatus(400)
            return {
                "status": "error",
                "msg": "byoc is required",
            }

        values = get_cached_response(byoc, force_refresh)

        return values
