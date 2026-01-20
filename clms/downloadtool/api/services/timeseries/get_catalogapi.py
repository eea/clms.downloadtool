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
from plone.memoize import ram
from plone.restapi.services import Service


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


def _ram_cache_key(method, byoc, cache_key):
    """Cache per BYOC and time bucket.

    Example:
      - First request for byoc "abc-123" on 2025-02-10 (key 2025-02-10-15)
        stores under: cdse.catalogapi.dates:abc-123:2025-02-10-15
      - Subsequent requests for the same BYOC/day return cached data without
        calling CDSE. Next day uses a new key and refreshes once.
    """
    return f"cdse.catalogapi.dates:{byoc}:{cache_key}"


@ram.cache(_ram_cache_key)
def _get_cached_full_response(byoc, cache_key):
    """ Get cached full response"""
    token = get_token()
    result = get_full_response(byoc, token)

    # cache only if it has actual data
    if "dates" in result and len(
        result["dates"]) > 0 and "metadata" in result and result[
            "metadata"] is not None:
        result["cached"] = cache_key
    return result


def get_cached_response(byoc, force_refresh=False):
    """
    Get cached response
    """
    cache_key = current_cache_key()
    if force_refresh:
        token = get_token()
        result = get_full_response(byoc, token)
        if "dates" in result and len(
            result["dates"]) > 0 and "metadata" in result and result[
                "metadata"] is not None:
            result["cached"] = cache_key
        return result
    return _get_cached_full_response(byoc, cache_key)


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
        if isinstance(force_refresh, str):
            force_refresh = force_refresh.strip().lower() == "true"
        if byoc is None:
            self.request.response.setStatus(400)
            return {
                "status": "error",
                "msg": "byoc is required",
            }

        values = get_cached_response(byoc, force_refresh)

        return values
