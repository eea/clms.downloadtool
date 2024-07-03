"""
Specific endpoint to get metadata from a dataset W(T)MS service URL
"""

# -*- coding: utf-8 -*-
from clms.downloadtool.api.services.timeseries.utils import (
    get_metadata_from_service,
)
from plone import api
from plone.restapi.services import Service


class GetTimeSeriesMetadata(Service):
    """Endpoint to get the time series metadata of a given dataset"""

    def reply(self):
        """endpoint response"""
        dataset_uid = self.request.get("dataset", None)
        if dataset_uid is None:
            self.request.response.setStatus(400)
            return {
                "status": "error",
                "msg": "dataset is required",
            }
        dataset = api.content.get(UID=dataset_uid)
        if dataset is None:
            self.request.response.setStatus(400)
            return {
                "status": "error",
                "msg": "The requested dataset is not valid",
            }

        service = dataset.mapviewer_viewservice
        if service and service.lower().find("getcapabilities") == -1:
            if not service.endswith("?"):
                service += "?"
            service += "REQUEST=GETCAPABILITIES"

        if dataset.geonetwork_identifiers is not None:
            # Improved solution:
            # Refs #271138 - filter years by geonetwork_identifiers
            try:
                geonetwork_identifiers = [
                    x["id"] for x in dataset.geonetwork_identifiers["items"]
                ]
            except Exception:
                geonetwork_identifiers = None
            value = get_metadata_from_service(
                service, geonetwork_identifiers=geonetwork_identifiers
            )
        else:
            # Merge all years into one list (old default solution)
            value = get_metadata_from_service(service)

        if isinstance(value, dict):
            value["download_limit_temporal_extent"] = (
                dataset.download_limit_temporal_extent
            )
            value["mapviewer_istimeseries"] = dataset.mapviewer_istimeseries

        return value
