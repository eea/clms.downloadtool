"""
Specific endpoint to get metadata from a dataset W(T)MS service URL
"""

# -*- coding: utf-8 -*-
from datetime import datetime
from clms.downloadtool.api.services.timeseries.utils import (
    get_metadata_from_service,
)
from clms.downloadtool.api.services.cdse.utils import is_cdse_dataset
from clms.downloadtool.api.services.timeseries.get_catalogapi import (
    get_cached_response
)
from plone import api
from plone.restapi.services import Service


class GetTimeSeriesMetadata(Service):
    """Endpoint to get the time series metadata of a given dataset"""

    def get_time_series_metadata_cdse(self, dataset):
        """Get time series metadata for CDSE dataset"""

        download_info_items = dataset.dataset_download_information.get(
            "items", [])
        for info in download_info_items:
            byoc_collection_id = info.get("byoc_collection", "")
            res = get_cached_response(byoc_collection_id, force_refresh=False)
            dates = res.get("dates", [])

            date_objs = [datetime.strptime(d, "%Y-%m-%d") for d in dates]
            start_date = min(date_objs).strftime("%Y-%m-%d")
            end_date = max(date_objs).strftime("%Y-%m-%d")

            # value = {
            #     'start': '2017-01-01',
            #     'end': '2023-01-01',
            #     'period': 'P1D',
            #     'data_arrays':
            #         ['2017-01-01', '2017-01-01', ..., '2023-01-01'],
            #     'download_limit_temporal_extent': 365,
            #     'mapviewer_istimeseries': True
            # }

            value = {}
            value['start'] = start_date
            value['end'] = end_date
            value['data_arrays'] = dates
            value['period'] = 'P1D'

            return value

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

        # CDSE?
        is_cdse = is_cdse_dataset(dataset)
        if is_cdse:
            print("CDSE dataset")
            value = self.get_time_series_metadata_cdse(dataset)
        else:
            print("NON CDSE dataset")

            service = dataset.mapviewer_viewservice
            if service and service.lower().find("getcapabilities") == -1:
                if not service.endswith("?"):
                    service += "?"
                service += "REQUEST=GETCAPABILITIES"

            if dataset.mapviewer_layers is not None:
                # Refs #276844 - use layers as filter
                try:
                    map_layers = [x["id"]
                                  for x in dataset.mapviewer_layers["items"]]
                except Exception:
                    map_layers = None
                value = get_metadata_from_service(
                    service, layers=map_layers
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
