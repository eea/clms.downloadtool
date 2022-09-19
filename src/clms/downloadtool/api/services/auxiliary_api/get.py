""" auxiliary endpoint REST API"""
# -*- coding: utf-8 -*-
from plone import api
from plone.restapi.services import Service
from clms.downloadtool.api.services.auxiliary_api.main import (
    get_landcover,
    get_wekeo,
    get_legacy,
)


class GetDownloadFileUrls(Service):
    """REST API for m2m users to obtain direct download links"""

    def reply(self):
        """return the result"""
        dataset_uid = self.request.get("dataset_uid")
        if dataset_uid is None:
            self.request.response.setStatus(400)
            return {
                "status": "error",
                "message": (
                    "Required parameters are missing: dataset_uid is "
                    "mandatory"
                ),
            }

        dataset = api.content.get(UID=dataset_uid)
        if dataset is None:
            self.request.response.setStatus(400)
            return {
                "status": "error",
                "message": "Dataset does not exist",
            }

        dataset_collection = self.request.get("dataset_collection")
        if dataset_collection is None:
            self.request.response.setStatus(400)
            return {
                "status": "error",
                "message": (
                    "Required parameters are missing: dataset_collection"
                    " is mandatory"
                ),
            }

        download_information = dataset.dataset_download_information.get(
            "items", []
        )
        collection = [item.get("collection") for item in download_information]
        if dataset_collection not in collection:
            self.request.response.setStatus(400)
            return {
                "status": "error",
                "message": ("Dataset collection does not exist"),
            }

        dataset_download_info = self.get_dataset_download_information(
            download_information, dataset_collection
        )

        if dataset_download_info.get("full_source") == "WEKEO":
            api_url = api.portal.get_registry_record(
                "clms.downloadtool.auxiliary_api_control_panel.wekeo_api_url"
            )
            api_username = api.portal.get_registry_record(
                # pylint: disable=line-too-long
                "clms.downloadtool.auxiliary_api_control_panel.wekeo_api_username"  # noqa
            )
            api_password = api.portal.get_registry_record(
                # pylint: disable=line-too-long
                "clms.downloadtool.auxiliary_api_control_panel.wekeo_api_password"  # noqa
            )
            full_path = dataset_download_info.get("full_path")
            wekeo_choices = dataset_download_info.get("wekeo_choices")

            date_from = self.request.get("date_from", "")
            date_to = self.request.get("date_to", "")

            x_max = self.request.get("x_max", "")
            y_max = self.request.get("y_max", "")
            x_min = self.request.get("x_min", "")
            y_min = self.request.get("y_min", "")

            return get_wekeo(
                api_url,
                api_username,
                api_password,
                full_path,
                wekeo_choices,
                date_from,
                date_to,
                x_max,
                y_max,
                x_min,
                y_min,
            )

        if dataset_download_info.get("full_source") == "LANDCOVER":
            api_url = api.portal.get_registry_record(
                # pylint: disable=line-too-long
                "clms.downloadtool.auxiliary_api_control_panel.landcover_api_url"  # noqa
            )
            x_max = self.request.get("x_max", "")
            y_max = self.request.get("y_max", "")
            x_min = self.request.get("x_min", "")
            y_min = self.request.get("y_min", "")
            full_path = dataset_download_info.get("full_path")
            return get_landcover(
                api_url, full_path, x_max, y_max, x_min, y_min
            )

        if dataset_download_info.get("full_source") == "LEGACY":
            full_path = dataset_download_info.get("full_path")
            date_from = self.request.get("date_from", "")
            date_to = self.request.get("date_to", "")

            return get_legacy(full_path, date_from, date_to)

        return {}

    def get_dataset_download_information(
        self, download_information, dataset_collection
    ):
        """get the download information related to the given
        dataset_collection"""
        for item in download_information:
            if item.get("collection") == dataset_collection:
                return item

        return {}
