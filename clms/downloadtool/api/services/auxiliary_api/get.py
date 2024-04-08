""" auxiliary endpoint REST API"""
# -*- coding: utf-8 -*-
from datetime import datetime

from clms.downloadtool.api.services.auxiliary_api.main import (get_landcover,
                                                               get_legacy,
                                                               get_wekeo)
from plone import api
from plone.restapi.services import Service


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
                    "Required parameters are missing: dataset_uid is mandatory"
                ),
            }

        dataset = api.content.get(UID=dataset_uid)
        if dataset is None:
            self.request.response.setStatus(400)
            return {
                "status": "error",
                "message": "Dataset does not exist",
            }

        download_information_id = self.request.get("download_information_id")
        if download_information_id is None:
            self.request.response.setStatus(400)
            return {
                "status": "error",
                "message": (
                    "Required parameters are missing: download_information_id"
                    " is mandatory"
                ),
            }

        download_information = dataset.dataset_download_information.get(
            "items", []
        )
        ids = [item.get("@id") for item in download_information]
        if download_information_id not in ids:
            self.request.response.setStatus(400)
            return {
                "status": "error",
                "message": "Dataset collection does not exist",
            }

        dataset_download_info = get_dataset_download_information(
            download_information, download_information_id
        )

        results = []

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

            results = get_wekeo(
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
            results = get_landcover(
                api_url, full_path, x_max, y_max, x_min, y_min
            )

        if dataset_download_info.get("full_source") == "LEGACY":
            username = api.portal.get_registry_record(
                # pylint: disable=line-too-long
                "clms.downloadtool.auxiliary_api_control_panel.legacy_username"  # noqa
            )
            password = api.portal.get_registry_record(
                # pylint: disable=line-too-long
                "clms.downloadtool.auxiliary_api_control_panel.legacy_password"  # noqa
            )
            full_path = dataset_download_info.get("full_path")
            date_from = self.request.get("date_from", "")
            date_to = self.request.get("date_to", "")

            if not date_from or not date_to:
                self.request.response.setStatus(400)
                return {
                    "status": "error",
                    "msg": "Error, both date_from and date_to parameters "
                           "are required",
                }

            try:
                datetime.strptime(date_to, "%Y-%m-%d")
            except ValueError:
                self.request.response.setStatus(400)
                return {
                    "status": "error",
                    "msg": "Error, date_to parameter must be a date in "
                           "YYYY-MM-DD format"
                }
            try:
                datetime.strptime(date_from, "%Y-%m-%d")
            except ValueError:
                self.request.response.setStatus(400)
                return {
                    "status": "error",
                    "msg": "Error, date_from parameter must be a date in "
                           "YYYY-MM-DD format",
                }

            if date_to < date_from:
                self.request.response.setStatus(400)
                return {
                    "status": "error",
                    "msg": "Error, date_from parameter must be smaller "
                           "than date_to"
                }

            results = get_legacy(
                username, password, full_path, date_from, date_to
            )

        if not results:
            return {
                "status": "error",
                "msg": "Error, there are no files in the specified range"
            }

        return results


def get_dataset_download_information(
    download_information, download_information_id
):
    """get the download information related to the given
    download_information_id"""
    for item in download_information:
        if item.get("@id") == download_information_id:
            return item

    return {}
