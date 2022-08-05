""" auxiliary endpoint REST API"""
# -*- coding: utf-8 -*-
from plone.restapi.services import Service
from clms.downloadtool.api.services.auxiliary_api.main import (
    get_landcover,
    get_wekeo,
    get_legacy,
)


class GetLandCoverService(Service):
    """REST API endpoint for LandCover"""

    def reply(self):
        """implementation"""
        api_url = "api_url" in self.request
        dataset_path = "dataset_path" in self.request
        x_max = "x_max" in self.request
        y_max = "y_max" in self.request
        x_min = "x_min" in self.request
        y_min = "y_min" in self.request

        # pylint: disable=too-many-boolean-expressions
        if api_url and dataset_path and x_max and y_max and x_min and y_min:

            return get_landcover(
                self.request.get("api_url"),
                self.request.get("dataset_path"),
                self.request.get("x_max"),
                self.request.get("y_max"),
                self.request.get("x_min"),
                self.request.get("y_min"),
            )

        self.request.response.setStatus(400)
        return {
            "status": "error",
            "message": "Required parameters are missing",
        }


class GetWekeoService(Service):
    """REST API endpoint for Wekeo"""

    def reply(self):
        """implementation"""
        api_url = "api_url" in self.request
        dataset_path = "dataset_path" in self.request
        wekeo_choices = "wekeo_choices" in self.request
        date_from = "date_from" in self.request
        date_to = "date_to" in self.request
        x_max = "x_max" in self.request
        y_max = "y_max" in self.request
        x_min = "x_min" in self.request
        y_min = "y_min" in self.request
        # pylint: disable=line-too-long too-many-boolean-expressions
        if (api_url and dataset_path and wekeo_choices and date_from and date_to and x_max and y_max and x_min and y_min):  # noqa
            result = get_wekeo(
                self.request.get("api_url"),
                self.request.get("dataset_path"),
                self.request.get("wekeo_choices"),
                self.request.get("date_from"),
                self.request.get("date_to"),
                self.request.get("x_max"),
                self.request.get("y_max"),
                self.request.get("x_min"),
                self.request.get("y_min"),
            )

            return result

        self.request.response.setStatus(400)
        return {
            "status": "error",
            "message": "Required parameters are missing",
        }


class GetLegacyService(Service):
    """REST API endpoint for Legacy"""

    def reply(self):
        """implementation"""
        path = "path" in self.request
        date_from = "date_from" in self.request
        date_to = "date_to" in self.request

        if path and date_from and date_to:
            return get_legacy(
                self.request.get("path"),
                self.request.get("date_from"),
                self.request.get("date_to"),
            )

        self.request.response.setStatus(400)
        return {
            "status": "error",
            "message": "Required parameters are missing",
        }
