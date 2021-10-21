# -*- coding: utf-8 -*-
"""
For HTTP GET operations we can use standard HTTP parameter passing
through the URL)

"""
import re
import datetime
from collections import defaultdict
from logging import getLogger

from plone.restapi.services import Service
from plone.restapi.deserializer import json_body
from zope.component import getUtility

from clms.downloadtool.api.services.utils import (
    COUNTRIES,
    GCS,
    DATASET_FORMATS,
    FORMAT_CONVERSION_TABLE,
)
from clms.downloadtool.utility import IDownloadToolUtility


log = getLogger(__name__)


class DataRequestPost(Service):
    """Set Data"""

    def reply(self):
        """ JSON response """
        body = json_body(self.request)

        user_id = str(body.get("UserID"))
        dataset_id = body.get("DatasetID")
        dataset_format = body.get("DatasetFormat")
        dataset_path = body.get("DatasetPath")
        bounding_box = body.get("BoundingBox")
        temporal_filter = body.get("TemporalFilter")
        output_format = body.get("OutputFormat")
        outputGCS = body.get("OutputGCS")
        nuts_id = body.get("NUTSID")
        mail = body.get("Mail")

        response_json = {}

        utility = getUtility(IDownloadToolUtility)

        if not user_id:
            self.request.response.setStatus(400)
            return {"status": "error", "msg": "Error, UserID is not defined"}

        if not dataset_id:
            self.request.response.setStatus(400)
            return {
                "status": "error",
                "msg": "Error, DatasetID is not defined"
            }

        response_json = {"UserID": user_id, "DatasetID": dataset_id}

        if mail:
            if not email_validation(mail):
                self.request.response.setStatus(400)
                return {
                    "status": "error",
                    "msg": "Error, inserted mail is not valid"
                }
            response_json.update({"Mail": mail})

        if nuts_id:
            if not validateNuts(nuts_id):
                self.request.response.setStatus(400)
                return {"status": "error", "msg": "NUTSID country error"}
            response_json.update({"NUTSID": nuts_id})

        if bounding_box:
            if nuts_id:
                self.request.response.setStatus(400)
                return {
                    "status": "error",
                    "msg": "Error, NUTSID is also defined"
                }

            if not validateSpatialExtent(bounding_box):
                self.request.response.setStatus(400)
                return {
                    "status": "error",
                    "msg": "Error, BoundingBox is not valid"
                }

            response_json.update({"BoundingBox": bounding_box})

        if dataset_format or output_format:
            if (
                not dataset_format and
                output_format or
                dataset_format and not output_format
            ):
                self.request.response.setStatus(400)
                return {
                    "status": "error",
                    "msg": "Error, you need to specify both formats"
                }
            if (
                dataset_format not in DATASET_FORMATS or
                output_format not in DATASET_FORMATS
            ):
                self.request.response.setStatus(400)
                return {
                    "status": "error",
                    "msg": "Error, specified formats are not in the list"
                }
            if (
                "GML" in dataset_format or not
                FORMAT_CONVERSION_TABLE[dataset_format][output_format]
            ):
                self.request.response.setStatus(400)
                # pylint: disable=line-too-long
                return {
                    "status": "error",
                    "msg": "Error, specified data formats are not supported in this way"  # noqa
                }

            response_json.update(
                {
                    "DatasetFormat": dataset_format,
                    "OutputFormat": output_format,
                }
            )

        if temporal_filter:
            log.info(validateDate1(temporal_filter))
            if not validateDate1(temporal_filter) and not validateDate2(
                temporal_filter
            ):
                self.request.response.setStatus(400)
                return {
                    "status": "error",
                    "msg": "Error, date format is not correct"
                }

            if not checkDateDifference(temporal_filter):
                self.request.response.setStatus(400)
                # pylint: disable=line-too-long
                return {
                    "status": "error",
                    "msg": "Error, difference between StartDate and EndDate is not coherent"  # noqa
                }

            if len(temporal_filter.keys()) > 2:
                self.request.response.setStatus(400)
                return {
                    "status": "error",
                    "msg": "Error, TemporalFilter has too many fields"
                }

            if (
                "StartDate" not in temporal_filter.keys() or
                "EndDate" not in temporal_filter.keys()  # noqa
            ):
                self.request.response.setStatus(400)
                # pylint: disable=line-too-long
                return {
                    "status": "error",
                    "msg": "Error, TemporalFilter does not have StartDate or EndDate"  # noqa
                }

            response_json.update({"TemporalFilter": temporal_filter})

        if outputGCS:
            if outputGCS not in GCS:
                self.request.response.setStatus(400)
                return {
                    "status": "error",
                    "msg": "Error, defined GCS not in the list"
                }
            response_json.update({"OutputGCS": outputGCS})

        if dataset_path:
            response_json.update({"DatasetPath": dataset_path})

        response_json["Status"] = "In_progress"
        response_json = utility.datarequest_post(response_json)

        log.info("AFTER CALLING INSERTION METHOD")
        log.info(response_json)

        self.request.response.setStatus(201)
        return response_json


def validateDownloadFormat():
    """ validate correct file format """
    the_table = defaultdict(dict)

    for input_iteration_format in DATASET_FORMATS:

        if input_iteration_format == "Shapefile":

            for output_iteration_format in DATASET_FORMATS:
                # pylint: disable=line-too-long
                the_table[input_iteration_format][
                    output_iteration_format
                ] = output_iteration_format in (
                    "GDB",
                    "GPKG",
                    "Geojson",
                    "GML",
                )  # noqa: E501

        elif input_iteration_format == "GDB":

            for output_iteration_format in DATASET_FORMATS:
                # pylint: disable=line-too-long
                the_table[input_iteration_format][
                    output_iteration_format
                ] = output_iteration_format in (
                    "Shapefile",
                    "GPKG",
                    "Geojson",
                    "GML",
                )  # noqa: E501
        elif input_iteration_format == "GPKG":

            for output_iteration_format in DATASET_FORMATS:
                # pylint: disable=line-too-long
                the_table[input_iteration_format][
                    output_iteration_format
                ] = output_iteration_format in (
                    "Shapefile",
                    "GDB",
                    "Geojson",
                    "GML",
                )  # noqa: E501

        elif input_iteration_format == "Geojson":

            for output_iteration_format in DATASET_FORMATS:
                # pylint: disable=line-too-long
                the_table[input_iteration_format][
                    output_iteration_format
                ] = output_iteration_format in (
                    "Shapefile",
                    "GDB",
                    "GPKG",
                    "GML",
                )  # noqa: E501

        elif input_iteration_format == "Geotiff":
            for output_iteration_format in DATASET_FORMATS:
                the_table[input_iteration_format][
                    output_iteration_format
                ] = False

        elif input_iteration_format == "Netcdf":
            for output_iteration_format in DATASET_FORMATS:
                the_table[input_iteration_format][output_iteration_format] = (
                    output_iteration_format == "Geotiff"
                )

        elif input_iteration_format == "WFS":

            for output_iteration_format in DATASET_FORMATS:
                # pylint: disable=line-too-long
                the_table[input_iteration_format][
                    output_iteration_format
                ] = output_iteration_format in (
                    "Shapefile",
                    "GDB",
                    "GPKG",
                    "Geojson",
                    "GML",
                )  # noqa: E501
    # pylint: disable=line-too-long
    log.info(
        "------------------------------------------VALIDATION TABLE------------------------------------------"  # noqa
    )
    log.info(the_table)
    return the_table


def validateDate1(temporal_filter):
    """ validate date format year-month day """
    start_date = temporal_filter.get("StartDate")
    end_date = temporal_filter.get("EndDate")

    date_format = "%Y-%m-%d"
    try:
        if start_date is not None and end_date is not None:
            date_obj1 = datetime.datetime.strptime(start_date, date_format)
            log.info(date_obj1)
            date_obj2 = datetime.datetime.strptime(end_date, date_format)
            log.info(date_obj2)
            return {"StartDate": date_obj1, "EndDate": date_obj2}
    except ValueError:
        log.info("Incorrect data format, should be YYYY-MM-DD")
    return False


def validateDate2(temporal_filter):
    """ validate date format day-month-year"""
    start_date = temporal_filter.get("StartDate")
    end_date = temporal_filter.get("EndDate")

    date_format = "%d-%m-%Y"
    try:
        if start_date and end_date:
            date_obj1 = datetime.datetime.strptime(start_date, date_format)
            log.info(date_obj1)
            date_obj2 = datetime.datetime.strptime(end_date, date_format)
            log.info(date_obj2)
            return {"StartDate": date_obj1, "EndDate": date_obj2}
    except ValueError:
        log.info("Incorrect data format, should be DD-MM-YYYY")
    return False


def validateSpatialExtent(bounding_box):
    """ validate Bounding Box """
    if not len(bounding_box) == 4:
        return False

    for x in bounding_box:
        if not isinstance(x, int) and not isinstance(x, float):
            return False

    return True


def checkDateDifference(temporal_filter):
    """ Check date difference """
    log.info(temporal_filter)
    start_date = temporal_filter["StartDate"]
    end_date = temporal_filter.get("EndDate")

    return start_date < end_date


def validateNuts(nuts_id):
    """ validate nuts """

    match = re.match(r"([a-z]+)([0-9]+)", nuts_id, re.I)
    if match:
        items = match.groups()
        return items[0] in COUNTRIES.keys()
    return False


def email_validation(mail):
    """ validate email address """
    a = 0
    y = len(mail)
    dot = mail.find(".")
    at = mail.find("@")
    log.info(mail)

    if "_" in mail[len(mail) - 1]:
        return False

    for i in range(0, at):
        if (mail[i] >= "a" and mail[i] <= "z") or (
            mail[i] >= "A" and mail[i] <= "Z"
        ):
            a = a + 1
    return a > 0 and at > 0 and (dot - at) > 0 and (dot + 1) < y
