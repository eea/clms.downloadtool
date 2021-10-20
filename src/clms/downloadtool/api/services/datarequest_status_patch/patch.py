# -*- coding: utf-8 -*-
"""
For HTTP GET operations we can use standard HTTP parameter passing
(through the URL)

"""
import datetime
import re

from logging import getLogger

from plone.restapi.deserializer import json_body
from plone.restapi.services import Service
from zope.component import getUtility

from clms.downloadtool.api.services.utils import (
    COUNTRIES,
    DATASET_FORMATS,
    FORMAT_CONVERSION_TABLE,
    GCS,
    STATUS_LIST,
)
from clms.downloadtool.utility import IDownloadToolUtility


log = getLogger(__name__)


class datarequest_status_patch(Service):
    """Nuts & BBox not at the same time"""

    def reply(self):
        """ JSON response """
        body = json_body(self.request)

        task_id = str(body.get("TaskID"))
        dataset_id = body.get("DatasetID")
        dataset_format = body.get("DatasetFormat")
        dataset_path = body.get("DatasetPath")
        bounding_box = body.get("BoundingBox")
        temporal_filter = body.get("TemporalFilter")
        output_format = body.get("OutputFormat")
        outputGCS = body.get("OutputGCS")
        nuts_id = body.get("NUTSID")
        mail = body.get("Mail")
        status = body.get("Status")
        user_id = body.get("UserID")

        response_json = {}

        utility = getUtility(IDownloadToolUtility)

        if not task_id:
            self.request.response.setStatus(400)
            return "Error TaskID is not defined"

        if not status:
            self.request.response.setStatus(400)
            return "Error, Status is not defined"

        if status not in STATUS_LIST:
            self.request.response.setStatus(400)
            return "Error, defined Status is not in the list"

        response_json = {"TaskID": task_id, "Status": status}

        if dataset_id:
            response_json.update({"DatasetID": dataset_id})

        if user_id:
            response_json.update({"UserID": user_id})

        if mail:
            if not email_validation(mail):
                self.request.response.setStatus(400)
                return "Error, inserted mail is not valid"
            response_json.update({"Mail": mail})

        if nuts_id:
            if bounding_box:
                self.request.response.setStatus(400)
                return "Error, BoundingBox is also defined"

            if not validateNuts(nuts_id):
                self.request.response.setStatus(400)
                return "NUTSID country error"
            response_json.update({"NUTSID": nuts_id})

        if bounding_box:
            if nuts_id:
                self.request.response.setStatus(400)
                return "Error, NUTSID is also defined"

            if not validateSpatialExtent(bounding_box):
                self.request.response.setStatus(400)
                return "Error, BoundingBox is not valid"

            response_json.update({"BoundingBox": bounding_box})

        if dataset_format or output_format:
            if not dataset_format and output_format:
                self.request.response.setStatus(400)
                return "Error, you need to specify both formats"
            if (dataset_format not in DATASET_FORMATS or
                    output_format not in DATASET_FORMATS):
                self.request.response.setStatus(400)
                return "Error, specified formats are not in the list"

            # pylint: disable=line-too-long
            if "GML" in dataset_format or not FORMAT_CONVERSION_TABLE[dataset_format][output_format]:  # noqa
                self.request.response.setStatus(400)
                # pylint: disable=line-too-long
                return "Error, specified data formats are not supported in this way"  # noqa

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
                return "Error, date format is not correct"

            if not checkDateDifference(temporal_filter):
                self.request.response.setStatus(400)
                # pylint: disable=line-too-long
                return "Error, difference between StartDate and EndDate is not coherent"  # noqa

            if len(temporal_filter.keys()) > 2:
                self.request.response.setStatus(400)
                return "Error, TemporalFilter has too many fields"

            # pylint: disable=line-too-long
            if "StartDate" not in temporal_filter.keys() or "EndDate" not in temporal_filter.keys():  # noqa
                self.request.response.setStatus(400)
                return (
                    "Error, TemporalFilter does not have StartDate or EndDate"
                )

            response_json.update({"TemporalFilter": temporal_filter})

        if outputGCS:
            if outputGCS not in GCS:
                self.request.response.setStatus(400)
                return "Error, defined GCS not in the list"
            response_json.update({"OutputGCS": outputGCS})

        if dataset_path:
            response_json.update({"DatasetPath": dataset_path})

        log.info(response_json)
        response_json = utility.datarequest_status_patch(
            response_json, task_id
        )

        log.info(response_json)

        if "Error" in response_json:
            self.request.response.setStatus(400)
            return response_json

        if "Error, task_id not registered" in response_json:
            self.request.response.setStatus(404)
            return response_json

        if (
            "Error, NUTSID and BoundingBox can't be defined in the same task"
            in response_json
        ):
            self.request.response.setStatus(400)
            return response_json

        self.request.response.setStatus(201)

        return response_json


def validateDate1(temporal_filter):
    """ Validate Dates year-month-day """
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
    """ Validate Dates day-month-year """
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
    """ Validate Bounding Box """

    if not len(bounding_box) == 4:
        return False

    for x in bounding_box:
        if not isinstance(x, int) and not isinstance(x, float):
            return False
    return True


def checkDateDifference(temporal_filter):
    """ Check date order"""
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
    """ Validate email address """
    a = 0
    y = len(mail)
    dot = mail.find(".")
    at = mail.find("@")
    if "_" in mail[0]:
        return False
    for i in range(0, at):
        if (mail[i] >= "a" and mail[i] <= "z") or (
            mail[i] >= "A" and mail[i] <= "Z"
        ):
            a = a + 1
    return a > 0 and at > 0 and (dot - at) > 0 and (dot + 1) < y
