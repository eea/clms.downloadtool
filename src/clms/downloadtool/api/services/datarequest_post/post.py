# -*- coding: utf-8 -*-
"""
For HTTP GET operations we can use standard HTTP parameter passing
through the URL)

"""
from logging import getLogger
import datetime
import json
import re
import urllib.request
from plone import api
from plone.restapi.deserializer import json_body
from plone.restapi.services import Service
from zope.component import getUtility
from clms.downloadtool.utility import IDownloadToolUtility
from clms.downloadtool.utils import (
    COUNTRIES,
    DATASET_FORMATS,
    FORMAT_CONVERSION_TABLE,
    GCS,
)


log = getLogger(__name__)


class DataRequestPost(Service):
    """Set Data"""

    def reply(self):
        """ JSON response """
        body = json_body(self.request)

        user_id = api.user.get_current()
        datasets_json = body.get("Datasets")
        mail = ""
        # mail = user.getProperty('mail')
        response_json = {}
        data_object = {}
        data_object["Datasets"] = []
        dataset_string = r"{"

        valid_dataset = False

        utility = getUtility(IDownloadToolUtility)
        datasets = utility.get_dataset_info()

        log.info(datasets_json)

        for dataset_json in datasets_json:

            log.info(user_id)
            log.info(dataset_json)
            if not dataset_json["DatasetID"]:
                self.request.response.setStatus(400)
                return {
                    "status": "error",
                    "msg": "Error, DatasetID is not defined",
                }
            valid_dataset = False
            dataset_download = []

            for dataset in datasets:
                if dataset_json["DatasetID"] == dataset["@id"]:
                    log.info(dataset)
                    valid_dataset = True
                    dataset_download.append(dataset_json["DatasetID"])

            if not valid_dataset:
                self.request.response.setStatus(400)
                return {
                    "status": "error",
                    "msg": "Error, the DatasetID is not valid",
                }

            response_json.update({"DatasetID": dataset_json["DatasetID"]})

            if len(dataset_string) == 1:
                dataset_string += r'"DatasetID": "'.join(
                    dataset_json["DatasetID"] + r'"'
                )
            else:
                dataset_string += r'},{"DatasetID": "'.join(
                    dataset_json["DatasetID"] + r'"'
                )

            if "NUTSID" in dataset_json:
                if not validateNuts(dataset_json["NUTSID"]):
                    self.request.response.setStatus(400)
                    return {"status": "error", "msg": "NUTSID country error"}
                response_json.update({"NUTSID": dataset_json["NUTSID"]})
                dataset_string += r', "NUTSID": "'.join(
                    dataset_json["NUTSID"] + r'"'
                )

            if "BoundingBox" in dataset_json:
                if "NUTSID" in dataset_json:
                    self.request.response.setStatus(400)
                    return {
                        "status": "error",
                        "msg": "Error, NUTSID is also defined",
                    }

                if not validateSpatialExtent(dataset_json["BoundingBox"]):
                    self.request.response.setStatus(400)
                    return {
                        "status": "error",
                        "msg": "Error, BoundingBox is not valid",
                    }

                response_json.update(
                    {"BoundingBox": dataset_json["BoundingBox"]}
                )
                dataset_string += r', "BoundingBox":['
                dataset_string += r"".join(
                    str(e) + ", " for e in dataset_json["BoundingBox"]
                )
                dataset_string = dataset_string[:-2]
                dataset_string += r"]"

            if (
                # pylint: disable=line-too-long
                "DatasetFormat" in dataset_json or "OutputFormat" in dataset_json  # noqa
            ):
                if (
                    # pylint: disable=line-too-long
                    "DatasetFormat" not in dataset_json and "OutputFormat" in dataset_json or "DatasetFormat" in dataset_json and "OutputFormat" not in dataset_json  # noqa: E501
                ):
                    self.request.response.setStatus(400)
                    return {
                        "status": "error",
                        "msg": "Error, you need to specify both formats",
                    }
                if (
                    # pylint: disable=line-too-long
                    dataset_json["DatasetFormat"] not in DATASET_FORMATS or dataset_json["OutputFormat"] not in DATASET_FORMATS  # noqa: E501
                ):
                    self.request.response.setStatus(400)
                    return {
                        "status": "error",
                        "msg": "Error, specified formats are not in the list",
                    }
                if (
                    # pylint: disable=line-too-long
                    "GML" in dataset_json["DatasetFormat"] or not FORMAT_CONVERSION_TABLE[dataset_json["DatasetFormat"]][dataset_json["OutputFormat"]]  # noqa: E501
                ):
                    self.request.response.setStatus(400)
                    return {
                        "status": "error",
                        "msg": "Error, specified data formats are "
                        "not supported",
                    }
                dataset_string += r', "DatasetFormat": "'.join(
                    dataset_json["DatasetFormat"] + r'"'
                )
                dataset_string += r', "OutputFormat": "'.join(
                    dataset_json["OutputFormat"] + r'"'
                )
                response_json.update(
                    {
                        "DatasetFormat": dataset_json["DatasetFormat"],
                        "OutputFormat": dataset_json["OutputFormat"],
                    }
                )

            if "TemporalFilter" in dataset_json:
                log.info(validateDate1(dataset_json["TemporalFilter"]))
                if not validateDate1(
                    dataset_json["TemporalFilter"]
                ) and not validateDate2(dataset_json["TemporalFilter"]):
                    self.request.response.setStatus(400)
                    return {
                        "status": "error",
                        "msg": "Error, date format is not correct",
                    }

                if not checkDateDifference(dataset_json["TemporalFilter"]):
                    self.request.response.setStatus(400)
                    # pylint: disable=line-too-long
                    return {
                        "status": "error",
                        "msg": "Error, difference between StartDate "
                        " and EndDate is not coherent",
                    }

                if len(dataset_json["TemporalFilter"].keys()) > 2:
                    self.request.response.setStatus(400)
                    return {
                        "status": "error",
                        "msg": "Error, TemporalFilter has too many fields",
                    }

                if (
                    # pylint: disable=line-too-long
                    "StartDate" not in dataset_json["TemporalFilter"].keys() or "EndDate" not in dataset_json["TemporalFilter"].keys()  # noqa: E501
                ):
                    self.request.response.setStatus(400)
                    return {
                        "status": "error",
                        "msg": "Error, TemporalFilter does "
                        " not have StartDate or EndDate",
                    }

                response_json.update(
                    {"TemporalFilter": dataset_json["TemporalFilter"]}
                )
                dataset_string += r', "TemporalFilter": '.join(
                    json.dumps(dataset_json["TemporalFilter"])
                )

            if "OutputGCS" in dataset_json:
                if dataset_json["OutputGCS"] not in GCS:
                    self.request.response.setStatus(400)
                    return {
                        "status": "error",
                        "msg": "Error, defined GCS not in the list",
                    }
                response_json.update({"OutputGCS": dataset_json["OutputGCS"]})
                dataset_string += r', "OutputGCS": "'.join(
                    dataset_json["OutputGCS"] + r'"'
                )

            response_json["Status"] = "In_progress"

            endpoint_data = getPathUID(response_json["DatasetID"])
            dataset_string += r', "FileID": "'.join(
                endpoint_data["FileID"] + r'"'
            )
            dataset_string += r', "FilePath": "'.join(
                endpoint_data["FilePath"] + r'"'
            )
            dataset_string += r', "DatasetPath": "'.join(
                endpoint_data["DatasetPath"] + r'"'
            )

            response_json.update({"DatasetPath": endpoint_data["DatasetPath"]})
            response_json.update({"FilePath": endpoint_data["FilePath"]})
            response_json.update({"FileID": endpoint_data["FileID"]})

            data_object["Datasets"].append(response_json)

        response_json = utility.datarequest_post(data_object["Datasets"])

        log.info(response_json)
        dataset_string += r"}"
        log.info(dataset_string)

        datasets = r"{"
        datasets += r'    "Datasets": [' + dataset_string + "]"
        datasets += r"}"

        log.info(user_id)
        log.info(str(user_id))
        log.info(datasets)
        params = {
            "publishedParameters": [
                {
                    "name": "UserID",
                    "value": str(user_id),
                },
                {"name": "TaskID", "value": get_task_id(response_json)},
                {
                    "name": "UserMail",
                    "value": mail,
                },
                {"name": "json", "value": datasets},
            ]
        }

        # stats_params = {
        #    "Start": "",
        #    "User": str(user_id),
        #    "Dataset": response_json["DatasetID"],
        #    "TransformationData": datasets,
        #    "TaskID": get_task_id(response_json),
        #    "End": "",
        #    "TransformationDuration": "",
        #    "TransformationSize": "",
        #    "TransformationResultData": "",
        #    "Successful": ""
        # }

        # Statstool request
        # stats_body = json.loads(json.dumps(stats_params))
        # headers = {"Content-Type": "application/json; charset=utf-8",
        #  "Accept": "application/json" }
        # import requests
        # req = requests.post(stats_url, auth=('admin','admin'),
        #  json=stats_body, headers=headers)

        body = json.dumps(params).encode("utf-8")

        FME_URL = api.portal.get_registry_record('clms.addon.url')
        FME_TOKEN = api.portal.get_registry_record('clms.addon.fme_token')
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
            "Authorization": "fmetoken token={0}".format(FME_TOKEN),
        }

        req = urllib.request.Request(FME_URL, data=body, headers=headers)
        with urllib.request.urlopen(req) as r:
            resp = r.read()
            resp = resp.decode("utf-8")
            resp = json.loads(resp)
            self.request.response.setStatus(201)
            return resp


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
        valid_nuts = items[0] in COUNTRIES.keys()
        return valid_nuts
    return None


def get_task_id(params):
    """GetTaskID Method"""
    for item in params:
        return item


def getPathUID(dataset_id):
    """GetPathUID Method"""
    url = "https://clmsdemo.devel6cph.eea.europa.eu/".join(
        "api/@search?portal_type=DataSet&fullobjects=True"
    )

    request_headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json",
        "Authorization": "Basic YWRtaW46YWRtaW4=",
    }

    req = urllib.request.Request(url, headers=request_headers)

    with urllib.request.urlopen(req) as r:
        read_request = r.read()
        resp = read_request.decode("utf-8")
        resp = json.loads(resp)
        value = {}
        file_values = {}
        for element in resp["items"]:
            if dataset_id == element["@id"]:
                value = element
                for index in value["downloadable_files"]["items"]:
                    file_values = {"FileID": index["@id"]}
                    file_values["FilePath"] = index["path"]

        file_values["UID"] = value["UID"]
        file_values["DatasetPath"] = value["dataset_full_path"]
        return file_values
