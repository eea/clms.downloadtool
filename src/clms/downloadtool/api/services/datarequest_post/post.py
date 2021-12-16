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
from clms.statstool.utility import IDownloadStatsUtility
from clms.downloadtool.utility import IDownloadToolUtility
from clms.downloadtool.utils import (
    COUNTRIES,
    GCS,
)


log = getLogger(__name__)


class DataRequestPost(Service):
    """Set Data"""

    def get_dataset_by_uid(self, uid):
        """ get the dataset by UID"""
        brains = api.content.find(UID=uid)
        if brains:
            return brains[0].getObject()

        return None

    def get_dataset_file_path_from_file_id(self, dataset_object, file_id):
        """ get the dataset file path from the file id"""
        downloadable_files_json = dataset_object.downloadable_files
        for file_object in downloadable_files_json.get("items", []):
            if file_object.get("@id") == file_id:
                return file_object.get("file_path", "")

        return None

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

        for dataset_json in datasets_json:
            if not dataset_json["DatasetID"]:
                self.request.response.setStatus(400)
                return {
                    "status": "error",
                    "msg": "Error, DatasetID is not defined",
                }
            valid_dataset = False

            dataset_object = self.get_dataset_by_uid(dataset_json["DatasetID"])
            if dataset_object is not None:
                valid_dataset = True

            if not valid_dataset:
                self.request.response.setStatus(400)
                return {
                    "status": "error",
                    "msg": "Error, the DatasetID is not valid",
                }

            response_json.update({"DatasetID": dataset_json["DatasetID"]})

            if len(dataset_string) == 1:
                dataset_string += (
                    r'"DatasetID": "' + dataset_json["DatasetID"] + r'"'
                )
            else:
                dataset_string += (
                    r'},{"DatasetID": "' + dataset_json["DatasetID"] + r'"'
                )

            # Handle FileID requests:
            # - get first the file_path from the dataset using the file_id
            # - if something is returned use it as FileID and FilePath
            # - if not return an error stating that the requested FileID is
            #   not valid
            if "FileID" in dataset_json:
                file_path = self.get_dataset_file_path_from_file_id(
                    dataset_object, dataset_json["FileID"]
                )
                if dataset_json is not None:
                    # pylint: disable=line-too-long
                    dataset_string += (
                        r', "FileID": "' + dataset_json["FileID"] + r'"'
                    )  # noqa
                    dataset_string += r', "FilePath": "' + file_path + r'"'

                    response_json.update({"FileID": dataset_json["FileID"]})
                    response_json.update({"FilePath": file_path})
                else:
                    self.request.response.setStatus(400)
                    return {
                        "status": "error",
                        "msg": "Error, the FileID is not valid",
                    }
            else:

                if "NUTSID" in dataset_json:
                    if not validateNuts(dataset_json["NUTSID"]):
                        self.request.response.setStatus(400)
                        return {
                            "status": "error",
                            "msg": "NUTSID country error",
                        }
                    response_json.update({"NUTSID": dataset_json["NUTSID"]})
                    dataset_string += (
                        r', "NUTSID": "' + dataset_json["NUTSID"] + r'"'
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

                if "TemporalFilter" in dataset_json:
                    # pylint: disable=line-too-long
                    if not validateDate1(
                        dataset_json["TemporalFilter"]
                    ) and not validateDate2(
                        dataset_json["TemporalFilter"]
                    ):  # noqa
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
                            "msg": (
                                "Error, difference between StartDate "
                                " and EndDate is not coherent"
                            ),
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
                            "msg": (
                                "Error, TemporalFilter does "
                                " not have StartDate or EndDate"
                            ),
                        }

                    response_json.update(
                        {"TemporalFilter": dataset_json["TemporalFilter"]}
                    )
                    dataset_string += r', "TemporalFilter": ' + json.dumps(
                        dataset_json["TemporalFilter"]
                    )

                if "OutputGCS" in dataset_json:
                    if dataset_json["OutputGCS"] not in GCS:
                        self.request.response.setStatus(400)
                        return {
                            "status": "error",
                            "msg": "Error, defined GCS not in the list",
                        }
                    response_json.update(
                        {"OutputGCS": dataset_json["OutputGCS"]}
                    )
                    dataset_string += (
                        r', "OutputGCS": "' + dataset_json["OutputGCS"] + r'"'
                    )

            response_json["Status"] = "In_progress"

            # Quick check if the dataset format value is None
            dataset_full_format = dataset_object.dataset_full_format
            if dataset_full_format is None:
                dataset_full_format = ""
            # pylint: disable=line-too-long
            dataset_string += (
                r', "DatasetFormat": "' + dataset_full_format + r'"'
            )  # noqa
            # pylint: disable=line-too-long
            dataset_string += r', "OutputFormat": "' + dataset_json.get("OutputFormat", "") + r'"'  # noqa
            response_json.update(
                {
                    "DatasetFormat": dataset_object.dataset_full_format,
                    "OutputFormat": dataset_json.get("OutputFormat", ""),
                }
            )
            # In any case, get the dataset_full_path and use it.
            dataset_string += (
                r', "DatasetPath": "' + dataset_object.dataset_full_path + r'"'
            )  # noqa
            response_json.update(
                {"DatasetPath": dataset_object.dataset_full_path}
            )

            if dataset_object.dataset_full_source is not None:
                dataset_string += r', "DatasetSource": "' + dataset_object.dataset_full_source + r'"'  # noqa
                response_json.update(
                    {"DatasetSource": dataset_object.dataset_full_source}
                )
            else:
                dataset_string += r', "DatasetSource": "' + "" + r'"'  # noqa
                response_json.update({"DatasetSource": ""})

            data_object["Datasets"].append(response_json)

        response_json = utility.datarequest_post(data_object["Datasets"])

        dataset_string += r"}"

        datasets = r"{"
        datasets += r'    "Datasets": [' + dataset_string + "]"
        datasets += r"}"

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

        stats_params = {
            "Start": "",
            "User": str(user_id),
            # pylint: disable=line-too-long
            "Dataset": [
                item["DatasetID"]
                for item in response_json.get(get_task_id(response_json), [])
            ],  # noqa
            "TransformationData": datasets,
            "TaskID": get_task_id(response_json),
            "End": "",
            "TransformationDuration": "",
            "TransformationSize": "",
            "TransformationResultData": "",
            "Successful": "",
        }
        save_stats(stats_params)

        body = json.dumps(params).encode("utf-8")

        FME_URL = api.portal.get_registry_record(
            "clms.addon.fme_config_controlpanel.url"
        )
        FME_TOKEN = api.portal.get_registry_record(
            "clms.addon.fme_config_controlpanel.fme_token"
        )
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
            "Authorization": "fmetoken token={0}".format(FME_TOKEN),
        }

        try:
            req = urllib.request.Request(FME_URL, data=body, headers=headers)
            with urllib.request.urlopen(req) as r:
                resp = r.read()
                resp = resp.decode("utf-8")
                resp = json.loads(resp)
                self.request.response.setStatus(201)
                return {"TaskID": get_task_id(response_json)}
        except Exception:
            # pylint: disable=line-too-long
            log.info(
                "There was an error registering the download request in"
                " FME: %s", json.dumps(body)
            )  # noqa
            self.request.response.setStatus(500)
            return {}


def validateDate1(temporal_filter):
    """ validate date format year-month day """
    start_date = temporal_filter.get("StartDate")
    end_date = temporal_filter.get("EndDate")

    date_format = "%Y-%m-%d"
    try:
        if start_date is not None and end_date is not None:
            date_obj1 = datetime.datetime.strptime(start_date, date_format)
            date_obj2 = datetime.datetime.strptime(end_date, date_format)
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
            date_obj2 = datetime.datetime.strptime(end_date, date_format)
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


def save_stats(stats_json):
    """ save the stats in the download stats utility"""
    try:
        utility = getUtility(IDownloadStatsUtility)
        utility.register_item(stats_json)
    except Exception:
        # pylint: disable=line-too-long
        log.info(
            "There was an error saving the stats: %s", json.dumps(stats_json)
        )  # noqa
