# -*- coding: utf-8 -*-
"""
For HTTP GET operations we can use standard HTTP parameter passing
through the URL)

"""
import base64
import json
import re

from datetime import datetime
from logging import getLogger
from clms.downloadtool.utility import IDownloadToolUtility
from clms.downloadtool.utils import COUNTRIES
from clms.downloadtool.utils import FORMAT_CONVERSION_TABLE
from clms.downloadtool.utils import GCS
from clms.statstool.utility import IDownloadStatsUtility
from plone import api
from plone.protect.interfaces import IDisableCSRFProtection
from plone.restapi.deserializer import json_body
from plone.restapi.services import Service
from zope.component import getUtility
from zope.interface import alsoProvides

import requests


log = getLogger(__name__)


EEA_GEONETWORK_BASE_URL = (
    "https://sdi.eea.europa.eu/catalogue/copernicus/"
    "api/records/{uid}/formatters/xml?approved=true"
)
VITO_GEONETWORK_BASE_URL = (
    "https://land.copernicus.vgt.vito.be/geonetwork/"
    "srv/api/records/{uid}/formatters/xml?approved=true"
)


def base64_encode_path(path):
    """ encode the given path as base64"""
    if isinstance(path, str):
        return base64.urlsafe_b64encode(path.encode("utf-8")).decode("utf-8")

    return base64.urlsafe_b64encode(path).decode("utf-8")


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

    def get_dataset_file_format_from_file_id(self, dataset_object, file_id):
        """ get the dataset file format from the file id"""
        downloadable_files_json = dataset_object.downloadable_files
        for file_object in downloadable_files_json.get("items", []):
            if file_object.get("@id") == file_id:
                return file_object.get("file_format", "")

        return None

    def reply(self):
        """ JSON response """
        alsoProvides(self.request, IDisableCSRFProtection)
        body = json_body(self.request)

        user = api.user.get_current()
        if not user:
            return {
                "status": "error",
                "msg": "You need to be logged in to use this service",
            }

        user_id = user.getId()
        datasets_json = body.get("Datasets")

        mail = user.getProperty("email")
        data_object = {}
        data_object["Datasets"] = []

        valid_dataset = False

        utility = getUtility(IDownloadToolUtility)

        for dataset_json in datasets_json:
            response_json = {}
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

            response_json.update(
                {
                    "DatasetID": dataset_json["DatasetID"],
                    "DatasetTitle": dataset_object.Title(),
                }
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
                file_format = self.get_dataset_file_format_from_file_id(
                    dataset_object, dataset_json["FileID"]
                )
                if dataset_json is not None:
                    response_json.update({"FileID": dataset_json["FileID"]})
                    response_json.update(
                        {"DatasetPath": base64_encode_path(file_path)}
                    )
                    response_json.update({"FilePath": "PREPACKAGE"})
                    response_json.update({"OutputFormat": file_format})
                else:
                    self.request.response.setStatus(400)
                    return {
                        "status": "error",
                        "msg": "Error, the FileID is not valid",
                    }
            else:
                if "NUTS" in dataset_json:
                    if not validateNuts(dataset_json["NUTS"]):
                        self.request.response.setStatus(400)
                        return {
                            "status": "error",
                            "msg": "NUTS country error",
                        }
                    response_json.update({"NUTSID": dataset_json["NUTS"]})

                if "BoundingBox" in dataset_json:
                    if "NUTS" in dataset_json:
                        self.request.response.setStatus(400)
                        return {
                            "status": "error",
                            "msg": "Error, NUTS is also defined",
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

                # Quick check if the dataset format value is None
                dataset_full_format = dataset_object.dataset_full_format
                if dataset_full_format is None:
                    dataset_full_format = ""

                response_json.update(
                    {
                        "DatasetFormat": dataset_object.dataset_full_format,
                        "OutputFormat": dataset_json.get("OutputFormat", ""),
                    }
                )

                if not FORMAT_CONVERSION_TABLE[
                    dataset_object.dataset_full_format
                ][dataset_json.get("OutputFormat", "")]:
                    self.request.response.setStatus(400)
                    return {
                        "status": "error",
                        "msg": "Error, specified formats are not compatible",
                    }
                # In any case, get the dataset_full_path and use it.
                response_json.update(
                    {
                        "DatasetPath": base64_encode_path(
                            dataset_object.dataset_full_path
                        )
                    }
                )

                if dataset_object.dataset_full_source is not None:
                    response_json.update(
                        {"DatasetSource": dataset_object.dataset_full_source}
                    )
                else:
                    response_json.update({"DatasetSource": ""})

                metadata = []
                for meta in dataset_object.geonetwork_identifiers.get(
                    "items", []
                ):
                    if meta.get("type", "") == "EEA":
                        metadata_url = EEA_GEONETWORK_BASE_URL.format(
                            uid=meta.get("id")
                        )
                    elif meta.get("type", "") == "VITO":
                        metadata_url = VITO_GEONETWORK_BASE_URL.format(
                            uid=meta.get("id")
                        )
                    else:
                        metadata_url = meta.get("id")
                    metadata.append(metadata_url)

                response_json["Metadata"] = metadata

            data_object["Datasets"].append(response_json)

        data_object["Status"] = "In_progress"
        data_object["UserID"] = user_id
        data_object["RegistrationDateTime"] = datetime.utcnow().isoformat()
        utility_response_json = utility.datarequest_post(data_object)
        utility_task_id = get_task_id(utility_response_json)
        new_datasets = {"Datasets": data_object['Datasets']}

        params = {
            "publishedParameters": [
                {
                    "name": "UserID",
                    "value": str(user_id),
                },
                {
                    "name": "TaskID",
                    "value": utility_task_id,
                },
                {
                    "name": "UserMail",
                    "value": mail,
                },
                {
                    "name": "CallbackUrl",
                    "value": "{}/{}".format(
                        api.portal.get().absolute_url(),
                        "@datarequest_status_patch",
                    ),

                },
                # dump the json into a string for FME
                {"name": "json", "value": json.dumps(new_datasets)},
            ]
        }

        # build the stat params and save them
        stats_params = {
            "Start": "",
            "User": str(user_id),
            # pylint: disable=line-too-long
            "Dataset": [item["DatasetID"] for item in data_object.get("Datasets", [])],  # noqa: E501
            "TransformationData": new_datasets,
            "TaskID": utility_task_id,
            "End": "",
            "TransformationDuration": "",
            "TransformationSize": "",
            "TransformationResultData": "",
            "Successful": "",
        }
        save_stats(stats_params)
        FME_URL = api.portal.get_registry_record(
            "clms.downloadtool.fme_config_controlpanel.url"
        )
        FME_TOKEN = api.portal.get_registry_record(
            "clms.downloadtool.fme_config_controlpanel.fme_token"
        )
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
            "Authorization": "fmetoken token={0}".format(FME_TOKEN),
        }
        resp = requests.post(FME_URL, json=params, headers=headers)
        if resp.ok:
            self.request.response.setStatus(201)
            log.info('Datarequest created: "%s"', params)
            fme_task_id = resp.json().get('id', None)
            if fme_task_id is not None:
                data_object["FMETaskId"] = fme_task_id
                utility.datarequest_status_patch(data_object, utility_task_id)

            return {"TaskID": utility_task_id}

        body = json.dumps(params)
        # pylint: disable=line-too-long
        log.info(
            "There was an error registering the download request in"
            " FME: %s",
            body,
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
            date_obj1 = datetime.strptime(start_date, date_format)
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
            date_obj1 = datetime.strptime(start_date, date_format)
            date_obj2 = datetime.strptime(end_date, date_format)
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
    match = re.match(r"([A-Z]+)([0-9]*)", nuts_id, re.I)
    if match:
        items = match.groups()
        # Only the first 2 chars represent the country
        # french NUTS codes have 3 alphanumeric chars and then numbers
        valid_nuts = items[0][:2] in COUNTRIES.keys()
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
