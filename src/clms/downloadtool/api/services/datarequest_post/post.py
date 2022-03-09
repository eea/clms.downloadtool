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

import requests
from plone import api
from plone.memoize.ram import cache
from plone.protect.interfaces import IDisableCSRFProtection
from plone.restapi.deserializer import json_body
from plone.restapi.services import Service
from zope.component import getUtility
from zope.interface import alsoProvides

from clms.statstool.utility import IDownloadStatsUtility
from clms.downloadtool.utility import IDownloadToolUtility
from clms.downloadtool.utils import COUNTRIES, FORMAT_CONVERSION_TABLE, GCS


def _cache_key(fun, self, nutsid):
    """ Cache key function """
    return nutsid


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
        general_download_data_object = {}
        general_download_data_object["Datasets"] = []

        prepacked_download_data_object = {}
        prepacked_download_data_object["Datasets"] = []

        valid_dataset = False

        utility = getUtility(IDownloadToolUtility)

        for dataset_json in datasets_json:
            response_json = {}
            if "DatasetID" not in dataset_json:
                self.request.response.setStatus(400)
                return {
                    "status": "error",
                    "msg": "Error, DatasetID is not defined",
                }

            if not dataset_json.get("DatasetID"):
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
                file_path = get_dataset_file_path_from_file_id(
                    dataset_object, dataset_json["FileID"]
                )
                file_format = get_dataset_file_format_from_file_id(
                    dataset_object, dataset_json["FileID"]
                )
                if file_path and file_format:
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
                prepacked_download_data_object["Datasets"].append(
                    response_json
                )
            else:
                if "NUTS" in dataset_json:
                    if not validate_nuts(dataset_json["NUTS"]):
                        self.request.response.setStatus(400)
                        return {
                            "status": "error",
                            "msg": "NUTS country error",
                        }
                    response_json.update({"NUTSID": dataset_json["NUTS"]})
                    response_json.update(
                        {"NUTSName": self.get_nuts_name(dataset_json["NUTS"])}
                    )

                if "BoundingBox" in dataset_json:
                    if "NUTS" in dataset_json:
                        self.request.response.setStatus(400)
                        return {
                            "status": "error",
                            "msg": "Error, NUTS is also defined",
                        }

                    if not validate_spatial_extent(
                        dataset_json["BoundingBox"]
                    ):
                        self.request.response.setStatus(400)
                        return {
                            "status": "error",
                            "msg": "Error, BoundingBox is not valid",
                        }

                    response_json.update(
                        {"BoundingBox": dataset_json["BoundingBox"]}
                    )

                if "TemporalFilter" in dataset_json:
                    if len(dataset_json["TemporalFilter"].keys()) > 2:
                        self.request.response.setStatus(400)
                        return {
                            "status": "error",
                            "msg": "Error, TemporalFilter has too many fields",
                        }

                    if "StartDate" not in dataset_json["TemporalFilter"]:
                        self.request.response.setStatus(400)
                        return {
                            "status": "error",
                            "msg": (
                                "Error, TemporalFilter does "
                                " not have StartDate or EndDate"
                            ),
                        }
                    if "EndDate" not in dataset_json["TemporalFilter"]:
                        self.request.response.setStatus(400)
                        return {
                            "status": "error",
                            "msg": (
                                "Error, TemporalFilter does "
                                " not have StartDate or EndDate"
                            ),
                        }

                    start_date, end_date = extract_dates_from_temporal_filter(
                        dataset_json["TemporalFilter"]
                    )

                    if start_date is None or end_date is None:
                        self.request.response.setStatus(400)
                        return {
                            "status": "error",
                            "msg": "Error, date format is not correct",
                        }

                    if start_date > end_date:
                        self.request.response.setStatus(400)
                        return {
                            "status": "error",
                            "msg": (
                                "Error, difference between StartDate "
                                " and EndDate is not coherent"
                            ),
                        }

                    response_json.update(
                        {
                            "TemporalFilter": {
                                "StartDate": start_date,
                                "EndDate": end_date,
                            }
                        }
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

                if "DatasetDownloadInformationID" not in dataset_json:
                    self.request.response.setStatus(400)
                    return {
                        "status": "error",
                        "msg": (
                            "Error, DatasetDownloadInformationID is not"
                            " defined."
                        ),
                    }

                download_information_id = dataset_json.get(
                    "DatasetDownloadInformationID"
                )
                # Check if the dataset format value is correct
                full_dataset_format = get_full_dataset_format(
                    dataset_object, download_information_id
                )
                if full_dataset_format is None:
                    self.request.response.setStatus(400)
                    return {
                        "status": "error",
                        "msg": "Error, the dataset format is not valid",
                    }

                requested_output_format = dataset_json.get(
                    "OutputFormat", None
                )
                if requested_output_format not in FORMAT_CONVERSION_TABLE:
                    self.request.response.setStatus(400)
                    return {
                        "status": "error",
                        "msg": (
                            "Error, the specified output format is not valid"
                        ),
                    }

                available_transformations_for_format = (
                    FORMAT_CONVERSION_TABLE.get(full_dataset_format)
                )

                if not available_transformations_for_format.get(
                    requested_output_format, None
                ):
                    self.request.response.setStatus(400)
                    return {
                        "status": "error",
                        "msg": "Error, specified formats are not compatible",
                    }

                # Check if the dataset source is OK
                full_dataset_source = get_full_dataset_source(
                    dataset_object, download_information_id
                )

                if not full_dataset_source:
                    self.request.response.setStatus(400)
                    return {
                        "status": "error",
                        "msg": "Error, the dataset source is not valid",
                    }

                # Check if the dataset path is OK
                full_dataset_path = get_full_dataset_path(
                    dataset_object, download_information_id
                )
                if not full_dataset_path:
                    self.request.response.setStatus(400)
                    return {
                        "status": "error",
                        "msg": "Error, the dataset path is not valid",
                    }

                #
                # # Check if wekeo choices are OK
                # wekeo_choices = get_full_dataset_wekeo_choices(
                #     dataset_object, download_information_id
                # )
                # if not wekeo_choices:
                #     self.request.response.setStatus(400)
                #     return {
                #         "status": "error",
                #         "msg": "Error, the dataset path is not valid",
                #     }

                response_json.update(
                    {
                        "DatasetFormat": full_dataset_format,
                        "OutputFormat": dataset_json.get("OutputFormat", ""),
                        "DatasetPath": base64_encode_path(full_dataset_path),
                        "DatasetSource": full_dataset_source,
                        # "WekeoChoices": wekeo_choices,
                    }
                )

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

                general_download_data_object["Datasets"].append(response_json)

        fme_results = {
            "ok": [],
            "error": [],
        }

        for data_object in [
            prepacked_download_data_object,
            general_download_data_object,
        ]:
            if data_object["Datasets"]:
                data_object["Status"] = "In_progress"
                data_object["UserID"] = user_id
                data_object[
                    "RegistrationDateTime"
                ] = datetime.utcnow().isoformat()
                utility_response_json = utility.datarequest_post(data_object)
                utility_task_id = get_task_id(utility_response_json)
                new_datasets = {"Datasets": data_object["Datasets"]}

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
                    "Dataset": [
                        item["DatasetID"]
                        for item in data_object.get("Datasets", [])
                    ],  # noqa: E501
                    "TransformationData": new_datasets,
                    "TaskID": utility_task_id,
                    "End": "",
                    "TransformationDuration": "",
                    "TransformationSize": "",
                    "TransformationResultData": "",
                    "Successful": "",
                }
                save_stats(stats_params)
                fme_result = self.post_request_to_fme(params)
                if fme_result:
                    data_object["FMETaskId"] = fme_result
                    utility.datarequest_status_patch(
                        data_object, utility_task_id
                    )
                    self.request.response.setStatus(201)
                    fme_results["ok"].append({"TaskID": utility_task_id})
                else:
                    fme_results["error"].append({"TaskID": utility_task_id})

        if fme_results["error"] and not fme_results["ok"]:
            # All requests failed
            self.request.response.setStatus(500)
            return {
                "status": "error",
                "msg": "Error, all requests failed",
            }

        self.request.response.setStatus(201)
        return {
            "TaskIds": fme_results["ok"],
            "ErrorTaskIds": fme_results["error"],
        }

    def post_request_to_fme(self, params):
        """ send the request to FME and let it process it"""
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
            fme_task_id = resp.json().get("id", None)
            return fme_task_id

        body = json.dumps(params)
        # pylint: disable=line-too-long
        log.info(
            "There was an error registering the download request in FME: %s",
            body,
        )  # noqa

        return {}

    @cache(_cache_key)
    def get_nuts_name(self, nutsid):
        """Based on the NUTS ID, return the name of
        the NUTS region.
        """
        url = api.portal.get_registry_record(
            "clms.downloadtool.fme_config_controlpanel.nuts_service"
        )
        if url:
            url += "where=NUTS_ID='{}'".format(nutsid)
            resp = requests.get(url)
            if resp.ok:
                resp_json = resp.json()
                features = resp_json.get("features", [])
                for feature in features:
                    attributes = feature.get("attributes", {})
                    nuts_name = attributes.get("NAME_LATN", "")
                    if nuts_name:
                        return nuts_name

        return nutsid


def extract_dates_from_temporal_filter(temporal_filter):
    """StartDate and EndDate are mandatory and come in miliseconds since
    epoch, so we need to convert them to datetime objects first and to
    ISO8601-like format then.
    """
    try:
        start_date = temporal_filter.get("StartDate")
        end_date = temporal_filter.get("EndDate")

        start_date_obj = datetime.fromtimestamp(start_date / 1000)
        end_date_obj = datetime.fromtimestamp(end_date / 1000)

        return (
            start_date_obj.strftime("%Y-%m-%d %H:%M:%S"),
            end_date_obj.strftime("%Y-%m-%d %H:%M:%S"),
        )
    except (TypeError, ValueError):
        return None, None


def validate_spatial_extent(bounding_box):
    """ validate Bounding Box """
    if not len(bounding_box) == 4:
        return False

    for x in bounding_box:
        if not isinstance(x, int) and not isinstance(x, float):
            return False

    return True


def validate_nuts(nuts_id):
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


def get_dataset_file_path_from_file_id(dataset_object, file_id):
    """ get the dataset file path from the file id"""
    downloadable_files_json = dataset_object.downloadable_files
    for file_object in downloadable_files_json.get("items", []):
        if file_object.get("@id") == file_id:
            return file_object.get("path", "")

    return None


def get_dataset_file_format_from_file_id(dataset_object, file_id):
    """ get the dataset file format from the file id"""
    downloadable_files_json = dataset_object.downloadable_files
    for file_object in downloadable_files_json.get("items", []):
        if file_object.get("@id") == file_id:
            return file_object.get("format", "")

    return None


def get_full_dataset_format(dataset_object, download_information_id):
    """get the dataset full format based on the requested
    download_information_id"""
    dataset_download_information_json = (
        dataset_object.dataset_download_information
    )
    for download_information in dataset_download_information_json.get(
        "items", []
    ):
        if download_information.get("@id") == download_information_id:
            return download_information.get("full_format", "")

    return None


def get_full_dataset_source(dataset_object, download_information_id):
    """get the dataset full source based on the requested
    download_information_id"""
    dataset_download_information_json = (
        dataset_object.dataset_download_information
    )
    for download_information in dataset_download_information_json.get(
        "items", []
    ):
        if download_information.get("@id") == download_information_id:
            return download_information.get("full_source", "")

    return None


def get_full_dataset_path(dataset_object, download_information_id):
    """get the dataset full path based on the requested
    download_information_id"""
    dataset_download_information_json = (
        dataset_object.dataset_download_information
    )
    for download_information in dataset_download_information_json.get(
        "items", []
    ):
        if download_information.get("@id") == download_information_id:
            return download_information.get("full_path", "")

    return None


def get_full_dataset_wekeo_choices(dataset_object, download_information_id):
    """get the dataset wekeo_choices based on the requested
    download_information_id"""
    dataset_download_information_json = (
        dataset_object.dataset_download_information
    )
    for download_information in dataset_download_information_json.get(
        "items", []
    ):
        if download_information.get("@id") == download_information_id:
            return download_information.get("wekeo_choices", "")

    return None
