"""Utils"""
import random
import json
import base64
from logging import getLogger
from datetime import datetime

import requests
from plone import api
from zope.component import getUtility

from clms.downloadtool.api.services.cdse.cdse_integration import (
    get_portal_config)
from clms.downloadtool.api.services.utils import get_extra_data
from clms.statstool.utility import IDownloadStatsUtility


ISO8601_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
log = getLogger(__name__)


EEA_GEONETWORK_BASE_URL = (
    "https://sdi.eea.europa.eu/catalogue/copernicus/"
    "api/records/{uid}/formatters/xml?approved=true"
)
VITO_GEONETWORK_BASE_URL = (
    "https://globalland.vito.be/geonetwork/"
    "srv/api/records/{uid}/formatters/xml?approved=true"
)


def get_dataset_by_uid(uid):
    """get the dataset by UID"""
    brains = api.content.find(UID=uid)
    if brains:
        return brains[0].getObject()

    return None


def get_callback_url():
    """get the callback url where FME should signal any status changes"""
    portal_url = api.portal.get().absolute_url()
    if portal_url.endswith("/api"):
        portal_url = portal_url.replace("/api", "")

    return "{}/++api++/{}".format(
        portal_url,
        "@datarequest_status_patch",
    )


def get_nuts_by_id(nutsid):
    """Get NUTS by ID"""
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


def base64_encode_path(path):
    """encode the given path as base64"""
    if isinstance(path, str):
        return base64.urlsafe_b64encode(path.encode("utf-8")).decode("utf-8")

    return base64.urlsafe_b64encode(path).decode("utf-8")


def to_iso8601(dt_str):
    """Convert datetime in format requested by CDSE"""
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    return dt.isoformat() + "Z"   # adding Z for UTC


def generate_task_group_id():
    """A CDSE parent task and its childs have the same group ID.
       Example: 4823-9501-3746-1835
    """
    groups = []
    for _ in range(4):
        group = ''.join(str(random.randint(0, 9)) for _ in range(4))
        groups.append(group)
    return '-'.join(groups)


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
            start_date_obj.strftime(ISO8601_DATETIME_FORMAT),
            end_date_obj.strftime(ISO8601_DATETIME_FORMAT),
        )
    except (TypeError, ValueError):
        return None, None


def get_task_id(params):
    """GetTaskID Method"""
    for item in params:
        return item


def save_stats(stats_json):
    """save the stats in the download stats utility"""
    try:
        utility = getUtility(IDownloadStatsUtility)
        stats_json.update(get_extra_data(stats_json))
        utility.register_item(stats_json)
    except Exception as e:
        log.exception(e)
        log.info(
            "There was an error saving the stats: %s", json.dumps(stats_json)
        )  # noqa


def get_dataset_file_path_from_file_id(dataset_object, file_id):
    """get the dataset file path from the file id"""
    downloadable_files_json = dataset_object.downloadable_files
    for file_object in downloadable_files_json.get("items", []):
        if file_object.get("@id") == file_id:
            return file_object.get("path", "")

    return None


def get_dataset_file_source_from_file_id(dataset_object, file_id):
    """get the dataset file format from the file id"""
    downloadable_files_json = dataset_object.downloadable_files
    for file_object in downloadable_files_json.get("items", []):
        if file_object.get("@id") == file_id:
            return file_object.get("source", "")

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
            value = download_information.get("full_format", "")
            if isinstance(value, dict):
                return value.get("token", "")

            return value

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
            value = download_information.get("full_source", "")
            if isinstance(value, dict):
                return value.get("token", "")

            return value

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


def get_full_dataset_layers(dataset_object, download_information_id):
    """get the available layers/bands based on the requested
    download_information_id
    """
    dataset_download_information_json = (
        dataset_object.dataset_download_information
    )
    for download_information in dataset_download_information_json.get(
        "items", []
    ):
        if download_information.get("@id") == download_information_id:
            return download_information.get("layers", [])

    return []


def post_request_to_fme(params, is_prepackaged=False):
    """send the request to FME and let it process it"""
    if is_prepackaged:
        fme_url = api.portal.get_registry_record(
            "clms.downloadtool.fme_config_controlpanel.url_prepackaged"
        )
    else:
        fme_url = api.portal.get_registry_record(
            "clms.downloadtool.fme_config_controlpanel.url"
        )
    fme_token = api.portal.get_registry_record(
        "clms.downloadtool.fme_config_controlpanel.fme_token"
    )
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json",
        "Authorization": "fmetoken token={0}".format(fme_token),
    }
    try:
        resp = requests.post(
            fme_url, json=params, headers=headers, timeout=10
        )
        if resp.ok:
            fme_task_id = resp.json().get("id", None)
            return fme_task_id
    except requests.exceptions.Timeout:
        log.info("FME request timed out")
    body = json.dumps(params)
    log.info(
        "There was an error registering the download request in FME: %s",
        body,
    )

    return {}


def params_for_fme(user_id, utility_task_id, mail, datasets):
    """Prepare params for FME task"""
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
                "value": get_callback_url(),
            },
            # dump the json into a string for FME
            {"name": "json", "value": json.dumps(datasets)},
        ]
    }
    return params


def build_stats_params(user_id, data_object, datasets, utility_task_id):
    """Prepare stats params"""
    stats_params = {
        "Start": datetime.utcnow().isoformat(),
        "User": str(user_id),
        "Dataset": [item["DatasetID"] for item in data_object.get(
            "Datasets", [])],
        "TransformationData": datasets,
        "TaskID": utility_task_id,
        "End": "",
        "TransformationDuration": "",
        "TransformationSize": "",
        "TransformationResultData": "",
        "Status": "Queued",
    }
    return stats_params


def build_metadata_urls(dataset_object):
    """Return list of metadata URLs for a dataset_object."""
    metadata = []
    for meta in getattr(
            dataset_object, "geonetwork_identifiers", {}).get("items", []):
        t = meta.get("type", "")
        if t == "EEA":
            url = EEA_GEONETWORK_BASE_URL.format(uid=meta.get("id"))
        elif t == "VITO":
            url = VITO_GEONETWORK_BASE_URL.format(uid=meta.get("id"))
        else:
            url = meta.get("id")
        metadata.append(url)
    return metadata


def get_s3_paths(batch_ids):
    """Prepare info for DatasetPath"""
    config = get_portal_config()
    s3_paths = []
    for batch_id in batch_ids:
        s3_path = f"s3://{config['s3_bucket_name']}/{batch_id}"
        s3_paths.append(s3_path)

    return s3_paths
