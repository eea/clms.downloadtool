"""Utils"""
import random
import re
import json
from logging import getLogger
from datetime import datetime
from zope.component import getUtility
from clms.downloadtool.utils import COUNTRIES
from clms.downloadtool.api.services.utils import get_extra_data
from clms.statstool.utility import IDownloadStatsUtility


ISO8601_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
log = getLogger(__name__)


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


def validate_spatial_extent(bounding_box):
    """validate Bounding Box"""
    if not len(bounding_box) == 4:
        return False

    for x in bounding_box:
        if not isinstance(x, int) and not isinstance(x, float):
            return False

    return True


def validate_nuts(nuts_id):
    """validate nuts"""
    if not nuts_id.isalnum():
        return False

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
