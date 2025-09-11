# -*- coding: utf-8 -*-
"""
For HTTP GET operations we can use standard HTTP parameter passing
through the URL)

"""
import copy
import json
import uuid
from datetime import datetime
from datetime import timedelta
from functools import reduce
from logging import getLogger

from clms.downloadtool.api.services.utils import (
    duplicated_values_exist,
)
from clms.downloadtool.utility import IDownloadToolUtility
from clms.downloadtool.utils import FORMAT_CONVERSION_TABLE
from clms.downloadtool.api.services.utils import (
    calculate_bounding_box_area,
    get_available_gcs_values,
)
from clms.downloadtool.api.services.cdse.cdse_integration import (
    create_batch, start_batch)
from clms.downloadtool.api.services.datarequest_post.utils import (
    EEA_GEONETWORK_BASE_URL,
    ISO8601_DATETIME_FORMAT,
    VITO_GEONETWORK_BASE_URL,
    base64_encode_path,
    extract_dates_from_temporal_filter,
    generate_task_group_id,
    get_callback_url,
    get_dataset_by_uid,
    get_dataset_file_path_from_file_id,
    get_dataset_file_source_from_file_id,
    get_full_dataset_format,
    get_full_dataset_layers,
    get_full_dataset_path,
    get_full_dataset_source,
    get_full_dataset_wekeo_choices,
    get_nuts_by_id,
    get_task_id,
    post_request_to_fme,
    save_stats,
    to_iso8601,
)
from clms.downloadtool.api.services.datarequest_post.validation import (
    MESSAGES,
    is_special,
    validate_nuts,
    validate_spatial_extent,
)

from plone import api
from plone.memoize.ram import cache
from plone.memoize.view import memoize
from plone.protect.interfaces import IDisableCSRFProtection
from plone.restapi.deserializer import json_body
from plone.restapi.services import Service
from zope.component import getUtility
from zope.interface import alsoProvides


log = getLogger(__name__)


def _cache_key(fun, self, nutsid):
    """Cache key function"""
    return nutsid


class DataRequestPost(Service):
    """Set Data"""

    def rsp(self, msg, code=400, status="error"):
        """Prepare an (usually error) response"""
        if code != 0:
            self.request.response.setStatus(code)
        return {
            "status": status,
            "msg": MESSAGES.get(msg, msg)
        }

    @cache(_cache_key)
    def get_nuts_name(self, nutsid):
        """Based on the NUTS ID, return the name of
        the NUTS region.
        """
        return get_nuts_by_id(nutsid)

    def post_request_to_fme(self, params, is_prepackaged=False):
        """send the request to FME and let it process it"""
        return post_request_to_fme(params, is_prepackaged)

    @memoize
    def max_area_extent(self):
        """return the max area allowed to be downloaded"""
        return api.portal.get_registry_record(
            "clms.types.download_limits.area_extent", default=1600000000000
        )

    def get_user_data_or_error(self):
        """Return (user_id, email) if logged in, else error response"""
        user = api.user.get_current()
        if not user:
            return None, None, self.rsp("NOT_LOGGED_IN", code=0)

        user_id = user.getId()
        mail = user.getProperty("email")
        return user_id, mail, None

    def validate_dataset_id(self, dataset_json):
        """Return error response if DatasetID is missing/empty, else None"""
        if "DatasetID" not in dataset_json:
            return self.rsp("UNDEFINED_DATASET_ID")
        if not dataset_json.get("DatasetID"):
            return self.rsp("UNDEFINED_DATASET_ID")
        return None

    def validate_dataset_object(self, dataset_json):
        """Return error resp if DatasetID is invalid, else dataset object"""
        dataset_object = get_dataset_by_uid(dataset_json.get("DatasetID"))
        if dataset_object is None:
            return None, self.rsp("INVALID_DATASET_ID")
        return dataset_object, None

    def process_cdse_dataset(self, dataset_json, dataset_obj, response_json):
        """
        Checks if dataset is CDSE and updates response_json accordingly.
        Returns True if dataset is CDSE, False otherwise.
        """
        is_cdse_dataset = False
        try:
            info_id = dataset_json.get('DatasetDownloadInformationID')

            info_item = next(
                (it for it in dataset_obj.dataset_download_information.get(
                    "items", []) if it.get("@id") == info_id),
                None
            )

            if info_item and info_item.get('full_source') == "CDSE":
                is_cdse_dataset = True
                response_json.update({
                    "ByocCollection": info_item.get('byoc_collection'),
                    "SpatialResolution": getattr(
                        dataset_obj, 'qualitySpatialResolution_line', None)
                })

        except Exception:
            log.exception("Error processing CDSE dataset")

        log.info("is_cdse_dataset: %s", is_cdse_dataset)
        return is_cdse_dataset

    def process_file_id(self, dataset_json, dataset_object, response_json,
                        prepacked_download_data_object):
        """
        Processes FileID requests:
        updates response_json and prepacked_download_data_object.
        Returns an error response if FileID is invalid, otherwise None.
        """
        file_id = dataset_json.get("FileID")
        if not file_id:
            return None

        file_path = get_dataset_file_path_from_file_id(dataset_object, file_id)
        file_source = get_dataset_file_source_from_file_id(
            dataset_object, file_id)

        if file_path and file_source:
            response_json.update({
                "FileID": file_id,
                "DatasetPath": "",
                "FilePath": base64_encode_path(file_path),
                "DatasetSource": file_source
            })
            prepacked_download_data_object["Datasets"].append(response_json)
            return None
        else:
            return self.rsp("INVALID_FILE_ID")

    def process_nuts(self, dataset_json, response_json):
        """
        Validates the NUTS code in dataset_json and updates response_json.
        Returns an error response if invalid, else None.
        """
        nuts_code = dataset_json.get("NUTS")
        if not nuts_code:
            return None

        if not validate_nuts(nuts_code):
            return self.rsp("NUTS_COUNTRY_ERROR")

        response_json.update({
            "NUTSID": nuts_code,
            "NUTSName": self.get_nuts_name(nuts_code)
        })
        return None

    def process_bounding_box(self, dataset_json, response_json):
        """
        Validates BoundingBox in dataset_json and updates response_json.
        Returns an error response if invalid, else None.
        """
        bbox = dataset_json.get("BoundingBox")
        if not bbox:
            return None

        if "NUTS" in dataset_json:
            return self.rsp("NUTS_ALSO_DEFINED")

        if not validate_spatial_extent(bbox):
            return self.rsp("INVALID_BOUNDINGBOX")

        requested_area = calculate_bounding_box_area(bbox)
        if requested_area > self.max_area_extent():
            return self.rsp(
                f"Error, the requested BoundingBox is too big. "
                f"The limit is {self.max_area_extent()}."
            )

        response_json.update({"BoundingBox": bbox})
        return None

    def process_temporal_filter(
            self, dataset_json, dataset_object, response_json):
        """
        Validates the TemporalFilter in dataset_json and updates response_json.
        Returns an error response if invalid, else None.

        Now, the temporal restriction can be controlled only with
        the maximum range, I mean if it is set as timeseries or has
        the aux calendar, in both cases it will have the maximum
        range filled. If the dataset does not have values in that
        setting, you do not need time parameters
        """
        temporal = dataset_json.get("TemporalFilter")
        if not temporal:
            return None

        d_l_t = getattr(dataset_object, "download_limit_temporal_extent", None)
        if not d_l_t or d_l_t <= 0:
            return self.rsp("TEMP_REST_NOT_ALLOWED")

        if len(temporal.keys()) > 2:
            return self.rsp("TEMP_TOO_MANY")

        if "StartDate" not in temporal or "EndDate" not in temporal:
            return self.rsp("TEMP_MISSING_RANGE")

        start_date, end_date = extract_dates_from_temporal_filter(temporal)
        if start_date is None or end_date is None:
            return self.rsp("INCORRECT_DATE")

        if start_date > end_date:
            return self.rsp("INCORRECT_DATE_RANGE")

        response_json.update({
            "TemporalFilter": {
                "StartDate": start_date,
                "EndDate": end_date
            }
        })
        return None

    def reply(self):  # pylint: disable=too-many-statements
        """JSON response"""
        alsoProvides(self.request, IDisableCSRFProtection)

        # Validate user
        user_id, mail, error = self.get_user_data_or_error()
        if error:
            return error

        # Get json request data
        datasets_json = json_body(self.request).get("Datasets")
        general_download_data_object = {}
        general_download_data_object["Datasets"] = []
        prepacked_download_data_object = {}
        prepacked_download_data_object["Datasets"] = []
        cdse_datasets = {}
        cdse_datasets["Datasets"] = []
        found_special = []

        utility = getUtility(IDownloadToolUtility)

        # Iterate through requested datasets
        for dataset_index, dataset_json in enumerate(datasets_json):
            response_json = {}

            # Validate dataset
            error = self.validate_dataset_id(dataset_json)
            if error:
                return error
            dataset_object, error = self.validate_dataset_object(dataset_json)
            if error:
                return error
            assert dataset_object is not None
            response_json.update(
                {
                    "DatasetID": dataset_json["DatasetID"],
                    "DatasetTitle": dataset_object.Title(),
                }
            )

            # CDSE check
            is_cdse_dataset = self.process_cdse_dataset(
                dataset_json, dataset_object, response_json)

            # Request by FileID
            if "FileID" in dataset_json:
                error = self.process_file_id(
                    dataset_json, dataset_object, response_json,
                    prepacked_download_data_object)
                if error:
                    return error

            else:
                # Request by NUTS
                if "NUTS" in dataset_json:
                    error = self.process_nuts(dataset_json, response_json)
                    if error:
                        return error

                # Request by BoundingBox
                if "BoundingBox" in dataset_json:
                    error = self.process_bounding_box(
                        dataset_json, response_json)
                    if error:
                        return error

                # Request by TemporalFilter
                if "TemporalFilter" in dataset_json:
                    error = self.process_temporal_filter(
                        dataset_json, dataset_object, response_json)
                    if error:
                        return None, error

                if "OutputGCS" in dataset_json:
                    available_gcs_values = get_available_gcs_values(
                        dataset_json["DatasetID"]
                    )

                    if dataset_json["OutputGCS"] not in available_gcs_values:
                        return self.rsp("UNDEFINED_GCS")

                    response_json.update(
                        {"OutputGCS": dataset_json["OutputGCS"]}
                    )
                else:
                    return self.rsp("MISSING_GCS")

                if "DatasetDownloadInformationID" not in dataset_json:
                    return self.rsp("UNDEFINED_INFO_ID")

                download_information_id = dataset_json.get(
                    "DatasetDownloadInformationID"
                )
                # Check if the dataset format value is correct

                full_dataset_format = get_full_dataset_format(
                    dataset_object, download_information_id
                )
                if not full_dataset_format:
                    return self.rsp("NOT_DOWNLOADABLE")

                requested_output_format = dataset_json.get(
                    "OutputFormat", None
                )
                if requested_output_format not in FORMAT_CONVERSION_TABLE:
                    return self.rsp("INVALID_OUTPUT")

                available_transformations_for_format = (
                    FORMAT_CONVERSION_TABLE.get(full_dataset_format)
                )

                if not available_transformations_for_format.get(
                    requested_output_format, None
                ):
                    return self.rsp("NOT_COMPATIBLE")

                # Check if the dataset source is OK
                full_dataset_source = get_full_dataset_source(
                    dataset_object, download_information_id
                )

                if not full_dataset_source:
                    return self.rsp("INVALID_SOURCE")

                # Check if the dataset path is OK
                full_dataset_path = get_full_dataset_path(
                    dataset_object, download_information_id
                )
                if not full_dataset_path:
                    return self.rsp("NOT_DOWNLOADABLE")

                # Check if we have wekeo_choices
                wekeo_choices = get_full_dataset_wekeo_choices(
                    dataset_object, download_information_id
                )
                # Check if layer is mandatory
                layers = get_full_dataset_layers(
                    dataset_object, download_information_id
                )
                if layers and "Layer" not in dataset_json:
                    # Check if user has not sent the Layer and
                    # is mandatory, because this dataset
                    # has layers
                    dataset_json['Layer'] = "ALL BANDS"

                elif layers and "Layer" in dataset_json:
                    # Check if we have a layer and it is valid
                    layers = get_full_dataset_layers(
                        dataset_object, download_information_id
                    )
                    if dataset_json.get("Layer") in layers:
                        response_json["Layer"] = dataset_json["Layer"]
                    else:
                        return self.rsp("INVALID_LAYER")

                # check time series restrictions:
                # if the dataset is a time_series enabled dataset
                # the temporal filter option is mandatory
                if (
                    dataset_object.mapviewer_istimeseries and
                    "TemporalFilter" not in dataset_json
                ):
                    return self.rsp("MISSING_TEMPORAL")

                # validate the date range by
                # “Maximum number of days allowed to be downloaded”
                # (instead of "Is Time Series" setting)
                #
                # if  “Maximum number of days allowed to be downloaded”
                # contains a value, we must check that the dates provided are
                # within the range
                # the requested range should not be bigger than
                # the limit set in the configuration

                d_l_t = dataset_object.download_limit_temporal_extent
                if d_l_t is not None and d_l_t > 0:
                    try:
                        end_date_datetime = datetime.strptime(
                            end_date, ISO8601_DATETIME_FORMAT
                        )
                        start_date_datetime = datetime.strptime(
                            start_date, ISO8601_DATETIME_FORMAT
                        )
                    except UnboundLocalError:
                        return self.rsp("TEMP_MISSING_RANGE")

                    if (end_date_datetime - start_date_datetime) > timedelta(
                        days=dataset_object.download_limit_temporal_extent
                    ):
                        return self.rsp(
                            "You are requesting to download a time series "
                            "enabled dataset and the requested date range "
                            "is bigger than the allowed range of "
                            f"{dataset_object.download_limit_temporal_extent}"
                            " days. Please check the download "
                            "documentation to get more information"
                        )

                is_special_case = is_special(dataset_json, dataset_object)
                if is_special_case:
                    found_special.append(dataset_json['DatasetID'])

                if dataset_index == len(datasets_json) - 1:
                    if len(found_special) > 0:
                        return self.rsp(
                            f"Please choose the Geotiff format as the NetCDF "
                            f"format is not allowed "
                            f"for the dataset(s) {', '.join(found_special)}"
                        )
                elif is_special_case:
                    continue

                # Check full dataset download restrictions
                if (
                    "NUTS" not in dataset_json and "BoundingBox" not in
                    dataset_json and "TemporalFilter" not in dataset_json
                ):
                    if full_dataset_source != "EEA":
                        return self.rsp("FULL_NOT_EEA")

                    if full_dataset_source == "EEA":
                        return self.rsp("FULL_EEA")

                # Check dataset download restrictions for
                # non-EEA datasets with no area specified
                if (
                    "NUTS" not in dataset_json and
                    "BoundingBox" not in dataset_json
                ):
                    if full_dataset_source != "EEA":
                        return self.rsp("MUST_HAVE_AREA")

                response_json.update(
                    {
                        "DatasetFormat": full_dataset_format,
                        "OutputFormat": dataset_json.get("OutputFormat", ""),
                        "DatasetPath": base64_encode_path(full_dataset_path),
                        "DatasetSource": full_dataset_source,
                        "WekeoChoices": wekeo_choices,
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

                if is_cdse_dataset:
                    cdse_datasets["Datasets"].append(response_json)

                else:
                    general_download_data_object["Datasets"].append(
                        response_json)

                # general_download_data_object["Datasets"].append(
                #     response_json)

        # Check for a maximum of 5 items general download items
        if len(general_download_data_object.get("Datasets", [])) > 5:
            return self.rsp("DOWNLOAD_LIMIT")

        inprogress_requests = utility.datarequest_search(
            user_id, "In_progress"
        ).values()

        queued_requests = utility.datarequest_search(
            user_id, "Queued"
        ).values()

        inprogress_datasets = reduce(
            lambda x, y: x + y,
            [item.get("Datasets", []) for item in inprogress_requests],
            [],
        )
        queued_datasets = reduce(
            lambda x, y: x + y,
            [item.get("Datasets", []) for item in queued_requests],
            [],
        )

        # Check that the request has no duplicates
        if duplicated_values_exist(
            general_download_data_object.get(
                "Datasets", []) + inprogress_datasets + queued_datasets
        ):
            return self.rsp("DUPLICATED")

        fme_results = {
            "ok": [],
            "error": [],
        }

        cdse_parent_task = {}  # contains all requested CDSE datasets, it is
        # a future FME task if all child tasks are finished in CDSE
        cdse_task_group_id = generate_task_group_id()
        cdse_batch_ids = []
        gpkg_filenames = []

        for cdse_dataset in cdse_datasets["Datasets"]:
            cdse_data_object = {}
            # cdse_data_object["Status"] = "CREATED"? #WIP get status
            cdse_data_object["UserID"] = user_id
            cdse_data_object[
                "RegistrationDateTime"
            ] = datetime.utcnow().isoformat()

            # generate unique geopackage file name
            unique_geopackage_id = str(uuid.uuid4())
            unique_geopackage_name = f"{unique_geopackage_id}.gpkg"
            print("unique_geopackage_name, ", unique_geopackage_name)
            cdse_data_object["GpkgFileName"] = unique_geopackage_name
            gpkg_filenames.append(unique_geopackage_name)

            # get batch_id
            try:
                cdse_dataset["TemporalFilter"]["StartDate"] = to_iso8601(
                    cdse_dataset["TemporalFilter"]["StartDate"])
                cdse_dataset["TemporalFilter"]["EndDate"] = to_iso8601(
                    cdse_dataset["TemporalFilter"]["EndDate"])
            except Exception:
                pass
            # create_batch("test_file.gpkg", cdse_dataset)
            cdse_batch_id_response = create_batch(
                unique_geopackage_name, cdse_dataset)
            cdse_batch_id = cdse_batch_id_response.get('batch_id')
            if cdse_batch_id is None:
                error = cdse_batch_id_response.get('error', '')

                return self.rsp(f"Error creating CDSE batch: {error}")

            cdse_batch_ids.append(cdse_batch_id)
            cdse_data_object["CDSEBatchID"] = cdse_batch_id
            cdse_data_object["Status"] = "QUEUED"

            # Save child task in downloadtool
            # CDSE tasks are split in child tasks, one for each dataset
            cdse_data_object['Datasets'] = cdse_dataset
            cdse_data_object['cdse_task_role'] = "child"
            cdse_data_object['cdse_task_group_id'] = cdse_task_group_id
            utility_response_json = utility.datarequest_post(cdse_data_object)
            utility_task_id = get_task_id(utility_response_json)

            # make sure parent task is independent of the child
            cdse_parent_task = copy.deepcopy(cdse_data_object)  # placeholder
            cdse_parent_task.pop('GpkgFileName', None)
            cdse_parent_task.pop('CDSEBatchID', None)

            # start batch
            start_batch(cdse_batch_id)

        if len(cdse_datasets["Datasets"]) > 0:
            # Save parent task in downloadtool, containing all CDSE datasets
            cdse_parent_task["cdse_task_role"] = "parent"
            cdse_parent_task["Status"] = "Queued"
            cdse_parent_task["Datasets"] = cdse_datasets["Datasets"]
            cdse_parent_task["CDSEBatchIDs"] = cdse_batch_ids
            cdse_parent_task["GpkgFileNames"] = gpkg_filenames
            utility_response_json = utility.datarequest_post(cdse_parent_task)
            utility_task_id = get_task_id(utility_response_json)

        for data_object, is_prepackaged in [
            (prepacked_download_data_object, True),
            (general_download_data_object, False),
        ]:
            if data_object["Datasets"]:
                data_object["Status"] = "Queued"
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
                            "value": get_callback_url(),
                        },
                        # dump the json into a string for FME
                        {"name": "json", "value": json.dumps(new_datasets)},
                    ]
                }

                # build the stat params and save them
                stats_params = {
                    "Start": datetime.utcnow().isoformat(),
                    "User": str(user_id),
                    "Dataset": [item["DatasetID"] for item in data_object.get(
                        "Datasets", [])],
                    "TransformationData": new_datasets,
                    "TaskID": utility_task_id,
                    "End": "",
                    "TransformationDuration": "",
                    "TransformationSize": "",
                    "TransformationResultData": "",
                    "Status": "Queued",
                }
                save_stats(stats_params)
                fme_result = self.post_request_to_fme(params, is_prepackaged)
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
            return self.rsp("ALL_FAILED", code=500)

        self.request.response.setStatus(201)
        return {
            "TaskIds": fme_results["ok"],
            "ErrorTaskIds": fme_results["error"],
        }
