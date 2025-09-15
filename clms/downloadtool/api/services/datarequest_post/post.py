# -*- coding: utf-8 -*-
"""
For HTTP GET operations we can use standard HTTP parameter passing
through the URL)
"""
import copy
import uuid
from datetime import datetime, timezone, timedelta
from functools import reduce
from logging import getLogger

from clms.downloadtool.api.services.utils import (
    duplicated_values_exist,
    calculate_bounding_box_area,
    get_available_gcs_values,
)
from clms.downloadtool.utility import IDownloadToolUtility
from clms.downloadtool.api.services.cdse.cdse_integration import (
    create_batch, start_batch)
from clms.downloadtool.api.services.datarequest_post.utils import (
    ISO8601_DATETIME_FORMAT,
    base64_encode_path,
    build_stats_params,
    build_metadata_urls,
    extract_dates_from_temporal_filter,
    generate_task_group_id,
    get_dataset_by_uid,
    get_dataset_file_path_from_file_id,
    get_dataset_file_source_from_file_id,
    get_full_dataset_layers,
    get_full_dataset_path,
    get_full_dataset_source,
    get_full_dataset_wekeo_choices,
    get_nuts_by_id,
    get_task_id,
    params_for_fme,
    post_request_to_fme,
    save_stats,
    to_iso8601,
)
from clms.downloadtool.api.services.datarequest_post.validation import (
    MESSAGES,
    is_special,
    validate_nuts,
    validate_spatial_extent,
    validate_full_download_restrictions,
    validate_dataset_format_and_output,
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

    def process_cdse_batches(self, cdse_datasets, user_id, utility):
        """Handle CDSE: create child + parent tasks and start batches."""
        cdse_parent_task = {}
        cdse_task_group_id = generate_task_group_id()
        cdse_batch_ids, gpkg_filenames = [], []

        for cdse_dataset in cdse_datasets["Datasets"]:
            cdse_data_object = {
                "UserID": user_id,
                "RegistrationDateTime": datetime.now(timezone.utc).isoformat()
            }

            unique_geopackage_id = str(uuid.uuid4())
            unique_geopackage_name = f"{unique_geopackage_id}.gpkg"
            log.debug("unique_geopackage_name: %s", unique_geopackage_name)
            cdse_data_object["GpkgFileName"] = unique_geopackage_name
            gpkg_filenames.append(unique_geopackage_name)

            try:
                cdse_dataset["TemporalFilter"]["StartDate"] = to_iso8601(
                    cdse_dataset["TemporalFilter"]["StartDate"])
                cdse_dataset["TemporalFilter"]["EndDate"] = to_iso8601(
                    cdse_dataset["TemporalFilter"]["EndDate"])
            except Exception:
                pass

            cdse_batch_id_response = create_batch(
                unique_geopackage_name, cdse_dataset)
            cdse_batch_id = cdse_batch_id_response.get('batch_id')
            if cdse_batch_id is None:
                error = cdse_batch_id_response.get('error', '')
                return None, self.rsp(f"Error creating CDSE batch: {error}")

            cdse_batch_ids.append(cdse_batch_id)
            cdse_data_object["CDSEBatchID"] = cdse_batch_id
            cdse_data_object["Status"] = "QUEUED"
            cdse_data_object['Datasets'] = cdse_dataset
            cdse_data_object['cdse_task_role'] = "child"
            cdse_data_object['cdse_task_group_id'] = cdse_task_group_id

            utility_response_json = utility.datarequest_post(cdse_data_object)
            utility_task_id = get_task_id(utility_response_json)
            log.info("utility_task_id: %s", utility_task_id)

            cdse_parent_task = copy.deepcopy(cdse_data_object)
            cdse_parent_task.pop('GpkgFileName', None)
            cdse_parent_task.pop('CDSEBatchID', None)

            start_batch(cdse_batch_id)

        if cdse_datasets["Datasets"]:
            cdse_parent_task.update({
                "cdse_task_role": "parent",
                "Status": "Queued",
                "Datasets": cdse_datasets["Datasets"],
                "CDSEBatchIDs": cdse_batch_ids,
                "GpkgFileNames": gpkg_filenames,
            })
            utility_response_json = utility.datarequest_post(cdse_parent_task)
            utility_task_id = get_task_id(utility_response_json)
            log.info("utility_task_id: %s", utility_task_id)

        return cdse_parent_task, None

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
        Returns start date, end date, possible error.

        Now, the temporal restriction can be controlled only with
        the maximum range, I mean if it is set as timeseries or has
        the aux calendar, in both cases it will have the maximum
        range filled. If the dataset does not have values in that
        setting, you do not need time parameters
        """
        temporal = dataset_json.get("TemporalFilter")
        if not temporal:
            return None, None, None

        d_l_t = getattr(dataset_object, "download_limit_temporal_extent", None)
        if not d_l_t or d_l_t <= 0:
            return None, None, self.rsp("TEMP_REST_NOT_ALLOWED")

        if len(temporal.keys()) > 2:
            return None, None, self.rsp("TEMP_TOO_MANY")

        if "StartDate" not in temporal or "EndDate" not in temporal:
            return None, None, self.rsp("TEMP_MISSING_RANGE")

        start_date, end_date = extract_dates_from_temporal_filter(temporal)
        if start_date is None or end_date is None:
            return None, None, self.rsp("INCORRECT_DATE")

        if start_date > end_date:
            return None, None, self.rsp("INCORRECT_DATE_RANGE")

        response_json.update({
            "TemporalFilter": {
                "StartDate": start_date,
                "EndDate": end_date
            }
        })
        return start_date, end_date, None

    def process_out_gcs(self, dataset_json, response_json):
        """
        Validate OutputGCS if present in dataset_json and update response_json.
        Return an error response if invalid, else None.
        """
        output_gcs = dataset_json.get("OutputGCS")
        if not output_gcs:
            return None

        available_gcs_values = get_available_gcs_values(
            dataset_json["DatasetID"])
        if output_gcs not in available_gcs_values:
            return self.rsp("UNDEFINED_GCS")

        response_json.update({"OutputGCS": output_gcs})
        return None

    def process_download_request(
        self, data_object, is_prepackaged, user_id, mail, utility, fme_results
    ):
        """Handles posting dataset requests to FME and updating task status."""
        data_object["Status"] = "Queued"
        data_object["UserID"] = user_id
        data_object["RegistrationDateTime"] = datetime.now(
            timezone.utc).isoformat()

        utility_response_json = utility.datarequest_post(data_object)
        utility_task_id = get_task_id(utility_response_json)
        new_datasets = {"Datasets": data_object["Datasets"]}

        save_stats(build_stats_params(
            user_id, data_object, new_datasets, utility_task_id
        ))

        fme_result = self.post_request_to_fme(
            params_for_fme(user_id, utility_task_id, mail, new_datasets),
            is_prepackaged,
        )

        if fme_result:
            data_object["FMETaskId"] = fme_result
            utility.datarequest_status_patch(data_object, utility_task_id)
            self.request.response.setStatus(201)
            fme_results["ok"].append({"TaskID": utility_task_id})
        else:
            fme_results["error"].append({"TaskID": utility_task_id})

    def validate_date_range(self, dataset_object, start_date, end_date):
        """
        Validate that the requested [start_date, end_date] interval
        does not exceed the dataset's download_limit_temporal_extent.
        Returns None if valid, otherwise an error response.
        """
        d_l_t = dataset_object.download_limit_temporal_extent
        if d_l_t is not None and d_l_t > 0:
            if start_date is None or end_date is None:
                return self.rsp("TEMP_MISSING_RANGE")

            end_date_datetime = datetime.strptime(
                end_date, ISO8601_DATETIME_FORMAT
            )
            start_date_datetime = datetime.strptime(
                start_date, ISO8601_DATETIME_FORMAT
            )

            if (end_date_datetime - start_date_datetime) > timedelta(
                    days=d_l_t):
                return self.rsp(
                    "You are requesting to download a time series enabled "
                    "dataset and the requested date range is bigger than the "
                    f"allowed range of {d_l_t} days. Please check the download"
                    " documentation to get more information"
                )
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
        general_download_data_object = {"Datasets": []}
        prepacked_download_data_object = {"Datasets": []}
        cdse_datasets = {"Datasets": []}
        found_special = []

        utility = getUtility(IDownloadToolUtility)

        # Iterate through requested datasets
        for dataset_index, dataset_json in enumerate(datasets_json):
            response_json = {}
            # Pre-init temporal bounds to avoid UnboundLocalError
            start_date = end_date = None

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
                # Check NUTS
                if "NUTS" in dataset_json:
                    error = self.process_nuts(dataset_json, response_json)
                    if error:
                        return error

                # Check BoundingBox
                if "BoundingBox" in dataset_json:
                    error = self.process_bounding_box(
                        dataset_json, response_json)
                    if error:
                        return error

                # Check TemporalFilter
                if "TemporalFilter" in dataset_json:
                    start_date, end_date, error = self.process_temporal_filter(
                        dataset_json, dataset_object, response_json)
                    if error:
                        return error

                # Check output GCS
                if "OutputGCS" in dataset_json:
                    error = self.process_out_gcs(dataset_json, response_json)
                    if error:
                        return error
                else:
                    return self.rsp("MISSING_GCS")

                if "DatasetDownloadInformationID" not in dataset_json:
                    return self.rsp("UNDEFINED_INFO_ID")

                download_information_id = dataset_json.get(
                    "DatasetDownloadInformationID"
                )

                full_dataset_format, requested_output_format, error = (
                    validate_dataset_format_and_output(
                        dataset_object, dataset_json,
                        download_information_id, self.rsp
                    )
                )
                log.info("requested output: %s", requested_output_format)
                if error:
                    return error

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
                if layers:
                    if "Layer" not in dataset_json:
                        # mandatory layers exist but not provided -> default
                        dataset_json["Layer"] = "ALL BANDS"
                    else:
                        # Validate layer
                        layer = dataset_json.get("Layer")
                        if layer in layers:
                            response_json["Layer"] = layer
                        else:
                            return self.rsp("INVALID_LAYER")

                # Check time series restrictions
                if (
                    dataset_object.mapviewer_istimeseries and
                    "TemporalFilter" not in dataset_json
                ):
                    return self.rsp("MISSING_TEMPORAL")

                error = self.validate_date_range(
                    dataset_object, start_date, end_date)
                if error:
                    return error

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

                error = validate_full_download_restrictions(
                    dataset_json, full_dataset_source, self.rsp
                )
                if error:
                    return error

                response_json.update(
                    {
                        "DatasetFormat": full_dataset_format,
                        "OutputFormat": dataset_json.get("OutputFormat", ""),
                        "DatasetPath": base64_encode_path(full_dataset_path),
                        "DatasetSource": full_dataset_source,
                        "WekeoChoices": wekeo_choices,
                    }
                )
                response_json["Metadata"] = build_metadata_urls(dataset_object)

                if is_cdse_dataset:
                    cdse_datasets["Datasets"].append(response_json)
                else:
                    general_download_data_object["Datasets"].append(
                        response_json)

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

        fme_results = {"ok": [], "error": []}

        cdse_parent_task, error = self.process_cdse_batches(
            cdse_datasets, user_id, utility)
        if error:
            return error
        log.info("CDSE parent task: %s", cdse_parent_task)

        for data_object, is_prepackaged in [
            (prepacked_download_data_object, True),
            (general_download_data_object, False),
        ]:
            if data_object["Datasets"]:
                self.process_download_request(
                    data_object, is_prepackaged, user_id, mail, utility,
                    fme_results
                )

        if fme_results["error"] and not fme_results["ok"]:
            return self.rsp("ALL_FAILED", code=500)

        self.request.response.setStatus(201)
        return {
            "TaskIds": fme_results["ok"],
            "ErrorTaskIds": fme_results["error"],
        }
