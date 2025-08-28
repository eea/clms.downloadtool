# -*- coding: utf-8 -*-
"""
For HTTP GET operations we can use standard HTTP parameter passing
through the URL)

"""
import copy
import base64
import json
import re
import uuid
import random
from datetime import datetime
from datetime import timedelta
from functools import reduce
from logging import getLogger

import requests
from clms.downloadtool.api.services.utils import (
    get_extra_data,
    duplicated_values_exist,
)
from clms.downloadtool.utility import IDownloadToolUtility
from clms.downloadtool.utils import COUNTRIES, FORMAT_CONVERSION_TABLE
from clms.downloadtool.api.services.utils import (
    calculate_bounding_box_area,
    get_available_gcs_values,
)
from clms.downloadtool.api.services.cdse.cdse_integration import (
    create_batch, start_batch)

from clms.statstool.utility import IDownloadStatsUtility
from plone import api
from plone.memoize.ram import cache
from plone.memoize.view import memoize
from plone.protect.interfaces import IDisableCSRFProtection
from plone.restapi.deserializer import json_body
from plone.restapi.services import Service
from zope.component import getUtility
from zope.interface import alsoProvides


ISO8601_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


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


def _cache_key(fun, self, nutsid):
    """Cache key function"""
    return nutsid


log = getLogger(__name__)


EEA_GEONETWORK_BASE_URL = (
    "https://sdi.eea.europa.eu/catalogue/copernicus/"
    "api/records/{uid}/formatters/xml?approved=true"
)
VITO_GEONETWORK_BASE_URL = (
    "https://globalland.vito.be/geonetwork/"
    "srv/api/records/{uid}/formatters/xml?approved=true"
)


def base64_encode_path(path):
    """encode the given path as base64"""
    if isinstance(path, str):
        return base64.urlsafe_b64encode(path.encode("utf-8")).decode("utf-8")

    return base64.urlsafe_b64encode(path).decode("utf-8")


class DataRequestPost(Service):
    """Set Data"""

    @memoize
    def max_area_extent(self):
        """return the max area allowed to be downloaded"""
        return api.portal.get_registry_record(
            "clms.types.download_limits.area_extent", default=1600000000000
        )

    def get_dataset_by_uid(self, uid):
        """get the dataset by UID"""
        brains = api.content.find(UID=uid)
        if brains:
            return brains[0].getObject()

        return None

    def get_callback_url(self):
        """get the callback url where FME should signal any status changes"""
        portal_url = api.portal.get().absolute_url()
        if portal_url.endswith("/api"):
            portal_url = portal_url.replace("/api", "")

        return "{}/++api++/{}".format(
            portal_url,
            "@datarequest_status_patch",
        )

    def reply(self):  # pylint: disable=too-many-statements
        """JSON response"""
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

        cdse_datasets = {}
        cdse_datasets["Datasets"] = []

        valid_dataset = False

        utility = getUtility(IDownloadToolUtility)

        # Refs #273099
        # when NETCDF format (OutputFormat) is selected for these 2:
        # - Water Bodies 2020-present (raster 100 m), global, monthly
        #   – version 1
        # - Water Bodies 2020-present (raster 300 m), global, monthly
        #   – version 2
        # display this error
        # [UID1, path1, ...]
        SPECIAL_CASES = [
            '7df9bdf94fe94cb5919c11c9ef5cac65',
            '/water-bodies/water-bodies-global-v1-0-100m',
            '0517fd1b7d944d8197a2eb5c13470db8',
            '/water-bodies/water-bodies-global-v2-0-300m'
        ]
        found_special = []

        for dataset_index, dataset_json in enumerate(datasets_json):
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

            # CDSE case
            is_cdse_dataset = False
            try:
                info_id = dataset_json.get(
                    'DatasetDownloadInformationID', None)

                info_item = next(
                    (it for it in dataset_object.dataset_download_information[
                        "items"] if it.get("@id") == info_id),
                    None
                )

                full_source = info_item['full_source']
                if full_source == "CDSE":
                    is_cdse_dataset = True
                    # WIP: check if ByocCollection in dataset
                    response_json.update(
                        {
                            "ByocCollection": dataset_object.byoc_collection_id
                        }
                    )
                    # WIP: check if mapviewer_viewservice in dataset
                    response_json.update({
                        "ViewService": dataset_object.mapviewer_viewservice
                    })
            except Exception:
                pass
            log.info("is_cdse_dataset: %s", is_cdse_dataset)

            # id cdse it should have byoc field

            # Handle FileID requests:
            # - get first the file_path from the dataset using the file_id
            # - if something is returned use it as FileID and FilePath
            # - if not return an error stating that the requested FileID is
            #   not valid
            if "FileID" in dataset_json:
                file_path = get_dataset_file_path_from_file_id(
                    dataset_object, dataset_json["FileID"]
                )
                file_source = get_dataset_file_source_from_file_id(
                    dataset_object, dataset_json["FileID"]
                )
                if file_path and file_source:
                    response_json.update({"FileID": dataset_json["FileID"]})
                    response_json.update({"DatasetPath": ""})
                    response_json.update(
                        {"FilePath": base64_encode_path(file_path)}
                    )
                    response_json.update({"DatasetSource": file_source})
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

                    requested_area = calculate_bounding_box_area(
                        dataset_json["BoundingBox"]
                    )
                    if requested_area > self.max_area_extent():
                        self.request.response.setStatus(400)
                        return {
                            "status": "error",
                            "msg": "Error, the requested BoundingBox is too "
                            "big. The limit is "
                            f"{self.max_area_extent()}.",
                        }
                    response_json.update(
                        {"BoundingBox": dataset_json["BoundingBox"]}
                    )

                # Now, the temporal restriction can be controlled only with
                # the maximum range, I mean if it is set as timeseries or has
                # the aux calendar, in both cases it will have the maximum
                # range filled. If the dataset does not have values in that
                # setting, you do not need time parameters
                if "TemporalFilter" in dataset_json:
                    d_l_t = dataset_object.download_limit_temporal_extent
                    has_maximum_range = False
                    if d_l_t is not None and d_l_t > 0:
                        has_maximum_range = True
                    if has_maximum_range is False:
                        self.request.response.setStatus(400)
                        return {
                            "status": "error",
                            "msg": "Error, temporal restriction is not "
                                   "allowed in not time-series enabled "
                                   "datasets",
                        }

                    if len(dataset_json["TemporalFilter"].keys()) > 2:
                        self.request.response.setStatus(400)
                        return {
                            "status": "error",
                            "msg": "Error, TemporalFilter has too many "
                                   "fields",
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
                    available_gcs_values = get_available_gcs_values(
                        dataset_json["DatasetID"]
                    )

                    if dataset_json["OutputGCS"] not in available_gcs_values:
                        self.request.response.setStatus(400)
                        return {
                            "status": "error",
                            "msg": "Error, defined GCS not in the list",
                        }
                    response_json.update(
                        {"OutputGCS": dataset_json["OutputGCS"]}
                    )

                else:
                    self.request.response.setStatus(400)
                    return {
                        "status": "error",
                        "msg": "The OutputGCS parameter is mandatory.",
                    }

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
                if not full_dataset_format:
                    self.request.response.setStatus(400)
                    return {
                        "status": "error",
                        "msg": "Error, this dataset is not downloadable",
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
                        "msg": "Error, this dataset is not downloadable",
                    }

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
                        self.request.response.setStatus(400)
                        return {
                            "status": "error",
                            "msg": (
                                "Error, the requested band/layer is not valid"
                            ),
                        }

                # check time series restrictions:
                # if the dataset is a time_series enabled dataset
                # the temporal filter option is mandatory
                # pylint: disable=line-too-long
                if (dataset_object.mapviewer_istimeseries and "TemporalFilter" not in dataset_json):  # noqa
                    self.request.response.setStatus(400)
                    return {
                        "status": "error",
                        "msg": (
                            "You are requesting to download a time series "
                            "enabled dataset and you are required to "
                            "request the download of an specific date "
                            "range. Please check the download "
                            "documentation to get more information"
                        ),
                    }

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
                        self.request.response.setStatus(400)
                        return {
                            "status": "error",
                            "msg": (
                                "Please add "
                                "TemporalFilter (with StartDate and EndDate)"
                            ),
                        }

                    if (end_date_datetime - start_date_datetime) > timedelta(
                        days=dataset_object.download_limit_temporal_extent
                    ):
                        self.request.response.setStatus(400)
                        return {
                            "status": "error",
                            "msg": (
                                "You are requesting to download a time series "
                                "enabled dataset and the requested date range "
                                "is bigger than the allowed range of "
                                f"{dataset_object.download_limit_temporal_extent} days. "  # noqa
                                "Please check the download "
                                "documentation to get more information"
                            ),
                        }

                is_special_case = False
                try:
                    dataset_id = dataset_json['DatasetID']
                    if dataset_id in SPECIAL_CASES or \
                        dataset_object.absolute_url().split(
                            '/en/products')[-1] in SPECIAL_CASES:
                        if dataset_json['OutputFormat'] == 'Netcdf':
                            is_special_case = True
                            found_special.append(dataset_id)
                except Exception:
                    pass
                if dataset_index == len(datasets_json) - 1:
                    if len(found_special) > 0:
                        self.request.response.setStatus(400)
                        special_msg = (
                            f"Please choose the Geotiff format as the NetCDF "
                            f"format is not allowed "
                            f"for the dataset(s) {', '.join(found_special)}"
                        )
                        return {
                            "status": "error",
                            "msg": special_msg
                        }
                elif is_special_case:
                    continue

                # Check full dataset download restrictions
                # pylint: disable=line-too-long
                if ("NUTS" not in dataset_json and "BoundingBox" not in dataset_json and "TemporalFilter" not in dataset_json):  # noqa
                    # We are requesting a full dataset download
                    # We need to check if this dataset is a EEA dataset
                    # to show an specific message
                    # pylint: disable=line-too-long
                    if (full_dataset_source and full_dataset_source != "EEA" or not full_dataset_source):  # noqa
                        self.request.response.setStatus(400)
                        return {
                            "status": "error",
                            "msg": (
                                "You are requesting to download the full"
                                " dataset but this dataset is not an EEA"
                                " dataset and thus you need to query an"
                                " specific endpoint to request its download."
                                " Please check the API documentation to get"
                                " more information about this specific"
                                " endpoint."
                            ),
                        }
                    if (full_dataset_source and full_dataset_source == "EEA" or not full_dataset_source):  # noqa
                        self.request.response.setStatus(400)
                        return {
                            "status": "error",
                            "msg": (
                                "To download the full dataset, please "
                                "download it through the corresponding "
                                "pre-packaged data collection"
                            ),
                        }

                # pylint: disable=line-too-long
                # Check dataset download restrictions for
                # non-EEA datasets with no area specified
                if ("NUTS" not in dataset_json and "BoundingBox" not in dataset_json):  # noqa
                    # We are requesting a full dataset download
                    # We need to check if this dataset is a non-EEA dataset
                    # to show a specific message
                    # pylint: disable=line-too-long
                    if (full_dataset_source and full_dataset_source != "EEA" or not full_dataset_source):  # noqa
                        # Non-EEA datasets must have an area specified
                        self.request.response.setStatus(400)
                        return {
                            "status": "error",
                            "msg": (
                                (
                                    "You have to select a specific area of"
                                    " interest. In case you want to download"
                                    " the full dataset, please use the"
                                    " Auxiliary API."
                                )
                            ),
                        }

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
            self.request.response.setStatus(400)
            return {
                "status": "error",
                "msg": (
                    "The download queue can only process 5 items at a time."
                    " Please try again with fewer items."
                ),
            }

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
            # pylint: disable=line-too-long
            general_download_data_object.get("Datasets", []) + inprogress_datasets + queued_datasets  # noqa
        ):
            self.request.response.setStatus(400)
            return {
                "status": "error",
                "msg": (
                    "You have requested to download the same thing at least"
                    " twice. Please check your download cart and remove any"
                    " duplicates."
                ),
            }

        fme_results = {
            "ok": [],
            "error": [],
        }

        # cdse_results = {
        #     "ok": [],
        #     "error": []
        # }

        cdse_parent_task = {}  # contains all requested CDSE datasets, it is
        # a future FME task if all child tasks are finished in CDSE
        cdse_task_group_id = generate_task_group_id()
        cdse_batch_ids = []

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

                self.request.response.setStatus(400)
                return {
                    "status": "error",
                    "msg": (
                        f"Error creating CDSE batch: {error}"
                    ),
                }
            cdse_batch_ids.append(cdse_batch_id)
            cdse_data_object["CDSEBatchID"] = cdse_batch_id
            cdse_data_object["Status"] = "QUEUED"

            # Save child task in downloadtool
            # CDSE tasks are split in child tasks, one for each dataset
            cdse_data_object['Datasets'] = cdse_dataset
            cdse_data_object['cdse_task_role'] = "child"
            cdse_data_object['cdse_task_group_id'] = cdse_task_group_id
            # pylint: disable=line-too-long
            utility_response_json = utility.datarequest_post(cdse_data_object)  # noqa: E501
            utility_task_id = get_task_id(utility_response_json)

            # make sure parent task is independent of the child
            cdse_parent_task = copy.deepcopy(cdse_data_object)  # placeholder
            cdse_parent_task.pop('GpkgFileName', None)
            cdse_parent_task.pop('CDSEBatchID', None)

            # start batch
            start_batch(cdse_batch_id)

            # Save task in statstool - probably only after finished in FME?
            # # build the stat params and save them
            # stats_params = {
            #         "Start": datetime.utcnow().isoformat(),
            #         "User": str(user_id),
            #         # pylint: disable=line-too-long
            #         "Dataset": [item["DatasetID"] for item in cdse_dataset.get("Datasets", [])],  # noqa: E501
            #         "TransformationData": new_datasets,
            #         "TaskID": utility_task_id,
            #         "CDSEBatchID": cdse_batch_id,
            #         "GpkgFileName": unique_geopackage_name,
            #         "End": "",
            #         "TransformationDuration": "",
            #         "TransformationSize": "",
            #         "TransformationResultData": "",
            #         "Status": "Queued",
            #     }
            # save_stats(stats_params)

        if len(cdse_datasets["Datasets"]) > 0:
            # Save parent task in downloadtool, containing all CDSE datasets
            cdse_parent_task["cdse_task_role"] = "parent"
            cdse_parent_task["Status"] = "Queued"
            cdse_parent_task["Datasets"] = cdse_datasets["Datasets"]
            cdse_parent_task["CDSEBatchIDs"] = cdse_batch_ids
            # pylint: disable=line-too-long
            utility_response_json = utility.datarequest_post(cdse_parent_task)  # noqa: E501
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
                            "value": self.get_callback_url(),
                        },
                        # dump the json into a string for FME
                        {"name": "json", "value": json.dumps(new_datasets)},
                    ]
                }

                # build the stat params and save them
                stats_params = {
                    "Start": datetime.utcnow().isoformat(),
                    "User": str(user_id),
                    # pylint: disable=line-too-long
                    "Dataset": [item["DatasetID"] for item in data_object.get("Datasets", [])],  # noqa: E501
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

    def post_request_to_fme(self, params, is_prepackaged=False):
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
