"""CDSE Tasks Queue is used to prevent timeout when CDSE tasks are created"""
import copy
from datetime import datetime, timezone
from logging import getLogger
from clms.downloadtool.api.services.cdse.cdse_integration import (
    create_batches)
from clms.downloadtool.api.services.datarequest_post.utils import (
    generate_task_group_id,
    get_s3_paths_encoded,
    get_task_id,
    to_iso8601,
)
from clms.downloadtool.utility import IDownloadToolUtility
from zope.component import getUtility


log = getLogger(__name__)


def process_cdse_batches(cdse_datasets, user_id):
    """Handle CDSE: create child + parent tasks and start batches."""
    utility = getUtility(IDownloadToolUtility)
    cdse_parent_task = {}
    cdse_task_group_id = generate_task_group_id()
    cdse_batch_ids, gpkg_filenames = [], []

    for cdse_dataset in cdse_datasets["Datasets"]:
        try:
            cdse_dataset["TemporalFilter"]["StartDate"] = to_iso8601(
                cdse_dataset["TemporalFilter"]["StartDate"])
            cdse_dataset["TemporalFilter"]["EndDate"] = to_iso8601(
                cdse_dataset["TemporalFilter"]["EndDate"])
        except Exception:
            pass

        # Call create_batches -> list of {batch_id, error, gpkg_name}
        batch_results = create_batches(cdse_dataset)

        if len(batch_results) == 0:
            log.info("Error creating CDSE batch: No batches were created.")

        # Keep track of batch_ids and gpkg_names for the current dataset
        dataset_batch_ids = []
        dataset_gpkgs = []

        for result in batch_results:
            batch_id = result.get("batch_id")
            if batch_id is None:
                error = result.get("error", "")
                log.info("Error creating CDSE batch: %s", error)

            gpkg_name = result.get("gpkg_name")
            dataset_batch_ids.append(batch_id)
            dataset_gpkgs.append(gpkg_name)

            # Keep also global tracking for the parent task
            cdse_batch_ids.append(batch_id)
            gpkg_filenames.append(gpkg_name)

            # Create child task object
            cdse_data_object = {
                "UserID": user_id,
                "RegistrationDateTime": datetime.now(
                    timezone.utc).isoformat(),
                "GpkgFileName": gpkg_name,
                "CDSEBatchID": batch_id,
                "Status": "QUEUED",
                "Datasets": cdse_dataset,
                "cdse_task_role": "child",
                "cdse_task_group_id": cdse_task_group_id,
            }

            # Post child task to utility
            utility_response_json = utility.datarequest_post(
                cdse_data_object)
            utility_task_id = get_task_id(utility_response_json)
            log.info("utility_task_id: %s", utility_task_id)

            # Prepare parent task base object (without file-specific info)
            cdse_parent_task = copy.deepcopy(cdse_data_object)
            cdse_parent_task.pop('GpkgFileName', None)
            cdse_parent_task.pop('CDSEBatchID', None)

        # Assign only the encoded S3 paths belonging to this dataset
        dataset_paths = get_s3_paths_encoded(dataset_batch_ids)
        cdse_dataset["DatasetPath"] = dataset_paths

    # After processing all datasets, create the parent task
    if cdse_datasets["Datasets"]:
        temp_datasets = cdse_datasets["Datasets"]
        cdse_parent_task.update({
            "cdse_task_role": "parent",
            "Status": "Queued",
            "Datasets": temp_datasets,
            "CDSEBatchIDs": cdse_batch_ids,
            "GpkgFileNames": gpkg_filenames,
        })
        utility_response_json = utility.datarequest_post(cdse_parent_task)
        utility_task_id = get_task_id(utility_response_json)
        log.info("utility_task_id: %s", utility_task_id)

    return cdse_parent_task, None
