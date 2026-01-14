# -*- coding: utf-8 -*-
"""Download tool utility with PostgreSQL-backed storage."""
import random
from datetime import datetime, timezone
import os
from logging import getLogger

from clms.downloadtool.api.services.cdse.cdse_integration import (
    clean_s3_bucket_files,
    stop_batch_ids_and_remove_s3_directory,
)
from clms.downloadtool.storage.db import DownloadtoolRepository
from clms.downloadtool.storage.memory import MemoryDownloadtoolRepository
from clms.downloadtool.utils import STATUS_LIST
from plone import api
from zope.interface import Interface, implementer

log = getLogger(__name__)


class IDownloadToolUtility(Interface):
    """Downloadtool utility interface"""


@implementer(IDownloadToolUtility)
class DownloadToolUtility:
    """Downloadtool request methods"""

    _repository = None

    def _get_repository(self):
        """Lazy-load the database repository."""
        if self._repository is None:
            if os.environ.get("CLMS_DOWNLOADTOOL_TESTING") == "1":
                self._repository = MemoryDownloadtoolRepository()
            else:
                self._repository = DownloadtoolRepository()
        return self._repository

    def remove_cdse_child_tasks(self, cdse_task_group_id):
        """Remove child tasks from DownloadTool"""
        tasks = self.datarequest_inspect()

        cdse_tasks = [
            task for task in tasks if task.get(
                'cdse_task_role', None) is not None]

        child_tasks = [task for task in cdse_tasks if task.get(
            'cdse_task_role', '') == 'child']
        child_tasks_group = [
            t for t in child_tasks if t[
                'cdse_task_group_id'] == cdse_task_group_id
        ]
        task_ids = [t["TaskId"] for t in child_tasks_group]

        log.info("Remove child tasks for %s", cdse_task_group_id)
        for task_id in task_ids:
            self.datarequest_remove_task(task_id)

    def datarequest_post(self, data_request):
        """register new download request"""
        repository = self._get_repository()
        if not data_request.get("UserID"):
            user = api.user.get_current()
            if user is not None:
                data_request["UserID"] = user.getId()
        if "RegistrationDateTime" not in data_request:
            data_request["RegistrationDateTime"] = datetime.now(
                timezone.utc
            ).isoformat()
        task_id = None

        while task_id is None:
            candidate = str(random.randint(0, 99999999999))
            if repository.insert_task(candidate, data_request):
                task_id = candidate

        log.info("DownloadToolUtility: TASK SAVED.")

        if "cdse_task_role" in data_request.keys():
            log.info(data_request['cdse_task_role'])
        return {task_id: data_request}

    def datarequest_delete(self, task_id, user_id):
        """cancel the download request"""
        repository = self._get_repository()
        data_object = repository.get_task(task_id)
        if data_object is None:
            return "Error, TaskID not registered"

        if user_id not in data_object["UserID"]:
            return "Error, permission denied"

        data_object["Status"] = "Cancelled"
        now_datetime = datetime.now(timezone.utc).isoformat()
        data_object["FinalizationDateTime"] = now_datetime

        is_cdse_task = False
        already_sent = data_object.get("FMETaskId", None) is not None
        if data_object.get('cdse_task_role', '') == 'parent':
            is_cdse_task = True
            cdse_task_group_id = data_object.get("cdse_task_group_id")

        if is_cdse_task and not already_sent:
            # Cancel child tasks in CDSE and delete files from s3
            cdse_batch_ids = data_object.get('CDSEBatchIDs', [])
            gpkg_filenames = data_object.get('GpkgFileNames', '')
            stop_batch_ids_and_remove_s3_directory(cdse_batch_ids)
            clean_s3_bucket_files(gpkg_filenames)
            log.info("Canceled CDSE tasks:")
            log.info(cdse_batch_ids)
            log.info("Removed s3 bucket files:")
            log.info(gpkg_filenames)
            # Also remove all child tasks for this parent task
            self.remove_cdse_child_tasks(cdse_task_group_id)

        repository.update_task(
            task_id,
            {
                "Status": data_object["Status"],
                "FinalizationDateTime": data_object["FinalizationDateTime"],
            },
            status=data_object["Status"],
        )

        return data_object

    def datarequest_search(self, user_id, status):
        """search for download requests"""
        data_object = {}
        repository = self._get_repository()

        if not user_id:
            return "Error, UserID not defined"

        if not status:
            rows = repository.search_tasks(user_id)
            for key, values in rows:
                data_object[key] = values
            return data_object

        if status not in STATUS_LIST:
            return "Error, status not recognized"

        rows = repository.search_tasks(user_id, status=status)
        for key, values in rows:
            data_object[key] = values

        return data_object

    def datarequest_status_get(self, task_id):
        """get a given download task's information"""
        repository = self._get_repository()
        task = repository.get_task(task_id)
        if task is None:
            return "Error, task not found"
        return task

    def datarequest_status_patch(self, data_object, task_id):
        """modify a given download task's information"""
        repository = self._get_repository()
        registry_item = repository.get_task(task_id)
        if registry_item is None:
            return "Error, task_id not registered"

        updates = {}

        if "Status" in data_object:
            updates["Status"] = data_object["Status"]
        if "DownloadURL" in data_object:
            updates["DownloadURL"] = data_object["DownloadURL"]
        if "FileSize" in data_object:
            updates["FileSize"] = data_object["FileSize"]
        if "FinalizationDateTime" in data_object:
            updates["FinalizationDateTime"] = data_object[
                "FinalizationDateTime"
            ]
        if "FMETaskId" in data_object:
            updates["FMETaskId"] = data_object["FMETaskId"]
        if "Message" in data_object:
            updates["Message"] = data_object["Message"]
        if "cdse_errors" in data_object:
            updates["cdse_errors"] = data_object["cdse_errors"]

        if not updates:
            return registry_item

        updated_item = repository.update_task(
            task_id, updates, status=updates.get("Status")
        )
        return updated_item if updated_item is not None else registry_item

    def datarequest_status_patch_multiple(self, updates):
        """modify multiple download tasks' information"""
        if not isinstance(updates, dict):
            return "Error, invalid payload"

        updated_items = {}
        errors = {}
        repository = self._get_repository()

        for task_id, data_object in updates.items():
            task_key = str(task_id)
            registry_item = repository.get_task(task_key)
            if registry_item is None:
                errors[task_key] = "Error, task_id not registered"
                continue

            if not isinstance(data_object, dict):
                errors[task_key] = "Error, invalid data_object"
                continue

            task_updates = {}

            if "Status" in data_object:
                task_updates["Status"] = data_object["Status"]
            if "DownloadURL" in data_object:
                task_updates["DownloadURL"] = data_object["DownloadURL"]
            if "FileSize" in data_object:
                task_updates["FileSize"] = data_object["FileSize"]
            if "FinalizationDateTime" in data_object:
                task_updates["FinalizationDateTime"] = data_object[
                    "FinalizationDateTime"
                ]
            if "FMETaskId" in data_object:
                task_updates["FMETaskId"] = data_object["FMETaskId"]
            if "Message" in data_object:
                task_updates["Message"] = data_object["Message"]
            if "cdse_errors" in data_object:
                task_updates["cdse_errors"] = data_object["cdse_errors"]

            if task_updates:
                updated_item = repository.update_task(
                    task_key, task_updates, status=task_updates.get("Status")
                )
                updated_items[task_key] = (
                    updated_item if updated_item is not None else registry_item
                )
            else:
                updated_items[task_key] = registry_item

        if errors:
            return {"updated": updated_items, "errors": errors}

        return updated_items

    def delete_data(self):
        """a method to delete all data from the registry"""
        repository = self._get_repository()
        if not repository.has_tasks():
            return {"status": "Error", "msg": "Registry is empty"}

        repository.delete_all()
        return {}

    def datarequest_remove_task(self, task_id):
        """Remove all data about the given task"""
        repository = self._get_repository()
        if not repository.delete_task(task_id):
            return "Error, TaskID not registered"

        return 1

    def datarequest_inspect(self, **query):
        """inspect the queries according to the query"""
        repository = self._get_repository()

        if "TaskID" in query:
            task_id = query.get("TaskID")
            task = repository.get_task(task_id)
            if task is not None:
                task.update({"TaskId": task_id})
                return [task]

            return []

        rows = repository.inspect_tasks(query if query else None)
        data_objects = []
        for key, db_value in rows:
            db_value.update({"TaskId": key})
            data_objects.append(db_value)

        return data_objects
