# -*- coding: utf-8 -*-
"""
The best way to save the download tool registry is to save plain data-types in
an annotation of the site object.

This way to store information is one of the techniques used in Plone to save
non-contentish information.

To achieve that we use the IAnnotations interface to abstract saving that
informations. This technique provides us with a dictionary-like interface
where we can save, update and retrieve information.

We will also encapsulate all operations with the download tool registry in
this utility, this way it will be the central point of the all functionality
involving the said registry.

Wherever we need to interact with it (ex, REST API) we will get the utility
and call its method.

We have to understand the utility as being a Singleton object.

"""
import random
from datetime import datetime
from logging import getLogger

from clms.downloadtool.utils import ANNOTATION_KEY, STATUS_LIST
from persistent.mapping import PersistentMapping
from zope.annotation.interfaces import IAnnotations
from zope.component.hooks import getSite
from zope.interface import Interface, implementer

log = getLogger(__name__)


class IDownloadToolUtility(Interface):
    """Downloadtool utility interface"""


@implementer(IDownloadToolUtility)
class DownloadToolUtility:
    """Downloadtool request methods"""

    def datarequest_post(self, data_request):
        """DatarequestPost method"""
        site = getSite()
        annotations = IAnnotations(site)
        task_id = random.randint(0, 99999999999)
        str_task_id = str(task_id)

        registry = annotations.get(ANNOTATION_KEY, PersistentMapping())

        while str_task_id in registry:
            task_id = random.randint(0, 99999999999)
            str_task_id = str(task_id)

        registry[str_task_id] = data_request
        annotations[ANNOTATION_KEY] = registry

        return {str_task_id: data_request}

    def datarequest_delete(self, task_id, user_id):
        """DatarequestDelete method"""
        site = getSite()
        annotations = IAnnotations(site)
        registry = annotations.get(ANNOTATION_KEY, PersistentMapping())

        data_object = None

        if task_id not in registry:
            return "Error, TaskID not registered"

        data_object = registry.get(str(task_id))
        if user_id not in data_object["UserID"]:
            return "Error, permission denied"

        data_object["Status"] = "Cancelled"
        data_object["FinalizationDateTime"] = datetime.utcnow().isoformat()

        registry[str(task_id)] = data_object
        annotations[ANNOTATION_KEY] = registry

        return data_object

    def datarequest_search(self, user_id, status):
        """DatarequestSearch method"""
        site = getSite()
        annotations = IAnnotations(site)
        registry = annotations.get(ANNOTATION_KEY, PersistentMapping())
        data_object = {}

        if not user_id:
            return "Error, UserID not defined"

        if not status:
            for key in registry.keys():
                values = registry.get(key)
                if str(user_id) == values.get("UserID"):
                    data_object[key] = values
            return data_object

        if status not in STATUS_LIST:
            return "Error, status not recognized"

        for key in registry.keys():
            values = registry.get(key)
            if status == values.get("Status") and str(user_id) == values.get(
                "UserID"
            ):
                data_object[key] = values

        return data_object

    def datarequest_status_get(self, task_id):
        """DataRequestStatusGet method"""
        site = getSite()
        annotations = IAnnotations(site)
        registry = annotations.get(ANNOTATION_KEY, PersistentMapping())
        if task_id not in registry:
            return "Error, task not found"
        return registry.get(task_id)

    def datarequest_status_patch(self, data_object, task_id):
        """DatarequestStatusPatch method"""
        site = getSite()
        annotations = IAnnotations(site)
        registry = annotations.get(ANNOTATION_KEY, PersistentMapping())

        if task_id not in registry:
            return "Error, task_id not registered"

        registry_item = registry.get(task_id, None)

        if "Status" in data_object:
            registry_item["Status"] = data_object["Status"]
        if "DownloadURL" in data_object:
            registry_item["DownloadURL"] = data_object["DownloadURL"]
        if "FileSize" in data_object:
            registry_item["FileSize"] = data_object["FileSize"]
        if "FinalizationDateTime" in data_object:
            registry_item["FinalizationDateTime"] = data_object[
                "FinalizationDateTime"
            ]
        if "Message" in data_object:
            registry_item["Message"] = data_object["Message"]
        registry[task_id] = registry_item
        annotations[ANNOTATION_KEY] = registry
        return registry_item

    def delete_data(self):
        """a method to delete all data from the registry"""
        site = getSite()
        annotations = IAnnotations(site)

        if annotations.get(ANNOTATION_KEY, None) is None:
            return {"status": "Error", "msg": "Registry is empty"}

        annotations[ANNOTATION_KEY] = PersistentMapping()
        return {}
