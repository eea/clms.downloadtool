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
from logging import getLogger

from persistent.mapping import PersistentMapping
from zope.annotation.interfaces import IAnnotations
from zope.interface import implementer
from zope.interface import Interface
from zope.site.hooks import getSite


log = getLogger(__name__)

ANNOTATION_KEY = "clms.downloadtool"
status_list = [
    "Rejected",
    "Queued",
    "In_progress",
    "Finished_ok",
    "Finished_nok",
    "Cancelled",
]


class IDownloadToolUtility(Interface):
    """Download interface"""


@implementer(IDownloadToolUtility)
class DownloadToolUtility:
    """Download utilites"""

    def datarequest_post(self, data_request):
        """ Add a new data request"""
        site = getSite()
        annotations = IAnnotations(site)
        task_id = random.randint(0, 99999999999)

        if annotations.get(ANNOTATION_KEY, None) is None:
            registry = {str(task_id): data_request}
            annotations[ANNOTATION_KEY] = registry

        else:
            registry = annotations.get(ANNOTATION_KEY, PersistentMapping())
            exists = True
            while exists:
                if task_id not in registry:
                    exists = False
                else:
                    task_id = random.randint(0, 99999999999)

            registry[str(task_id)] = data_request
            annotations[ANNOTATION_KEY] = registry

        return {task_id: data_request}

    def datarequest_delete(self, task_id, user_id):
        """ Delete a data task """
        site = getSite()
        annotations = IAnnotations(site)
        registry = annotations.get(ANNOTATION_KEY, PersistentMapping())

        dataObject = None

        if task_id not in registry:
            return {"status": "error", "msg": "Error, TaskID not registered"}

        dataObject = registry.get(str(task_id))
        if user_id not in dataObject["UserID"]:
            return {"status": "error", "msg": "Error, permission denied"}

        dataObject["Status"] = "Cancelled"
        registry[str(task_id)] = dataObject
        annotations[ANNOTATION_KEY] = registry

        return dataObject

    def datarequest_search(self, user_id, status):
        """ search user task """
        site = getSite()
        annotations = IAnnotations(site)
        registry = annotations.get(ANNOTATION_KEY, PersistentMapping())
        dataObject = {}

        if not user_id:
            return {"status": "error", "msg": "Error, UserID not defined"}

        if not status:
            for key in registry.keys():
                values = registry.get(key)
                if str(user_id) == values.get("UserID"):
                    dataObject[key] = values
            return dataObject

        if status not in status_list:
            return {"status": "error", "msg": "Error, status not recognized"}

        for key in registry.keys():
            values = registry.get(key)
            if str(user_id) == values.get("UserID") and status == values.get(
                "Status"
            ):
                dataObject[key] = values

        return dataObject

    def dataset_get(self, key):
        """ Get dataset """
        site = getSite()
        annotations = IAnnotations(site)
        registry = annotations.get(ANNOTATION_KEY, PersistentMapping())
        return registry.get(key)

    def datarequest_status_get(self, task_id):
        """ Get request status """
        site = getSite()
        annotations = IAnnotations(site)
        registry = annotations.get(ANNOTATION_KEY, PersistentMapping())
        if task_id not in registry:
            return {"status": "error", "msg": "Error, task not found"}
        return registry.get(task_id)

    def datarequest_status_patch(self, dataObject, task_id):
        """ Update request status """
        site = getSite()
        annotations = IAnnotations(site)
        registry = annotations.get(ANNOTATION_KEY, PersistentMapping())
        tempObject = {}

        if task_id not in registry:
            return {"status": "error", "msg": "Error, task_id not registered"}

        # Disable check because need python >= 3.5
        # linter base image is based in 2-alpine which runs
        # python 2.7
        # pylint: disable=syntax-error
        tempObject = {**registry[task_id], **dataObject}

        # pylint: disable=line-too-long
        if "NUTSID" in tempObject.keys() and "BoundingBox" in tempObject.keys():  # noqa:
            dataObject = {}
            # pylint: disable=line-too-long
            return {"status": "error", "msg": "Error, NUTSID and BoundingBox can't be defined in the same task"}  # noqa
        registry[str(task_id)] = tempObject

        annotations[ANNOTATION_KEY] = registry

        return tempObject
