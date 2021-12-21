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
from logging import getLogger
import random

from plone import api
from plone.restapi.interfaces import ISerializeToJson
from persistent.mapping import PersistentMapping
from zope.annotation.interfaces import IAnnotations
from zope.component import getMultiAdapter
from zope.globalrequest import getRequest
from zope.interface import implementer
from zope.interface import Interface
from zope.site.hooks import getSite
from clms.downloadtool.utils import ANNOTATION_KEY, STATUS_LIST

log = getLogger(__name__)


class IDownloadToolUtility(Interface):
    """ Downloadtool utility interface
    """


@implementer(IDownloadToolUtility)
class DownloadToolUtility():
    """ Downloadtool request methods
    """
    def datarequest_post(self, data_request):
        """ DatarequestPost method
        """
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
        """ DatarequestDelete method
        """
        site = getSite()
        annotations = IAnnotations(site)
        registry = annotations.get(ANNOTATION_KEY, PersistentMapping())

        dataObject = None

        if task_id not in registry:
            return "Error, TaskID not registered"

        dataObject = registry.get(str(task_id))
        if user_id not in dataObject["UserID"]:
            return "Error, permission denied"

        dataObject["Status"] = "Cancelled"
        registry[str(task_id)] = dataObject
        annotations[ANNOTATION_KEY] = registry

        return dataObject

    def datarequest_search(self, user_id, status):
        """ DatarequestSearch method
        """
        site = getSite()
        annotations = IAnnotations(site)
        registry = annotations.get(ANNOTATION_KEY, PersistentMapping())
        dataObject = {}

        log.info(type(registry))
        log.info(registry)
        if not user_id:
            return "Error, UserID not defined"

        if not status:
            for key in registry.keys():
                values = registry.get(key)
                if str(user_id) == values.get("UserID"):
                    dataObject[key] = values
            return dataObject

        if status not in STATUS_LIST:
            return "Error, status not recognized"

        for key in registry.keys():
            values = registry.get(key)
            if status == values.get(
                "Status"
            ) and str(user_id) == values.get(
                "UserID"
            ):
                dataObject[key] = values

        return dataObject

    def dataset_get(self, title):
        """ DatasetGet method
        """
        log.info("Before the for")
        datasets = self.get_dataset_info()

        log.info(datasets)
        # if "items" not in datasets:
        #    return "Error, there are no datasets to query"

        if not title:
            return datasets

        search_list = []

        for i in datasets:
            log.info(i)
            log.info(i["title"])
            if title in i["title"]:
                search_list.append(i)
        if not search_list:
            return "Error, dataset not found"
        return search_list

    def datarequest_status_get(self, task_id):
        """ DataRequestStatusGet method
        """
        site = getSite()
        annotations = IAnnotations(site)
        registry = annotations.get(ANNOTATION_KEY, PersistentMapping())
        if task_id not in registry:
            return "Error, task not found"
        return registry.get(task_id)

    def datarequest_status_patch(self, data_object, task_id):
        """ DatarequestStatusPatch method
        """
        site = getSite()
        annotations = IAnnotations(site)
        registry = annotations.get(ANNOTATION_KEY, PersistentMapping())
        tempObject = {}

        if task_id not in registry:
            return "Error, task_id not registered"

        for element in registry[task_id]:
            element["DownloadURL"] = data_object["DownloadURL"]
            element["FileSize"] = data_object["FileSize"]

        registry[task_id]["Status"] = data_object["Status"]
        tempObject = registry[task_id]
        annotations[ANNOTATION_KEY] = registry

        return tempObject

    def get_dataset_info(self):
        """ GetDatasetInfo method
        """
        brains = api.content.find(portal_type="DataSet")
        # pylint: disable=line-too-long
        items = getMultiAdapter((brains, getRequest()), ISerializeToJson)(fullobjects=True)  # noqa
        return items.get('items', [])

    def get_item(self, key):
        """ GetItem method
        """
        site = getSite()
        annotations = IAnnotations(site)
        registry = annotations.get(ANNOTATION_KEY, PersistentMapping())
        return registry.get(key)

    def delete_data(self):
        site = getSite()
        annotations = IAnnotations(site)

        if annotations.get(ANNOTATION_KEY, None) is None:
            self.request.response.setStatus(400)    
            return {"status": "Error", "msg": "Registry is empty"}
            
        else:
            registry = None
            annotations[ANNOTATION_KEY] = registry

        self.request.response.setStatus(200)
        return {"status": "OK", "msg": "Registry deleted successfully"}