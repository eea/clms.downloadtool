# -*- coding: utf-8 -*-
"""
The best way to save the download tool registry is to save plain data-types in an annotation of the site object.

This way to store information is one of the techniques used in Plone to save non-contentish information.

To achieve that we use the IAnnotations interface to abstract saving that informations. This technique provides us
with a dictionary-like interface where we can save, update and retrieve information.

We will also encapsulate all operations with the download tool registry in this utility, this way it will be the
central point of the all functionality involving the said registry.

Wherever we need to interact with it (ex, REST API) we will get the utility and call its method.

We have to understand the utility as being a Singleton object.

"""
from persistent.mapping import PersistentMapping
from zope.annotation.interfaces import IAnnotations
from zope.interface import implementer
from zope.interface import Interface
from zope.site.hooks import getSite

import random
from logging import getLogger
log = getLogger(__name__)

ANNOTATION_KEY = "clms.downloadtool"
status_list = ["Rejected", "Queued", "In_progress", "Finished_ok", "Finished_nok", "Cancelled"]


class IDownloadToolUtility(Interface):
    pass


@implementer(IDownloadToolUtility)
class DownloadToolUtility(object):

    def datarequest_post(self, data_request):
        site = getSite()
        annotations = IAnnotations(site)
        task_id = random.randint(0,99999999999)

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
                    task_id = random.randint(0,99999999999)

            registry[str(task_id)] = data_request
            annotations[ANNOTATION_KEY] = registry


        return {task_id: data_request}

    def datarequest_delete(self, task_id, user_id):
        site = getSite()
        annotations = IAnnotations(site)
        registry = annotations.get(ANNOTATION_KEY, PersistentMapping())     

        dataObject = None

        if task_id not in registry:
            return "Error, TaskID not registered"

        dataObject = registry.get(str(task_id))
        if user_id not in dataObject["UserID"]:
            return "Error, permission denied" 
        
        dataObject["Status"] =  "Cancelled"
        registry[str(task_id)] = dataObject
        annotations[ANNOTATION_KEY] = registry

        return dataObject

    def datarequest_search(self, user_id, status):
        site = getSite()
        annotations = IAnnotations(site)
        registry = annotations.get(ANNOTATION_KEY, PersistentMapping())
        dataObject = {}

        if user_id:
            return "Error, UserID not defined"

        if not status:
            for key in registry.keys():
                values = registry.get(key)
                if str(user_id) == values.get("UserID"):
                    dataObject[key] = values
            return dataObject

        if status not in status_list:
            return "Error, status not recognized"

        for key in registry.keys():
            values = registry.get(key)
            if str(user_id) == values.get("UserID") and status == values.get("Status"):
                dataObject[key] = values

        return dataObject

    def dataset_get(self, key):
        site = getSite()
        annotations = IAnnotations(site)
        registry = annotations.get(ANNOTATION_KEY, PersistentMapping())
        return registry.get(key)

    def datarequest_status_get(self, task_id):
        site = getSite()
        annotations = IAnnotations(site)
        registry = annotations.get(ANNOTATION_KEY, PersistentMapping())
        if task_id not in registry:
            return "Error, task not found"
        return registry.get(task_id)


    def datarequest_status_patch(self, dataObject, task_id):
        site = getSite()
        annotations = IAnnotations(site)
        registry = annotations.get(ANNOTATION_KEY, PersistentMapping())
        resp = {}

        if task_id not in registry:
            return "Error, task_id not registered"
        
        tempObject = registry[str(task_id)]
        tempObject.update(dataObject)
        registry[str(task_id)] = tempObject
        log.info("DATA OBJ")
        log.info(tempObject)
        annotations[ANNOTATION_KEY] = registry
        

        return registry[str(task_id)]
