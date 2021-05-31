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
            registry[str(task_id)] = data_request

        return {task_id: data_request}

    def datarequest_delete(self, task_id, user_id):
        site = getSite()
        annotations = IAnnotations(site)
        registry = annotations.get(ANNOTATION_KEY, PersistentMapping())     

        dataObject = registry.get(str(task_id))
        if dataObject["user_id"] == user_id:
            dataObject["status"] =  "Cancelled"
        
        registry[str(task_id)] = dataObject
        annotations[ANNOTATION_KEY] = registry
        return dataObject


    def datarequest_search(self, user_id, status):
        #Controlar que no sea None 
        site = getSite()
        annotations = IAnnotations(site)
        registry = annotations.get(ANNOTATION_KEY, PersistentMapping())
        dataObject = {}

        if user_id:
            if status in status_list:
                for key in registry.keys():
                    values = registry.get(key)
                    if str(user_id) == values.get("user_id") and status == values.get("status"):
                        dataObject[key] = values
            elif status:
                return "Error, bad request status not recognized"
            else:
                for key in registry.keys():
                    values = registry.get(key)
                    if str(user_id) == values.get("user_id"):
                        dataObject[key] = values
        else:
            return "Error, bad request user_id not defined"

        

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
        return registry.get(task_id)


    def datarequest_status_patch(self, dataObject, task_id):
        site = getSite()
        annotations = IAnnotations(site)
        registry = annotations.get(ANNOTATION_KEY, PersistentMapping())
        registry[str(task_id)] = dataObject
        annotations[ANNOTATION_KEY] = registry
        resp = {}
        resp[task_id]  = dataObject
        return resp

        ##-----------------------------------------------------------------------------------------------------------------------------------------

'''

    def register_item(self, status, task_id, user_id):
        site = getSite()
        annotations = IAnnotations(site)
        registry = annotations.get(ANNOTATION_KEY, PersistentMapping())

        if annotations.get(ANNOTATION_KEY, None) is None:
            log.info("IS NONE")
            registry = annotations[ANNOTATION_KEY] = {"status":status, "user_id":user_id}

        else:
            log.info("IS NOT NONE")
            log.info(annotations.get(ANNOTATION_KEY, None))
            registry[task_id] = {"status":status, "user_id":user_id}
        
        if registry is None:
           log.info("IF SENTENCE")
           #{"status":status, "user_id":user_id}
        else:
           log.info("ELSE SENTENCE")

           #registry = annotations[ANNOTATION_KEY] = Item(user_id, status)
        
        log.info(registry)
        #annotations[ANNOTATION_KEY] = registry
        log.info(ANNOTATION_KEY)

    def get_item(self, task_id):
        
        site = getSite()
        annotations = IAnnotations(site)
        registry = annotations.get(ANNOTATION_KEY, PersistentMapping())
        log.info('GET VALUE')
        log.info(registry)
        return registry.get(task_id)
'''