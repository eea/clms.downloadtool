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


ANNOTATION_KEY = "clms.downloadtool"


class IDownloadToolUtility(Interface):
    pass


@implementer(IDownloadToolUtility)
class DownloadToolUtility(object):
    def register_item(self, key, value):
        site = getSite()
        annotations = IAnnotations(site)
        registry = annotations.get(ANNOTATION_KEY, PersistentMapping())
        registry[key] = value
        annotations[ANNOTATION_KEY] = registry

    def get_item(self, key):
        site = getSite()
        annotations = IAnnotations(site)
        registry = annotations.get(ANNOTATION_KEY, PersistentMapping())
        return registry.get(key)
