# -*- coding: utf-8 -*-
from zope.interface import Interface
from zope.annotations.interfaces import IAnnotations
from zope.site.hooks import getSite
from persistent import PersistentDict

ANNOTATION_KEY = "clms.downloadtool"


class IDownloadToolUtility(Interface):
    pass


class DownloadToolUtility(Interface):
    def register_item(self, key, value):
        site = getSite()
        annotations = IAnnotations(site)
        registry = annotations.get(ANNOTATION_KEY, PersistentDict())
        registry[key] = value
        annotations[ANNOTATION_KEY] = registry
