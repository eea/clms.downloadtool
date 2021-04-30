# -*- coding: utf-8 -*-
"""
For HTTP GET operations we can use standard HTTP parameter passing (through the URL)

"""
from plone import api
from plone.restapi.services import Service

from zope.component import getUtility
from clms.downloadtool.utility import IDownloadToolUtility

# logger, do log.info('XXXX') to print in the console
from logging import getLogger

log = getLogger(__name__)


class dataset_get(Service):
    def reply(self):

        #key = self.request.get("key")
        log.info('DATASET_GET')
        log.info(self.request.get("dataset_title"))
        utility = getUtility(IDownloadToolUtility)
        #value = utility.dataset_get(key)

        key = "dataset_title"
        value = self.request.get("dataset_title")

        self.request.response.setStatus(200)
        return {key: value}
