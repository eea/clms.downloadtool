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


class datarequest_search(Service):
    def reply(self):
        #task_id, user_id

        log.info('DATAREQUEST_SEARCH')
        utility = getUtility(IDownloadToolUtility)
        #value = utility.datarequest_search(key)
        task_id = self.request.get("task_id")
        user_id = self.request.get("user_id")
        self.request.response.setStatus(200)
        responseJson = {"task_id": task_id, "user_id":user_id}
        
        return responseJson
