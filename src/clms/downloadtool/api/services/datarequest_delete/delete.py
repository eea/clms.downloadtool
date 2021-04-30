# -*- coding: utf-8 -*-
"""
For HTTP GET operations we can use standard HTTP parameter passing (through the URL)

"""
from plone import api
from plone.restapi.services import Service
from plone.restapi.deserializer import json_body

from zope.component import getUtility
from clms.downloadtool.utility import IDownloadToolUtility

# logger, do log.info('XXXX') to print in the console
from logging import getLogger

log = getLogger(__name__)


class datarequest_delete(Service):
    def reply(self):
        
        body = json_body(self.request)
        user_id = body.get("user_id")
        task_id = body.get("task_id")
        log.info('DATAREQUEST_DELETE')
        utility = getUtility(IDownloadToolUtility)
        #value = utility.datarequest_delete(key)


        responseJson = {"user_id":user_id, "task_id":task_id}
        self.request.response.setStatus(200)
        return responseJson
