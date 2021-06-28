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
        user_id = str(body.get("UserID"))
        task_id = str(body.get("TaskID"))
        response_json = None
        log.info('DATAREQUEST_DELETE')
        utility = getUtility(IDownloadToolUtility)

        if not task_id:
            self.request.response.setStatus(400)
            return "Error, TaskID not defined"
        if not user_id:
            self.request.response.setStatus(400)
            return "Error, UserID not defined"
        
        response_json = utility.datarequest_delete(task_id, user_id)
        
        if "Error, TaskID not registered" in response_json:
            self.request.response.setStatus(403)
            return response_json

        if "Error, permission denied" in response_json:
            self.request.response.setStatus(404)
            return response_json

        self.request.response.setStatus(204)
        return response_json
