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
        user_id = str(body.get("user_id"))
        task_id = str(body.get("task_id"))
        response_json = None
        log.info('DATAREQUEST_DELETE')
        utility = getUtility(IDownloadToolUtility)
        log.info(user_id)
        log.info(task_id)
        #try:
        if task_id:
            log.info(task_id)
            response_json = utility.datarequest_delete(task_id, user_id)
            log.info("RETURNED VALUE")
            log.info(response_json)
            self.request.response.setStatus(200)
        else:
            response_json="BAD REQUEST"
        '''except TypeError:
            response_json="BAD REQUEST, THE ELEMENT DOES NOT EXIST"
            self.request.response.setStatus(400)'''

        log.info(response_json)
        if "Error" in response_json or response_json is None or "BAD" in response_json:
            self.request.response.setStatus(400)

   
        
        return response_json
