# -*- coding: utf-8 -*-
"""
For HTTP GET operations we can use standard HTTP parameter passing (through the URL)

"""
import json
from datetime import datetime
from plone import api
from plone.restapi.services import Service

from zope.component import getUtility
from clms.downloadtool.utility import IDownloadToolUtility

# logger, do log.info('XXXX') to print in the console
from logging import getLogger

log = getLogger(__name__)


class datarequest_status_get(Service):
    def reply(self):

        task_id = self.request.get("task_id")
        log.info('datarequest_status_get')
        utility = getUtility(IDownloadToolUtility)
        #value = utility.datarequest_status_get(key)

        if not task_id:
            self.request.response.setStatus(400)
            log.info("BAD REQUEST")
            response_json = "BAD REQUEST"
        else:
            self.request.response.setStatus(200)
            response_json = {"task_id":task_id, "creation_date":str(datetime.now()).split(".")[0], "start_date":str(datetime.now()).split(".")[0], "end_date":str(datetime.now()).split(".")[0]}
            



        return response_json

'''
Request status (GET)
Method: GET/datarequest/status
Parameters:
TaskID
Result: Information about the task
(creation date, status, start_date and end_date, or start_date and time in process…)
'''