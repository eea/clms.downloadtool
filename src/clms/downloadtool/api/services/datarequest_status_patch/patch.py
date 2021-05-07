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


'''

Request status (PATCH)
This method will be used by server tools to change the status of the requests
Method: GET/datarequest/status

Parameters:
TaskID
Status
{result data}
Result: task is changed to specified status, and the result data are set (request shall return TaskID and status)

'''

class datarequest_status_patch(Service):


    def reply(self):
        status_list = ["Rejected", "Queued", "In_progress", "Finished_ok", "Finished_nok", "Cancelled"]

        body = json_body(self.request)

        task_id = body.get("task_id")
        status = body.get("status")
        log.info('datarequest_status_patch')
        log.info(task_id)

        utility = getUtility(IDownloadToolUtility)
        #value = utility.datarequest_status_patch(key)

        task_validation = checkNumber(task_id)

        if not task_id or status not in status_list:
            self.request.response.setStatus(400)
            log.info("BAD REQUEST")
            response_json = "BAD REQUEST"
        else:
            self.request.response.setStatus(200)
            response_json = {"task_id":task_id, "status":status}
            
        return response_json





def checkNumber(task_id):
    try:
        task_id = int(task_id)
        return True
    except:
        return False

