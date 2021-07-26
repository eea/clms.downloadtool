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

from logging import getLogger

log = getLogger(__name__)


class datarequest_status_get(Service):
    def reply(self):

        task_id = self.request.get("TaskID")
        log.info("datarequest_status_get")
        utility = getUtility(IDownloadToolUtility)

        if not task_id:
            self.request.response.setStatus(400)
            return "Error, TaskID not defined"

        response_json = utility.datarequest_status_get(task_id)
        if "Error, task not found" in response_json:
            self.request.response.setStatus(404)
            return "Error, the task does not exist"

        self.request.response.setStatus(200)
        return response_json
