# -*- coding: utf-8 -*-
"""
For HTTP GET operations we can use standard HTTP parameter passing
(through the URL)

"""
from logging import getLogger

from plone.restapi.services import Service
from plone.restapi.deserializer import json_body

from zope.component import getUtility
from clms.downloadtool.utility import IDownloadToolUtility

# logger, do log.info('XXXX') to print in the console

log = getLogger(__name__)


class datarequest_delete(Service):
    """Delete data"""

    def reply(self):
        """ JSON response """
        body = json_body(self.request)
        user_id = str(body.get("UserID"))
        task_id = str(body.get("TaskID"))
        log.info("DATAREQUEST_DELETE")
        utility = getUtility(IDownloadToolUtility)

        if not task_id:
            self.request.response.setStatus(400)
            return {"status": "error", "msg": "Error, TaskID not defined"}
        if not user_id:
            self.request.response.setStatus(400)
            return {"status": "error", "msg": "Error, UserID not defined"}

        response_json = utility.datarequest_delete(task_id, user_id)

        if "Error, TaskID not registered" in response_json.get("msg", ""):
            self.request.response.setStatus(403)
            return response_json

        if "Error, permission denied" in response_json.get("msg", ""):
            self.request.response.setStatus(404)
            return response_json

        self.request.response.setStatus(204)
        return response_json
