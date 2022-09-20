# -*- coding: utf-8 -*-
"""
DELETE endpoint for the download tool.
"""
from logging import getLogger

import requests
from clms.downloadtool.utility import IDownloadToolUtility
from plone import api
from plone.restapi.deserializer import json_body
from plone.restapi.services import Service, _no_content_marker
from zope.component import getUtility

log = getLogger(__name__)


class datarequest_delete(Service):
    """Delete data"""

    def reply(self):
        """ JSON response """
        body = json_body(self.request)
        user = api.user.get_current()
        user_id = user.getId()
        task_id = body.get("TaskID")

        if not task_id:
            self.request.response.setStatus(400)
            return {"status": "error", "msg": "Error, TaskID not defined"}

        utility = getUtility(IDownloadToolUtility)
        str_task_id = str(body.get("TaskID"))
        response_json = utility.datarequest_delete(str_task_id, user_id)

        if "Error, TaskID not registered" in response_json:
            self.request.response.setStatus(403)
            return {"status": "error", "msg": response_json}

        if "Error, permission denied" in response_json:
            self.request.response.setStatus(404)
            return {"status": "error", "msg": response_json}

        # Try to get the FME task id to signal finalization
        fme_task_id = response_json.get("FMETaskId", None)
        if fme_task_id:
            self.signal_finalization_to_fme(fme_task_id)
        else:
            log.info("No FME task id found for task: %s", str_task_id)

        self.request.response.setStatus(204)
        return _no_content_marker

    def signal_finalization_to_fme(self, task_id):
        """ Signal finalization to FME """
        FME_DELETE_URL = api.portal.get_registry_record(
            "clms.downloadtool.fme_config_controlpanel.delete_url"
        )
        FME_TOKEN = api.portal.get_registry_record(
            "clms.downloadtool.fme_config_controlpanel.fme_token"
        )
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
            "Authorization": "fmetoken token={0}".format(FME_TOKEN),
        }

        if FME_DELETE_URL.endswith("/"):
            FME_DELETE_URL = FME_DELETE_URL[:-1]

        fme_url = "{}/{}".format(FME_DELETE_URL, task_id)

        resp = requests.delete(fme_url, headers=headers)
        if resp.ok:
            log.info("Task finalized in FME: %s", task_id)
        else:
            log.info("Error finalizing task in FME: %s", task_id)
