# -*- coding: utf-8 -*-
"""
For HTTP GET operations we can use standard HTTP parameter passing
(through the URL)

"""
from clms.downloadtool.api.services.utils import (
    COUNTRIES,
    DATASET_FORMATS,
    FORMAT_CONVERSION_TABLE,
    GCS,
    STATUS_LIST,
)
from logging import getLogger

from plone.restapi.services import Service
from plone.restapi.deserializer import json_body


from zope.component import getUtility
from clms.downloadtool.utility import IDownloadToolUtility

log = getLogger(__name__)


class datarequest_status_patch(Service):
    """Nuts & BBox not at the same time"""

    def reply(self):
        """ JSON response """
        body = json_body(self.request)
        task_id = str(body.get("TaskID"))
        status = body.get("Status")
        download_url = body.get("DownloadURL")
        filesize = body.get("FileSize")

        response_json = {}

        utility = getUtility(IDownloadToolUtility)

        if not task_id:
            self.request.response.setStatus(400)
            return {"status": "error", "msg": "Error, TaskID is not defined"}

        if not status:
            self.request.response.setStatus(400)
            return {"status": "error", "msg": "Error, Status is not defined"}

        if status not in STATUS_LIST:
            self.request.response.setStatus(400)
            return {
                "status": "error",
                "msg": "Error, defined Status is not in the list",
            }
        response_json = {"TaskID": task_id, "Status": status}

        if filesize:
            response_json.update({"FileSize": filesize})

        if download_url:
            response_json.update({"DownloadURL": download_url})

        log.info(response_json)
        response_json = utility.datarequest_status_patch(
            response_json, task_id
        )

        log.info(response_json)

        if "Error, task_id not registered" in response_json:
            self.request.response.setStatus(404)
            return {"status": "error", "msg": response_json}

        if (
            "Error, NUTSID and BoundingBox can't be defined in the same task"
            in response_json
        ):
            self.request.response.setStatus(400)
            return {"status": "error", "msg": response_json}

        if "Error" in response_json:
            self.request.response.setStatus(400)
            return {"status": "error", "msg": response_json}

        self.request.response.setStatus(201)

        return response_json
