# -*- coding: utf-8 -*-
"""
Download tool patch operation
"""
import json
from datetime import datetime
from logging import getLogger

from clms.statstool.utility import IDownloadStatsUtility
from plone.restapi.deserializer import json_body
from plone.restapi.services import Service
from zope.component import getUtility

from clms.downloadtool.utility import IDownloadToolUtility
from clms.downloadtool.utils import STATUS_LIST

log = getLogger(__name__)


def save_stats(stats_json):
    """save the stats in the download stats utility"""
    try:
        utility = getUtility(IDownloadStatsUtility)
        task_id = stats_json.get("TaskID")
        utility.patch_item(stats_json, task_id)
    except Exception:
        # pylint: disable=line-too-long
        log.info(
            "There was an error saving the stats: %s", json.dumps(stats_json)
        )  # noqa


class datarequest_status_patch(Service):
    """Nuts & BBox not at the same time"""

    def reply(self):
        """JSON response"""
        body = json_body(self.request)

        if not body.get("TaskID", None):
            self.request.response.setStatus(400)
            return {"status": "error", "msg": "Error, TaskID is not defined"}

        task_id = str(body.get("TaskID"))
        status = body.get("Status")
        download_url = body.get("DownloadURL")
        filesize = body.get("FileSize")
        message = body.get("Message")

        response_json = {}

        utility = getUtility(IDownloadToolUtility)

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
        # Dict top save stats data
        stats_data = {}
        if filesize:
            response_json.update({"FileSize": filesize})
            stats_data.update({"TransformationSize": filesize})

        if download_url:
            response_json.update({"DownloadURL": download_url})

        if message:
            response_json.update({"Message": message})

        if status not in ["Queued", "In_progress"]:
            # pylint: disable=line-too-long
            response_json[
                "FinalizationDateTime"
            ] = datetime.utcnow().isoformat()  # noqa: E501
            stats_data.update({"End": datetime.utcnow().isoformat()})
        stats_data.update({"Status": status})
        stats_data.update({"TaskID": task_id})
        # Save stats data
        save_stats(stats_data)

        response_json = utility.datarequest_status_patch(
            response_json, task_id
        )

        if "Error, task_id not registered" in response_json:
            self.request.response.setStatus(404)
            return {"status": "error", "msg": response_json}

        self.request.response.setStatus(200)

        return response_json
