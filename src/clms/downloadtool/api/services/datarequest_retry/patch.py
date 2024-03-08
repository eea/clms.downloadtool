"""
Retry a download request
"""

import json

import requests
from clms.downloadtool.utility import IDownloadToolUtility
from plone import api
from plone.restapi.services import Service
from zope.component import getUtility

log = getLogger(__name__)

class DataRequestPatch(Service):
    """ retry a given request"""

    def get_callback_url(self):
        """get the callback url where FME should signal any status changes"""
        portal_url = api.portal.get().absolute_url()
        if portal_url.endswith("/api"):
            portal_url = portal_url.replace("/api", "")

        return "{}/++api++/{}".format(
            portal_url,
            "@datarequest_status_patch",
        )

    def reply(self):
        """implementation"""
        task_id = self.request.get('TaskID', None)
        if task_id is None:
            self.request.response.setStatus(400)
            return {
                "status": "error",
                "msg": "Error, TaskID is mandatory",
            }

        utility = getUtility(IDownloadToolUtility)
        item = utility.datarequest_status_get(task_id)
        if isinstance(item, str):
            self.request.response.setStatus(400)
            return {
                "status": "error",
                "msg": "Error, TaskID does not exist",
            }

        user_id = item.get('UserID')
        user = api.user.get(userid=user_id)
        mail = user.getProperty("email")
        utility_task_id = item.get("TaskId")
        callback_url = self.get_callback_url()
        new_datasets = item.get('Datasets')
        params = {
            "publishedParameters": [
                {
                    "name": "UserID",
                    "value": str(user_id),
                },
                {
                    "name": "TaskID",
                    "value": utility_task_id,
                },
                {
                    "name": "UserMail",
                    "value": mail,
                },
                {
                    "name": "CallbackUrl",
                    "value": callback_url,
                },
                # dump the json into a string for FME
                {"name": "json", "value": json.dumps(new_datasets)},
            ]
        }

        fme_results = {
            "ok": [],
            "error": [],
        }

        fme_result = self.post_request_to_fme(params)
        if fme_result:
            item["FMETaskId"] = fme_result
            utility.datarequest_status_patch(
                item, utility_task_id
            )
            fme_results["ok"].append({"TaskID": utility_task_id})
        else:
            fme_results["error"].append({"TaskID": utility_task_id})

        self.request.response.setStatus(200)
        return fme_results

    def post_request_to_fme(self, params):
        """send the request to FME and let it process it"""
        FME_URL = api.portal.get_registry_record(
            "clms.downloadtool.fme_config_controlpanel.url"
        )
        FME_TOKEN = api.portal.get_registry_record(
            "clms.downloadtool.fme_config_controlpanel.fme_token"
        )
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
            "Authorization": "fmetoken token={0}".format(FME_TOKEN),
        }
        try:
            resp = requests.post(FME_URL, json=params, headers=headers, timeout=10)
            if resp.ok:
                fme_task_id = resp.json().get("id", None)
                return fme_task_id
        except requests.exceptions.Timeout:
            log.info("FME request timed out")
        body = json.dumps(params)
        log.info(
            "There was an error registering the download request in FME: %s",
            body,
        )

        return {}
