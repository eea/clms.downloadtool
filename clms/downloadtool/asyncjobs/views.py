"""Views used by async workers to process CDSE background jobs."""

import json
import logging
import os
from Products.Five.browser import BrowserView
from plone.api.env import adopt_user
from plone.protect.interfaces import IDisableCSRFProtection
from zope.interface import alsoProvides
from zope.component import getUtility
from zExceptions import Unauthorized
from clms.downloadtool.api.services.cdse.cdse_tasks_queue import (
    process_cdse_batches,
)
from clms.downloadtool.utility import IDownloadToolUtility

logger = logging.getLogger(__name__)
PLONE_AUTH_TOKEN = os.environ.get("PLONE_AUTH_TOKEN", "hello1234")


def check_token_security(request):
    """Ensure request comes from a trusted worker using a shared secret."""
    token = request.getHeader("Authentication")
    if token != PLONE_AUTH_TOKEN:
        raise Unauthorized("Invalid or missing authentication token")


class StartCDSEBatch(BrowserView):
    """Called by the async worker to create CDSE batches in the background."""

    def __call__(self):
        alsoProvides(self.request, IDisableCSRFProtection)
        check_token_security(self.request)

        with adopt_user(username="admin"):
            try:
                data = json.loads(self.request.get("BODY", "{}"))
                user_id = data.get("user_id")
                cdse_datasets = data.get("cdse_datasets")

                parent_task, _ = process_cdse_batches(cdse_datasets, user_id)
                result = {"status": "ok", "parent_task": parent_task}

            except Exception as e:
                logger.exception("Error while processing CDSE batches")
                result = {"error": str(e)}

        self.request.response.setHeader("Content-Type", "application/json")
        return json.dumps(result)


class DownloadToolUpdates(BrowserView):
    """
        Called by the async worker to update/remove tasks in DownloadTool.

        Receives an object containing:
        - operation (name of the utility method to be called)
            datarequest_remove_task
            datarequest_status_patch
            datarequest_status_patch_multiple
        - updates (parameters to be used when calling utility method)

        So, this is just an indirect (async) call of methods is DownloadTool
        used to update any data we save in that tool.
    """

    def __call__(self):
        alsoProvides(self.request, IDisableCSRFProtection)
        check_token_security(self.request)

        with adopt_user(username="admin"):
            try:
                data = json.loads(self.request.get("BODY", "{}"))
                operation = data.get("operation")
                updates = data.get("updates")
                utility = getUtility(IDownloadToolUtility)

                print("DOWNLOAD TOOL ASYNC ------------------------------")
                print("OPERATION")
                print(operation)
                print("UPDATES")
                print(updates)

                if operation == "datarequest_status_patch_multiple":
                    logger.info(
                        "ASYNC DownloadTool datarequest_status_patch_multiple")
                    res = utility.datarequest_status_patch_multiple(updates)
                    logger.info(res)

                if operation == "datarequest_status_patch":
                    logger.info(
                        "ASYNC DownloadTool datarequest_status_patch")

                    data_object = updates['data_object']
                    utility_task_id = updates['utility_task_id']
                    res = utility.datarequest_status_patch(
                        data_object, utility_task_id)
                    logger.info(res)

                if operation == "datarequest_remove_task":
                    logger.info(
                        "ASYNC DownloadTool datarequest_remove_task")
                    task_id = updates
                    res = utility.datarequest_remove_task(task_id)
                    logger.info(res)
                result = {"status": "ok"}

            except Exception as e:
                logger.exception("Error while trying to update DownloadTool.")
                result = {"error": str(e)}

        self.request.response.setHeader("Content-Type", "application/json")
        return json.dumps(result)
