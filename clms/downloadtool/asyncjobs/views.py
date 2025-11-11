"""Views used by async workers to process CDSE background jobs."""

import json
import logging
import os
from Products.Five.browser import BrowserView
from plone.api.env import adopt_user
from plone.protect.interfaces import IDisableCSRFProtection
from zope.interface import alsoProvides
from zExceptions import Unauthorized
from clms.downloadtool.api.services.cdse.cdse_tasks_queue import (
    process_cdse_batches,
)

logger = logging.getLogger(__name__)
PLONE_AUTH_TOKEN = os.environ.get("PLONE_AUTH_TOKEN", "")


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
