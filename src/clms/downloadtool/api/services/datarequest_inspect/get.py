# -*- coding: utf-8 -*-
"""
For HTTP GET operations we can use standard HTTP parameter passing
(through the URL)

"""
from clms.downloadtool.utility import IDownloadToolUtility
from plone.restapi.services import Service
from zope.component import getUtility


AVAILABLE_FILTERS = ["Status", "UserID", "TaskID", "FMETaskID"]


class DatarequestInspect(Service):
    """Inspect download requests"""

    def reply(self):
        """JSON endpoint"""
        utility = getUtility(IDownloadToolUtility)

        query = {}

        for available_filter in AVAILABLE_FILTERS:
            if available_filter in self.request:
                query[available_filter] = self.request.get(available_filter)

        if query:
            response_json = utility.datarequest_inspect(**query)
            self.request.response.setStatus(200)
        else:
            response_json = {
                "status": "error",
                "msg": "You need to provide at least one filter among"
                       f"the allowed ones: {', '.join(AVAILABLE_FILTERS)}"

            }
            self.request.response.setStatus(400)

        return response_json
