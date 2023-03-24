# -*- coding: utf-8 -*-
"""
For HTTP GET operations we can use standard HTTP parameter passing
(through the URL)

"""
from clms.downloadtool.utility import IDownloadToolUtility
from plone import api
from plone.restapi.services import Service
from zope.component import getUtility


class datarequest_search(Service):
    """Search datarequest"""

    def reply(self):
        """JSON endpoint"""
        utility = getUtility(IDownloadToolUtility)
        status = self.request.get("status")

        user = api.user.get_current()

        if not user:
            return {
                "status": "error",
                "msg": "You need to be logged in to use this service",
            }

        user_id = user.getId()

        # Allow portal managers users to search someone else's
        # download requests
        if api.user.has_permission("clms.downloadtool.managedownloadtool"):
            requested_userid = self.request.get("user", None)
            if requested_userid:
                requested_user = api.user.get(userid=requested_userid)
                if requested_user is not None:
                    user_id = requested_userid
            else:
                user_id = None

        response_json = utility.datarequest_search(user_id, status)

        if "Error, UserID not defined" in response_json:
            self.request.response.setStatus(400)
            return {"status": "error", "msg": response_json}

        if "Error, status not recognized" in response_json:
            self.request.response.setStatus(400)
            return {"status": "error", "msg": response_json}

        self.request.response.setStatus(200)
        return response_json
