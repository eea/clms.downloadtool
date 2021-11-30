# -*- coding: utf-8 -*-
"""
For HTTP GET operations we can use standard HTTP parameter passing
(through the URL)

"""
from logging import getLogger

from plone.restapi.services import Service
from plone import api

from zope.component import getUtility
from clms.downloadtool.utility import IDownloadToolUtility


log = getLogger(__name__)


class datarequest_search(Service):
    """ Search datarequest
    """
    def reply(self):
        """ JSON endpoint """
        log.info("DATAREQUEST_SEARCH")
        utility = getUtility(IDownloadToolUtility)
        status = self.request.get("Status")
        user_id = api.user.get_current()

        self.request.response.setStatus(200)
        response_json = utility.datarequest_search(user_id, status)

        if "Error, UserID not defined" in response_json:
            self.request.response.setStatus(400)
            return {"status": "error", "msg": response_json}

        if "Error, status not recognized" in response_json:
            self.request.response.setStatus(400)
            return {"status": "error", "msg": response_json}

        return response_json
