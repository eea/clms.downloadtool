# -*- coding: utf-8 -*-
"""
An endpoint to reset all the data in the download tool
available only for site managers
"""
from plone.restapi.services import Service, _no_content_marker
from zope.component import getUtility

from clms.downloadtool.utility import IDownloadToolUtility


class delete_data(Service):
    """Delete data"""

    def reply(self):
        """ JSON response """
        response_json = None
        utility = getUtility(IDownloadToolUtility)
        response_json = utility.delete_data()
        if response_json.get("status", "") == "Error":
            self.request.response.setStatus(400)
            return response_json

        self.request.response.setStatus(204)
        return _no_content_marker
