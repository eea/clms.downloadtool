# -*- coding: utf-8 -*-
"""
For HTTP GET operations we can use standard HTTP parameter passing
(through the URL)

"""
from logging import getLogger

from plone.restapi.services import Service
from zope.component import getUtility
from clms.downloadtool.utility import IDownloadToolUtility

# logger, do log.info('XXXX') to print in the console

log = getLogger(__name__)


class delete_data(Service):
    """Delete data
    """
    def reply(self):
        """ JSON response """
        response_json = None
        log.info("DELETE_DATA")
        utility = getUtility(IDownloadToolUtility)
        response_json = utility.delete_data()
        if response_json.get('status', "") == 'Error':
            self.request.response.setStatus(400)
        else:
            self.request.response.setStatus(204)
        return response_json
