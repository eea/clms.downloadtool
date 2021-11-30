# -*- coding: utf-8 -*-
"""
For HTTP GET operations we can use standard HTTP parameter passing
(through the URL)

"""
from logging import getLogger
from zope.component import getUtility

from plone.restapi.services import Service
from clms.downloadtool.utility import IDownloadToolUtility

# logger, do log.info('XXXX') to print in the console

log = getLogger(__name__)


class dataset_get(Service):
    """ Get datase info
    """
    def reply(self):
        """ JSON response """
        # key = self.request.get("key")
        log.info("DATASET_GET")
        log.info(self.request.get("dataset_title"))

        title = self.request.get("dataset_title")
        utility = getUtility(IDownloadToolUtility)

        self.request.response.setStatus(200)
        response_json = utility.dataset_get(title)

        if "Error, dataset not found" in response_json:
            self.request.response.setStatus(404)
            return {"status": "error", "msg": response_json}
        
        if "Error, there are no datasets to query" in response_json:
            self.request.response.setStatus(404)
            return {"status": "error", "msg": response_json}

        log.info(response_json)
        return response_json
