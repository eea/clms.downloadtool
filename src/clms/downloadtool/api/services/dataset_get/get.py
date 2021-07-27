# -*- coding: utf-8 -*-
"""
For HTTP GET operations we can use standard HTTP parameter passing
(through the URL)

"""
from logging import getLogger

from plone.restapi.services import Service

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

        value = self.request.get("dataset_title")

        if not value:
            self.request.response.setStatus(400)
            log.info("BAD REQUEST")
            response_json = "BAD REQUEST"
        else:
            self.request.response.setStatus(200)
            response_json = {"dataset_title": value}
        return response_json
