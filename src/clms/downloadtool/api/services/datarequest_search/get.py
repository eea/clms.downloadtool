# -*- coding: utf-8 -*-
"""
For HTTP GET operations we can use standard HTTP parameter passing (through the URL)

"""
from plone import api
from plone.restapi.services import Service

from zope.component import getUtility
from clms.downloadtool.utility import IDownloadToolUtility

# logger, do log.info('XXXX') to print in the console
from logging import getLogger

log = getLogger(__name__)

#devuelve una lista de IDs que cumplen con los requisitos

class datarequest_search(Service):
    def reply(self):
        status_list = ["Rejected", "Queued", "In_progress", "Finished_ok", "Finished_nok", "Cancelled"]

        log.info('DATAREQUEST_SEARCH')
        utility = getUtility(IDownloadToolUtility)
        status = self.request.get("status")
        user_id = str(self.request.get("user_id"))
        bad = False

        try:
            user_id = int(user_id)
            if status and status not in status_list:
                raise ValueError("Status not recognized")
        except:
            log.info("BAD REQUEST INCOMING")
            bad = True

        if not bad:
            self.request.response.setStatus(200)
            response_json = utility.datarequest_search(user_id, status)
        else:
            self.request.response.setStatus(400)
            response_json = "BAD REQUEST"
        return response_json
