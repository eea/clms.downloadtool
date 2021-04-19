# -*- coding: utf-8 -*-
"""
For HTTP GET operations we can use standard HTTP parameter passing (through the URL)

"""
from plone import api
from plone.restapi.services import Service

from zope.component import getUtility
from clms.downloadtool.utility import IDownloadToolUtility


class GetItem(Service):
    def reply(self):
        key = self.request.get("key")

        utility = getUtility(IDownloadToolUtility)
        value = utility.get_item(key)

        self.request.response.setStatus(200)
        return {key: value}
