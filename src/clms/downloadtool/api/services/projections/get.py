# -*- coding: utf-8 -*-
"""
Endpoint to return the information of the FORMAT_CONVERSION_TABLE
"""
from clms.downloadtool.utils import GCS
from plone.restapi.services import Service


class GetProjectionsList(Service):
    """Get the projections list"""

    def reply(self):
        """ JSON endpoint """

        self.request.response.setStatus(200)
        return GCS
