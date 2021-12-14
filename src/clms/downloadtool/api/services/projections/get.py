# -*- coding: utf-8 -*-
"""
Endpoint to return the information of the FORMAT_CONVERSION_TABLE
"""
from plone.restapi.services import Service

from clms.downloadtool.utils import GCS


class GetProjectionsList(Service):
    """Get the projections list"""

    def reply(self):
        """ JSON endpoint """

        self.request.response.setStatus(200)
        return GCS
