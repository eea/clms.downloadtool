# -*- coding: utf-8 -*-
"""
Endpoint to return the information of the FORMAT_CONVERSION_TABLE
"""
from plone.restapi.services import Service

from clms.downloadtool.api.services.utils import FORMAT_CONVERSION_TABLE


class GetFormatConversionTable(Service):
    """Get the format conversion table"""

    def reply(self):
        """ JSON endpoint """

        self.request.response.setStatus(200)
        return FORMAT_CONVERSION_TABLE
