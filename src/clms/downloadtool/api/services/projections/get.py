# -*- coding: utf-8 -*-
"""
Endpoint to return the information of the FORMAT_CONVERSION_TABLE
"""
from clms.downloadtool.utils import GCS
from plone.restapi.services import Service

from ..utils import get_available_gcs_values


class GetProjectionsList(Service):
    """Get the projections list"""

    def reply(self):
        """ JSON endpoint """
        uid = self.request.form.get('uid', None)
        if uid is None:
            self.request.response.setStatus(200)
            return GCS

        self.request.response.setStatus(200)
        return get_available_gcs_values(uid)
