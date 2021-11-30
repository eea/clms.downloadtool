# -*- coding: utf-8 -*-
"""
For HTTP GET operations we can use standard HTTP parameter passing
(through the URL)

"""
from logging import getLogger

from plone import api
from plone.restapi.services import Service
import datetime
from zope.component import getUtility
from clms.downloadtool.utility import IDownloadToolUtility

# logger, do log.info('XXXX') to print in the console


log = getLogger(__name__)


class user_get(Service):
    """ Get authenticated data
    """
    def reply(self):
        """ JSON response """
        utility = getUtility(IDownloadToolUtility)
        user = str(api.user.get_current())
        log.info(user)
        user_data = utility.get_user(user)
        log.info(user_data)
        #last_connection = user.getProperty('login')
        if not user:
            return "Error, User not defined"
        self.request.response.setStatus(200)
        return user_data
