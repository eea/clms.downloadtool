"""
Test the delete_data endpoint
"""
# -*- coding: utf-8 -*-
import unittest

from clms.downloadtool.orm import DownloadRegistry, Session
from clms.downloadtool.testing import CLMS_DOWNLOADTOOL_RESTAPI_TESTING
from plone.app.testing import (SITE_OWNER_NAME, SITE_OWNER_PASSWORD,
                               TEST_USER_ID, setRoles)
from plone.restapi.testing import RelativeSession
from sqlalchemy import delete


class TestDeleteData(unittest.TestCase):
    """ base class"""

    layer = CLMS_DOWNLOADTOOL_RESTAPI_TESTING

    def setUp(self):
        """Set up the test."""
        self.portal = self.layer["portal"]
        self.portal_url = self.portal.absolute_url()
        setRoles(self.portal, TEST_USER_ID, ["Manager"])
        self.api_session = RelativeSession(self.portal_url)
        self.api_session.headers.update({"Accept": "application/json"})
        self.api_session.auth = (SITE_OWNER_NAME, SITE_OWNER_PASSWORD)

        self.anonymous_session = RelativeSession(self.portal_url)
        self.anonymous_session.headers.update({"Accept": "application/json"})

    def tearDown(self):
        """ tear down cleanup"""
        self.api_session.close()
        self.anonymous_session.close()
        session = Session()
        session.execute(delete(DownloadRegistry))

    def test_delete_method_as_anonymous(self):
        """ test anonymous user cannot access datarequest_delete endpoint """
        response = self.anonymous_session.delete("@delete_data")
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 401)

    # def test_delete_empty(self):
    #     """ test deleting an empty download registry"""
    #     response = self.api_session.delete("@delete_data")
    #     self.assertEqual(response.status_code, 400)

    # def test_delete_all_data(self):
    #     """ delete all existing data"""
    #     utility = getUtility(IDownloadToolUtility)
    #     data_dict = {
    #         "UserId": "test_user",
    #         "TaskId": "test_task",
    #         "Status": "In_Progress",
    #     }
    #     utility.datarequest_post(data_dict)
    #     utility.datarequest_post(data_dict)
    #     utility.datarequest_post(data_dict)

    #     transaction.commit()

    #     response = self.api_session.delete("@delete_data")
    #     self.assertEqual(response.status_code, 204)
