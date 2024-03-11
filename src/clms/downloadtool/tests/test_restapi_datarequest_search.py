"""
Test the datarequest_status_search endpoint
"""
# -*- coding: utf-8 -*-
import unittest

import transaction
from clms.downloadtool.testing import CLMS_DOWNLOADTOOL_RESTAPI_TESTING
from clms.downloadtool.utility import IDownloadToolUtility
from clms.downloadtool.utils import STATUS_LIST
from plone.app.testing import (SITE_OWNER_NAME, SITE_OWNER_PASSWORD,
                               TEST_USER_ID, setRoles)
from plone.restapi.testing import RelativeSession
from zope.component import getUtility
from clms.downloadtool.orm import Session, DownloadRegistry
from sqlalchemy import delete

class TestDatarequestStatusGet(unittest.TestCase):
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

    def test_status_method_as_anonymous(self):
        """test anonymous user cannot access datarequest_status_get
        endpoint"""
        response = self.anonymous_session.get("@datarequest_search")
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 401)

    def test_status_method_without_status(self):
        """ status is a required parameter """

        response = self.api_session.get("@datarequest_search")
        self.assertEqual(response.status_code, 200)

    def test_status_method_with_invalid_status(self):
        """ status is a required parameter and must be a real status """

        invalid_status = "invalid_status"
        self.assertNotIn(invalid_status, STATUS_LIST)

        response = self.api_session.get(
            "@datarequest_search", params={"status": invalid_status}
        )
        self.assertEqual(response.status_code, 400)
        result = response.json()
        self.assertIn("status", result)

    def test_status_method_with_valid_status(self):
        """ status is a required parameter and must be a real status """
        utility = getUtility(IDownloadToolUtility)
        data_dict_1 = {"Status": "In_progress", "UserID": SITE_OWNER_NAME}
        data_dict_2 = {"Status": "Cancelled", "UserID": SITE_OWNER_NAME}
        data_dict_3 = {"Status": "In_progress", "UserID": SITE_OWNER_NAME}
        data_dict_4 = {"Status": "In_progress", "UserID": "test_user_id"}
        utility.datarequest_post(data_dict_1)
        utility.datarequest_post(data_dict_2)
        utility.datarequest_post(data_dict_3)
        utility.datarequest_post(data_dict_4)

        transaction.commit()

        response = self.api_session.get(
            "@datarequest_search", params={"status": "In_progress"}
        )
        self.assertEqual(response.status_code, 200)

        result = response.json()
        self.assertEqual(len(result.keys()), 2)

        response = self.api_session.get(
            "@datarequest_search", params={"status": "Cancelled"}
        )
        self.assertEqual(response.status_code, 200)
        result = response.json()
        self.assertEqual(len(result.keys()), 1)

        response = self.api_session.get(
            "@datarequest_search", params={"status": "Rejected"}
        )
        self.assertEqual(response.status_code, 200)
        result = response.json()
        self.assertEqual(len(result.keys()), 0)
