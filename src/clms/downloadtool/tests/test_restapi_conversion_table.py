""" test that the @format_conversion_table endpoint works as expected"""
# -*- coding: utf-8 -*-
import unittest

from plone.app.testing import (
    SITE_OWNER_NAME,
    SITE_OWNER_PASSWORD,
    TEST_USER_ID,
    setRoles,
)
from plone.restapi.testing import RelativeSession

from clms.downloadtool.testing import CLMS_DOWNLOADTOOL_RESTAPI_TESTING
from clms.downloadtool.utils import FORMAT_CONVERSION_TABLE


class TestProjectionsEndpoint(unittest.TestCase):
    """ base class for testing """

    layer = CLMS_DOWNLOADTOOL_RESTAPI_TESTING

    def setUp(self):
        """ setup """
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

    def test_anonymous_user_cannot_access_format_conversion_table(self):
        """test anonymous user cannot access format_conversion_table
        endpoint"""
        response = self.anonymous_session.get("@format_conversion_table")
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 401)

    def test_user_can_access_format_conversion_table(self):
        """ test user can access format_conversion_table endpoint """
        response = self.api_session.get("@format_conversion_table")
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 200)

        for item in FORMAT_CONVERSION_TABLE:
            self.assertIn(item, response.json())
