""" test that the @projections endpoint works as expected"""
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
from clms.downloadtool.utils import GCS


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

    def test_anonymous_user_cannot_access_projections(self):
        """ test anonymous user cannot access projections endpoint """
        response = self.anonymous_session.get("@projections")
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 401)

    def test_user_can_access_projections(self):
        """ test user can access projections endpoint """
        response = self.api_session.get("@projections")
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 200)

        for item in GCS:
            self.assertIn(item, response.json())
