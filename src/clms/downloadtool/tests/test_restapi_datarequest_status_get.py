"""
Test the datarequest_status_get endpoint
"""
# -*- coding: utf-8 -*-
import unittest

import transaction
from clms.downloadtool.testing import CLMS_DOWNLOADTOOL_RESTAPI_TESTING
from clms.downloadtool.utility import IDownloadToolUtility
from plone.app.testing import (SITE_OWNER_NAME, SITE_OWNER_PASSWORD,
                               TEST_USER_ID, setRoles)
from plone.restapi.testing import RelativeSession
from zope.component import getUtility


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

    def test_status_method_as_anonymous(self):
        """test anonymous user cannot access datarequest_status_get
        endpoint"""
        response = self.anonymous_session.get("@datarequest_status_get")
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 401)

    def test_status_method_without_task_id(self):
        """ task_id is a required parameter """
        data = {"something": "else"}
        response = self.api_session.get("@datarequest_status_get", json=data)
        self.assertEqual(response.status_code, 400)
        self.assertIn("status", response.json())

    def test_status_method_with_invalid_task_id(self):
        """ task_id is a required parameter """
        invalid_task_id = "not-valid-task-id"
        data = {"TaskID": invalid_task_id}
        utility = getUtility(IDownloadToolUtility)
        result = utility.datarequest_status_get(invalid_task_id)
        self.assertEqual(result, "Error, task not found")

        response = self.api_session.get("@datarequest_status_get", params=data)
        self.assertEqual(response.status_code, 404)

    def test_status_method_with_task_id(self):
        """ get the status of a task"""
        utility = getUtility(IDownloadToolUtility)
        data_dict = {"My": "Data"}
        result = utility.datarequest_post(data_dict)
        key = list(result.keys())[0]

        transaction.commit()
        response = self.api_session.get(
            "@datarequest_status_get", params={"TaskID": key}
        )
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["My"], "Data")
