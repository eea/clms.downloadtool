"""
Test the datarequest_status_patch endpoint
"""
# -*- coding: utf-8 -*-
import unittest

import transaction
from plone.app.testing import (
    SITE_OWNER_NAME,
    SITE_OWNER_PASSWORD,
    TEST_USER_ID,
    setRoles,
)
from plone.restapi.testing import RelativeSession
from zope.component import getUtility

from clms.downloadtool.testing import CLMS_DOWNLOADTOOL_RESTAPI_TESTING
from clms.downloadtool.utility import IDownloadToolUtility
from clms.downloadtool.utils import STATUS_LIST


class TestDatarequestPatch(unittest.TestCase):
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

        self.utility = getUtility(IDownloadToolUtility)
        data = {
            "DatasetID": "some-id",
            "OutputFormat": "Shapefile",
            "Status": "In_progress",
            "FMETaskID": 12345,
            "UserID": SITE_OWNER_NAME,
        }
        result = self.utility.datarequest_post(data)
        self.task_id = list(result.keys())[0]

        transaction.commit()

    def tearDown(self):
        """ tear down cleanup"""
        self.api_session.close()
        self.anonymous_session.close()

    def test_status_method_as_anonymous(self):
        """test anonymous user cannot access datarequest_status_patch
        endpoint"""
        data = {}
        response = self.anonymous_session.patch(
            "@datarequest_status_patch", json=data
        )
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 401)

    def test_patch_without_task_id(self):
        """ TaskID is a required parameter """
        data = {"ThisIsNotTaskID": 1}
        response = self.api_session.patch(
            "@datarequest_status_patch", json=data
        )
        self.assertEqual(response.status_code, 400)

    def test_patch_with_empty_task_id(self):
        """ TaskID is a required parameter """
        data = {"TaskID": ""}
        response = self.api_session.patch(
            "@datarequest_status_patch", json=data
        )
        self.assertEqual(response.status_code, 400)

    def test_patch_with_invalid_task_id(self):
        """ test update status of a datarequest """
        data = {"TaskID": "some-invalid-task-id", "Status": "Finished_ok"}
        response = self.api_session.patch(
            "@datarequest_status_patch", json=data
        )
        self.assertEqual(response.status_code, 404)

    def test_patch_without_status(self):
        """ status is a required parameter """
        data = {"TaskID": self.task_id}
        response = self.api_session.patch(
            "@datarequest_status_patch", json=data
        )
        self.assertEqual(response.status_code, 400)

    def test_patch_with_an_invalid_status(self):
        """ status must be in the list of allowed values """
        some_invalid_status = "some-invalid-status"
        data = {"TaskID": self.task_id, "Status": some_invalid_status}
        self.assertNotIn(some_invalid_status, STATUS_LIST)

        response = self.api_session.patch(
            "@datarequest_status_patch", json=data
        )
        self.assertEqual(response.status_code, 400)

    def test_update_status(self):
        """ test update status of a datarequest """
        data = {"TaskID": self.task_id, "Status": "Finished_ok"}

        self.assertIn("Finished_ok", STATUS_LIST)
        response = self.api_session.patch(
            "@datarequest_status_patch", json=data
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["Status"], "Finished_ok")

    def test_update_status_provide_filesize(self):
        """when FileSize parameter is provided, its value
        should be included"""

        data = {
            "TaskID": self.task_id,
            "Status": "Finished_ok",
            "FileSize": 1000000,
        }

        self.assertIn("Finished_ok", STATUS_LIST)
        response = self.api_session.patch(
            "@datarequest_status_patch", json=data
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["Status"], "Finished_ok")
        self.assertEqual(response.json()["FileSize"], 1000000)

    def test_update_status_provide_download_url(self):
        """when DownloadURL parameter is provided, its value
        should be included"""

        data = {
            "TaskID": self.task_id,
            "Status": "Finished_ok",
            "DownloadURL": "https://some.download.com/url",
        }

        self.assertIn("Finished_ok", STATUS_LIST)
        response = self.api_session.patch(
            "@datarequest_status_patch", json=data
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["Status"], "Finished_ok")
        self.assertEqual(
            response.json()["DownloadURL"], "https://some.download.com/url"
        )

    def test_update_status_provide_size_and_url(self):
        """when FileSize and DownloadURL parameters are provided,
        they should be included"""
        data = {
            "TaskID": self.task_id,
            "Status": "Finished_ok",
            "DownloadURL": "https://some.download.com/url",
            "FileSize": 1000000,
        }

        self.assertIn("Finished_ok", STATUS_LIST)
        response = self.api_session.patch(
            "@datarequest_status_patch", json=data
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["Status"], "Finished_ok")
        self.assertEqual(response.json()["FileSize"], 1000000)
        self.assertEqual(
            response.json()["DownloadURL"], "https://some.download.com/url"
        )
