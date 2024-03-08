"""
Test the datarequest_delete endpoint
"""
# -*- coding: utf-8 -*-
import unittest

import transaction
from clms.downloadtool.api.services.datarequest_delete.delete import \
    datarequest_delete
from clms.downloadtool.testing import CLMS_DOWNLOADTOOL_RESTAPI_TESTING
from clms.downloadtool.utility import IDownloadToolUtility
from plone.app.testing import (SITE_OWNER_NAME, SITE_OWNER_PASSWORD,
                               TEST_USER_ID, setRoles)
from plone.restapi.testing import RelativeSession
from zope.component import getUtility
from clms.downloadtool.orm import Session, DownloadRegistry
from sqlalchemy import delete

class TestDatarequestDelete(unittest.TestCase):
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
        response = self.anonymous_session.delete("@datarequest_delete")
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 401)

    def test_delete_method_without_task_id(self):
        """ task_id is a required parameter """
        data = {"something": "else"}
        response = self.api_session.delete("@datarequest_delete", json=data)
        self.assertEqual(response.status_code, 400)
        result = response.json()
        self.assertIn("status", result)
        self.assertIn("msg", result)

    def test_delete_method_with_invalid_task_id(self):
        """ test delete method with an invalid task_id """
        new_task_id = "invalid_task_id"

        utility = getUtility(IDownloadToolUtility)
        result = utility.datarequest_status_get(new_task_id)
        self.assertEqual(result, "Error, task not found")

        data = {"TaskID": new_task_id}
        response = self.api_session.delete("@datarequest_delete", json=data)
        self.assertEqual(response.status_code, 403)
        result = response.json()
        self.assertIn("status", result)
        self.assertIn("msg", result)

    def test_delete_method_other_users_task(self):
        """ test delete method with a task created by another user """

        my_test_user_id = "my-test-user-id"
        self.assertNotEqual(my_test_user_id, SITE_OWNER_NAME)

        utility = getUtility(IDownloadToolUtility)
        data_dict_1 = {
            "Status": "In_progress",
            "Key1": "Value1",
            "UserID": my_test_user_id,
        }
        result = utility.datarequest_post(data_dict_1)
        key_1 = list(result.keys())[0]

        # transaction.commit()

        data = {"TaskID": key_1}
        response = self.api_session.delete("@datarequest_delete", json=data)
        self.assertEqual(response.status_code, 404)
        result = response.json()
        self.assertIn("status", result)
        self.assertIn("msg", result)

    def test_delete_method_with_valid_task_id_and_user(self):
        """ test delete method with a valid task_id and user """
        data_dict_1 = {
            "Status": "In_progress",
            "Key1": "Value1",
            "UserID": SITE_OWNER_NAME,
        }
        utility = getUtility(IDownloadToolUtility)
        result = utility.datarequest_post(data_dict_1)
        key_1 = list(result.keys())[0]

        # transaction.commit()

        data = {"UserID": SITE_OWNER_NAME, "TaskID": key_1}
        response = self.api_session.delete("@datarequest_delete", json=data)
        self.assertEqual(response.status_code, 204)

    def test_delete_method_with_valid_task_id_and_user_and_fme_task_id(self):
        """ test delete method with a valid task_id, user and FME task id """
        data_dict_1 = {
            "Status": "In_progress",
            "Key1": "Value1",
            "UserID": SITE_OWNER_NAME,
            "FMETaskId": "12345",
        }
        utility = getUtility(IDownloadToolUtility)
        result = utility.datarequest_post(data_dict_1)
        key_1 = list(result.keys())[0]
        fme_task_id = "12345"

        # We need to patch the internals of the delete endpoint
        # because the endpoint makes an external request to the FME server
        # to delete the task.
        #
        # To do this, we are patching the FME query to return whatever we want
        def my_signal_finalization_to_fme(self, task_id):
            return 1

        datarequest_delete.signal_finalization_to_fme = (
            my_signal_finalization_to_fme
        )

        # transaction.commit()

        data = {
            "UserID": SITE_OWNER_NAME,
            "TaskID": key_1,
            "FMETaskId": fme_task_id,
        }
        response = self.api_session.delete("@datarequest_delete", json=data)
        self.assertEqual(response.status_code, 204)
