"""
The utility holds all operations of the download tool
"""
# -*- coding: utf-8 -*-
import unittest

from clms.downloadtool.testing import CLMS_DOWNLOADTOOL_INTEGRATION_TESTING
from clms.downloadtool.utility import IDownloadToolUtility
from zope.component import getUtility


class TestUtility(unittest.TestCase):
    """ base class for testing """

    layer = CLMS_DOWNLOADTOOL_INTEGRATION_TESTING

    def setUp(self):
        """ setup """
        self.portal = self.layer["portal"]
        self.utility = getUtility(IDownloadToolUtility)

    def test_datarequest_post(self):
        """ test datarequest_post method """
        data_dict = {"key1": "value1", "key2": "value2"}
        result = self.utility.datarequest_post(data_dict)
        self.assertEqual(list(result.values())[0], data_dict)
        key = list(result.keys())[0]
        self.assertEqual(self.utility.datarequest_status_get(key), data_dict)

    def test_datarequest_delete(self):
        """ test datarequest_delete method """
        data_dict_1 = {"key1": "value1", "key2": "value2", "UserID": "john"}
        result_1 = self.utility.datarequest_post(data_dict_1)
        key_1 = list(result_1.keys())[0]

        data_dict_2 = {"key1": "value1", "key2": "value2", "UserID": "mike"}
        result_2 = self.utility.datarequest_post(data_dict_2)

        result_1 = self.utility.datarequest_delete(key_1, "john")
        self.assertEqual(result_1["Status"], "Cancelled")

        result_2 = self.utility.datarequest_delete(key_1, "mike")
        self.assertEqual(result_2, "Error, permission denied")

        result_3 = self.utility.datarequest_delete("XXXXX", "john")
        self.assertEqual(result_3, "Error, TaskID not registered")

    def test_datarequest_delete_other_user_entry(self):
        """ test datarequest_delete method """
        data_dict_1 = {"key1": "value1", "key2": "value2", "UserID": "john"}
        result_1 = self.utility.datarequest_post(data_dict_1)
        key_1 = list(result_1.keys())[0]

        result_2 = self.utility.datarequest_delete(key_1, "mike")
        self.assertEqual(result_2, "Error, permission denied")

    def test_datarequest_delete_unexisting_entry(self):
        """ test datarequest_delete method """
        data_dict_1 = {"key1": "value1", "key2": "value2", "UserID": "john"}
        result_1 = self.utility.datarequest_post(data_dict_1)
        key_1 = list(result_1.keys())[0]

        new_key = "XXXX"

        self.assertNotEqual(key_1, new_key)

        result_3 = self.utility.datarequest_delete("XXXXX", "john")
        self.assertEqual(result_3, "Error, TaskID not registered")

    def test_datarequest_search(self):
        """ test datarequest_search method """
        data_dict_1 = {
            "key1": "value1",
            "key2": "value2",
            "UserID": "john",
            "Status": "In_progress",
        }
        self.utility.datarequest_post(data_dict_1)

        result = self.utility.datarequest_search("john", "In_progress")
        self.assertIsInstance(result, dict)

    def test_datarequest_search_no_results(self):
        """ test inexistent key search"""
        data_dict_1 = {
            "key1": "value1",
            "key2": "value2",
            "UserID": "john",
            "Status": "In_progress",
        }
        self.utility.datarequest_post(data_dict_1)

        result = self.utility.datarequest_search("john", "Cancelled")
        self.assertEqual(result, {})

    def test_datarequest_search_invalid_status(self):
        """ test datarequest_search method with an invalid status """
        result = self.utility.datarequest_search("john", "INVALID STATUS")
        self.assertEqual(result, "Error, status not recognized")

    def test_datarequest_search_valid_status_other_user(self):
        """ test datarequest_search method with some other user """
        data_dict_1 = {
            "key1": "value1",
            "key2": "value2",
            "UserID": "john",
            "Status": "In_progress",
        }
        self.utility.datarequest_post(data_dict_1)

        result = self.utility.datarequest_search("mike", "In_progress")
        self.assertEqual(result, {})

    def test_datarequest_search_without_status(self):
        """ test datarequest_search with an empty status """
        data_dict_1 = {
            "key1": "value1",
            "key2": "value2",
            "UserID": "john",
            "Status": "In_progress",
        }
        self.utility.datarequest_post(data_dict_1)

        data_dict_2 = {
            "key1": "value1",
            "key2": "value2",
            "UserID": "john",
            "Status": "Cancelled",
        }
        self.utility.datarequest_post(data_dict_2)

        result = self.utility.datarequest_search("john", "")
        self.assertEqual(len(result.keys()), 2)

    def test_datarequest_search_without_user(self):
        """ test datarequest_search with an empty user """
        data_dict_1 = {
            "key1": "value1",
            "key2": "value2",
            "UserID": "john",
            "Status": "In_progress",
        }
        self.utility.datarequest_post(data_dict_1)

        data_dict_2 = {
            "key1": "value1",
            "key2": "value2",
            "UserID": "john",
            "Status": "Cancelled",
        }
        self.utility.datarequest_post(data_dict_2)

        result = self.utility.datarequest_search("", "In_progress")
        self.assertEqual(result, "Error, UserID not defined")

    def test_datarequest_status_get(self):
        """ test datarequest_status_get method """
        data_dict_1 = {"key1": "value1", "key2": "value2", "UserID": "john"}
        result_1 = self.utility.datarequest_post(data_dict_1)
        key_1 = list(result_1.keys())[0]

        result = self.utility.datarequest_status_get(key_1)
        self.assertIsInstance(result, dict)

    def test_datarequest_status_get_unexisting_entry(self):
        """ test datarequest_status_get method with an unexisting entry """
        data_dict_1 = {"key1": "value1", "key2": "value2", "UserID": "john"}
        result_1 = self.utility.datarequest_post(data_dict_1)
        key_1 = list(result_1.keys())[0]

        new_key = "XXXX"

        self.assertNotEqual(key_1, new_key)
        result = self.utility.datarequest_status_get(new_key)
        self.assertEqual(result, "Error, task not found")

    def test_datarequest_status_patch(self):
        """ test datarequest_status_patch method """
        data_dict_1 = {
            "key1": "value1",
            "key2": "value2",
            "UserID": "john",
            "Status": "In_progress",
        }
        result_1 = self.utility.datarequest_post(data_dict_1)
        key_1 = list(result_1.keys())[0]

        data_dict_2 = {"Status": "Finished"}

        result = self.utility.datarequest_status_patch(data_dict_2, key_1)
        data_dict_1.update(data_dict_2)
        self.assertEqual(result, data_dict_1)

    def test_datarequest_status_patch_invalid_key(self):
        """ test datarequest_status_patch method with an invalid key """
        data_dict_1 = {
            "key1": "value1",
            "key2": "value2",
            "UserID": "john",
            "Status": "In_progress",
        }
        result_1 = self.utility.datarequest_post(data_dict_1)
        key_1 = list(result_1.keys())[0]

        new_key = "XXXX"
        self.assertNotEqual(key_1, new_key)

        data_dict_2 = {"Status": "Finished"}

        result = self.utility.datarequest_status_patch(data_dict_2, new_key)

        self.assertEqual(result, "Error, task_id not registered")

    def test_datarequest_status_patch_download_url(self):
        """ test datarequest_status_patch method with an invalid key """
        data_dict_1 = {
            "key1": "value1",
            "key2": "value2",
            "UserID": "john",
            "Status": "In_progress",
        }
        result_1 = self.utility.datarequest_post(data_dict_1)
        key_1 = list(result_1.keys())[0]

        data_dict_2 = {
            "Status": "Finished",
            "DownloadURL": "http://google.com",
        }

        result = self.utility.datarequest_status_patch(data_dict_2, key_1)
        data_dict_1.update(data_dict_2)
        self.assertEqual(data_dict_1, result)

    def test_datarequest_status_patch_file_size(self):
        """ test datarequest_status_patch method with an invalid key """
        data_dict_1 = {
            "key1": "value1",
            "key2": "value2",
            "UserID": "john",
            "Status": "In_progress",
        }
        result_1 = self.utility.datarequest_post(data_dict_1)
        key_1 = list(result_1.keys())[0]

        data_dict_2 = {"Status": "Finished", "FileSize": 20000}

        result = self.utility.datarequest_status_patch(data_dict_2, key_1)
        data_dict_1.update(data_dict_2)
        self.assertEqual(data_dict_1, result)

    def test_datarequest_status_patch_finalization_time(self):
        """ test datarequest_status_patch method with an invalid key """
        data_dict_1 = {
            "key1": "value1",
            "key2": "value2",
            "UserID": "john",
            "Status": "In_progress",
        }
        result_1 = self.utility.datarequest_post(data_dict_1)
        key_1 = list(result_1.keys())[0]

        data_dict_2 = {
            "Status": "Finished",
            "FinalizationDateTime": "2022-02-14T09:45:45.615188",
        }

        result = self.utility.datarequest_status_patch(data_dict_2, key_1)
        data_dict_1.update(data_dict_2)
        self.assertEqual(data_dict_1, result)

    def test_datarequest_status_patch_other_data(self):
        """ test datarequest_status_patch method with some other ignored data"""
        data_dict_1 = {
            "key1": "value1",
            "key2": "value2",
            "UserID": "john",
            "Status": "In_progress",
        }
        result_1 = self.utility.datarequest_post(data_dict_1)
        key_1 = list(result_1.keys())[0]

        data_dict_2 = {
            "OtherKey": "Other Value",
        }

        result = self.utility.datarequest_status_patch(data_dict_2, key_1)

        self.assertNotIn("OtherKey", result)

    def test_delete_data(self):
        """ test delete_data method """
        data_dict_1 = {"key1": "value1", "key2": "value2", "UserID": "john"}
        result_1 = self.utility.datarequest_post(data_dict_1)
        key_1 = list(result_1.keys())[0]
        result = self.utility.datarequest_status_get(key_1)
        self.assertIsInstance(result, dict)
        result = self.utility.delete_data()
        self.assertEqual(result, {})
        result = self.utility.datarequest_status_get(key_1)
        self.assertEqual(result, "Error, task not found")

    def test_delete_in_empty_registry(self):
        """ test delete_data method with an empty registry """
        result = self.utility.delete_data()
        self.assertIsInstance(result, dict)
        self.assertIn("status", result)
        self.assertIn("msg", result)
        self.assertEqual(result["status"], "Error")
        self.assertEqual(result["msg"], "Registry is empty")

    def test_remove_task(self):
        """ test removing a task"""
        data_dict = {"key1": "value1", "key2": "value2"}
        result = self.utility.datarequest_post(data_dict)
        self.assertEqual(list(result.values())[0], data_dict)
        key = list(result.keys())[0]
        result = self.utility.datarequest_remove_task(key)
        self.assertEqual(result, 1)

    def test_remove_unexisting_task(self):
        """ test removing an unexisting"""
        result = self.utility.datarequest_remove_task("unexisting-key")
        self.assertEqual(result, "Error, TaskID not registered")
