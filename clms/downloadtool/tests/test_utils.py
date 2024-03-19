"""
Test some util methods
"""
# -*- coding: utf-8 -*-
import unittest

from clms.downloadtool.api.services.utils import (clean,
                                                  get_available_gcs_values)
from clms.downloadtool.testing import CLMS_DOWNLOADTOOL_INTEGRATION_TESTING
from clms.downloadtool.utils import GCS, OTHER_AVAILABLE_GCS
from plone import api
from plone.app.testing import TEST_USER_ID, setRoles


class TestDownloadUtils(unittest.TestCase):
    """ base class for testing """

    layer = CLMS_DOWNLOADTOOL_INTEGRATION_TESTING

    def setUp(self):
        """ setup """
        self.portal = self.layer["portal"]
        setRoles(self.portal, TEST_USER_ID, ["Manager"])
        self.product = api.content.create(
            container=self.portal,
            type="Product",
            title="Product 1",
            id="product1",
        )
        self.dataset1 = api.content.create(
            container=self.product,
            type="DataSet",
            title="DataSet 1",
            id="dataset1",
            geonetwork_identifiers={
                "items": []
            },
            dataset_download_information={
                "items": [
                ]
            },
            characteristics_projection='EPSG:3035'
        )
        self.dataset2 = api.content.create(
            container=self.product,
            type="DataSet",
            title="DataSet 2",
            id="dataset2",
            geonetwork_identifiers={"items": []},
            dataset_download_information={"items": []},
            characteristics_projection="EPSG:32625/ 32626/ 32627/ ",
        )

        self.dataset3 = api.content.create(
            container=self.product,
            type="DataSet",
            title="DataSet 3",
            id="dataset3",
            geonetwork_identifiers={"items": []},
            dataset_download_information={"items": []},
            characteristics_projection="EPSG:9999/ 32626/ ",
        )

    def test_standard_epsg(self):
        """ test that for a dataset with just 1 EPSG code in
            characteristics, the returned ones are the standard ones
        """
        output_gcs = get_available_gcs_values(self.dataset1.UID())
        self.assertCountEqual(output_gcs, GCS)

    def test_other_epsg(self):
        """ test that a dataset with multiple projections in characteristics
            returns the list of the standard ones and the list of those codes
            that are in the other GCS list
        """
        output_gcs = get_available_gcs_values(self.dataset2.UID())
        self.assertCountEqual(
            output_gcs,
            GCS + ['EPSG:32625', 'EPSG:32626', 'EPSG:32627']
        )
        self.assertIn("EPSG:32625", OTHER_AVAILABLE_GCS)
        self.assertIn("EPSG:32626", OTHER_AVAILABLE_GCS)
        self.assertIn("EPSG:32627", OTHER_AVAILABLE_GCS)

    def test_other_epsg_not_in_list(self):
        """ test that when a datasets lists multiple projections and one of
            those is not in the other list, it is not returned
        """
        output_gcs = get_available_gcs_values(self.dataset3.UID())
        self.assertCountEqual(
            output_gcs, GCS + ["EPSG:32626"]
        )
        self.assertIn("EPSG:32626", OTHER_AVAILABLE_GCS)
        self.assertNotIn("EPSG:9999", OTHER_AVAILABLE_GCS)


class TestUtils(unittest.TestCase):
    """ test some utility functions"""

    def test_epsg_leave_as_it_is(self):
        """ test that a correctly written EPSG code is left as it is"""
        value = clean('EPSG:3035')
        self.assertEqual(value, 'EPSG:3035')

    def test_remove_trailing_space(self):
        """test that we remove the trailing space"""
        value = clean("EPSG:3035 ")
        self.assertEqual(value, "EPSG:3035")

    def test_add_epsg_letters(self):
        """ the code needs to have the EPSG: letters in front"""
        value = clean("3035")
        self.assertEqual(value, "EPSG:3035")

    def test_add_epsg_letters_remove_trailing_space(self):
        """the code needs to have the EPSG: letters in front and removed
            any trailing space
        """
        value = clean("3035 ")
        self.assertEqual(value, "EPSG:3035")
