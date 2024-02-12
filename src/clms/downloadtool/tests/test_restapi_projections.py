""" test that the @projections endpoint works as expected"""

# -*- coding: utf-8 -*-
import unittest

from clms.downloadtool.testing import CLMS_DOWNLOADTOOL_RESTAPI_TESTING
from clms.downloadtool.utils import GCS
from plone.app.testing import (
    SITE_OWNER_NAME,
    SITE_OWNER_PASSWORD,
    TEST_USER_ID,
    setRoles,
)
from plone.restapi.testing import RelativeSession
from plone import api
import transaction


class TestProjectionsEndpoint(unittest.TestCase):
    """base class for testing"""

    layer = CLMS_DOWNLOADTOOL_RESTAPI_TESTING

    def setUp(self):
        """setup"""
        self.portal = self.layer["portal"]
        self.portal_url = self.portal.absolute_url()
        setRoles(self.portal, TEST_USER_ID, ["Manager"])

        self.api_session = RelativeSession(self.portal_url)
        self.api_session.headers.update({"Accept": "application/json"})
        self.api_session.auth = (SITE_OWNER_NAME, SITE_OWNER_PASSWORD)

        self.anonymous_session = RelativeSession(self.portal_url)
        self.anonymous_session.headers.update({"Accept": "application/json"})

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
            geonetwork_identifiers={"items": []},
            dataset_download_information={"items": []},
            characteristics_projection="EPSG:3035",
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

        transaction.commit()

    def tearDown(self):
        """tear down cleanup"""
        self.api_session.close()
        self.anonymous_session.close()

    def test_anonymous_user_cannot_access_projections(self):
        """test anonymous user cannot access projections endpoint"""
        response = self.anonymous_session.get("@projections")
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 401)

    def test_user_can_access_projections(self):
        """test user can access projections endpoint"""
        response = self.api_session.get("@projections")
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 200)

        for item in GCS:
            self.assertIn(item, response.json())

    def test_projections_with_dataset1_uid(self):
        """test that when calling the endpoint with the dataset_uid
        it returns only the projections in the dataset.
        """
        response = self.api_session.get(
            f"@projections?uid={self.dataset1.UID()}"
        )
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 200)

        self.assertCountEqual(GCS, response.json())

    def test_projections_with_dataset2_uid(self):
        """test that when calling the endpoint with the dataset_uid
        it returns only the projections in the dataset.
        """
        response = self.api_session.get(
            f"@projections?uid={self.dataset2.UID()}"
        )
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 200)

        self.assertCountEqual(GCS + ['EPSG:32625', 'EPSG:32626', 'EPSG:32627'], response.json())

    def test_projections_with_dataset3_uid(self):
        """test that when calling the endpoint with the dataset_uid
        it returns only the projections in the dataset.
        """
        response = self.api_session.get(
            f"@projections?uid={self.dataset3.UID()}"
        )
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 200)

        self.assertCountEqual(
            GCS + ["EPSG:32626"], response.json()
        )
