"""
Test the delete_data endpoint
"""
# -*- coding: utf-8 -*-
import unittest

import transaction
from clms.downloadtool.api.services.auxiliary_api import main
from clms.downloadtool.testing import CLMS_DOWNLOADTOOL_RESTAPI_TESTING
from plone import api
from plone.app.testing import (SITE_OWNER_NAME, SITE_OWNER_PASSWORD,
                               TEST_USER_ID, setRoles)
from plone.restapi.testing import RelativeSession


# patch for tests
def my_get_legacy(*args, **kargs):
    """ custom function to get urls from legacy
        that returns a list instead of going to the FTP or HTTP
        services
    """
    return []

main.get_legacy = my_get_legacy


class TestAuxiliaryAPI(unittest.TestCase):
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
                "items": [
                    {
                        "@id": "some-id",
                        "type": "EEA",
                        "id": "some-geonetwork-id",
                    }
                ]
            },
            dataset_download_information={
                "items": [
                    {
                        "@id": "id-1",
                        "full_format": "Netcdf",
                        "full_path": "/this/is/a/path/to/dataset1",
                        "full_source": "LEGACY",
                        "layers": [],
                    }
                ]
            },
        )

        transaction.commit()

    def test_anonymous_usage(self):
        """ test anonymous usage"""
        response = self.anonymous_session.get(
            f"@get-download-file-urls?dataset_uid={self.dataset1.UID()}"
        )
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 401)

    def test_no_dataset_uid(self):
        """ dataset_uid is required """
        response = self.api_session.get(
            "@get-download-file-urls"
        )
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 400)

    def test_invalid_uid(self):
        """dataset_uid is required"""
        response = self.api_session.get(
            "@get-download-file-urls?dataset_uid=invaliduid"
        )
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 400)

    def test_no_dataset_information_uid(self):
        """dataset_uid is required"""
        response = self.api_session.get(
            f"@get-download-file-urls?dataset_uid={self.dataset1.UID()}"
        )
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 400)

    def test_invalid_download_information_id(self):
        """dataset_uid is required"""
        response = self.api_session.get(
            # pylint: disable=line-too-long
            f"@get-download-file-urls?dataset_uid={self.dataset1.UID()}&download_information_id=invalid"
        )
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 400)

    def test_legacy_no_dates(self):
        """test anonymous usage"""
        response = self.api_session.get(
            # pylint: disable=line-too-long
            f"@get-download-file-urls?dataset_uid={self.dataset1.UID()}&download_information_id=id-1"
        )
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 400)

    def test_legacy_no_date_to(self):
        """test anonymous usage"""
        response = self.api_session.get(
            # pylint: disable=line-too-long
            f"@get-download-file-urls?dataset_uid={self.dataset1.UID()}&download_information_id=id-1&date_from=2022/04/04"
        )
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 400)

    def test_legacy_no_date_from(self):
        """test anonymous usage"""
        response = self.api_session.get(
            # pylint: disable=line-too-long
            f"@get-download-file-urls?dataset_uid={self.dataset1.UID()}&download_information_id=id-1&date_to=2022/04/04"
        )
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 400)

    def test_legacy_incorrect_date_to(self):
        """test anonymous usage"""
        response = self.api_session.get(
            # pylint: disable=line-too-long
            f"@get-download-file-urls?dataset_uid={self.dataset1.UID()}&download_information_id=id-1&date_to=johndoe&date_from=2022/04/04"
        )
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 400)

    def test_legacy_incorrect_date_from(self):
        """test anonymous usage"""
        response = self.api_session.get(
            # pylint: disable=line-too-long
            f"@get-download-file-urls?dataset_uid={self.dataset1.UID()}&download_information_id=id-1&date_to=2022/04/04&date_from=johndoe"
        )
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 400)

    def test_legacy_incorrect_dates(self):
        """test anonymous usage"""
        response = self.api_session.get(
            # pylint: disable=line-too-long
            f"@get-download-file-urls?dataset_uid={self.dataset1.UID()}&download_information_id=id-1&date_to=janedoe&date_from=johndoe"
        )
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 400)

    def test_legacy_incorrect_date_to_less_than_date_from(self):
        """test anonymous usage"""
        response = self.api_session.get(
            # pylint: disable=line-too-long
            f"@get-download-file-urls?dataset_uid={self.dataset1.UID()}&download_information_id=id-1&date_to=2022-04-01&date_from=2022-04-30"
        )
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 400)

    def test_legacy_download(self):
        """test anonymous usage"""

        response = self.api_session.get(
            # pylint: disable=line-too-long
            f"@get-download-file-urls?dataset_uid={self.dataset1.UID()}&download_information_id=id-1&date_from=2022-04-01&date_to=2022-04-30"
        )
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), [])

    def tearDown(self):
        """ tear down cleanup"""
        self.api_session.close()
        self.anonymous_session.close()
