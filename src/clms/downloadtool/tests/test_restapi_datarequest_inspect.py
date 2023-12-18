"""
tests of the @datarequest_inspect endpoint
"""
# -*- coding: utf-8 -*-
import base64
import unittest
from datetime import datetime

import transaction

from clms.downloadtool.testing import (
    CLMS_DOWNLOADTOOL_INTEGRATION_TESTING,
    CLMS_DOWNLOADTOOL_RESTAPI_TESTING,
)
from clms.downloadtool.utility import IDownloadToolUtility
from clms.downloadtool.utils import DATASET_FORMATS, GCS
from plone import api
from plone.app.testing import (
    SITE_OWNER_NAME,
    SITE_OWNER_PASSWORD,
    TEST_USER_ID,
    setRoles,
)
from plone.restapi.testing import RelativeSession
from zope.component import getUtility

FME_TASK_ID = 123456


def custom_ok_post_request_to_fme(self, params):
    """return a custom response for the post request to FME"""
    return FME_TASK_ID


def custom_not_ok_post_request_to_fme(self, params):
    """return a custom response for the post request to FME"""
    return None


class TestDatarequestPost(unittest.TestCase):
    """base class"""

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
                        "full_source": "EEA",
                        "wekeo_choices": "choice-1",
                        "layers": ["layer-1", "layer-2"],
                    }
                ]
            },
        )

        self.dataset2 = api.content.create(
            container=self.product,
            type="DataSet",
            title="DataSet 2",
            id="dataset2",
            geonetwork_identifiers={
                "items": [
                    {
                        "@id": "some-id-2",
                        "type": "VITO",
                        "id": "some-geonetwork-id-2",
                    }
                ]
            },
            dataset_download_information={
                "items": [
                    {
                        "@id": "id-2",
                        "full_format": "GDB",
                        "full_path": "/this/is/a/path/to/dataset2",
                        "full_source": "WEKEO",
                        "wekeo_choices": "choice-2",
                    }
                ]
            },
        )

        self.dataset3 = api.content.create(
            container=self.product,
            type="DataSet",
            title="DataSet 3",
            id="dataset3",
            downloadable_files={
                "items": [
                    {
                        "@id": "demo-id-1",
                        "path": "7path/to/file1",
                        "format": "GDB",
                        "source": "EEA",
                    },
                    {
                        "@id": "demo-id-2",
                        "path": "7path/to/file2",
                        "format": "Shapefile",
                        "source": "EEA",
                    },
                    {
                        "@id": "demo-id-3",
                        "path": "7path/to/file3",
                        "format": "Netcdf",
                        "source": "EEA",
                    },
                ]
            },
            dataset_download_information={
                "items": [
                    {
                        "@id": "id-3",
                        "full_format": "GDB",
                        "full_path": "/this/is/a/path/to/dataset3",
                        "full_source": "EEA",
                        "wekeo_choices": "choice-3",
                    }
                ]
            },
        )

        self.dataset4 = api.content.create(
            container=self.product,
            type="DataSet",
            title="DataSet 4",
            id="dataset4",
            mapviewer_istimeseries=True,
            geonetwork_identifiers={
                "items": [
                    {
                        "@id": "some-id-2",
                        "type": "VITO",
                        "id": "some-geonetwork-id-2",
                    }
                ]
            },
            dataset_download_information={
                "items": [
                    {
                        "@id": "id-2",
                        "full_format": "GDB",
                        "full_path": "/this/is/a/path/to/dataset2",
                        "full_source": "WEKEO",
                        "wekeo_choices": "choice-2",
                    }
                ]
            },
        )

        transaction.commit()

    def tearDown(self):
        """tear down cleanup"""
        self.api_session.close()
        self.anonymous_session.close()

    def test_status_method_as_anonymous(self):
        """test anonymous user cannot access datarequest_post endpoint"""
        data = {}
        response = self.anonymous_session.get("@datarequest_inspect")
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 401)

    def test_status_method_without_filters(self):
        """test anonymous user cannot access datarequest_post endpoint"""
        response = self.api_session.get(
            "@datarequest_inspect",
        )
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 400)

    def test_status_method_with_allowed_filters(self):
        """test anonymous user cannot access datarequest_post endpoint"""
        response = self.api_session.get(
            f"@datarequest_inspect?UserID={TEST_USER_ID}",
        )
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 200)

    def test_status_method_with_invalid_filters(self):
        """test anonymous user cannot access datarequest_post endpoint"""
        response = self.api_session.get(
            f"@datarequest_inspect?MyCustomFilter={TEST_USER_ID}",
        )
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 400)
