"""
Test the datarequest_post endpoint
"""
# -*- coding: utf-8 -*-
import base64
import unittest
from datetime import datetime

import transaction
from clms.downloadtool.api.services.datarequest_post.post import (
    DataRequestPost,
    base64_encode_path,
    extract_dates_from_temporal_filter,
    get_dataset_file_path_from_file_id,
    get_full_dataset_format,
    get_full_dataset_layers,
    get_full_dataset_path,
    get_full_dataset_source,
    get_full_dataset_wekeo_choices,
    validate_nuts,
    validate_spatial_extent,
)
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
                        "layers": [],
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

        self.dataset5 = api.content.create(
            container=self.product,
            type="DataSet",
            title="DataSet 5",
            id="dataset5",
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

        transaction.commit()

    def tearDown(self):
        """tear down cleanup"""
        self.api_session.close()
        self.anonymous_session.close()

    def test_status_method_as_anonymous(self):
        """test anonymous user cannot access datarequest_post endpoint"""
        data = {}
        response = self.anonymous_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )
        self.assertEqual(response.status_code, 401)

    def test_eea_full_dataset_download(self):
        """test to download a EEA full dataset"""

        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset1.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                }
            ]
        }
        # Patch FME call to return an OK response
        DataRequestPost.post_request_to_fme = custom_ok_post_request_to_fme

        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 400)

    def test_non_eea_full_dataset_download(self):
        """test to download a non-EEA full dataset"""
        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset2.UID(),
                    "DatasetDownloadInformationID": "id-2",
                    "OutputFormat": "GDB",
                    "OutputGCS": "EPSG:4326",
                },
            ]
        }
        # Patch FME call to return an OK response
        DataRequestPost.post_request_to_fme = custom_ok_post_request_to_fme

        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 400)

    def test_nuts_restriction(self):
        """test post with valid data"""

        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset1.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                    "NUTS": "BE",
                },
                {
                    "DatasetID": self.dataset2.UID(),
                    "DatasetDownloadInformationID": "id-2",
                    "OutputFormat": "GDB",
                    "OutputGCS": "EPSG:4326",
                    "NUTS": "ITC11",
                },
            ]
        }
        # Patch FME call to return an OK response
        DataRequestPost.post_request_to_fme = custom_ok_post_request_to_fme

        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 201)
        self.assertIn("TaskIds", response.json())
        self.assertTrue(len(response.json()["TaskIds"]), 1)

    def test_bbox_restriction(
        self,
    ):
        """test post with valid data"""

        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset1.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                    "BoundingBox": [
                        2.354736328128108,
                        46.852958688910306,
                        4.639892578127501,
                        45.88264619696234,
                    ],
                },
                {
                    "DatasetID": self.dataset2.UID(),
                    "DatasetDownloadInformationID": "id-2",
                    "OutputFormat": "GDB",
                    "OutputGCS": "EPSG:4326",
                    "BoundingBox": [
                        2.354736328128108,
                        46.852958688910306,
                        4.639892578127501,
                        45.88264619696234,
                    ],
                },
            ]
        }
        # Patch FME call to return an OK response
        DataRequestPost.post_request_to_fme = custom_ok_post_request_to_fme

        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 201)
        self.assertIn("TaskIds", response.json())
        self.assertTrue(len(response.json()["TaskIds"]), 1)

    def test_nuts_and_bbox_restriction(self):
        """test post with valid data"""

        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset1.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                    "NUTS": "ITC11",
                },
                {
                    "DatasetID": self.dataset2.UID(),
                    "DatasetDownloadInformationID": "id-2",
                    "OutputFormat": "GDB",
                    "OutputGCS": "EPSG:4326",
                    "BoundingBox": [
                        2.354736328128108,
                        46.852958688910306,
                        4.639892578127501,
                        45.88264619696234,
                    ],
                },
            ]
        }
        # Patch FME call to return an OK response
        DataRequestPost.post_request_to_fme = custom_ok_post_request_to_fme

        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 201)
        self.assertIn("TaskIds", response.json())
        self.assertTrue(len(response.json()["TaskIds"]), 1)

    def test_temporal_restriction(self):
        """test post with valid data"""

        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset4.UID(),
                    "DatasetDownloadInformationID": "id-2",
                    "OutputFormat": "GDB",
                    "OutputGCS": "EPSG:4326",
                    "TemporalFilter": {
                        "StartDate": 1546333200000,
                        "EndDate": 1547974800000,
                    },
                }
            ]
        }
        # Patch FME call to return an OK response
        DataRequestPost.post_request_to_fme = custom_ok_post_request_to_fme

        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 201)
        self.assertIn("TaskIds", response.json())
        self.assertTrue(len(response.json()["TaskIds"]), 1)

    def test_nuts_and_temporal_restriction(self):
        """test post with valid data"""

        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset4.UID(),
                    "DatasetDownloadInformationID": "id-2",
                    "OutputFormat": "GDB",
                    "OutputGCS": "EPSG:4326",
                    "NUTS": "ITC11",
                    "TemporalFilter": {
                        "StartDate": 1546333200000,
                        "EndDate": 1547974800000,
                    },
                }
            ]
        }
        # Patch FME call to return an OK response
        DataRequestPost.post_request_to_fme = custom_ok_post_request_to_fme

        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 201)
        self.assertIn("TaskIds", response.json())
        self.assertTrue(len(response.json()["TaskIds"]), 1)

    def test_bbox_and_temporal_restriction(self):
        """test post with valid data"""

        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset4.UID(),
                    "DatasetDownloadInformationID": "id-2",
                    "OutputFormat": "GDB",
                    "OutputGCS": "EPSG:4326",
                    "TemporalFilter": {
                        "StartDate": 1546333200000,
                        "EndDate": 1547974800000,
                    },
                    "BoundingBox": [
                        2.354736328128108,
                        46.852958688910306,
                        4.639892578127501,
                        45.88264619696234,
                    ],
                },
            ]
        }
        # Patch FME call to return an OK response
        DataRequestPost.post_request_to_fme = custom_ok_post_request_to_fme

        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 201)
        self.assertIn("TaskIds", response.json())
        self.assertTrue(len(response.json()["TaskIds"]), 1)

    def test_combined_restrictions(self):
        """test post with valid data"""

        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset4.UID(),
                    "DatasetDownloadInformationID": "id-2",
                    "OutputFormat": "GDB",
                    "OutputGCS": "EPSG:4326",
                    "BoundingBox": [
                        2.354736328128108,
                        46.852958688910306,
                        4.639892578127501,
                        45.88264619696234,
                    ],
                    "TemporalFilter": {
                        "StartDate": 1546333200000,
                        "EndDate": 1547974800000,
                    },
                },
                {
                    "DatasetID": self.dataset4.UID(),
                    "DatasetDownloadInformationID": "id-2",
                    "OutputFormat": "GDB",
                    "OutputGCS": "EPSG:4326",
                    "NUTS": "BE",
                    "TemporalFilter": {
                        "StartDate": 1546333200000,
                        "EndDate": 1547974800000,
                    },
                },
                {
                    "DatasetID": self.dataset2.UID(),
                    "DatasetDownloadInformationID": "id-2",
                    "OutputFormat": "GDB",
                    "OutputGCS": "EPSG:4326",
                    "NUTS": "ES",
                },
            ]
        }
        # Patch FME call to return an OK response
        DataRequestPost.post_request_to_fme = custom_ok_post_request_to_fme

        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 201)
        self.assertIn("TaskIds", response.json())
        self.assertTrue(len(response.json()["TaskIds"]), 1)

    def test_invalid_fme_response(self):
        """when FME fails, it must return an error"""
        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset4.UID(),
                    "DatasetDownloadInformationID": "id-2",
                    "OutputFormat": "GDB",
                    "OutputGCS": "EPSG:4326",
                    "BoundingBox": [
                        2.354736328128108,
                        46.852958688910306,
                        4.639892578127501,
                        45.88264619696234,
                    ],
                    "TemporalFilter": {
                        "StartDate": 1546333200000,
                        "EndDate": 1547974800000,
                    },
                },
                {
                    "DatasetID": self.dataset2.UID(),
                    "DatasetDownloadInformationID": "id-2",
                    "OutputFormat": "GDB",
                    "OutputGCS": "EPSG:4326",
                    "NUTS": "ES",
                },
            ]
        }
        # Patch FME call to return an OK response
        DataRequestPost.post_request_to_fme = custom_not_ok_post_request_to_fme

        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 500)
        self.assertIn("status", response.json())

    def test_no_datasets_in_request(self):
        """test post with no dataset info"""
        data = {
            "Datasets": [
                {
                    # No DatasetID
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                    "BoundingBox": [
                        2.354736328128108,
                        46.852958688910306,
                        4.639892578127501,
                        45.88264619696234,
                    ],
                    "TemporalFilter": {
                        "StartDate": 1546333200000,
                        "EndDate": 1559289600000,
                    },
                },
                {
                    # DatasetID is None
                    "DatasetID": None,
                    "DatasetDownloadInformationID": "id-2",
                    "OutputFormat": "GDB",
                    "OutputGCS": "EPSG:4326",
                    "NUTS": "BE",
                    "TemporalFilter": {
                        "StartDate": 1546333200000,
                        "EndDate": 1559289600000,
                    },
                },
                {
                    # DatasetID is empty
                    "DatasetID": "",
                    "OutputFormat": "GDB",
                    "OutputGCS": "EPSG:4326",
                    "NUTS": "ES",
                },
            ]
        }

        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("status", response.json())

    def test_invalid_dataset_id(self):
        """test post with no dataset info"""
        invalid_dataset_id = "invalid-dataset-id"
        data = {
            "Datasets": [
                {
                    "DatasetID": invalid_dataset_id,
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                    "BoundingBox": [
                        2.354736328128108,
                        46.852958688910306,
                        4.639892578127501,
                        45.88264619696234,
                    ],
                    "TemporalFilter": {
                        "StartDate": 1546333200000,
                        "EndDate": 1559289600000,
                    },
                },
            ]
        }

        self.assertNotEqual(invalid_dataset_id, self.dataset1.UID())
        self.assertNotEqual(invalid_dataset_id, self.dataset2.UID())

        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("status", response.json())

    def test_invalid_nuts(self):
        """test request with an invalid NUTS code"""
        invalid_nuts_code = "8937834"
        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset1.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                    "NUTS": invalid_nuts_code,
                },
            ]
        }

        self.assertFalse(validate_nuts(invalid_nuts_code))
        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("status", response.json())

    def test_invalid_bbox(self):
        """test request with an invalid bbox"""
        invalid_bbox = [0, 0, 0, "foo"]
        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset1.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                    "BoundingBox": invalid_bbox,
                },
            ]
        }

        self.assertFalse(validate_spatial_extent(invalid_bbox))
        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("status", response.json())

    def test_too_big_bbox(self):
        """test that too big bounding box creates an error"""
        bbox = [51.463116, 11.145046, 33.15542, -16.542289]
        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset1.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                    "BoundingBox": bbox,
                },
            ]
        }

        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("status", response.json())

    def test_bbox_and_nuts_are_mutually_exclusive(self):
        """test request with a bbox and a NUTS code"""
        valid_bbox = [1, 43, 2, 44]
        valid_nuts = "ITC11"
        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset1.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                    "BoundingBox": valid_bbox,
                    "NUTS": valid_nuts,
                },
            ]
        }
        self.assertTrue(validate_spatial_extent(valid_bbox))
        self.assertTrue(validate_nuts(valid_nuts))
        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("status", response.json())

    def test_invalid_temporal_filter(self):
        """test request with an invalid temporal filter"""
        invalid_temporal_filter = {
            "StartDate": "foo",
            "EndDate": "bar",
        }
        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset1.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                    "TemporalFilter": invalid_temporal_filter,
                },
            ]
        }

        start, end = extract_dates_from_temporal_filter(
            invalid_temporal_filter
        )
        self.assertIsNone(start)
        self.assertIsNone(end)
        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("status", response.json())

    def test_invalid_temporal_filter_too_many_keys(self):
        """test request with an invalid temporal filter"""
        invalid_temporal_filter = {
            "StartDate": 1546333200000,
            "EndDate": 1559289600000,
            "foo": "bar",
        }
        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset1.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                    "TemporalFilter": invalid_temporal_filter,
                },
            ]
        }

        start, end = extract_dates_from_temporal_filter(
            invalid_temporal_filter
        )
        self.assertIsNotNone(start)
        self.assertIsNotNone(end)
        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("status", response.json())

    def test_invalid_temporal_filter_only_start(self):
        """test request with an invalid temporal filter"""
        invalid_temporal_filter = {
            "StartDate": 1559289600000,
        }
        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset1.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                    "TemporalFilter": invalid_temporal_filter,
                },
            ]
        }

        start, end = extract_dates_from_temporal_filter(
            invalid_temporal_filter
        )
        self.assertIsNone(start)
        self.assertIsNone(end)
        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("status", response.json())

    def test_invalid_temporal_filter_only_end(self):
        """test request with an invalid temporal filter"""
        invalid_temporal_filter = {
            "EndDate": 1559289600000,
        }
        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset1.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                    "TemporalFilter": invalid_temporal_filter,
                },
            ]
        }

        start, end = extract_dates_from_temporal_filter(
            invalid_temporal_filter
        )
        self.assertIsNone(start)
        self.assertIsNone(end)
        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("status", response.json())

    def test_invalid_temporal_filter_end_before_start(self):
        """test request with an invalid temporal filter"""
        invalid_temporal_filter = {
            "StartDate": 1559289600000,
            "EndDate": 1559289500000,
        }
        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset1.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                    "TemporalFilter": invalid_temporal_filter,
                },
            ]
        }

        start, end = extract_dates_from_temporal_filter(
            invalid_temporal_filter
        )
        self.assertIsNotNone(start)
        self.assertIsNotNone(end)
        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("status", response.json())

    def test_invalid_output_projection(self):
        """test request with an invalid output projection"""
        invalid_projection = "this-is-not-a-projection"
        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset1.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": invalid_projection,
                },
            ]
        }

        self.assertNotIn(invalid_projection, GCS)
        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("status", response.json())

    def test_invalid_output_format(self):
        """test request with an invalid output format"""
        invalid_format = "this-is-not-a-format"
        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset1.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": invalid_format,
                },
            ]
        }

        self.assertNotIn(invalid_format, DATASET_FORMATS)
        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("status", response.json())

    def test_invalid_transformation(self):
        """test request with an invalid output format"""
        # For instance: netcdf -> GDB is not valid
        gdb_format = "GDB"
        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset1.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": gdb_format,
                },
            ]
        }

        self.assertIn(gdb_format, DATASET_FORMATS)
        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("status", response.json())

    def test_invalid_download_information_id(self):
        """test request with an invalid download information id"""
        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset1.UID(),
                    "DatasetDownloadInformationID": "this-is-an-invalid-id",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                },
            ]
        }
        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(response.status_code, 400)
        self.assertIn("status", response.json())

    def test_download_with_layer(self):
        """test a download request with a band"""

        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset5.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                    "Layer": "layer-1",
                    "NUTS": "ES"
                },
            ]
        }
        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(response.status_code, 201)
        self.assertIn("TaskIds", response.json())
        self.assertTrue(len(response.json()["TaskIds"]), 1)

    def test_download_with_invalid_layer(self):
        """test that requesting an invalid band raises an error"""
        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset5.UID(),
                    "DatasetDownloadInformationID": "this-is-an-invalid-id",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                    "Layer": "this band does not exist",
                },
            ]
        }
        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(response.status_code, 400)
        self.assertIn("status", response.json())

    def test_download_without_layer_when_dataset_has_layers(self):
        """ in this case FME receives the 'ALL BANDS' layer as default"""
        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset5.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                    "NUTS": "ES"
                },
            ]
        }
        response = self.api_session.post("@datarequest_post", json=data)

        self.assertEqual(response.status_code, 201)
        self.assertIn("TaskIds", response.json())
        self.assertTrue(len(response.json()["TaskIds"]), 1)

    def test_download_prepackaged_file_id(self):
        """some files can be downloaded directly, providing their file_id"""

        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset3.UID(),
                    "FileID": self.dataset3.downloadable_files["items"][0][
                        "@id"
                    ],
                },
                {
                    "DatasetID": self.dataset3.UID(),
                    "FileID": self.dataset3.downloadable_files["items"][1][
                        "@id"
                    ],
                },
            ]
        }

        # Patch FME call to return an OK response
        DataRequestPost.post_request_to_fme = custom_ok_post_request_to_fme

        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 201)
        self.assertIn("TaskIds", response.json())
        self.assertTrue(len(response.json()["TaskIds"]), 1)

    def test_download_invalid_prepackaged_file_id(self):
        """some files can be downloaded directly, providing their file_id"""

        some_invalid_file_id_1 = "this-is-not-a-valid-file-id-1"
        some_invalid_file_id_2 = "this-is-not-a-valid-file-id-2"

        self.assertNotIn(
            some_invalid_file_id_1,
            [
                item["@id"]
                for item in self.dataset3.downloadable_files["items"]
            ],
        )
        self.assertNotIn(
            some_invalid_file_id_2,
            [
                item["@id"]
                for item in self.dataset3.downloadable_files["items"]
            ],
        )

        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset3.UID(),
                    "FileID": some_invalid_file_id_1,
                },
                {
                    "DatasetID": self.dataset3.UID(),
                    "FileID": some_invalid_file_id_2,
                },
            ]
        }

        # Patch FME call to return an OK response
        DataRequestPost.post_request_to_fme = custom_ok_post_request_to_fme

        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("status", response.json())

    def test_download_general_and_prepackaged_file_id(self):
        """in a single query users can request a generic download
        and also a download of a prepackaged file.
        In such a case two FME tasks will be created
        """

        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset4.UID(),
                    "DatasetDownloadInformationID": "id-2",
                    "OutputFormat": "GDB",
                    "OutputGCS": "EPSG:4326",
                    "BoundingBox": [
                        2.354736328128108,
                        46.852958688910306,
                        4.639892578127501,
                        45.88264619696234,
                    ],
                    "TemporalFilter": {
                        "StartDate": 1546333200000,
                        "EndDate": 1547974800000,
                    },
                },
                {
                    "DatasetID": self.dataset3.UID(),
                    "FileID": self.dataset3.downloadable_files["items"][0][
                        "@id"
                    ],
                },
                {
                    "DatasetID": self.dataset3.UID(),
                    "FileID": self.dataset3.downloadable_files["items"][1][
                        "@id"
                    ],
                },
            ]
        }

        # Patch FME call to return an OK response
        DataRequestPost.post_request_to_fme = custom_ok_post_request_to_fme

        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 201)
        self.assertIn("TaskIds", response.json())
        self.assertTrue(len(response.json()["TaskIds"]), 2)

    def test_download_maximum_5_items(self):
        """test that the download queue only allows 5 items"""
        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset1.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                    "NUTS": "ES",
                },
                {
                    "DatasetID": self.dataset2.UID(),
                    "DatasetDownloadInformationID": "id-2",
                    "OutputFormat": "GDB",
                    "OutputGCS": "EPSG:4326",
                    "NUTS": "FR",
                },
                {
                    "DatasetID": self.dataset2.UID(),
                    "DatasetDownloadInformationID": "id-2",
                    "OutputFormat": "GDB",
                    "OutputGCS": "EPSG:4326",
                    "NUTS": "IT",
                },
                {
                    "DatasetID": self.dataset2.UID(),
                    "DatasetDownloadInformationID": "id-2",
                    "OutputFormat": "GDB",
                    "OutputGCS": "EPSG:4326",
                    "NUTS": "NL",
                },
                {
                    "DatasetID": self.dataset2.UID(),
                    "DatasetDownloadInformationID": "id-2",
                    "OutputFormat": "GDB",
                    "OutputGCS": "EPSG:4326",
                    "NUTS": "DE",
                },
                {
                    "DatasetID": self.dataset2.UID(),
                    "DatasetDownloadInformationID": "id-2",
                    "OutputFormat": "GDB",
                    "OutputGCS": "EPSG:4326",
                    "NUTS": "SE",
                },
            ]
        }
        # Patch FME call to return an OK response
        DataRequestPost.post_request_to_fme = custom_ok_post_request_to_fme

        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 400)

    def test_downloading_duplicates_not_allowed(self):
        """test that requesting the download of the same dataset with the same
        restrictions is not allowed
        """

        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset1.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                    "NUTS": "ES",
                },
                {
                    "DatasetID": self.dataset1.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                    "NUTS": "ES",
                },
            ]
        }
        # Patch FME call to return an OK response
        DataRequestPost.post_request_to_fme = custom_ok_post_request_to_fme

        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 400)

    def test_download_timeseries_without_temporal_extent(self):
        """test downloading a timeseries without temporal extent:
        should be an error"""

        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset4.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                    "NUTS": "ES",
                },
            ]
        }

        # Patch FME call to return an OK response
        DataRequestPost.post_request_to_fme = custom_ok_post_request_to_fme

        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 400)

    def test_no_outputgcs(self):
        """test not sending the outputGCS parameter"""
        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset4.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "NUTS": "ES",
                },
            ]
        }

        # Patch FME call to return an OK response
        DataRequestPost.post_request_to_fme = custom_ok_post_request_to_fme

        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 400)

    def test_invalid_outputgcs(self):
        """test outputGCS being invalid"""
        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset4.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "NUTS": "ES",
                    "OutputGCS": "some-invalid-GCS",
                },
            ]
        }

        # Patch FME call to return an OK response
        DataRequestPost.post_request_to_fme = custom_ok_post_request_to_fme

        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 400)

    def test_empty_outputgcs(self):
        """test outputgcs parameter being empty"""
        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset4.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "NUTS": "ES",
                    "OutputGCS": "",
                },
            ]
        }

        # Patch FME call to return an OK response
        DataRequestPost.post_request_to_fme = custom_ok_post_request_to_fme

        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 400)

    def test_download_duplicated(self):
        """try to download the same item twice"""
        data = {
            "Datasets": [
                {
                    "DatasetFormat": "Netcdf",
                    "DatasetID": "64d1bd55caa0461da8e7a124b9382d70",
                    "DatasetPath": "L3RoaXMvaXMvYS9wYXRoL3RvL2RhdGFzZXQx",
                    "DatasetSource": "EEA",
                    "DatasetTitle": "DataSet 1",
                    "Metadata": [
                        # pylint: disable=line-too-long
                        "https://sdi.eea.europa.eu/catalogue/copernicus/api/records/some-geonetwork-id/formatters/xml?approved=true"  # noqa
                    ],
                    "NUTSID": "ES",
                    "NUTSName": "España",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                    "WekeoChoices": "choice-1",
                },
                {
                    "DatasetFormat": "Netcdf",
                    "DatasetID": "64d1bd55caa0461da8e7a124b9382d70",
                    "DatasetPath": "L3RoaXMvaXMvYS9wYXRoL3RvL2RhdGFzZXQx",
                    "DatasetSource": "EEA",
                    "DatasetTitle": "DataSet 1",
                    "Metadata": [
                        # pylint: disable=line-too-long
                        "https://sdi.eea.europa.eu/catalogue/copernicus/api/records/some-geonetwork-id/formatters/xml?approved=true"  # noqa
                    ],
                    "NUTSID": "ES",
                    "NUTSName": "España",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                    "WekeoChoices": "choice-1",
                },
            ]
        }
        # Patch FME call to return an OK response
        DataRequestPost.post_request_to_fme = custom_ok_post_request_to_fme

        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 400)

    def test_download_duplicated_items_already_queued(self):
        """try to download the same item twice"""

        utility = getUtility(IDownloadToolUtility)
        data_queued = {
            "Datasets": [
                {
                    "DatasetID": self.dataset1.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                    "NUTS": "ES",
                }
            ],
            "UserID": SITE_OWNER_NAME,
            "Status": "Queued",
        }
        data_pending = {
            "Datasets": [
                {
                    "DatasetID": self.dataset1.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                    "NUTS": "ES",
                }
            ],
            "UserID": SITE_OWNER_NAME,
            "Status": "In_progress",
        }

        # register the request directly in the utility
        # to mark that it exists
        utility.datarequest_post(data_queued)
        utility.datarequest_post(data_pending)

        transaction.commit()

        data_to_download = {
            "Datasets": [
                {
                    "DatasetID": self.dataset1.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "Netcdf",
                    "OutputGCS": "EPSG:4326",
                    "NUTS": "ES",
                }
            ],
        }

        # Patch FME call to return an OK response
        DataRequestPost.post_request_to_fme = custom_ok_post_request_to_fme

        response = self.api_session.post(
            "@datarequest_post", json=data_to_download
        )
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 400)

    def test_download_timeseries_larger_temporal_extent_than_allowed(self):
        """test downloading a timeseries with a larger temporal extent
        than allowed"""

        data = {
            "Datasets": [
                {
                    "DatasetID": self.dataset4.UID(),
                    "DatasetDownloadInformationID": "id-1",
                    "OutputFormat": "GDB",
                    "OutputGCS": "EPSG:4326",
                    "NUTS": "ES",
                    "TemporalFilter": {
                        "StartDate": 1546333200000,
                        "EndDate": 1547974800000,
                    },
                },
            ]
        }

        # Patch FME call to return an OK response
        DataRequestPost.post_request_to_fme = custom_ok_post_request_to_fme

        response = self.api_session.post("@datarequest_post", json=data)
        self.assertEqual(
            response.headers.get("Content-Type"), "application/json"
        )

        self.assertEqual(response.status_code, 400)


class TestDatarequestPostTemporalFilter(unittest.TestCase):
    """test temporal filter validator"""

    def test_empty_temporal_filter(self):
        """when no parameters are passed, the temporal filter is None"""

        start, end = extract_dates_from_temporal_filter({})
        self.assertIsNone(start)
        self.assertIsNone(end)

    def test_only_start_temporal_filter(self):
        """when no parameters are passed, the temporal filter is None"""

        start, end = extract_dates_from_temporal_filter(
            {"StartDate": 1644847260215}
        )
        self.assertIsNone(start)
        self.assertIsNone(end)

    def test_only_end_temporal_filter(self):
        """when no parameters are passed, the temporal filter is None"""

        start, end = extract_dates_from_temporal_filter(
            {"EndDate": 1644847260215}
        )
        self.assertIsNone(start)
        self.assertIsNone(end)

    def test_start_and_end_temporal_filter(self):
        """test with valid start and end dates"""
        start_milis = 1644847160215
        end_milis = 1644847260215
        start, end = extract_dates_from_temporal_filter(
            {"StartDate": start_milis, "EndDate": end_milis}
        )

        self.assertEqual(
            start,
            datetime.fromtimestamp(start_milis / 1000).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        )
        self.assertEqual(
            end,
            datetime.fromtimestamp(end_milis / 1000).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        )


class TestDatarequestPostSpatialExtent(unittest.TestCase):
    """test temporal filter validator"""

    def test_empty_spatial_extent(self):
        """the bounding box must have exactly 4 items"""
        bounding_box_0 = []
        bounding_box_1 = [42.25454]
        bounding_box_2 = [42.25454, 2.25454]
        bounding_box_3 = [42.25454, 2.25454, 40.25454]

        self.assertFalse(validate_spatial_extent(bounding_box_0))
        self.assertFalse(validate_spatial_extent(bounding_box_1))
        self.assertFalse(validate_spatial_extent(bounding_box_2))
        self.assertFalse(validate_spatial_extent(bounding_box_3))

    def test_spatial_extent_with_4_items(self):
        """values must be ints or floats"""
        bounding_box_0 = [42.25454, 2.25454, 40.25454, "foo"]
        bounding_box_1 = [42.25454, 2.25454, 40.25454, 4.25454]
        bounding_box_2 = [42, 2, 40.25454, 4]
        bounding_box_3 = [42.2545, 2, 40.25454, 4]
        bounding_box_4 = [42, 2, 40, 4]

        self.assertFalse(validate_spatial_extent(bounding_box_0))
        self.assertTrue(validate_spatial_extent(bounding_box_1))
        self.assertTrue(validate_spatial_extent(bounding_box_2))
        self.assertTrue(validate_spatial_extent(bounding_box_3))
        self.assertTrue(validate_spatial_extent(bounding_box_4))


class TestDatarequestPostNuts(unittest.TestCase):
    """test nuts validator"""

    def test_nuts_validator(self):
        """validate NUTS codes"""

        # Valid codes
        self.assertTrue(validate_nuts("AT"))
        self.assertTrue(validate_nuts("BE10"))
        self.assertTrue(validate_nuts("IT10"))
        self.assertTrue(validate_nuts("DE101"))
        self.assertTrue(validate_nuts("FRI10"))
        self.assertTrue(validate_nuts("DK56"))

        # Invalid codes

        # invalid format
        self.assertFalse(validate_nuts("548"))
        self.assertFalse(validate_nuts("45FR"))

        # invalid country code
        self.assertFalse(validate_nuts("WE21W10"))

        # invalid chars
        self.assertFalse(validate_nuts("NUTS:DK56"))


class TestDatarequestPostEncodePath(unittest.TestCase):
    """test encode_path"""

    def test_encode_path_str(self):
        """paths are encoded in base64"""
        path = "/this/is/a/path"

        self.assertEqual(
            base64_encode_path(path),
            base64.urlsafe_b64encode(path.encode()).decode(),
        )

    def test_encode_path_bytes(self):
        """paths are encoded in base64"""
        path = b"/this/is/a/path"

        self.assertEqual(
            base64_encode_path(path),
            base64.urlsafe_b64encode(path).decode(),
        )


class TestDatarequestPostUtilMethods(unittest.TestCase):
    """test util methods to extract data from dataset objects"""

    layer = CLMS_DOWNLOADTOOL_INTEGRATION_TESTING

    def setUp(self):
        """set up"""
        self.portal = self.layer["portal"]
        self.portal_url = self.portal.absolute_url()
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
                "items": [
                    {
                        "@id": "some-id",
                        "type": "EEA",
                        "id": "some-geonetwork-id",
                    }
                ]
            },
            downloadable_files={
                "items": [
                    {
                        "@id": "id-1",
                        "format": "GDB",
                        "path": "/path/to/file1",
                    },
                    {
                        "@id": "id-2",
                        "format": "GDB",
                        "path": "/path/to/file2",
                    },
                ]
            },
            dataset_download_information={
                "items": [
                    {
                        "@id": "id-1",
                        "full_format": "GDB",
                        "full_path": "/this/is/a/path/to/dataset1",
                        "full_source": "EEA",
                        "wekeo_choices": "choice-1",
                        "layers": ["this-layer", "that-layer"],
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
            downloadable_files={
                "items": [
                    {
                        "@id": "id-3",
                        "format": "Shapefile",
                        "path": "/path/to/file3",
                    },
                    {
                        "@id": "id-4",
                        "format": "GDB",
                        "path": "/path/to/file4",
                    },
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
                    },
                    {
                        "@id": "id-3",
                        "full_format": {"title": "GDB", "token": "GDB"},
                        "full_path": "/this/is/a/path/to/dataset2",
                        "full_source": "WEKEO",
                        "wekeo_choices": "choice-2",
                    },
                    {
                        "@id": "id-4",
                        "full_format": "GDB",
                        "full_path": "/this/is/a/path/to/dataset2",
                        "full_source": {"title": "WEKEO", "token": "WEKEO"},
                        "wekeo_choices": "choice-2",
                    },
                ]
            },
        )

    def test_get_prepackaged_path_from_id(self):
        """return the path of a prepackaged file based on its id"""
        path = get_dataset_file_path_from_file_id(self.dataset1, "id-1")
        self.assertEqual(path, "/path/to/file1")

        path = get_dataset_file_path_from_file_id(self.dataset2, "id-3")
        self.assertEqual(path, "/path/to/file3")

    def test_get_prepackaged_path_from_id_not_found(self):
        """return None if the file id is not found"""
        path = get_dataset_file_path_from_file_id(self.dataset1, "id-4")
        self.assertIsNone(path)

    def test_get_full_dataset_source(self):
        """return the source of the dataset based on download_information_id"""
        item = get_full_dataset_source(self.dataset1, "id-1")
        self.assertEqual(item, "EEA")

        item = get_full_dataset_source(self.dataset2, "id-2")
        self.assertEqual(item, "WEKEO")

    def test_get_full_dataset_source_as_dict(self):
        """return the source of the dataset based on download_information_id
        from a value saved as a dict"""
        item = get_full_dataset_source(self.dataset2, "id-4")
        self.assertEqual(item, "WEKEO")

    def test_get_full_dataset_source_with_invalid_id(self):
        """with an invalid id, None is returned"""
        item = get_full_dataset_source(self.dataset1, "invalid-id")
        self.assertIsNone(item)

    def test_get_full_dataset_path(self):
        """return the path of the dataset based on download_information_id"""
        item = get_full_dataset_path(self.dataset1, "id-1")
        self.assertEqual(item, "/this/is/a/path/to/dataset1")

        item = get_full_dataset_path(self.dataset2, "id-2")
        self.assertEqual(item, "/this/is/a/path/to/dataset2")

    def test_get_full_dataset_path_with_invalid_id(self):
        """with an invalid id, None is returned"""
        item = get_full_dataset_path(self.dataset1, "invalid-id")
        self.assertIsNone(item)

    def test_get_full_dataset_wekeo_choices(self):
        """return the wekeo_choices of the dataset based on
        download_information_id"""
        item = get_full_dataset_wekeo_choices(self.dataset1, "id-1")
        self.assertEqual(item, "choice-1")

        item = get_full_dataset_wekeo_choices(self.dataset2, "id-2")
        self.assertEqual(item, "choice-2")

    def test_get_full_dataset_wekeo_choices_with_invalid_id(self):
        """with an invalid id, None is returned"""
        item = get_full_dataset_wekeo_choices(self.dataset1, "invalid-id")
        self.assertIsNone(item)

    def test_get_full_dataset_format(self):
        """return the format of the dataset based on download_information_id"""
        item = get_full_dataset_format(self.dataset1, "id-1")
        self.assertEqual(item, "GDB")

        item = get_full_dataset_format(self.dataset2, "id-2")
        self.assertEqual(item, "GDB")

    def test_get_full_dataset_format_as_dict(self):
        """return the format of the dataset based on download_information_id
        from a value saved as a dict"""
        item = get_full_dataset_format(self.dataset2, "id-3")
        self.assertEqual(item, "GDB")

    def test_get_full_dataset_format_with_invalid_id(self):
        """with an invalid id, None is returned"""
        item = get_full_dataset_format(self.dataset1, "invalid-id")
        self.assertIsNone(item)

    def test_get_full_dataset_layers(self):
        """return the layers of the datased based on download_information_id"""
        item = get_full_dataset_layers(self.dataset1, "id-1")
        self.assertEqual(item, ["this-layer", "that-layer"])

    def test_get_full_dataset_layers_empty_layers(self):
        """return the layers of the datased based on download_information_id
        of a item without any layers
        """
        item = get_full_dataset_layers(self.dataset1, "id-2")
        self.assertEqual(item, [])

    def test_get_full_dataset_layers_with_invalid_id(self):
        """return the layers of the datased based on
        download_information_id of an invalid id"""
        item = get_full_dataset_layers(self.dataset1, "invalid-id")
        self.assertEqual(item, [])
