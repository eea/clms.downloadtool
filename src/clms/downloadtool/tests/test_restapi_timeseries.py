""" test that the @timeseries endpoint works as expected"""
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
from clms.downloadtool.api.services.timeseries import utils
from lxml import etree


class TestTimeSeriesEndpoint(unittest.TestCase):
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

    def tearDown(self):
        """tear down cleanup"""
        self.api_session.close()
        self.anonymous_session.close()


class TestTimeSeriesUtils(unittest.TestCase):
    """base class for testing the WMS parsing utils"""

    def test_extract_data_from_dimension_with_p(self):
        """test when the dimension parameter has the P value"""
        text = "1984-03-01/2023-05-24/P1D"
        result = utils.extract_data_from_dimension(text)
        expected_result = {
            "start": "1984-03-01",
            "end": "2023-05-24",
            "period": "P1D",
        }
        self.assertDictEqual(result, expected_result)

    def test_extract_data_from_dimension_without_p(self):
        """test when the dimension parameter hasn't the P value"""
        text = "1984-03-01,2023-05-24"
        result = utils.extract_data_from_dimension(text)
        expected_result = {"array": ["1984-03-01", "2023-05-24"]}
        self.assertDictEqual(result, expected_result)

    def test_extract_data_from_dimension_no_value(self):
        """test when the dimension parameter is empty"""
        result = utils.extract_data_from_dimension("")
        self.assertEqual(result, "")

        result = utils.extract_data_from_dimension(None)
        self.assertEqual(result, "")

        result = utils.extract_data_from_dimension([])
        self.assertEqual(result, "")

        result = utils.extract_data_from_dimension({})
        self.assertEqual(result, "")

    def extract_dimension_wms(self):
        xml = b"""<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<WMS_Capabilities version="1.3.0" xsi:schemaLocation="https:/inspire.ec.europa.eu/schemas/inspire_vs/1.0 https://inspire.ec.europa.eu/schemas/inspire_vs/1.0/inspire_vs.xsd" xmlns:inspire_common="https://inspire.ec.europa.eu/schemas/common/1.0" xmlns:inspire_vs="https://inspire.ec.europa.eu/schemas/inspire_vs/1.0" xmlns="http://www.opengis.net/wms" xmlns:sld="http://www.opengis.net/sld" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
<Service>
    <Capability>
        <Layer>
            <Dimension name="time" units="ISO8601"/>
            <Extent name="time" default="2023-05-24">1984-03-01/2023-05-24/P1D</Extent>
        </Layer>
    </Capability>
    </Service></WMS_Capabilities>"""
        result = utils.extract_dimension_wms(etree.fromstring(xml))
        expected_result = {
            "start": "1984-03-01",
            "end": "2023-05-24",
            "period": "P1D",
        }
        self.assertDictEqual(result, expected_result)
