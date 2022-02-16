""" controlpanel tests """
# -*- coding: utf-8 -*-
import unittest

from plone import api

from clms.downloadtool.testing import CLMS_DOWNLOADTOOL_INTEGRATION_TESTING


class TestControlPanel(unittest.TestCase):
    """Test that clms.downloadtool is properly installed."""

    layer = CLMS_DOWNLOADTOOL_INTEGRATION_TESTING

    def setUp(self):
        """Custom shared utility setup for tests."""
        self.portal = self.layer["portal"]
        self.controlpanel = api.portal.get_tool("portal_controlpanel")

    def test_fme_config_controlpanel_installed(self):
        """ test FME configuration control panel is installed"""
        self.assertIn(
            "fme_config-controlpanel",
            [item.id for item in self.controlpanel.listActions()],
        )
