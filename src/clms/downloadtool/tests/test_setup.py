# -*- coding: utf-8 -*-
"""Setup tests for this package."""
import unittest

from plone import api
from plone.app.testing import TEST_USER_ID, setRoles
from plone.browserlayer import utils
from Products.CMFPlone.utils import get_installer

from clms.downloadtool.interfaces import IClmsDownloadtoolLayer
from clms.downloadtool.testing import (
    CLMS_DOWNLOADTOOL_INTEGRATION_TESTING,
)  # noqa: E501


class TestSetup(unittest.TestCase):
    """Test that clms.downloadtool is properly installed."""

    layer = CLMS_DOWNLOADTOOL_INTEGRATION_TESTING

    def setUp(self):
        """Custom shared utility setup for tests."""
        self.portal = self.layer["portal"]
        self.installer = get_installer(self.portal, self.layer["request"])

    def test_product_installed(self):
        """Test if clms.downloadtool is installed."""
        self.assertTrue(
            self.installer.is_product_installed("clms.downloadtool")
        )

    def test_browserlayer(self):
        """Test that IClmsDownloadtoolLayer is registered."""
        self.assertIn(IClmsDownloadtoolLayer, utils.registered_layers())


class TestUninstall(unittest.TestCase):
    """ test uninstall base class"""

    layer = CLMS_DOWNLOADTOOL_INTEGRATION_TESTING

    def setUp(self):
        """setup"""
        self.portal = self.layer["portal"]
        self.installer = get_installer(self.portal, self.layer["request"])
        roles_before = api.user.get_roles(TEST_USER_ID)
        setRoles(self.portal, TEST_USER_ID, ["Manager"])
        self.installer.uninstall_product("clms.downloadtool")
        setRoles(self.portal, TEST_USER_ID, roles_before)

    def test_product_uninstalled(self):
        """Test if clms.downloadtool is cleanly uninstalled."""
        self.assertFalse(
            self.installer.is_product_installed("clms.downloadtool")
        )

    def test_browserlayer_removed(self):
        """Test that IClmsDownloadtoolLayer is removed."""
        self.assertNotIn(IClmsDownloadtoolLayer, utils.registered_layers())
