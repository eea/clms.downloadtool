# -*- coding: utf-8 -*-
"""Test plone site
"""
import clms.downloadtool
import plone.restapi
from plone.app.contenttypes.testing import PLONE_APP_CONTENTTYPES_FIXTURE
from plone.app.testing import (FunctionalTesting, IntegrationTesting,
                               PloneSandboxLayer, applyProfile)
from plone.testing.zope import WSGI_SERVER_FIXTURE


class ClmsDownloadtoolLayer(PloneSandboxLayer):
    """Plone sandbox"""

    defaultBases = (PLONE_APP_CONTENTTYPES_FIXTURE,)

    def setUpZope(self, app, configurationContext):
        """Custom shared utility setup for tests."""
        # Load any other ZCML that is required for your tests.
        # The z3c.autoinclude feature is disabled in the Plone fixture base
        # layer.

        self.loadZCML(package=plone.restapi)
        self.loadZCML(package=clms.downloadtool)

    def setUpPloneSite(self, portal):
        """ Setup cms site """
        applyProfile(portal, "clms.downloadtool:default")


CLMS_DOWNLOADTOOL_FIXTURE = ClmsDownloadtoolLayer()


CLMS_DOWNLOADTOOL_INTEGRATION_TESTING = IntegrationTesting(
    bases=(CLMS_DOWNLOADTOOL_FIXTURE,),
    name="ClmsDownloadtoolLayer:IntegrationTesting",
)


CLMS_DOWNLOADTOOL_FUNCTIONAL_TESTING = FunctionalTesting(
    bases=(CLMS_DOWNLOADTOOL_FIXTURE,),
    name="ClmsDownloadtoolLayer:FunctionalTesting",
)

CLMS_DOWNLOADTOOL_RESTAPI_TESTING = FunctionalTesting(
    bases=(CLMS_DOWNLOADTOOL_FIXTURE, WSGI_SERVER_FIXTURE),
    name="ClmstypesLayer:RestApiTesting",
)
