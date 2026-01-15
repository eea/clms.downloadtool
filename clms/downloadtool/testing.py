# -*- coding: utf-8 -*-
"""Test plone site."""
import os

import clms.downloadtool
import plone.restapi
from plone.app.contenttypes.testing import PLONE_APP_CONTENTTYPES_FIXTURE
from plone.app.testing import (
    FunctionalTesting,
    IntegrationTesting,
    PloneSandboxLayer,
    applyProfile,
)
from plone.testing.zope import WSGI_SERVER_FIXTURE
from zope.component import getUtility

from clms.downloadtool.storage.memory import MemoryDownloadtoolRepository
from clms.downloadtool.utility import IDownloadToolUtility


class ClmsDownloadtoolLayer(PloneSandboxLayer):
    """Plone sandbox"""

    defaultBases = (PLONE_APP_CONTENTTYPES_FIXTURE,)

    def setUpZope(self, app, configurationContext):
        """Custom shared utility setup for tests."""
        # Load any other ZCML that is required for your tests.
        # The z3c.autoinclude feature is disabled in the Plone fixture base
        # layer.
        os.environ.setdefault("CLMS_DOWNLOADTOOL_TESTING", "1")

        self.loadZCML(package=plone.restapi)
        self.loadZCML(package=clms.downloadtool)

    def setUpPloneSite(self, portal):
        """ Setup cms site """
        applyProfile(portal, "clms.downloadtool:default")

    def testSetUp(self):
        """Reset in-memory download task storage between tests."""
        os.environ.setdefault("CLMS_DOWNLOADTOOL_TESTING", "1")
        utility = getUtility(IDownloadToolUtility)
        repository = getattr(utility, "_repository", None)
        if isinstance(repository, MemoryDownloadtoolRepository):
            repository.delete_all()
        else:
            utility._repository = MemoryDownloadtoolRepository()


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
