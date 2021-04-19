# -*- coding: utf-8 -*-
from plone.app.contenttypes.testing import PLONE_APP_CONTENTTYPES_FIXTURE
from plone.app.robotframework.testing import REMOTE_LIBRARY_BUNDLE_FIXTURE
from plone.app.testing import applyProfile
from plone.app.testing import FunctionalTesting
from plone.app.testing import IntegrationTesting
from plone.app.testing import PloneSandboxLayer
from plone.testing import z2

import clms.downloadtool


class ClmsDownloadtoolLayer(PloneSandboxLayer):

    defaultBases = (PLONE_APP_CONTENTTYPES_FIXTURE,)

    def setUpZope(self, app, configurationContext):
        # Load any other ZCML that is required for your tests.
        # The z3c.autoinclude feature is disabled in the Plone fixture base
        # layer.
        import plone.restapi
        self.loadZCML(package=plone.restapi)
        self.loadZCML(package=clms.downloadtool)

    def setUpPloneSite(self, portal):
        applyProfile(portal, 'clms.downloadtool:default')


CLMS_DOWNLOADTOOL_FIXTURE = ClmsDownloadtoolLayer()


CLMS_DOWNLOADTOOL_INTEGRATION_TESTING = IntegrationTesting(
    bases=(CLMS_DOWNLOADTOOL_FIXTURE,),
    name='ClmsDownloadtoolLayer:IntegrationTesting',
)


CLMS_DOWNLOADTOOL_FUNCTIONAL_TESTING = FunctionalTesting(
    bases=(CLMS_DOWNLOADTOOL_FIXTURE,),
    name='ClmsDownloadtoolLayer:FunctionalTesting',
)


CLMS_DOWNLOADTOOL_ACCEPTANCE_TESTING = FunctionalTesting(
    bases=(
        CLMS_DOWNLOADTOOL_FIXTURE,
        REMOTE_LIBRARY_BUNDLE_FIXTURE,
        z2.ZSERVER_FIXTURE,
    ),
    name='ClmsDownloadtoolLayer:AcceptanceTesting',
)
