# -*- coding: utf-8 -*-
from clms.downloadtool import _
from clms.downloadtool.interfaces import IClmsDownloadtoolLayer
from plone.app.registry.browser.controlpanel import (
    ControlPanelFormWrapper,
    RegistryEditForm,
)
from plone.restapi.controlpanels import RegistryConfigletPanel
from plone.z3cform import layout
from zope import schema
from zope.component import adapter
from zope.interface import Interface


class IAuxiliaryAPIControlPanel(Interface):
    wekeo_api_url = schema.TextLine(
        title=_(
            u"Wekeo API URL",
        ),
        description=_(
            u"",
        ),
        default=u"",
        required=True,
        readonly=False,
    )

    wekeo_api_username = schema.TextLine(
        title=_(
            u"Wekeo API Username",
        ),
        description=_(
            u"",
        ),
        default=u"",
        required=True,
        readonly=False,
    )

    wekeo_api_password = schema.TextLine(
        title=_(
            u"Wekeo API password",
        ),
        description=_(
            u"",
        ),
        default=u"",
        required=True,
        readonly=False,
    )

    landcover_api_url = schema.TextLine(
        title=_(
            u"LandCover API URL",
        ),
        description=_(
            u"",
        ),
        default=u"",
        required=True,
        readonly=False,
    )


class AuxiliaryAPIControlPanel(RegistryEditForm):
    schema = IAuxiliaryAPIControlPanel
    schema_prefix = "clms.downloadtool.auxiliary_api_control_panel"
    label = _("Auxiliary API Control Panel")


AuxiliaryAPIControlPanelView = layout.wrap_form(
    AuxiliaryAPIControlPanel, ControlPanelFormWrapper
)


@adapter(Interface, IClmsDownloadtoolLayer)
class AuxiliaryAPIControlPanelConfigletPanel(RegistryConfigletPanel):
    """Control Panel endpoint"""

    schema = IAuxiliaryAPIControlPanel
    configlet_id = "auxiliary_api_control_panel-controlpanel"
    configlet_category_id = "Products"
    title = _("Auxiliary API Control Panel")
    group = ""
    schema_prefix = "clms.downloadtool.auxiliary_api_control_panel"
