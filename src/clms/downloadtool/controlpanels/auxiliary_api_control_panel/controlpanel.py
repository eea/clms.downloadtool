# -*- coding: utf-8 -*-
""" control panel to save some download settings"""
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
    """control panel schema"""

    wekeo_api_url = schema.TextLine(
        title=_(
            "Wekeo API URL",
        ),
        description=_(
            "",
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
        default="",
        required=True,
        readonly=False,
    )

    wekeo_api_password = schema.TextLine(
        title=_(
            "Wekeo API password",
        ),
        description=_(
            "",
        ),
        default="",
        required=True,
        readonly=False,
    )

    landcover_api_url = schema.TextLine(
        title=_(
            "LandCover API URL",
        ),
        description=_(
            "",
        ),
        default="",
        required=True,
        readonly=False,
    )

    legacy_username = schema.TextLine(
        title=_(
            "LEGACY ftp server username",
        ),
        description=_(
            "",
        ),
        default="",
        required=True,
        readonly=False,
    )

    legacy_password = schema.TextLine(
        title=_(
            "LEGACY ftp server password",
        ),
        description=_(
            "",
        ),
        default="",
        required=True,
        readonly=False,
    )


class AuxiliaryAPIControlPanel(RegistryEditForm):
    """control panel implementation"""

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
