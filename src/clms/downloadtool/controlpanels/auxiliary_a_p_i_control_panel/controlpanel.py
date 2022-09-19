# -*- coding: utf-8 -*-
from clms.downloadtool import _
from clms.downloadtool.interfaces import IClmsDownloadtoolLayer
from plone.app.registry.browser.controlpanel import (ControlPanelFormWrapper,
                                                     RegistryEditForm)
from plone.restapi.controlpanels import RegistryConfigletPanel
from plone.z3cform import layout
from zope import schema
from zope.component import adapter
from zope.interface import Interface


class IAuxiliaryAPIControlPanel(Interface):
    myfield_name = schema.TextLine(
        title=_(
            "This is an example field for this control panel",
        ),
        description=_(
            "",
        ),
        default="",
        required=False,
        readonly=False,
    )


class AuxiliaryAPIControlPanel(RegistryEditForm):
    schema = IAuxiliaryAPIControlPanel
    schema_prefix = "clms.downloadtool.auxiliary_a_p_i_control_panel"
    label = _("Auxiliary A P I Control Panel")


AuxiliaryAPIControlPanelView = layout.wrap_form(
    AuxiliaryAPIControlPanel, ControlPanelFormWrapper
)



@adapter(Interface, IClmsDownloadtoolLayer)
class AuxiliaryAPIControlPanelConfigletPanel(RegistryConfigletPanel):
    """Control Panel endpoint"""

    schema = IAuxiliaryAPIControlPanel
    configlet_id = "auxiliary_a_p_i_control_panel-controlpanel"
    configlet_category_id = "Products"
    title = _("Auxiliary A P I Control Panel")
    group = ""
    schema_prefix = "clms.downloadtool.auxiliary_a_p_i_control_panel"
