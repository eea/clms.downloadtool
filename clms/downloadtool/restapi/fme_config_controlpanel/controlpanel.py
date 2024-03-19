""" Control Panel RestAPI endpoint
"""
from plone.restapi.controlpanels import RegistryConfigletPanel
from zope.component import adapter
from zope.interface import Interface

# pylint: disable=line-too-long
from clms.downloadtool.controlpanels.fme_config_controlpanel.controlpanel import IFMEConfigControlPanel  # noqa: E501
from clms.downloadtool.interfaces import IClmsDownloadtoolLayer


@adapter(Interface, IClmsDownloadtoolLayer)
class FMEConfigControlPanel(RegistryConfigletPanel):
    """Control Panel endpoint"""

    schema = IFMEConfigControlPanel
    configlet_id = "fme_config-controlpanel"
    configlet_category_id = "Products"
    title = "FME Config Control Panel"
    group = ""
    schema_prefix = "clms.downloadtool.fme_config_controlpanel"
