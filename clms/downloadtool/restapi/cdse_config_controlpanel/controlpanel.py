""" Control Panel RestAPI endpoint
"""
from plone.restapi.controlpanels import RegistryConfigletPanel
from zope.component import adapter
from zope.interface import Interface

# pylint: disable=line-too-long
from clms.downloadtool.controlpanels.cdse_config_controlpanel.controlpanel import ICDSEConfigControlPanel  # noqa: E501
from clms.downloadtool.interfaces import IClmsDownloadtoolLayer


@adapter(Interface, IClmsDownloadtoolLayer)
class CDSEConfigControlPanel(RegistryConfigletPanel):
    """Control Panel endpoint"""

    schema = ICDSEConfigControlPanel
    configlet_id = "cdse_config-controlpanel"
    configlet_category_id = "Products"
    title = "CDSE Config Control Panel"
    group = ""
    schema_prefix = "clms.downloadtool.cdse_config_controlpanel"
