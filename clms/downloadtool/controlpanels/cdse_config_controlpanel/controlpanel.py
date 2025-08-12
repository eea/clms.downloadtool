# -*- coding: utf-8 -*-
"""
This is the control panel for cdse configuration
"""
from plone.app.registry.browser.controlpanel import ControlPanelFormWrapper
from plone.app.registry.browser.controlpanel import RegistryEditForm
from plone.z3cform import layout
from zope import schema
from zope.interface import Interface

from clms.downloadtool import _


class ICDSEConfigControlPanel(Interface):
    """Control Panel Schema"""

    token_url = schema.TextLine(
        title=_(
            "Enter the token URL for the CDSE configuration "
        ),
        description=_(
            "This URL be used to get the token for CDSE download"
        ),
        default="https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",  # pylint: disable=line-too-long
        required=True,
        readonly=False,
    )

    batch_url = schema.TextLine(
        title=_(
            "Enter the batch URL "
        ),
        description=_(
            "This is the URL to the batch API "
        ),
        default="https://sh.dataspace.copernicus.eu/api/v2/batch/process",
        required=True,
        readonly=False,
    )

    client_id = schema.TextLine(
        title=_(
            "SentinelHub client ID "
        ),
        description=_(
            "This ID will be used for the CDSE configuration "
        ),
        default=u"XXXXXXXXXXXXXXXXXXX",
        required=True,
        readonly=False,
    )

    client_secret = schema.TextLine(
        title=_(
            "SentinelHub client secret "
        ),
        description=_(
            "This client secret will be used for the CDSE configuration "
        ),
        default=u"XXXXXXXXXXXXXXXXXXX",
        required=True,
        readonly=False,
    )

    s3_bucket_name = schema.TextLine(
        title=_(
            "S3 bucket name "
        ),
        description=_(
            "This bucket name will be used to access the S3 bucket "
        ),
        default="",
        required=True,
        readonly=False,
    )

    s3_bucket_accesskey = schema.TextLine(
        title=_(
            "S3 bucket access key "
        ),
        description=_(
            "This access key will be used to connect to the S3 bucket "
        ),
        default=u"XXXXXXXXXXXXXXXXXXX",
        required=True,
        readonly=False,
    )

    s3_bucket_secretaccesskey = schema.TextLine(
        title=_(
            "S3 bucket secret access key "
        ),
        description=_(
            "This secret access key will be used to connect the S3 bucket "
        ),
        default=u"XXXXXXXXXXXXXXXXXXX",
        required=True,
        readonly=False,
    )


class CDSEConfigControlPanel(RegistryEditForm):
    """control panel rest API endpoint configuration"""

    schema = ICDSEConfigControlPanel
    schema_prefix = "clms.downloadtool.cdse_config_controlpanel"
    label = _("Download Tool Configuration Control Panel")


CDSEConfigControlPanelView = layout.wrap_form(
    CDSEConfigControlPanel, ControlPanelFormWrapper
)
