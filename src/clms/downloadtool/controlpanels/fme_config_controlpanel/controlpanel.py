# -*- coding: utf-8 -*-
"""
This is the control panel for fme configuration
"""
from clms.addon import _
from plone.app.registry.browser.controlpanel import ControlPanelFormWrapper
from plone.app.registry.browser.controlpanel import RegistryEditForm
from plone.z3cform import layout
from zope import schema
from zope.interface import Interface


class IFMEConfigControlPanel(Interface):
    """ Control Panel Schema """

    url = schema.TextLine(
        title=_(
            "Enter the URL of the FME server",
        ),
        description=_(
            "This url will be used to make dataset download and "
            "transformation requests",
        ),
        # pylint: disable=line-too-long
        default="https://copernicus-fme.eea.europa.eu/fmerest/v3/transformations/submit/CLMS/CLMS_Download.fmw",  # noqa: E501
        required=True,
        readonly=False,
    )

    delete_url = schema.TextLine(
        title=_(
            "Enter the URL of the FME server where DELETE requests "
            "will be sent",
        ),
        description=_(
            "This url will be used to signal the deletion of a given "
            "download request dataset download and ",
        ),
        # pylint: disable=line-too-long
        default="https://copernicus-fme.eea.europa.eu/fmerest/v3/transformations/jobs/active",  # noqa: E501
        required=True,
        readonly=False,
    )

    fme_token = schema.TextLine(
        title=_(
            "Enter the FME Authorization token",
        ),
        description=_(
            "This token will be used when connecting to FME to "
            "authorize requests",
        ),
        default=u"XXXXXXXXXXXXXXXXXXX",
        required=True,
        readonly=False,
    )

    nuts_service = schema.TextLine(
        title=_(
            "Enter the URL of the NUTS REST service",
        ),
        description=_(
            "This service is used to get the names of NUTS IDs",
        ),
        default="https://trial.discomap.eea.europa.eu/arcgis/rest/services/CLMS/NUTS_2021/MapServer/0/query?f=json&text=&objectIds=&time=&timeRelation=esriTimeRelationOverlaps&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam=&outFields=*&returnGeometry=false&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&havingClause=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&returnExtentOnly=false&sqlFormat=none&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&featureEncoding=esriDefault&",
        required=True,
        readonly=False,
    )


class FMEConfigControlPanel(RegistryEditForm):
    """ control panel rest API endpoint configuration """

    schema = IFMEConfigControlPanel
    schema_prefix = "clms.downloadtool.fme_config_controlpanel"
    label = _("FME Config Control Panel")


FMEConfigControlPanelView = layout.wrap_form(
    FMEConfigControlPanel, ControlPanelFormWrapper
)
