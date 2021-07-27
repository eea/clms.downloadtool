# -*- coding: utf-8 -*-
"""
For HTTP GET operations we can use standard HTTP parameter passing
through the URL)

"""
import re
import datetime
from collections import defaultdict
from logging import getLogger

from plone.restapi.services import Service
from plone.restapi.deserializer import json_body

from zope.component import getUtility
from clms.downloadtool.utility import IDownloadToolUtility


log = getLogger(__name__)

countries = {
    "BD": "BGD",
    "BE": "BEL",
    "BF": "BFA",
    "BG": "BGR",
    "BA": "BIH",
    "BB": "BRB",
    "WF": "WLF",
    "BL": "BLM",
    "BM": "BMU",
    "BN": "BRN",
    "BO": "BOL",
    "BH": "BHR",
    "BI": "BDI",
    "BJ": "BEN",
    "BT": "BTN",
    "JM": "JAM",
    "BV": "BVT",
    "BW": "BWA",
    "WS": "WSM",
    "BQ": "BES",
    "BR": "BRA",
    "BS": "BHS",
    "JE": "JEY",
    "BY": "BLR",
    "BZ": "BLZ",
    "RU": "RUS",
    "RW": "RWA",
    "RS": "SRB",
    "TL": "TLS",
    "RE": "REU",
    "TM": "TKM",
    "TJ": "TJK",
    "RO": "ROU",
    "TK": "TKL",
    "GW": "GNB",
    "GU": "GUM",
    "GT": "GTM",
    "GS": "SGS",
    "GR": "GRC",
    "GQ": "GNQ",
    "GP": "GLP",
    "JP": "JPN",
    "GY": "GUY",
    "GG": "GGY",
    "GF": "GUF",
    "GE": "GEO",
    "GD": "GRD",
    "GB": "GBR",
    "GA": "GAB",
    "SV": "SLV",
    "GN": "GIN",
    "GM": "GMB",
    "GL": "GRL",
    "GI": "GIB",
    "GH": "GHA",
    "OM": "OMN",
    "TN": "TUN",
    "JO": "JOR",
    "HR": "HRV",
    "HT": "HTI",
    "HU": "HUN",
    "HK": "HKG",
    "HN": "HND",
    "HM": "HMD",
    "VE": "VEN",
    "PR": "PRI",
    "PS": "PSE",
    "PW": "PLW",
    "PT": "PRT",
    "SJ": "SJM",
    "PY": "PRY",
    "IQ": "IRQ",
    "PA": "PAN",
    "PF": "PYF",
    "PG": "PNG",
    "PE": "PER",
    "PK": "PAK",
    "PH": "PHL",
    "PN": "PCN",
    "PL": "POL",
    "PM": "SPM",
    "ZM": "ZMB",
    "EH": "ESH",
    "EE": "EST",
    "EG": "EGY",
    "ZA": "ZAF",
    "EC": "ECU",
    "IT": "ITA",
    "VN": "VNM",
    "SB": "SLB",
    "ET": "ETH",
    "SO": "SOM",
    "ZW": "ZWE",
    "SA": "SAU",
    "ES": "ESP",
    "ER": "ERI",
    "ME": "MNE",
    "MD": "MDA",
    "MG": "MDG",
    "MF": "MAF",
    "MA": "MAR",
    "MC": "MCO",
    "UZ": "UZB",
    "MM": "MMR",
    "ML": "MLI",
    "MO": "MAC",
    "MN": "MNG",
    "MH": "MHL",
    "MK": "MKD",
    "MU": "MUS",
    "MT": "MLT",
    "MW": "MWI",
    "MV": "MDV",
    "MQ": "MTQ",
    "MP": "MNP",
    "MS": "MSR",
    "MR": "MRT",
    "IM": "IMN",
    "UG": "UGA",
    "TZ": "TZA",
    "MY": "MYS",
    "MX": "MEX",
    "IL": "ISR",
    "FR": "FRA",
    "IO": "IOT",
    "SH": "SHN",
    "FI": "FIN",
    "FJ": "FJI",
    "FK": "FLK",
    "FM": "FSM",
    "FO": "FRO",
    "NI": "NIC",
    "NL": "NLD",
    "NO": "NOR",
    "NA": "NAM",
    "VU": "VUT",
    "NC": "NCL",
    "NE": "NER",
    "NF": "NFK",
    "NG": "NGA",
    "NZ": "NZL",
    "NP": "NPL",
    "NR": "NRU",
    "NU": "NIU",
    "CK": "COK",
    "XK": "XKX",
    "CI": "CIV",
    "CH": "CHE",
    "CO": "COL",
    "CN": "CHN",
    "CM": "CMR",
    "CL": "CHL",
    "CC": "CCK",
    "CA": "CAN",
    "CG": "COG",
    "CF": "CAF",
    "CD": "COD",
    "CZ": "CZE",
    "CY": "CYP",
    "CX": "CXR",
    "CR": "CRI",
    "CW": "CUW",
    "CV": "CPV",
    "CU": "CUB",
    "SZ": "SWZ",
    "SY": "SYR",
    "SX": "SXM",
    "KG": "KGZ",
    "KE": "KEN",
    "SS": "SSD",
    "SR": "SUR",
    "KI": "KIR",
    "KH": "KHM",
    "KN": "KNA",
    "KM": "COM",
    "ST": "STP",
    "SK": "SVK",
    "KR": "KOR",
    "SI": "SVN",
    "KP": "PRK",
    "KW": "KWT",
    "SN": "SEN",
    "SM": "SMR",
    "SL": "SLE",
    "SC": "SYC",
    "KZ": "KAZ",
    "KY": "CYM",
    "SG": "SGP",
    "SE": "SWE",
    "SD": "SDN",
    "DO": "DOM",
    "DM": "DMA",
    "DJ": "DJI",
    "DK": "DNK",
    "VG": "VGB",
    "DE": "DEU",
    "YE": "YEM",
    "DZ": "DZA",
    "US": "USA",
    "UY": "URY",
    "YT": "MYT",
    "UM": "UMI",
    "LB": "LBN",
    "LC": "LCA",
    "LA": "LAO",
    "TV": "TUV",
    "TW": "TWN",
    "TT": "TTO",
    "TR": "TUR",
    "LK": "LKA",
    "LI": "LIE",
    "LV": "LVA",
    "TO": "TON",
    "LT": "LTU",
    "LU": "LUX",
    "LR": "LBR",
    "LS": "LSO",
    "TH": "THA",
    "TF": "ATF",
    "TG": "TGO",
    "TD": "TCD",
    "TC": "TCA",
    "LY": "LBY",
    "VA": "VAT",
    "VC": "VCT",
    "AE": "ARE",
    "AD": "AND",
    "AG": "ATG",
    "AF": "AFG",
    "AI": "AIA",
    "VI": "VIR",
    "IS": "ISL",
    "IR": "IRN",
    "AM": "ARM",
    "AL": "ALB",
    "AO": "AGO",
    "AQ": "ATA",
    "AS": "ASM",
    "AR": "ARG",
    "AU": "AUS",
    "AT": "AUT",
    "AW": "ABW",
    "IN": "IND",
    "AX": "ALA",
    "AZ": "AZE",
    "IE": "IRL",
    "ID": "IDN",
    "UA": "UKR",
    "QA": "QAT",
    "MZ": "MOZ",
}
GCS = ["EPGS:4326", "EPGS:3035", "EPGS:3857", "EPGS:4258"]
dataset_formats = [
    "Shapefile",
    "GDB",
    "GPKG",
    "Geojson",
    "Geotiff",
    "Netcdf",
    "GML",
    "WFS",
]
table = {
    "Shapefile": {
        "Shapefile": False,
        "GDB": True,
        "GPKG": True,
        "Geojson": True,
        "Geotiff": False,
        "Netcdf": False,
        "GML": True,
        "WFS": False,
    },
    "GDB": {
        "Shapefile": True,
        "GDB": False,
        "GPKG": True,
        "Geojson": True,
        "Geotiff": False,
        "Netcdf": False,
        "GML": True,
        "WFS": False,
    },
    "GPKG": {
        "Shapefile": True,
        "GDB": True,
        "GPKG": False,
        "Geojson": True,
        "Geotiff": False,
        "Netcdf": False,
        "GML": True,
        "WFS": False,
    },
    "Geojson": {
        "Shapefile": True,
        "GDB": True,
        "GPKG": True,
        "Geojson": False,
        "Geotiff": False,
        "Netcdf": False,
        "GML": True,
        "WFS": False,
    },
    "Geotiff": {
        "Shapefile": False,
        "GDB": False,
        "GPKG": False,
        "Geojson": False,
        "Geotiff": False,
        "Netcdf": False,
        "GML": False,
        "WFS": False,
    },
    "Netcdf": {
        "Shapefile": False,
        "GDB": False,
        "GPKG": False,
        "Geojson": False,
        "Geotiff": True,
        "Netcdf": False,
        "GML": False,
        "WFS": False,
    },
    "WFS": {
        "Shapefile": True,
        "GDB": True,
        "GPKG": True,
        "Geojson": True,
        "Geotiff": False,
        "Netcdf": False,
        "GML": True,
        "WFS": False,
    },
}


class DataRequestPost(Service):
    """ Set Data
    """
    def reply(self):
        """ JSON response """
        body = json_body(self.request)

        user_id = str(body.get("UserID"))
        dataset_id = body.get("DatasetID")
        dataset_format = body.get("DatasetFormat")
        dataset_path = body.get("DatasetPath")
        bounding_box = body.get("BoundingBox")
        temporal_filter = body.get("TemporalFilter")
        output_format = body.get("OutputFormat")
        outputGCS = body.get("OutputGCS")
        nuts_id = body.get("NUTSID")
        mail = body.get("Mail")

        response_json = {}

        utility = getUtility(IDownloadToolUtility)

        if not user_id:
            self.request.response.setStatus(400)
            return "Error, UserID is not defined"

        if not dataset_id:
            self.request.response.setStatus(400)
            return "Error, DatasetID is not defined"

        response_json = {"UserID": user_id, "DatasetID": dataset_id}

        if mail:
            if not email_validation(mail):
                self.request.response.setStatus(400)
                return "Error, inserted mail is not valid"
            response_json.update({"Mail": mail})

        if nuts_id:
            if not validateNuts(nuts_id):
                self.request.response.setStatus(400)
                return "NUTSID country error"
            response_json.update({"NUTSID": nuts_id})

        if bounding_box:
            if nuts_id:
                self.request.response.setStatus(400)
                return "Error, NUTSID is also defined"

            if not validateSpatialExtent(bounding_box):
                self.request.response.setStatus(400)
                return "Error, BoundingBox is not valid"

            response_json.update({"BoundingBox": bounding_box})

        if dataset_format or output_format:
            if (
                not dataset_format and output_format or
                dataset_format and not output_format
            ):
                self.request.response.setStatus(400)
                return "Error, you need to specify both formats"
            if (
                dataset_format not in dataset_formats or
                output_format not in dataset_formats
            ):
                self.request.response.setStatus(400)
                return "Error, specified formats are not in the list"
            if (
                "GML" in dataset_format or not
                table[dataset_format][output_format]
            ):
                self.request.response.setStatus(400)
                # pylint: disable=line-too-long
                return "Error, specified data formats are not supported in this way"  # noqa
            response_json.update(
                {
                    "DatasetFormat": dataset_format,
                    "OutputFormat": output_format,
                }
            )

        if temporal_filter:
            log.info(validateDate1(temporal_filter))
            if not validateDate1(temporal_filter) and not validateDate2(
                temporal_filter
            ):
                self.request.response.setStatus(400)
                return "Error, date format is not correct"

            if not checkDateDifference(temporal_filter):
                self.request.response.setStatus(400)
                # pylint: disable=line-too-long
                return "Error, difference between StartDate and EndDate is not coherent"  # noqa

            if len(temporal_filter.keys()) > 2:
                self.request.response.setStatus(400)
                return "Error, TemporalFilter has too many fields"

            if (
                "StartDate" not in temporal_filter.keys() or
                "EndDate" not in temporal_filter.keys()
            ):
                self.request.response.setStatus(400)
                return (
                    "Error, TemporalFilter does not have StartDate or EndDate"
                )

            response_json.update({"TemporalFilter": temporal_filter})

        if outputGCS:
            if outputGCS not in GCS:
                self.request.response.setStatus(400)
                return "Error, defined GCS not in the list"
            response_json.update({"OutputGCS": outputGCS})

        if dataset_path:
            response_json.update({"DatasetPath": dataset_path})

        response_json["Status"] = "In_progress"
        response_json = utility.datarequest_post(response_json)

        log.info("AFTER CALLING INSERTION METHOD")
        log.info(response_json)

        self.request.response.setStatus(201)
        return response_json


def validateDownloadFormat():
    """ validate correct file format """
    the_table = defaultdict(dict)

    for input_iteration_format in dataset_formats:

        if input_iteration_format == "Shapefile":

            for output_iteration_format in dataset_formats:
                # pylint: disable=line-too-long
                the_table[input_iteration_format][
                        output_iteration_format
                    ] = output_iteration_format in ("GDB", "GPKG", "Geojson", "GML") # noqa: E501
                
        elif input_iteration_format == "GDB":

            for output_iteration_format in dataset_formats:
                # pylint: disable=line-too-long
                the_table[input_iteration_format][
                        output_iteration_format
                    ] = output_iteration_format in ("Shapefile", "GPKG", "Geojson", "GML") # noqa: E501
        elif input_iteration_format == "GPKG":

            for output_iteration_format in dataset_formats:
                # pylint: disable=line-too-long
                the_table[input_iteration_format][
                        output_iteration_format
                    ] = output_iteration_format in ("Shapefile", "GDB", "Geojson", "GML") # noqa: E501
                
        elif input_iteration_format == "Geojson":

            for output_iteration_format in dataset_formats:
                # pylint: disable=line-too-long
                the_table[input_iteration_format][
                        output_iteration_format
                    ] = output_iteration_format in ("Shapefile", "GDB", "GPKG", "GML") # noqa: E501
    
        elif input_iteration_format == "Geotiff":
            for output_iteration_format in dataset_formats:
                the_table[input_iteration_format][
                    output_iteration_format
                ] = False

        elif input_iteration_format == "Netcdf":
            for output_iteration_format in dataset_formats:

                if output_iteration_format == "Geotiff":
                    the_table[input_iteration_format][
                        output_iteration_format
                    ] = True
                else:
                    the_table[input_iteration_format][
                        output_iteration_format
                    ] = False

        elif input_iteration_format == "WFS":

            for output_iteration_format in dataset_formats:
                # pylint: disable=line-too-long
                the_table[input_iteration_format][
                        output_iteration_format
                    ] = output_iteration_format in ("Shapefile", "GDB", "GPKG", "Geojson", "GML") # noqa: E501
    # pylint: disable=line-too-long
    log.info(
        "------------------------------------------VALIDATION TABLE------------------------------------------"  # noqa
    )
    log.info(the_table)
    return the_table


def validateDate1(temporal_filter):
    """ validate date format year-month day """
    start_date = temporal_filter.get("StartDate")
    end_date = temporal_filter.get("EndDate")

    date_format = "%Y-%m-%d"
    try:
        if start_date is not None and end_date is not None:
            date_obj1 = datetime.datetime.strptime(start_date, date_format)
            log.info(date_obj1)
            date_obj2 = datetime.datetime.strptime(end_date, date_format)
            log.info(date_obj2)
            return {"StartDate": date_obj1, "EndDate": date_obj2}
        else:
            return False
    except ValueError:
        log.info("Incorrect data format, should be YYYY-MM-DD")
        return False


def validateDate2(temporal_filter):
    """ validate date format day-month-year"""
    start_date = temporal_filter.get("StartDate")
    end_date = temporal_filter.get("EndDate")

    date_format = "%d-%m-%Y"
    try:
        if start_date and end_date:
            date_obj1 = datetime.datetime.strptime(start_date, date_format)
            log.info(date_obj1)
            date_obj2 = datetime.datetime.strptime(end_date, date_format)
            log.info(date_obj2)
            return {"StartDate": date_obj1, "EndDate": date_obj2}
    except ValueError:
        log.info("Incorrect data format, should be DD-MM-YYYY")
        return False


def validateSpatialExtent(bounding_box):
    """ validate Bounding Box """
    if not len(bounding_box) == 4:
        return False

    for x in bounding_box:
        if not isinstance(x, int) and not isinstance(x, float):
            return False

    return True


def checkDateDifference(temporal_filter):
    """ Check date difference """
    log.info(temporal_filter)
    start_date = temporal_filter["StartDate"]
    end_date = temporal_filter.get("EndDate")

    return start_date < end_date


def validateNuts(nuts_id):
    """ validate nuts """

    match = re.match(r"([a-z]+)([0-9]+)", nuts_id, re.I)
    if match:
        items = match.groups()
        return items[0] in countries.keys()
    else:
        return False


def email_validation(mail):
    """ validate email address """
    a = 0
    y = len(mail)
    dot = mail.find(".")
    at = mail.find("@")
    log.info(mail)

    if "_" in mail[len(mail) - 1]:
        return False

    for i in range(0, at):
        if (mail[i] >= "a" and mail[i] <= "z") or (
            mail[i] >= "A" and mail[i] <= "Z"
        ):
            a = a + 1
    return a > 0 and at > 0 and (dot - at) > 0 and (dot + 1) < y
