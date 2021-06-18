# -*- coding: utf-8 -*-
"""
For HTTP GET operations we can use standard HTTP parameter passing (through the URL)

"""
from plone import api
from plone.restapi.services import Service
from plone.restapi.deserializer import json_body

from zope.component import getUtility
from clms.downloadtool.utility import IDownloadToolUtility

import datetime

# logger, do log.info('XXXX') to print in the console
from logging import getLogger

log = getLogger(__name__)

countries = {"BD": "BGD", "BE": "BEL", "BF": "BFA", "BG": "BGR", "BA": "BIH", "BB": "BRB", "WF": "WLF", "BL": "BLM", "BM": "BMU", "BN": "BRN", "BO": "BOL", "BH": "BHR", "BI": "BDI", "BJ": "BEN", "BT": "BTN", "JM": "JAM", "BV": "BVT", "BW": "BWA", "WS": "WSM", "BQ": "BES", "BR": "BRA", "BS": "BHS", "JE": "JEY", "BY": "BLR", "BZ": "BLZ", "RU": "RUS", "RW": "RWA", "RS": "SRB", "TL": "TLS", "RE": "REU", "TM": "TKM", "TJ": "TJK", "RO": "ROU", "TK": "TKL", "GW": "GNB", "GU": "GUM", "GT": "GTM", "GS": "SGS", "GR": "GRC", "GQ": "GNQ", "GP": "GLP", "JP": "JPN", "GY": "GUY", "GG": "GGY", "GF": "GUF", "GE": "GEO", "GD": "GRD", "GB": "GBR", "GA": "GAB", "SV": "SLV", "GN": "GIN", "GM": "GMB", "GL": "GRL", "GI": "GIB", "GH": "GHA", "OM": "OMN", "TN": "TUN", "JO": "JOR", "HR": "HRV", "HT": "HTI", "HU": "HUN", "HK": "HKG", "HN": "HND", "HM": "HMD", "VE": "VEN", "PR": "PRI", "PS": "PSE", "PW": "PLW", "PT": "PRT", "SJ": "SJM", "PY": "PRY", "IQ": "IRQ", "PA": "PAN", "PF": "PYF", "PG": "PNG", "PE": "PER", "PK": "PAK", "PH": "PHL", "PN": "PCN", "PL": "POL", "PM": "SPM", "ZM": "ZMB", "EH": "ESH", "EE": "EST", "EG": "EGY", "ZA": "ZAF", "EC": "ECU", "IT": "ITA", "VN": "VNM", "SB": "SLB", "ET": "ETH", "SO": "SOM", "ZW": "ZWE", "SA": "SAU", "ES": "ESP", "ER": "ERI", "ME": "MNE", "MD": "MDA", "MG": "MDG", "MF": "MAF", "MA": "MAR", "MC": "MCO", "UZ": "UZB", "MM": "MMR", "ML": "MLI", "MO": "MAC", "MN": "MNG", "MH": "MHL", "MK": "MKD", "MU": "MUS", "MT": "MLT", "MW": "MWI", "MV": "MDV", "MQ": "MTQ", "MP": "MNP", "MS": "MSR", "MR": "MRT", "IM": "IMN", "UG": "UGA", "TZ": "TZA", "MY": "MYS", "MX": "MEX", "IL": "ISR", "FR": "FRA", "IO": "IOT", "SH": "SHN", "FI": "FIN", "FJ": "FJI", "FK": "FLK", "FM": "FSM", "FO": "FRO", "NI": "NIC", "NL": "NLD", "NO": "NOR", "NA": "NAM", "VU": "VUT", "NC": "NCL", "NE": "NER", "NF": "NFK", "NG": "NGA", "NZ": "NZL", "NP": "NPL", "NR": "NRU", "NU": "NIU", "CK": "COK", "XK": "XKX", "CI": "CIV", "CH": "CHE", "CO": "COL", "CN": "CHN", "CM": "CMR", "CL": "CHL", "CC": "CCK", "CA": "CAN", "CG": "COG", "CF": "CAF", "CD": "COD", "CZ": "CZE", "CY": "CYP", "CX": "CXR", "CR": "CRI", "CW": "CUW", "CV": "CPV", "CU": "CUB", "SZ": "SWZ", "SY": "SYR", "SX": "SXM", "KG": "KGZ", "KE": "KEN", "SS": "SSD", "SR": "SUR", "KI": "KIR", "KH": "KHM", "KN": "KNA", "KM": "COM", "ST": "STP", "SK": "SVK", "KR": "KOR", "SI": "SVN", "KP": "PRK", "KW": "KWT", "SN": "SEN", "SM": "SMR", "SL": "SLE", "SC": "SYC", "KZ": "KAZ", "KY": "CYM", "SG": "SGP", "SE": "SWE", "SD": "SDN", "DO": "DOM", "DM": "DMA", "DJ": "DJI", "DK": "DNK", "VG": "VGB", "DE": "DEU", "YE": "YEM", "DZ": "DZA", "US": "USA", "UY": "URY", "YT": "MYT", "UM": "UMI", "LB": "LBN", "LC": "LCA", "LA": "LAO", "TV": "TUV", "TW": "TWN", "TT": "TTO", "TR": "TUR", "LK": "LKA", "LI": "LIE", "LV": "LVA", "TO": "TON", "LT": "LTU", "LU": "LUX", "LR": "LBR", "LS": "LSO", "TH": "THA", "TF": "ATF", "TG": "TGO", "TD": "TCD", "TC": "TCA", "LY": "LBY", "VA": "VAT", "VC": "VCT", "AE": "ARE", "AD": "AND", "AG": "ATG", "AF": "AFG", "AI": "AIA", "VI": "VIR", "IS": "ISL", "IR": "IRN", "AM": "ARM", "AL": "ALB", "AO": "AGO", "AQ": "ATA", "AS": "ASM", "AR": "ARG", "AU": "AUS", "AT": "AUT", "AW": "ABW", "IN": "IND", "AX": "ALA", "AZ": "AZE", "IE": "IRL", "ID": "IDN", "UA": "UKR", "QA": "QAT", "MZ": "MOZ"}
GCS = ["EPGS:4326", "EPGS:3035", "EPGS:3857", "EPGS:4258"]
class DataRequestPost(Service):

    def reply(self):
        dataset_formats = ["Shapefile", "GDB", "GPKG", "Geojson", "Geotiff", "Netcdf", "GML", "WFS"]
        body = json_body(self.request)

        user_id = str(body.get("UserID"))
        dataset_format = body.get("DatasetFormat")
        dataset_id = body.get("DatasetID")
        spatial_extent = body.get("BoundingBox")
        temporal_extent = body.get("TemporalFilter")
        output_format = body.get("OutputFormat")
        outputGCS = body.get("OutputGCS")
        nuts_id = body.get("NUTSID")

        response_json = {}

        utility = getUtility(IDownloadToolUtility)

        
        if user_id and dataset_format and dataset_id and nuts_id:
            
            if validateDownloadFormat(dataset_format, output_format):

                if temporal_extent and outputGCS:
                    temporal_extent_validate1 = validateDate1(temporal_extent)
                    temporal_extent_validate2 = validateDate2(temporal_extent)

                    if validateSpatialExtent(spatial_extent) and temporal_extent_validate1 or temporal_extent_validate2:
                        for key in countries.keys():
                            if key in nuts_id:
                                response_json = {"UserID": user_id, "DatasetFormat": dataset_format,
                                "DatasetID": dataset_id, "NUTSID":nuts_id, "OutputGCS":outputGCS, "OutputFormat": output_format,  "TemporalFilter": {"StartDate":temporal_extent.get("StartDate"), "EndDate":temporal_extent.get("EndDate")}} 

                    else: 
                        response_json = {"UserID": user_id, "DatasetFormat": dataset_format, "DatasetID": dataset_id, "NUTSID":nuts_id,
                        "OutputGCS":outputGCS, "OutputFormat": output_format, "BoundingBox": [spatial_extent[0],spatial_extent[1],spatial_extent[2],spatial_extent[3]],"TemporalFilter": {"StartDate":temporal_extent.get("StartDate"), "EndDate":temporal_extent.get("EndDate")}}


                elif validateSpatialExtent(spatial_extent):
                    if spatial_extent_validate:
                        response_json = {"UserID": user_id, "DatasetFormat": dataset_format, "DatasetID": dataset_id, "NUTSID":nuts_id, "OutputFormat": output_format, "BoundingBox": [spatial_extent[0],spatial_extent[1],spatial_extent[2],spatial_extent[3]]}
                    

            else:
                return "Error, format not valid"
                #response_json = {"UserID": user_id, "DatasetFormat": dataset_format, "DatasetID": dataset_id}
            
            response_json["Status"] = "In_progress"
            response_json = utility.datarequest_post(response_json)

            log.info('BEFORE CALLING INSERTION METHOD')
            log.info(response_json)


            self.request.response.setStatus(201)
            return response_json

        else:
            self.request.response.setStatus(400)
            return "Error, required fields not filled"


def validateDate1(temporal_extent):

    start_date = temporal_extent.get('StartDate')
    end_date = temporal_extent.get('EndDate')

    date_format = '%Y-%m-%d'
    try:
        if start_date and end_date:
            date_obj = datetime.datetime.strptime(start_date, date_format)
            log.info(date_obj)
            date_obj = datetime.datetime.strptime(end_date, date_format)
            log.info(date_obj)
            return True
        else:
            return False
    except ValueError:
        log.info("Incorrect data format, should be YYYY-MM-DD")
        return False

def validateDate2(temporal_extent):

    start_date = temporal_extent.get('StartDate')
    end_date = temporal_extent.get('EndDate')

    date_format = '%d-%m-%Y'
    try:
        if start_date and end_date:
            date_obj = datetime.datetime.strptime(start_date, date_format)
            log.info(date_obj)
            date_obj = datetime.datetime.strptime(end_date, date_format)
            log.info(date_obj)
            return True
    except ValueError:
        log.info("Incorrect data format, should be DD-MM-YYYY")
        return False
    
def validateSpatialExtent(spatial_extent):
    
    try:
        if len(spatial_extent) == 4:
            spatial_extent1 = float(spatial_extent[0])
            spatial_extent2 = float(spatial_extent[1])
            spatial_extent3 = float(spatial_extent[2])
            spatial_extent4 = float(spatial_extent[3])
            return True
        else:
            return False    
    except ValueError:
        return False

def validateDownloadFormat(input_format, output_format):
    dataset_formats = ["Shapefile", "GDB", "GPKG", "Geojson", "Geotiff", "Netcdf", "GML", "WFS"]

    for input_iteration_format in dataset_formats:

        if input_iteration_format == "Shapefile":
            
            for output_iteration_format in dataset_formats:
                
                if output_iteration_format == "GDB" or output_iteration_format == "GPKG" or output_iteration_format == "Geojson" or output_iteration_format == "GML":
                    return True
        
        elif input_iteration_format == "GDB":

            for output_iteration_format in dataset_formats:
                
                if output_iteration_format == "Shapefile" or output_iteration_format == "GPKG" or output_iteration_format == "Geojson" or output_iteration_format == "GML":
                    return True
        
        elif input_iteration_format == "GPKG":

            for output_iteration_format in dataset_formats:
                
                if output_iteration_format == "Shapefile" or output_iteration_format == "GDB" or output_iteration_format == "Geojson" or output_iteration_format == "GML":
                    return True

        elif input_iteration_format == "Geojson":

            for output_iteration_format in dataset_formats:
                
                if output_iteration_format == "Shapefile" or output_iteration_format == "GDB" or output_iteration_format == "GPKG" or output_iteration_format == "GML":
                    return True

        elif input_iteration_format == "Geotiff":
            return False
        
        elif input_iteration_format == "Netcdf":
                
            if output_format == "Geotiff":
                return True

        elif input_iteration_format == "WFS":
            
            for output_iteration_format in dataset_formats:

                if output_iteration_format == "Shapefile" or output_iteration_format == "GDB" or output_iteration_format == "GPKG" or output_iteration_format == "Geojson" or output_iteration_format == "GML":
                    return True
        
        else:
            return False

    return False

