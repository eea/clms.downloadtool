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


'''

Request status (PATCH)
This method will be used by server tools to change the status of the requests
Method: GET/datarequest/status

Parameters:
TaskID
Status
{result data}
Result: task is changed to specified status, and the result data are set (request shall return TaskID and status)

'''

class datarequest_status_patch(Service):


    def reply(self):
        status_list = ["Rejected", "Queued", "In_progress", "Finished_ok", "Finished_nok", "Cancelled"]

        body = json_body(self.request)

        task_id = body.get("task_id")
        user_id = body.get("user_id")
        download_format = body.get("download_format")
        dataset_id = body.get("dataset_id")
        spatial_extent = body.get("spatial_extent")
        temporal_extent = body.get("temporal_extent")

        response_json = {}

        utility = getUtility(IDownloadToolUtility)
        #utility.datarequest_post(key, value)

        
        if user_id and download_format and dataset_id:
            
            if temporal_extent:
                temporal_extent_validate1 = validateDate1(temporal_extent)
                temporal_extent_validate2 = validateDate2(temporal_extent)

                if validateSpatialExtent(spatial_extent) and temporal_extent_validate1 or temporal_extent_validate2:
                    response_json = {"user_id": user_id, "download_format": download_format,
                    "dataset_id": dataset_id, "temporal_extent": {"start_date":temporal_extent.get("start_date"), "end_date":temporal_extent.get("end_date")}} 

                else: 
                    response_json = {"user_id": user_id, "download_format": download_format, "dataset_id": dataset_id,
                    "spatial_extent": [spatial_extent[0],spatial_extent[1],spatial_extent[2],spatial_extent[3]],"temporal_extent": {"start_date":temporal_extent.get("start_date"), "end_date":temporal_extent.get("end_date")}}


            elif validateSpatialExtent(spatial_extent):
                if spatial_extent_validate:
                    response_json = {"user_id": user_id, "download_format": download_format, "dataset_id": dataset_id, "spatial_extent": [spatial_extent[0],spatial_extent[1],spatial_extent[2],spatial_extent[3]]}
                

            else:
                response_json = {"user_id": user_id, "download_format": download_format, "dataset_id": dataset_id}
            
            response_json = utility.datarequest_status_patch(response_json, task_id)

            self.request.response.setStatus(201)
            return response_json

        else:
            self.request.response.setStatus(400)
            return "Error, required fields not filled"


def validateDate1(temporal_extent):

    start_date = temporal_extent.get('start_date')
    end_date = temporal_extent.get('end_date')

    date_format = '%Y-%m-%d'
    try:
        date_obj = datetime.datetime.strptime(start_date, date_format)
        log.info(date_obj)
        date_obj = datetime.datetime.strptime(end_date, date_format)
        log.info(date_obj)
        return True
    except ValueError:
        log.info("Incorrect data format, should be YYYY-MM-DD")
        return False

def validateDate2(temporal_extent):

    start_date = temporal_extent.get('start_date')
    end_date = temporal_extent.get('end_date')

    date_format = '%d-%m-%Y'
    try:
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



