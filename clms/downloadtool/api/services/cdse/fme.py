# -*- coding: utf-8 -*-
"""
When child CDSE tasks are finished parent task is sent to FME. (Temporary.)

Example of params in request to FME
(Pdb) pp params
{'publishedParameters': [{'name': 'UserID', 'value': 'nicenickname'},
                         {'name': 'TaskID', 'value': '512723423423'},
                         {'name': 'UserMail', 'value': 'asd@aasd.com'},
                         {'name': 'CallbackUrl',
                          'value': 'http://lo../++api++/@da.._status_patch'},
                         {'name': 'json',
                          'value': '{"Datasets": [{"DatasetID": '
                                   '"9e8be500204945a48ce15fc8d1c57482", '
                                   '"DatasetTitle": "Lake Water Quality '
                                   '2024-present (raster 300 m), global, '
                                   '10-daily \\u2013 version 2", "NUTSID": '
                                   '"FR", "NUTSName": "France", '
                                   '"TemporalFilter": {"StartDate": '
                                   '"2025-08-11 21:00:00", "EndDate": '
                                   '"2025-08-12 21:00:00"}, "OutputGCS": '
                                   '"EPSG:4326", "Layer": "ALL BANDS", '
                                   '"DatasetFormat": "Netcdf", "OutputFormat":'
                                   '"Netcdf", "DatasetPath": '
                                   '"asdasd", '
                                   '"DatasetSource": "LEGACY", "WekeoChoices":'
                                   '"", "Metadata": '
                                   '["https://xsdasdml?approved=true"]}]}'}]}
"""

import json
from logging import getLogger

import requests
from plone import api
from clms.downloadtool.asyncjobs.queues import queue_job
from zope.component import getUtility

from clms.downloadtool.utility import IDownloadToolUtility


log = getLogger(__name__)


def get_callback_url():
    """get the callback url where FME should signal any status changes
       THIS CODE IS DUPLICATE, but we will use it only temporary.
    """
    portal_url = api.portal.get().absolute_url()
    if portal_url.endswith("/api"):
        portal_url = portal_url.replace("/api", "")

    return "{}/++api++/{}".format(
        portal_url,
        "@datarequest_status_patch",
    )


def get_task_info(task_id):
    """ Get task info from downloadtool utility
    """
    utility = getUtility(IDownloadToolUtility)
    return utility.datarequest_status_get(task_id)


def post_request_to_fme(params, is_prepackaged=False):
    """send the request to FME and let it process it
       THIS CODE IS DUPLICATE, but we will use it only temporary.
    """
    if is_prepackaged:
        fme_url = api.portal.get_registry_record(
            "clms.downloadtool.fme_config_controlpanel.url_prepackaged"
        )
    else:
        fme_url = api.portal.get_registry_record(
            "clms.downloadtool.fme_config_controlpanel.url"
        )
    fme_token = api.portal.get_registry_record(
        "clms.downloadtool.fme_config_controlpanel.fme_token"
    )
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json",
        "Authorization": "fmetoken token={0}".format(fme_token),
    }
    try:
        resp = requests.post(
            fme_url, json=params, headers=headers, timeout=10
        )
        if resp.ok:
            fme_task_id = resp.json().get("id", None)
            return fme_task_id
    except requests.exceptions.Timeout:
        log.info("FME request timed out")
    body = json.dumps(params)
    log.info(
        "There was an error registering the download request in FME: %s",
        body,
    )

    return {}


def send_task_to_fme(task_id):
    """ Prepare the params and send the task to FME
    """
    task_info = get_task_info(task_id)
    user_id = task_info.get("UserID", "")
    mail = api.user.get(user_id).getProperty('email')
    utility_task_id = task_id
    callback_url = get_callback_url()
    datasets = task_info.get("Datasets", [])
    cdse_batch_ids = task_info.get("CDSEBatchIDs", [])
    gpkg_filenames = task_info.get("GpkgFileNames", [])
    datasets_info = {
        "Datasets": datasets,
        "CDSEBatchIDs": cdse_batch_ids,
        "GpkgFileNames": gpkg_filenames
    }

    params = {
        "publishedParameters": [
            {
                "name": "UserID",
                "value": str(user_id),
            },
            {
                "name": "TaskID",
                "value": utility_task_id,
            },
            {
                "name": "UserMail",
                "value": mail,
            },
            {
                "name": "CallbackUrl",
                "value": callback_url,
            },
            # dump the json into a string for FME
            {"name": "json", "value": json.dumps(datasets_info)},
        ]
    }

    fme_result = post_request_to_fme(params)  # CDSE is_prepackaged?

    data_object = task_info
    if fme_result:
        data_object["FMETaskId"] = fme_result
        queue_job("downloadtool_jobs", "downloadtool_updates", {
            'operation': 'datarequest_status_patch',
            'updates': {
                'data_object': data_object,
                'utility_task_id': utility_task_id
            }
        })
        return "SUCCESS"
    return "ERROR"
