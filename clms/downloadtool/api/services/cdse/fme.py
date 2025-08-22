# -*- coding: utf-8 -*-
"""
When child CDSE tasks are finished parent task is sent to FME. (Temporary.)
"""


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


def prepare_params_for_fme(task_id):
    """ Prepare params using data from parent task, and the same structure
        as usually for FME tasks.
    """
    user_id = ""  # get it from downloadtool utility
    mail = ""  # get it from database
    utility_task_id = task_id
    callback_url = ""  # we need a function for this
    datasets = []  # get them from downloadtool utility

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
            {"name": "json", "value": json.dumps(datasets)},
        ]
    }

    return params


def send_task_to_fme(task_id):
    """ With params prepared send the task to FME
    """
    params = prepare_params_for_fme(task_id)
    result = post_request_to_fme(params)
