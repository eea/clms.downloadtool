import code


""" some util methods"""
# -*- coding: utf-8 -*-

from plone import api


def get_extra_data(data_json):
    """append extra data to the stats json extracting the needed
    User information
    """
    data = {}
    user_id = data_json.get("User")
    if user_id is not None:
        user = api.user.get(user_name=user_id)
        if user is not None:
            data["user_country"] = user.getProperty("country")
            data["user_affiliation"] = user.getProperty("affiliation")
            data["user_thematic_activity"] = user.getProperty(
                "thematic_activity"
            )
            data["user_sector_of_activity"] = user.getProperty(
                "sector_of_activity"
            )

    return data
