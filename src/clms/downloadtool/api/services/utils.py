""" some util methods"""
# -*- coding: utf-8 -*-

from plone import api
from zope.component import getUtility
from zope.globalrequest import getRequest
from zope.i18n import translate
from zope.schema.interfaces import IVocabularyFactory
from zope.site.hooks import getSite


def get_extra_data(data_json):
    """append extra data to the stats json extracting the needed
    User information
    """
    data = {}
    user_id = data_json.get("User")
    if user_id is not None:
        user = api.user.get(user_name=user_id)
        if user is not None:
            data["user_country"] = get_user_profile_value_country(
                user.getProperty("country")
            )
            data["user_affiliation"] = get_user_profile_value_affiliation(
                user.getProperty("affiliation")
            )
            data[
                "user_thematic_activity"
            ] = get_user_profile_value_thematic_activity(
                user.getProperty("thematic_activity")
            )
            data[
                "user_sector_of_activity"
            ] = get_user_profile_value_sector_of_activity(
                user.getProperty("sector_of_activity")
            )

    return data


def get_values_from_vocabulary(item, vocabulary_name):
    """get the domain names checking the vocabulary"""
    site = getSite()
    voc = getUtility(
        IVocabularyFactory,
        name=vocabulary_name,
    )(site)

    term = voc.getTerm(item)
    return translate(term.title, context=getRequest())


def get_user_profile_value_country(term):
    """get the value from the relevant vocabulary"""
    return get_values_from_vocabulary(
        term, "collective.taxonomy.user_profile_country"
    )


def get_user_profile_value_affiliation(term):
    """get the value from the relevant vocabulary"""
    return get_values_from_vocabulary(
        term, "collective.taxonomy.user_profile_affiliation"
    )


def get_user_profile_value_thematic_activity(term):
    """get the value from the relevant vocabulary"""
    return get_values_from_vocabulary(
        term, "collective.taxonomy.user_profile_thematic_activity"
    )


def get_user_profile_value_sector_of_activity(term):
    """get the value from the relevant vocabulary"""
    return get_values_from_vocabulary(
        term, "collective.taxonomy.user_profile_sector_of_activity"
    )
