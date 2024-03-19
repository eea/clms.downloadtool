""" some util methods"""
# -*- coding: utf-8 -*-

import hashlib
import json
from typing import Any, Dict, List

import pyproj
from plone import api
from zope.component import getUtility
from zope.globalrequest import getRequest
from zope.i18n import translate
from zope.schema.interfaces import IVocabularyFactory
from zope.site.hooks import getSite
from clms.downloadtool.utils import GCS, OTHER_AVAILABLE_GCS


def dict_hash(dictionary: Dict[str, Any]) -> str:
    """SHA512 hash of a dictionary."""
    dhash = hashlib.sha512()
    # We need to sort arguments so {'a': 1, 'b': 2} is
    # the same as {'b': 2, 'a': 1}
    encoded = json.dumps(dictionary, sort_keys=True).encode()
    dhash.update(encoded)
    return dhash.hexdigest()


def get_extra_data(data_json):
    """append extra data to the stats json extracting the needed
    User information
    """
    data = {}
    user_id = data_json.get("User")
    if user_id is not None:
        user = api.user.get(username=user_id)
        if user is not None:
            try:
                data["user_country"] = get_user_profile_value_country(
                    user.getProperty("country")
                )
            except KeyError:
                data["user_country"] = ""

            try:
                data["user_affiliation"] = get_user_profile_value_affiliation(
                    user.getProperty("affiliation")
                )
            except KeyError:
                data["user_affiliation"] = ""

            try:
                data[
                    "user_thematic_activity"
                ] = get_user_profile_value_thematic_activity(
                    user.getProperty("thematic_activity")
                )
            except KeyError:
                data["user_thematic_activity"] = ""

            try:
                data[
                    "user_sector_of_activity"
                ] = get_user_profile_value_sector_of_activity(
                    user.getProperty("sector_of_activity")
                )
            except KeyError:
                data["user_sector_of_activity"] = ""

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


def duplicated_values_exist(item_list: List[Dict]):
    """check if any item in this list has a duplicate"""
    seen = []
    for item in item_list:
        hashed_item = dict_hash(item)
        if hashed_item in seen:
            return True
        seen.append(hashed_item)

    return False


def convert_to_epsg_3587(x: int, y: int) -> tuple:
    """converted latitude and longited to X, Y in EPSG:3587"""
    projection = pyproj.Proj(3587)
    return projection(x, y)


def calculate_bounding_box_area(bounding_box) -> int:
    """calculate the area of a given bounding box"""
    if len(bounding_box) == 4:
        width_1, height_1, width_2, height_2 = bounding_box
        converted_width_1, converted_height_1 = convert_to_epsg_3587(
            int(width_1), int(height_1)
        )
        converted_width_2, converted_height_2 = convert_to_epsg_3587(
            int(width_2), int(height_2)
        )

        return abs(
            # pylint: disable=line-too-long
            (converted_width_1 - converted_width_2) * (converted_height_1 - converted_height_2)  # noqa
        )

    return 0


def clean(item):
    """ clean a EPSG value"""
    new_item = item.strip()
    if not new_item.startswith('EPSG'):
        new_item = f'EPSG:{new_item}'

    return new_item


def get_available_gcs_values(dataset_uid):
    """ given a dataset uid, compute the list of selectable
        GCSs.

        When the dataset_object lists just one projection, return the
            standard set
        When the dataset_object lists multiple, return the standard set +
            the listed ones
    """
    brains = api.content.find(UID=dataset_uid)
    if brains:
        dataset_object = brains[0].getObject()
        dataset_projection = dataset_object.characteristics_projection
        projections = dataset_projection.split('/')
        if len(projections) == 1:
            return GCS

        cleaned_projections = map(clean, projections)
        # pylint: disable=line-too-long
        return GCS + [proj for proj in cleaned_projections if proj in OTHER_AVAILABLE_GCS]  # noqa

    return []
