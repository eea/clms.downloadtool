"""
Return NUTS region names


"""
from plone import api
from plone.memoize.ram import cache
from plone.restapi.search.utils import unflatten_dotted_dict
from plone.restapi.services import Service

import requests


def _cache_key(fun, self, nutsid):
    """ Cache key function """
    return nutsid


class NUTSName(Service):
    """ Service to return nuts region names """

    def reply(self):
        """ return the names"""
        query = self.request.form.copy()
        query = unflatten_dotted_dict(query)
        new_query = {}
        for k, v in query.items():
            if isinstance(v, list):
                new_query[k] = []
                for item in v:
                    if "," in v:
                        new_query[k].extend(item.split(","))
                    else:
                        new_query[k].append(item)
            else:
                if "," in v:
                    new_query[k] = v.split(",")
                else:
                    new_query[k] = [v]

        nuts_ids = new_query.get("nuts_ids", [])
        res = {}
        for nuts_id in nuts_ids:
            res[nuts_id] = self.get_nuts_name(nuts_id)

        return res

    @cache(_cache_key)
    def get_nuts_name(self, nutsid):
        """Based on the NUTS ID, return the name of
        the NUTS region.
        """
        url = api.portal.get_registry_record(
            "clms.downloadtool.fme_config_controlpanel.nuts_service"
        )
        if url:
            url += "where=NUTS_ID='{}'".format(nutsid)
            resp = requests.get(url)
            if resp.ok:
                resp_json = resp.json()
                features = resp_json.get("features", [])
                for feature in features:
                    attributes = feature.get("attributes", {})
                    nuts_name = attributes.get("NAME_LATN", "")
                    if nuts_name:
                        return nuts_name

        return nutsid
