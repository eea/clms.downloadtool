"""
Return NUTS region names


"""
import requests
from plone import api
from plone.restapi.search.utils import unflatten_dotted_dict
from plone.restapi.services import Service
from eea.cache import cache

LAYER_PER_LEVEL = {
    "0": "0",
    "1": "1",
    "2": "3",
    "3": "6",
}


def _cache_key(fun, self, nutsid):
    """Cache key function"""
    return nutsid


class NUTSName(Service):
    """Service to return nuts region names"""

    def reply(self):
        """return the names"""
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
            name = self.get_nuts_name(nuts_id)
            if not name or name == nuts_id:
                name = self.get_country_name(nuts_id)

            res[nuts_id] = name

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

            nuts_level = str(len(nutsid[2:]))

            layer = LAYER_PER_LEVEL.get(nuts_level)

            url = url.replace(
                "/MapServer/0/query", f"/MapServer/{layer}/query"
            )

            resp = requests.get(url, timeout=5)
            if resp.ok:
                resp_json = resp.json()
                features = resp_json.get("features", [])
                for feature in features:
                    attributes = feature.get("attributes", {})
                    nuts_name = attributes.get("NAME_LATN", "")
                    if nuts_name:
                        return nuts_name

        return nutsid

    @cache(_cache_key)
    def get_country_name(self, country_id):
        """based on the country id, return the name of it"""
        url = api.portal.get_registry_record(
            "clms.downloadtool.fme_config_controlpanel.countries_service"
        )
        if url:
            url += f"where=ISO_2DIGIT='{country_id}'"

            resp = requests.get(url, timeout=5)
            if resp.ok:
                resp_json = resp.json()
                features = resp_json.get("features", [])
                for feature in features:
                    attributes = feature.get("attributes", {})
                    country_name = attributes.get("MIN_CNTRY_", "")
                    if country_name:
                        return country_name

        return country_id
