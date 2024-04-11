# ==========================================================================
#                           ARDA Broker API
#   It fetches PCH URLs from the ARDA Broker API.
#   Now ARDA Broker has been integrated into the ARDA API.
#   Still, this module is useful for fetching PCH URLs from the ARDA API.
#   @Author: Martin, 2022
# ===========================================================================
import requests

API_URL = "https://api.demo.the-maravian.com/"


def cached_urls(start_date, end_date, scope="region", scope_name="AF"):
    url = API_URL + f"/api/pch/urls?start_date={start_date}&end_date={end_date}&scope={scope}&scope_name={scope_name}"
    data = requests.get(url)
    return data.json()


# print(cached_urls('2022-5-1','2022-5-2', scope="country", scope_name="MW"))

def get_data_urls(start_date, end_date, scope="region", scope_name="AF"):
    """Return a list of PCH urls.
    @param start_date: start date in the format YYYY-MM-DD
    @param end_date: end date in the format YYYY-MM-DD
    @param scope: scope of the data. Can be region, country, city or ixp
    @param scope_name: name of the scope. For example, if scope is country, scope_name can be Kenya"""
    url = API_URL + f"api/routecollector-urls/?scope_name={scope_name}&scope={scope}&start_date={start_date}&end_date={end_date}"
    data = requests.get(url)
    return data.json()


class ARDABrokerAPI:
    """Provides an API to the ARDA PCH URL Broker"""

    @staticmethod
    def get_cached_urls(start_date, end_date, scope="Africa", scope_name="region"):
        """Return a list of urls."""
        return cached_urls(start_date, end_date, scope, scope_name)

    @staticmethod
    def get_pch_urls(start_date, end_date, scope="Africa", scope_name="region"):
        """Return a list of PCH urls."""
        return get_data_urls(start_date, end_date, scope, scope_name)
