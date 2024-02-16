import requests
import pandas
import json


class APIClient:
    def __init__(self):
        self.host_url = "https://ec.europa.eu/eurostat/api/dissemination/"

    def get_data(self, dataset, params):
        if params:
            url = f"{self.host_url}statistics/1.0/data/{dataset}?{params[0]}&lang=EN&{params[1]}"
        else:
            url = f"{self.host_url}statistics/1.0/data/{dataset}?lang=EN"

        url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/env_wat_abs?format=JSON&sinceTimePeriod=2012&geo=BE&geo=BG&geo=CZ&geo=DK&geo=DE&geo=EE&geo=EL&unit=MIO_M3&wat_proc=ABST&wat_src=FSW&lang=en"
        response = requests.get(url)
        return response.json()


