import requests
import pandas
import json
import typing

class APIClient:
    def __init__(self):
        self.host_url = "https://ec.europa.eu/eurostat/api/dissemination/"

    def get_data(self, dataset, params=None):
        try:
            base_url = f"{self.host_url}statistics/1.0/data/{dataset}?format=JSON"
            param_str = ""
            for param, val in params.items():
                if isinstance(val, list):
                    param_lst = "&".join([f"{param}={v}" for v in val])
                elif isinstance(val, str):
                    param_lst = f"{param}={val}"
                    
                param_str += f"&{param_lst}"
                
            url = f"{base_url}{param_str}&lang=en"
            response = requests.get(url)
            return response.json()
        except:
            print(f"request not valid adjust params.")
