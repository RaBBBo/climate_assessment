from api_connect import APIClient
import pandas as pd
from utils import Utils
import math

class DataProcessor:

    # Initialize the class with the dataset and params attributes
    def __init__(self, dataset, params):
        self.dataset = dataset
        self.params = params

    # Define a method to get the data from the API
    def get_data(self):
        # Create an instance of the APIClient class
        client = APIClient()
        # Call the get_data method with the dataset and params attributes
        data = client.get_data(dataset=self.dataset, params=self.params)
        # Return the data
        return data

    # Define a method to process the data
    def process_data(self):
        # Get the data from the API
        data = self.get_data()
        # Extract the meta data
        meta_data = {
            "version":data["version"],
            "class":data["class"],
            "label":data["label"],
            "source":data["source"],
            "updated":data["updated"]
        }
        # Extract the series
        series = data["value"]
        # Extract the data dimensions
        data_dimensions = {
            "A": data["dimension"]['freq']['category']['label'].get('A', None),
            "Q": data["dimension"]['freq']['category']['label'].get('Q', None),
            "Process": data["dimension"]['wat_proc']['category']['label'],
            "Unit":data["dimension"]['unit']['category']['label'].values(),
            "geo_label":data["dimension"]['geo']['category']['label'],
            "geo_idx":data["dimension"]['geo']['category']['index'],
            "time_idx":data["dimension"]['time']['category']['index']
        }
        # Calculate number of sectors
        sector_count = len(data_dimensions["Process"].items())
        sector_dict = {i: v for i, v in enumerate(data_dimensions["Process"].values())}
        # Calculate the count
        count = len([key for index, key in enumerate(data_dimensions["time_idx"])])
        # Create a dictionary to map the country codes to names
        countries_dict = {v: k for k, v in data_dimensions['geo_idx'].items()}
        # Create a dictionary to map the time indices to periods
        ts_dict = {v: k for k, v in data_dimensions['time_idx'].items()}
        # Create a dataframe from the series
        df = pd.DataFrame.from_dict(series, orient='index', columns=['Values'])
        # Reset the index
        df = df.reset_index()
        # Convert the index to integer
        df['index'] = df["index"].astype('int64')
        # Calculate the idx column for the time series
        df['idx'] = df["index"]/ count
        df['idx'] = df['idx'].astype('int64')
        # Calculate the idx column for the sectors
        df['sector_idx'] = df["index"] / (len(series) / sector_count)
        df['sector_idx'] = df['sector_idx'].astype('int64')
        # Calculate the multiple for the time series
        multiple = df['idx'].astype('int64').nunique()
        # Create a dictionary to map the index to the period
        dict_map = Utils().make_ts_dict(ts_dict, multiple)
        # Create the multiple for the sectors
        multiple_sectors = df['sector_idx'].astype('int64').nunique()
        sector_map = Utils().make_sector_dict(sector_dict, multiple_sectors)
        # Create a dictionary to map the index to the period
        country_map = Utils().make_ts_dict(countries_dict, multiple)

        # Add the country and period columns to the dataframe
        df['country'] = df['idx'].map(country_map)
        df['period'] = df['index'].map(dict_map)
        df['sector'] = df['sector_idx'].map(sector_map)

        # Return the dataframe and the meta data
        return df, meta_data


