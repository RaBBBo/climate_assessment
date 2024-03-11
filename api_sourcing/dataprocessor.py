from api_sourcing.api_connect import APIClient
import pandas as pd
from api_sourcing.utils import Utils
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
            "Source": data["dimension"]['wat_src']['category']['label'],
            "Unit":data["dimension"]['unit']['category']['label'],
            "geo_label":data["dimension"]['geo']['category']['label'],
            "geo_idx":data["dimension"]['geo']['category']['index'],
            "time_idx":data["dimension"]['time']['category']['index']
        }
        # Calculate number of sectors
        sector_dict = {i: v for i, v in enumerate(data_dimensions["Process"].values())}
        n_sectors = len(sector_dict)
        # Calculate number of sources
        source_dict = {i: v for i, v in enumerate(data_dimensions["Source"].values())}
        n_sources = len(source_dict)
        # Calculate number of sources
        unit_dict = {i: v for i, v in enumerate(data_dimensions["Unit"].values())}
        n_units = len(unit_dict)
        # Create a dictionary to map the country codes to names
        countries_dict = {v: k for k, v in data_dimensions['geo_idx'].items()}
        countries = len(countries_dict)
        # Create a dictionary to map the time indices to periods
        ts_dict = {v: k for k, v in data_dimensions['time_idx'].items()}
        n_years = len(ts_dict)

        # Create a dataframe from the series
        df = pd.DataFrame.from_dict(series, orient='index', columns=['Values'])
        # Reset the index
        df = df.reset_index()

        # Convert the index to integer
        df['index'] = df["index"].astype('int64').sort_values()

        # Calculate the idx column for the sectors
        df['sector_idx'] = df["index"] / (n_sources * n_years * n_units * countries)
        df['sector_idx'] = df['sector_idx'].astype('int64')

        # Calculate the idx column for the sources
        df['source_idx'] = df["index"] / (n_units * n_years * countries)
        df['source_idx'] = df['source_idx'].astype('int64') % n_sources

        # Calculate the idx column for the units
        df['unit_idx'] = df["index"] / (n_years * countries)
        df['unit_idx'] = df['unit_idx'].astype('int64') % n_units

        # Calculate the idx column for the time series
        df['years'] = df["index"] % n_years

        # Calculate the idx column for the time series
        df['countries'] = df["index"] / n_years
        df['countries'] = df['countries'].astype('int64') % countries

        # Create the multiple for the sectors
        multiple_sectors = df['sector_idx'].astype('int64').nunique()
        sector_map = Utils().make_sector_dict(sector_dict, multiple_sectors)

        # Create the multiple for the sectors
        multiple_source = df['source_idx'].astype('int64').nunique()
        source_map = Utils().make_sector_dict(source_dict, multiple_source)

        # Add the country and period columns to the dataframe
        df['country'] = df['countries'].map(countries_dict)
        df['period'] = df['years'].map(ts_dict)
        df['sector'] = df['sector_idx'].map(sector_map)
        df['source'] = df['source_idx'].map(source_map)
        df['unit'] = df['unit_idx'].map(unit_dict)

        df = df.drop(columns=["unit_idx", "source_idx", "sector_idx", "years"])
        # Return the dataframe and the meta data
        return df, meta_data
