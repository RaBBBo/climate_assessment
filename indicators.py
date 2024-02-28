import pandas as pd
import numpy as np
from settings import input_path, country_map
from forex_python.converter import CurrencyRates
from datetime import datetime
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt


def min_max_scaling(x, min_x, max_x):
    # #NOTE: How to treat GVA's that are zero?
    # x.replace([np.inf, -np.inf], np.nan, inplace=True)
    #NOTE: How to treat missing values
    x = x.fillna(x.mean(skipna=True))

    x = (x - min_x) / (max_x - min_x)

    return x

scaler = MinMaxScaler()

# GVA 
df_gva = pd.read_excel(input_path / 'National Accounts.xlsx', sheet_name='VA_CP')
df_gva = df_gva.drop(columns=['var'])
data = df_gva[['geo_code', 'nace_r2_name', '2018']].rename(columns={'2018': 'GVA'})

# >>>>> Proxy indicators <<<<<

# - Operation costs (Mortgages)
df_h_income = pd.read_csv(input_path / 'H_INCOME.csv').rename(columns={
    'LOCATION': 'geo_code', 'Value': 'H_Income'})
df_h_income.geo_code = df_h_income.geo_code.map(country_map)
df_h_spending = pd.read_csv(input_path / 'H_SPENDING.csv').rename(columns={
    'LOCATION': 'geo_code', 'Value': 'H_Spending'})
df_h_spending.geo_code = df_h_spending.geo_code.map(country_map)

data = data.merge(df_h_income[['geo_code', 'H_Income']], on='geo_code', how='left')
data = data.merge(df_h_spending[['geo_code', 'H_Spending']], on='geo_code', how='left')

data['indOperationCostsMortgages'] = (data['H_Spending'] / data['H_Income'])
data['indOperationCostsMortgages'].replace([np.inf, -np.inf], np.nan, inplace=True)

data['indOperationCostsMortgages'] = min_max_scaling(x=data['indOperationCostsMortgages'])

# - Water abstraction
params = {
    "format": "format=JSON",
    "filters": {"time":"2019"},
    "sinceTimePeriod" : "sinceTimePeriod = 2012",
    "unit":"unit=MIO_M3"
}
dataset = "env_wat_abs"
d = DataProcessor(dataset=dataset, params=params)
df, meta_data = d.process_data()

df_water_abstraction = df

data = data.merge(df_water_abstraction[['geo_code', 'Water_Abstraction']], on='geo_code', how='left')

data['indWaterAbstraction'] = min_max_scaling(data['Water_Abstraction'] / data['GVA'])


# - Transport and supply
df_Ip_TraEq = pd.read_csv(input_path / 'old_proxies' / 'capital accounts.csv')
df_Ip_TraEq = df_Ip_TraEq[['geo_code', 'nace_r2_name', 'Ip_TraEq']]

data = data.merge(df_Ip_TraEq, on=['nace_r2_name', 'geo_code'], how='left')
data['TransportSupply'] = data['Ip_TraEq'] / data['GVA']
data['indTransportSupply'] = data.groupby('nace_r2_name')['TransportSupply'].transform('mean')

data['indTransportSupply'] = min_max_scaling(
    x=data['indTransportSupply'],
    min_x=data['indTransportSupply'].min(skipna=True),
    max_x=data['indTransportSupply'].max(skipna=True))

data['indTransportSupply'] = scaler.fit_transform(data['indTransportSupply'].values.reshape(-1, 1))











