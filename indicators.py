import pandas as pd
import numpy as np
from settings import input_path, country_map
from forex_python.converter import CurrencyRates
from datetime import datetime


def min_max_scaling(x, min_x, max_x):
    #NOTE: How to treat GVA's that are zero?
    x.replace([np.inf, -np.inf], np.nan, inplace=True)
    #NOTE: How to treat missing values
    x = x.fillna(x.mean(skipna=True))

    x = (x - min_x) / (max_x - min_x)

    return x

# GVA 
df_gva = pd.read_excel(input_path / 'National Accounts.xlsx', sheet_name='VA_CP')
df_gva = df_gva.drop(columns=['var'])

# >>>>> Proxy indicators <<<<<

# - Operation costs (Mortgages)
df_h_income = pd.read_csv(input_path / 'H_INCOME.csv').rename(columns={
    'LOCATION': 'geo_code', 'Value': 'H_Income'})
df_h_income.geo_code = df_h_income.geo_code.map(country_map)
df_h_spending = pd.read_csv(input_path / 'H_SPENDING.csv').rename(columns={
    'LOCATION': 'geo_code', 'Value': 'H_Spending'})
df_h_spending.geo_code = df_h_spending.geo_code.map(country_map)

data = df_gva[['geo_code', 'nace_r2_name', '2018']].rename(columns={'2018': 'GVA'})

data = data.merge(df_h_income[['geo_code', 'H_Income']], on='geo_code', how='left')
data = data.merge(df_h_spending[['geo_code', 'H_Spending']], on='geo_code', how='left')

data['indOperationCostsMortgages'] = ((data['H_Spending'] / data['H_Income']) / data['GVA'])
data['indOperationCostsMortgages'].replace([np.inf, -np.inf], np.nan, inplace=True)

data['indOperationCostsMortgages'] = min_max_scaling(
    x=data['indOperationCostsMortgages'],
    min_x=data['indOperationCostsMortgages'].min(skipna=True),
    max_x=data['indOperationCostsMortgages'].max(skipna=True))




