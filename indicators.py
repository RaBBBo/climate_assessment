import pandas as pd
from settings import input_path

#1 GVA
df_gva = pd.read_excel(input_path / 'National Accounts.xlsx', sheet_name='VA_CP')
df_gva = df_gva.drop(columns=['var'])
