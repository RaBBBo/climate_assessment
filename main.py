# Import modules
import pandas as pd
import numpy as np
import csv
import openpyxl
import networkx as nx
import math
import matplotlib.pyplot as plt
from settings import input_path

# Load in data
EUROSTAT_ENV_WAT_ABS = pd.read_excel(input_path / 'water_abstraction.xlsx', sheet_name=None)
db_map_EUROSTAT = pd.read_excel(input_path / "Data_map.xlsx", sheet_name='Eurostat')
EUKLEMS_INTANPROD_naccounts = pd.read_csv(input_path / "national accounts.csv"
                                          ,quotechar='"'
                                          ,quoting=1
                                          ,doublequote=True)                                
EUKLEMS_INTANPROD_caccounts = pd.read_csv(input_path / "capital accounts.csv"
                                          ,quotechar = '"'
                                          ,quoting=1
                                          ,doublequote=True) 
# Read in database sector mapping to heatmap sectors
db_map_EUKLEMS = pd.read_excel(input_path / "Data_map.xlsx", sheet_name='EUKLEMS')
IEA = pd.read_excel(input_path / "IEA EEI database_Highlights.xlsb",
                    sheet_name=['Services - Energy', 'Industry - Energy', 'Transport - Energy']) 
db_map_IEA = pd.read_excel(input_path / "Data_map.xlsx", sheet_name='IEA')

# Read in currency exchange rates
e_rates = pd.read_csv(input_path / "API_PA.NUS.FCRF_DS2_en_csv_v2_4772354.csv"
                      ,quotechar = '"'
                      ,quoting=1
                      ,doublequote = True
                      ,skiprows=[0,1,2,3]
                      ,on_bad_lines='skip')

# Reformat e_rates db
e_rates = e_rates[['Country Name','2018']]
e_rates = e_rates.rename(columns={e_rates.columns[0]:'Country',
                                  e_rates.columns[1]:'e_rate'})

# Assign e_rates to euro area countries in 2018
euro_area = ['Austria', 'Belgium','Cyprus','Estonia','Finland'
             ,'France','Germany','Greece','Ireland','Italy','Latvia'
             ,'Lithuania','Luxembourg','Malta','Netherlands','Portugal'
             ,'Slovakia','Slovenia','Spain']

# euro_area_rate = e_rates.loc[e_rates['Country'].eq('Euro area')]
# euro_area_rate = e_rates.loc[e_rates['Country'].isin(euro_area)] # NOTE: rates don't exist

euro_area_rate = float(e_rates[e_rates['Country'] == 'Euro area']['e_rate'])
e_rates.loc[e_rates['Country'].isin(euro_area),'e_rate'] = euro_area_rate # fill missing values by the euro area

# Read and reformat database structure
db_structure = EUROSTAT_ENV_WAT_ABS['Summary'].drop(index=range(13)) # Drop rows without data
db_structure = db_structure.drop(columns=db_structure.columns[0], axis=1) # Drop first column without data
db_structure.columns = db_structure.iloc[0] # Set first row as column name
db_structure = db_structure.tail(-1) # Remove first row
# print(f"db_structure \n {db_structure}")

# Create empty db to store outpu
NR_data_all = pd.DataFrame(columns = ['Country','Value','Sector'])

# Iterate for each sector and find NR data
for index,row in db_map_EUROSTAT.iterrows():
    
    # Find sheet of sector based on db map
    sheet = db_structure[
        (db_structure ['Water process'] == row['Water process'])
        &(db_structure ['Water sources'] == row['Water sources'])
        &(db_structure ['Unit of measure'] == row['Unit of measure'])
        ]['Contents'].item()

    # Keep data for 2018 and remove all others
    r,c = np.where(EUROSTAT_ENV_WAT_ABS[sheet]=='2018')
    NR_data = EUROSTAT_ENV_WAT_ABS[sheet].iloc[int(r)+2:,[0,int(c)]]
    # Drop nan values
    NR_data= NR_data[
        (NR_data.iloc[:,1].notna())
        & (NR_data.iloc[:,1] != ':')
        ]
#     print(f"NR_data \n {NR_data}")
    # Reform NR_Data
    NR_data = NR_data.rename(columns={NR_data.columns[0]: "Country",
                                   NR_data.columns[1]: "Value"}
                          )   
#     print(f"NR_data \n {NR_data}")
    NR_data['Sector'] = row['Heatmap_sector_level_3_cd'] 

#     NR_data_all = NR_data_all.append(NR_data, ignore_index = True)
    NR_data_all = pd.concat([NR_data_all,NR_data], ignore_index = True)

# NR_data_all["Value"] = NR_data_all["Value"].astype(str)


print(f"NR_data_all \n {NR_data_all}")

#read database
IEA = pd.read_excel((input_path / "IEA EEI database_Highlights.xlsb"),
                    sheet_name=['Services - Energy', 'Industry - Energy', 'Transport - Energy']) 
db_map_IEA = pd.read_excel((input_path / "Data_map.xlsx"),
                         sheet_name = 'IEA')


ES_data_all = pd.DataFrame(columns = ['Country','Value','Sector'])

# Iterate for each sector and find NR data
for index,row in db_map_IEA.iterrows():
#     print(row['Heatmap_sector_level_3_cd'])
    # if row['Heatmap_sector_level_3_cd'] == '10.1.2': #'1.1.1': #
    #     break

    #Some sectors have to no data, so assume skip these
    try:
        # Find sheet of sector based on db map
        sheet = row['Sheet']
        # Find sector mapping 
        # Some heatmap sectors map to multiple db sectors
        smap = row['Subsector'].split(", ")
        
        ES_data = IEA[sheet]
        ES_data.columns = ES_data.iloc[0] # Set first row as column names 
        ES_data = ES_data.tail(-1) # Then delete first row
        ES_data = ES_data[ES_data.iloc[:,1].isin(smap)]
        
        # Keep data for 2018 and remove all others
        ES_data = ES_data.loc[:,['Country',2018]]
        # Drop no data values
        ES_data= ES_data[(ES_data[2018] != '..')
            ]
        
        # Sum data for the same country
        # Some heatmap sectors map to multiple db sectors
        ES_data = ES_data.groupby('Country').sum()
          
        # Reformat ER_Data
        ES_data = ES_data.rename(columns={ES_data.columns[0]: "Value"})
        ES_data['Country'] = ES_data.index
        ES_data['Sector'] = row['Heatmap_sector_level_3_cd'] 
    
#         ES_data_all = ES_data_all.append(ES_data, ignore_index = True)
        ES_data_all = pd.concat([ES_data_all,ES_data], ignore_index = True)
    except:
        continue
print(f"ES_data_all \n {ES_data_all}")
# print(f"smap \n {smap}")

### from EUKLEMS_INTANPROD dbs and convert all national currency to USD
# print(f"smap \n {smap}")
def EUKLEMS(variables,db_input,smap, convert = False, e_rates = e_rates):
     
    data = db_input[db_input['nace_r2_code'].isin(smap)]
#     print(f"data00 \n \n {data}")
    # Select relavant variable only
    data = data.loc[:,['nace_r2_code','geo_name', 'year']+variables]
#     print(f"data11 \n \n {data}")
    
    # Keep data for 2018 and remove all others
    data = data[data['year']==2018]
    # Drop no data values
    data = data[data[variables].notna().all(axis=1)]
    #Drop irrelevant columns
    data = data.drop(columns=['nace_r2_code','year'])
#     print(f"data22 \n \n {data}")
     
###################################################################    
    # Sum data for the same country
    # Some heatmap sectors map to multiple db sectors
#     data = data.groupby('geo_name').sum()
    data = data.groupby(['geo_name']).sum()
#     print(f"dataAB \n \n {data}")
    # Sum across variables
    # Some proxy datasets map to multiple variables (e.g. CAPEX)
    data = data.sum(axis = 1)
    #print(f"dataBB \n \n {data}")
    
#################################################################    
    
    # Reformat Data
    data = pd.DataFrame(data)
    data = data.rename(columns={data.columns[0]: "Value"})
#     print(f"dataAA renamed column \n \n {data}")
    data['Country'] = data.index
    data['Sector'] = row['Heatmap_sector_level_3_cd'] 
#     print(f"dataCC{data.head()}")
#     print(f"e_rates{e_rates}")
    
    if convert == True:
        # Assign e_rate for countries
        data = data.rename(columns={'Value':'Value_NAC'})
        #data = pd.merge(data,e_rates)
#         print(f"data22{data}")
        # Convert NAC to USD
        #data['Value'] = data['Value_NAC'] / data['e_rate']
        data['Value'] = data['Value_NAC'] / 0.846772667108111
        #data = data.drop(columns=['Value_NAC','e_rate'])
        data = data.drop(columns=['Value_NAC'])               
#   print(f"data converted to USD \n {data}")
    return data

RM_data_all = pd.DataFrame(columns = ['Country','Value','Sector'])
T_data_all = pd.DataFrame(columns = ['Country','Value','Sector'])
OPEX_data_all = pd.DataFrame(columns = ['Country','Value','Sector'])
CAPEX_data_all = pd.DataFrame(columns = ['Country','Value','Sector'])
BA_data_all = pd.DataFrame(columns = ['Country','Value','Sector'])
WF_data_all = pd.DataFrame(columns = ['Country','Value','Sector'])

# Iterate for each sector and find NR data
for index,row in db_map_EUKLEMS.iterrows():
#     print(row['Heatmap_sector_level_3_cd'])
    
    #Some sectors have no data, so skip these
    try:
        # Find sector mapping 
        # Some heatmap sectors map to multiple db sectors
        smap = row['nace_r2_code'].split(";")
        
        RM_data = EUKLEMS(variables = ['II_CP'],
                          db_input = EUKLEMS_INTANPROD_naccounts,
                          smap = smap,
                          convert = True)
       ## print(f"RM_data {RM_data}")
#         RM_data_all = RM_data_all.append(RM_data, ignore_index = True)
        RM_data_all = pd.concat([RM_data_all, RM_data], ignore_index = True)
        
        
        T_data = EUKLEMS(variables = ['I_TraEq'],
                          db_input = EUKLEMS_INTANPROD_caccounts,
                          smap = smap,
                          convert = True)
#         T_data_all = T_data_all.append(T_data, ignore_index = True)
        T_data_all = pd.concat([T_data_all, T_data], ignore_index = True)
        
        OPEX_data = EUKLEMS(variables = ['I_GFCF'],
                          db_input = EUKLEMS_INTANPROD_caccounts,
                          smap = smap,
                          convert = True)
#         OPEX_data_all = OPEX_data_all.append(OPEX_data, ignore_index = True)
        OPEX_data_all = pd.concat([OPEX_data_all, OPEX_data], ignore_index = True)
        
        CAPEX_data = EUKLEMS(variables = ['K_IT','K_CT','K_Soft_DB','K_TraEq'
                                          ,'K_OMach','K_OCon','K_Rstruc'
                                          ,'K_RD', 'K_OIPP']
                             ,db_input = EUKLEMS_INTANPROD_caccounts
                             ,smap = smap,
                             convert = True)
#         CAPEX_data_all = CAPEX_data_all.append(CAPEX_data, ignore_index = True)
        CAPEX_data_all = pd.concat([CAPEX_data_all, CAPEX_data], ignore_index = True)
        
        BA_data = EUKLEMS(variables = ['K_Cult'],
                          db_input = EUKLEMS_INTANPROD_caccounts,
                          smap = smap,
                          convert = True)
#         BA_data_all = BA_data_all.append(BA_data, ignore_index = True)
        BA_data_all = pd.concat([BA_data_all, BA_data], ignore_index = True)
        
        WF_data = EUKLEMS(variables = ['H_EMP'],
                          db_input = EUKLEMS_INTANPROD_naccounts,
                          smap = smap)
#         WF_data_all = WF_data_all.append(WF_data, ignore_index = True)
        WF_data_all = pd.concat([WF_data_all, WF_data], ignore_index = True)
    except:
        continue
# print(f"WF_data_all {WF_data_all}")

# Get GVA for country x sector from EUKLEMS_INTANPROD dbs and convert to USD from NAC

# Create empty output database
GVA_data_all = pd.DataFrame(columns = ['Country','Value','Sector'])

# Iterate for each sector and find NR data
for index,row in db_map_EUKLEMS.iterrows():
#     print(row['Heatmap_sector_level_3_cd'])
    # if row['Heatmap_sector_level_3_cd'] == '3.1.1': #'10.1.2': #
    #     break
    #Some sectors have no data, so assume skip these
    try:
        # Find sector mapping 
        # Some heatmap sectors map to multiple db sectors
        smap = row['nace_r2_code'].split(";")
        GVA_data = EUKLEMS(variables = ['VA_CP'],
                          db_input = EUKLEMS_INTANPROD_naccounts,
                          smap = smap,
                          convert=True)
        # Drop those with 0 value added
        GVA_data = GVA_data[GVA_data['Value'] != 0]
        #print(f"GVA_data {GVA_data}")
        # Append sector data to output db
#         GVA_data_all = GVA_data_all.append(GVA_data, ignore_index = True) 
        GVA_data_all = pd.concat([GVA_data_all, GVA_data], ignore_index = True)
    except:
        continue
    
print(f"GVA_data_all {GVA_data_all}")