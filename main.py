# Import modules
import pandas as pd
import numpy as np
import csv
import openpyxl
import networkx as nx
import math
import matplotlib.pyplot as plt
from settings import input_path, output_path

# define range
year = 2018

# Load data

# water abstraction - natural resources
EUROSTAT_ENV_WAT_ABS = pd.read_excel(input_path / 'water_abstraction.xlsx', sheet_name=None)
# heatmap section mapping - sector, water process, water sources, unit
db_map_EUROSTAT = pd.read_excel(input_path / "Data_map.xlsx", sheet_name='Eurostat')
#
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
e_rates = e_rates[['Country Name',f'{year}']]
e_rates = e_rates.rename(columns={e_rates.columns[0]:'Country',
                                  e_rates.columns[1]:'e_rate'})

# Assign e_rates to euro area countries in year
euro_area = ['Austria', 'Belgium','Cyprus','Estonia','Finland'
             ,'France','Germany','Greece','Ireland','Italy','Latvia'
             ,'Lithuania','Luxembourg','Malta','Netherlands','Portugal'
             ,'Slovakia','Slovenia','Spain']

# euro_area_rate = e_rates.loc[e_rates['Country'].eq('Euro area')]
# euro_area_rate = e_rates.loc[e_rates['Country'].isin(euro_area)] # NOTE: rates don't exist

euro_area_rate = float(e_rates[e_rates['Country'] == 'Euro area']['e_rate'])
e_rates.loc[e_rates['Country'].isin(euro_area),'e_rate'] = euro_area_rate # fill missing values by the euro area

# Read and reformat database structure
# db_structure: structure of EUROSTAT_ENV_WAT_ABS with water abstraction
db_structure = EUROSTAT_ENV_WAT_ABS['Summary'].drop(index=range(13)) # Drop rows without data
db_structure = db_structure.drop(columns=db_structure.columns[0], axis=1) # Drop first column without data
db_structure.columns = db_structure.iloc[0] # Set first row as column name
db_structure = db_structure.tail(-1) # Remove first row
# print(f"db_structure \n {db_structure}")

# Create empty db to store output
NR_data_all = pd.DataFrame(columns = ['Country','Value','Sector'])

# Iterate for each sector and find NR data
for index,row in db_map_EUROSTAT.iterrows():
    
    # Find sheet of sector based on db map
    sheet = db_structure[
        (db_structure ['Water process'] == row['Water process'])
        &(db_structure ['Water sources'] == row['Water sources'])
        &(db_structure ['Unit of measure'] == row['Unit of measure'])
        ]['Contents'].item()

    # Locate for a certain year
    r,c = np.where(EUROSTAT_ENV_WAT_ABS[sheet]==f'{year}')
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
    # row['Heatmap_sector_level_3_cd': sector mapping
    NR_data['Sector'] = row['Heatmap_sector_level_3_cd'] 

#     NR_data_all = NR_data_all.append(NR_data, ignore_index = True)
    NR_data_all = pd.concat([NR_data_all,NR_data], ignore_index = True)

# NR_data_all["Value"] = NR_data_all["Value"].astype(str)


print(f"NR_data_all \n {NR_data_all}")

#read database energy
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
        
        # Keep data for year and remove all others
        ES_data = ES_data.loc[:,['Country',year]]
        # Drop no data values
        ES_data= ES_data[(ES_data[year] != '..')
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


### from EUKLEMS_INTANPROD dbs and convert all national currency to USD
def EUKLEMS(variables,db_input,smap, convert = False, e_rates = e_rates):
     
    data = db_input[db_input['nace_r2_code'].isin(smap)]
#     print(f"data00 \n \n {data}")
    # Select relavant variable only
    data = data.loc[:,['nace_r2_code','geo_name', 'year']+variables]
#     print(f"data11 \n \n {data}")
    
    # Keep data for year and remove all others
    data = data[data['year']==year]
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

RM_data_all = pd.DataFrame(columns = ['Country','Value','Sector']) #raw material
T_data_all = pd.DataFrame(columns = ['Country','Value','Sector']) #transport
OPEX_data_all = pd.DataFrame(columns = ['Country','Value','Sector']) #operations
CAPEX_data_all = pd.DataFrame(columns = ['Country','Value','Sector']) # non-biological assets
BA_data_all = pd.DataFrame(columns = ['Country','Value','Sector']) #biological assets
WF_data_all = pd.DataFrame(columns = ['Country','Value','Sector']) #workforce

# Iterate for each sector and find 6 categories of data
for index,row in db_map_EUKLEMS.iterrows():
#     print(row['Heatmap_sector_level_3_cd'])
    
    #Some sectors have no data, so skip these
    try:
        # Find sector mapping 
        # Some heatmap sectors map to multiple db sectors
        smap = row['nace_r2_code'].split(";")
        
        RM_data = EUKLEMS(variables = ['II_CP'], # don't know what is II_CP, but related to RM
                          db_input = EUKLEMS_INTANPROD_naccounts,
                          smap = smap,
                          convert = True)

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

# Iterate for each sector and find GVA data
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

#%% Normalise countryxsector VI by countryxsector GVA (IntOutput_Normalisation_VI.csv)
def GVA_norm(VI,
             VI_data,
             db_map_data,
             db_group,
             GVA_data=GVA_data_all,
             db_map_GVA=db_map_EUKLEMS):
    #     print(f"VI_data {VI_data}")
    # Find group overlap between GVA and VI dbs
    # Sector groups for GVA and VI must match before normalisation
    VI_data = pd.merge(VI_data, db_map_data, left_on='Sector', right_on='Heatmap_sector_level_3_cd')
    db_map_data = db_map_data.set_index(db_map_data['Heatmap_sector_level_3_cd'])
    data_groups = db_map_data.groupby(by=db_group).groups
    #     print(f"data_groups {data_groups}")

    GVA_data = pd.merge(GVA_data, db_map_GVA, left_on='Sector', right_on='Heatmap_sector_level_3_cd')
    db_map_GVA = db_map_GVA.set_index(db_map_data['Heatmap_sector_level_3_cd'])
    GVA_groups = db_map_GVA.groupby(by='nace_r2_code').groups
    # print(f"GVA_data {GVA_data}")

    VI_data_groups = VI_data.groupby(['Country', db_group], as_index=False).first()
    GVA_data_groups = GVA_data.groupby(['Country', 'nace_r2_code'], as_index=False).first()

    merge_group = data_groups | GVA_groups
    group_list = list(merge_group.values())

    G = nx.Graph()
    for l in group_list:
        nx.add_path(G, l)
    conn_group = list(nx.connected_components(G))

    # Create empy normalise dataframe
    data_norm = pd.DataFrame(columns=['Country', 'Sector', 'Value_' + VI
        , 'Value_GVA', 'Value_norm_' + VI])

    # Create group map
    for group in conn_group:
        rel_groups_GVA = []
        rel_groups_VI = []

        rel_groups_GVA += {i for i in GVA_groups if any(group.intersection(GVA_groups[i]))}
        rel_groups_VI += {i for i in data_groups if any(group.intersection(data_groups[i]))}

        # Get unique values only
        rel_groups_GVA = list(set(rel_groups_GVA))
        rel_groups_VI = list(set(rel_groups_VI))

        select_GVA = GVA_data_groups[GVA_data_groups['nace_r2_code'].isin(rel_groups_GVA)]
        # print(f"select_GVA {select_GVA}")
        sum_GVA = select_GVA.groupby(by='Country', as_index=False)['Value'].sum()
        # print(f"sum_GVA {sum_GVA}")

        select_VI = VI_data_groups[VI_data_groups[db_group].isin(rel_groups_VI)]
        sum_VI = select_VI.groupby(by='Country', as_index=False)['Value'].sum()

        data_merge = pd.merge(sum_VI, sum_GVA,
                              on='Country',
                              suffixes=['_' + VI, '_GVA'])

        data_merge['Value_norm_' + VI] = (data_merge['Value_' + VI]
                                          / data_merge['Value_GVA'])
        for sector in group:
            data_merge['Sector'] = sector
            # print(f"data_norm00 {data_norm}")
            # print(f"data_merge {data_merge}")
            data_norm = pd.concat([data_norm, data_merge], ignore_index=True)
    #             data_norm = data_norm.append(data_merge, ignore_index = True)
    #     print(f"data_norm {data_norm}")
    return data_norm


NR_data_all_norm = GVA_norm(VI='NR'
                            , db_map_data=db_map_EUROSTAT
                            , db_group='Water process'
                            , VI_data=NR_data_all)
# print(f"NR_data_all {NR_data_all}")

ES_data_all_norm = GVA_norm(VI='ES'
                            , db_map_data=db_map_IEA
                            , db_group='Subsector'
                            , VI_data=ES_data_all)
print(f"ES_data_all {ES_data_all}")

RM_data_all_norm = GVA_norm(VI='RM'
                            , db_map_data=db_map_EUKLEMS
                            , db_group='nace_r2_code'
                            , VI_data=RM_data_all)

T_data_all_norm = GVA_norm(VI='T'
                           , db_map_data=db_map_EUKLEMS
                           , db_group='nace_r2_code'
                           , VI_data=T_data_all)

OPEX_data_all_norm = GVA_norm(VI='OPEX'
                              , db_map_data=db_map_EUKLEMS
                              , db_group='nace_r2_code'
                              , VI_data=OPEX_data_all)

CAPEX_data_all_norm = GVA_norm(VI='CAPEX'
                               , db_map_data=db_map_EUKLEMS
                               , db_group='nace_r2_code'
                               , VI_data=CAPEX_data_all)

BA_data_all_norm = GVA_norm(VI='BA'
                            , db_map_data=db_map_EUKLEMS
                            , db_group='nace_r2_code'
                            , VI_data=BA_data_all)

WF_data_all_norm = GVA_norm(VI='WF'
                            , db_map_data=db_map_EUKLEMS
                            , db_group='nace_r2_code'
                            , VI_data=WF_data_all)
# print(f"NR_data_all_norm {NR_data_all_norm.head()}")
# print(f"ES_data_all_norm {ES_data_all_norm.head()}")
NR_data_all_norm.to_csv((f'{output_path}/IntOutput_Normalisation_NR.csv'), sep=';', decimal=',', float_format='%.2f')
ES_data_all_norm.to_csv((f'{output_path}/IntOutput_Normalisation_ES.csv'), sep=';', decimal=',', float_format='%.2f')
RM_data_all_norm.to_csv((f'{output_path}/IntOutput_Normalisation_RM.csv'), sep=';', decimal=',', float_format='%.2f')
T_data_all_norm.to_csv((f'{output_path}/IntOutput_Normalisation_T.csv'), sep=';', decimal=',', float_format='%.2f')
OPEX_data_all_norm.to_csv((f'{output_path}/IntOutput_Normalisation_OPEX.csv'), sep=';', decimal=',', float_format='%.2f')
CAPEX_data_all_norm.to_csv((f'{output_path}/IntOutput_Normalisation_CAPEX.csv'), sep=';', decimal=',',
                           float_format='%.2f')
BA_data_all_norm.to_csv((f'{output_path}/IntOutput_Normalisation_BA.csv'), sep=';', decimal=',', float_format='%.2f')
WF_data_all_norm.to_csv((f'{output_path}/IntOutput_Normalisation_WF.csv'), sep=';', decimal=',', float_format='%.2f')

# %% Get global averages value per sector
# Sectors w/out private individuals as this was separately calculated.
sectors = pd.read_excel((f'{input_path}/Data_map.xlsx'),
                        sheet_name='EUKLEMS',
                        usecols=['Heatmap_sector_level_3_cd'])
Global_VInorm = pd.DataFrame(index=sectors['Heatmap_sector_level_3_cd'],
                             columns=['NR', 'ES', 'RM', 'T', 'OPEX',
                                      'CAPEX', 'BA', 'WF'])

Global_VInorm['NR'] = NR_data_all_norm.groupby(by='Sector')['Value_norm_NR'].mean()
Global_VInorm['ES'] = ES_data_all_norm.groupby(by='Sector')['Value_norm_ES'].mean()
Global_VInorm['RM'] = RM_data_all_norm.groupby(by='Sector')['Value_norm_RM'].mean()
Global_VInorm['T'] = T_data_all_norm.groupby(by='Sector')['Value_norm_T'].mean()
Global_VInorm['OPEX'] = OPEX_data_all_norm.groupby(by='Sector')['Value_norm_OPEX'].mean()
Global_VInorm['CAPEX'] = CAPEX_data_all_norm.groupby(by='Sector')['Value_norm_CAPEX'].mean()
Global_VInorm['BA'] = BA_data_all_norm.groupby(by='Sector')['Value_norm_BA'].mean()
Global_VInorm['WF'] = WF_data_all_norm.groupby(by='Sector')['Value_norm_WF'].mean()

Global_VI = pd.DataFrame(index=sectors['Heatmap_sector_level_3_cd'],
                         columns=['NR', 'ES', 'RM', 'T', 'OPEX',
                                  'CAPEX', 'BA', 'WF'])

Global_VI['NR'] = NR_data_all_norm.groupby(by='Sector')['Value_NR'].mean()
Global_VI['ES'] = ES_data_all_norm.groupby(by='Sector')['Value_ES'].mean()
Global_VI['RM'] = RM_data_all_norm.groupby(by='Sector')['Value_RM'].mean()
Global_VI['T'] = T_data_all_norm.groupby(by='Sector')['Value_T'].mean()
Global_VI['OPEX'] = OPEX_data_all_norm.groupby(by='Sector')['Value_OPEX'].mean()
Global_VI['CAPEX'] = CAPEX_data_all_norm.groupby(by='Sector')['Value_CAPEX'].mean()
Global_VI['BA'] = BA_data_all_norm.groupby(by='Sector')['Value_BA'].mean()
Global_VI['WF'] = WF_data_all_norm.groupby(by='Sector')['Value_WF'].mean()

Global_GVA = pd.DataFrame(index=sectors['Heatmap_sector_level_3_cd'],
                          columns=['GVA'])

Global_GVA['GVA'] = ES_data_all_norm.groupby(by='Sector')['Value_GVA'].mean()

Global_VInorm.to_csv((f'{output_path}/Global_norm_VI.csv'), sep=';', decimal=',')
Global_VI.to_csv((f'{output_path}/Global_VI.csv'), sep=';', decimal=',')
Global_GVA.to_csv((f'{output_path}/Global_GVA.csv'), sep=';', decimal=',')

#%% Disaggregate Agricultural Sector using water footprint

WF_crop = pd.read_excel((f'{input_path}/Report47-Appendix-II.xlsx'),
                         sheet_name='App-II-WF_perTon',
                         skiprows=[0,1,2,3],
                         usecols=[3,8,9])
WF_crop= WF_crop.rename(columns={WF_crop.columns[0] : 'Product description (HS)'
                                      ,WF_crop.columns[1] : 'WF'
                                      ,WF_crop.columns[2] : 'Global average'})
WF_crop_edit = WF_crop.copy(deep=True)
for index,row in WF_crop.iterrows():
    if pd.isna(row['Product description (HS)']):
        continue
    else:
        WF_crop_edit.loc[index:index+2, 'Product description (HS)'] = row['Product description (HS)']
WF_crop_edit = WF_crop_edit[WF_crop_edit['WF']=='Blue']

WF_animal = pd.read_excel((f'{input_path}/Report48-Appendix-V.xlsx'),
                         sheet_name='App-V_WF_HS_SITC',
                         skiprows=[0,1],
                         usecols=[2,8,12])
WF_animal = WF_animal.rename(columns={WF_animal.columns[0] : 'Product description (HS)'
                                      ,WF_animal.columns[1] : 'WF'
                                      ,WF_animal.columns[2] : 'Global average'})
WF_animal_edit = WF_animal.copy(deep=True)
for index,row in WF_animal.iterrows():
    if pd.isna(row['Product description (HS)']):
        continue
    else:
        WF_animal_edit.loc[index:index+2, 'Product description (HS)'] = row['Product description (HS)']
WF_animal_edit = WF_animal_edit[WF_animal_edit['WF']=='Blue']

db_map_WF = pd.read_excel((f'{input_path}/Data_map.xlsx'),
                         sheet_name = 'WF')

# Create empty dataframe
WF_data_ag = pd.DataFrame(columns=['Water use'],
                          index=db_map_WF['Heatmap_sector_level_3_cd'])

# Read data from database
for index, row in db_map_WF.iterrows():
    #     print(row['Heatmap_sector_level_3_cd'])
    # if row['Heatmap_sector_level_3_cd'] == '1.1.2':
    #     break
    # Some sectors have no data, so assume skip these
    try:
        sector = row['Heatmap_sector_level_3_cd']
        smap = row['Product description (HS)'].split(";")

        if row['Type'] == 'Crop':
            WF_data_ag.loc[sector, 'Water use'] = float(WF_crop_edit[WF_crop_edit[
                'Product description (HS)'].isin(smap)]['Global average']
                                                        .mean())
        else:
            WF_data_ag.loc[sector, 'Water use'] = float(WF_animal_edit[WF_animal_edit[
                'Product description (HS)'].isin(smap)]['Global average']
                                                        .mean())
    except:
        continue
# Fill sectors with no data.
# Use the average of crop sectors for floriculture and logging.
crop_sectors = ['1.1.1', '1.1.2', '1.1.3', '1.2.1', '1.2.2', '1.2.3']
nan_crop_sectors = ['1.3.1', '1.4.1']
WF_crop_average = float(WF_data_ag.loc[crop_sectors].mean())
WF_data_ag.loc[nan_crop_sectors, 'Water use'] = WF_crop_average
# Use the average of protein sectors for Remaining Animal Protein
animal_sectors = ['1.5.1', '1.6.1', '1.8.1', '1.9.1', '1.10.1']
nan_animal_sectors = ['1.10.2']
WF_animal_average = float(WF_data_ag.loc[animal_sectors].mean())
WF_data_ag.loc[nan_animal_sectors, 'Water use'] = WF_animal_average
# Use 0 for aquaculture and wildcatch.
aq_sectors = ['1.7.1', '1.7.2']
WF_data_ag.loc[aq_sectors, 'Water use'] = 0.0

# Calculate water use ratio
WF_av = WF_data_ag['Water use'].mean()
# Fill no data sectors with average to maintain the NR/GVA average over the
# Sector
WF_data_ag['Water use ratio'] = WF_data_ag['Water use'] / WF_av
new_NR = (Global_VInorm.loc[WF_data_ag.index, 'NR'] * WF_data_ag['Water use ratio']).dropna()

Final_VI_00 = Global_VInorm.copy(deep=True)
Final_VI_00.loc[new_NR.index, 'NR'] = new_NR

WF_data_ag.to_csv((f'{output_path}/WaterFootprint_Ag.csv'), sep=';', decimal=',')
Final_VI_00.to_csv((f'{output_path}/VI.csv'), sep=';', decimal=',')

# %% For sectors with primarily NL exposure (listed in NL_sectors) use NL data only

# Sectors with primarily NL exposure (listed in NL_sectors)
NL_exposure = ['5.1.1', '9.1.1', '7.1.1', '7.1.2', '7.1.3', '7.2.1', '8.1.1',
               '8.1.2', '10.1.1', '10.1.2']
# Create db to change data
Final_VI_01 = Final_VI_00.copy(deep=True)


def NL_data_check(VI, db, sector, output_db=Final_VI_01):
    NL_data = db[(db['Country'] == 'Netherlands')
                 & (db['Sector'] == sector)][
        'Value_norm_' + VI]
    if len(NL_data) > 0:
        NL_data = NL_data.item()
        output_db.loc[sector, VI] = NL_data

    return output_db


for sector in NL_exposure:
    #     print(sector)
    # Check if there is data for NL. If there is replace data.
    Final_VI_01 = NL_data_check('NR', NR_data_all_norm, sector)
    Final_VI_01 = NL_data_check('ES', ES_data_all_norm, sector)
    Final_VI_01 = NL_data_check('RM', RM_data_all_norm, sector)
    Final_VI_01 = NL_data_check('T', T_data_all_norm, sector)
    Final_VI_01 = NL_data_check('OPEX', OPEX_data_all_norm, sector)
    Final_VI_01 = NL_data_check('CAPEX', CAPEX_data_all_norm, sector)
    Final_VI_01 = NL_data_check('BA', BA_data_all_norm, sector)
    Final_VI_01 = NL_data_check('WF', WF_data_all_norm, sector)

# %% Add mortgage sector VI's
# Household spending to income ratio as normalised OPEX
H_spending = pd.read_csv((f'{input_path}/DP_LIVE_18102022164331234.csv'),
                         sep=';')
# Only use NL data since this is the primary country where the bank has exposure
NL_H_spending = float(H_spending[H_spending['LOCATION'] == 'NLD']['Value'])
# Convert % to decimal notation
NL_H_spending = NL_H_spending / 100

# Household price to income ratio as normalised CAPEX
H_price = pd.read_csv((f'{input_path}/DP_LIVE_18102022160852406.csv'),
                      sep=',')
# Only use NL data since this is the primary country where the bank has exposure
NL_H_price = float(H_price[H_price['LOCATION'] == 'NLD']['Value'])
# Convert % to decimal notation
NL_H_price = NL_H_price / 100

# Append to VI
Mortgage_VI = pd.DataFrame({
    "OPEX": NL_H_spending,
    "CAPEX": NL_H_price
}, index=["12.1.1"])

# Final_VI_00 = Final_VI_00.append(Mortgage_VI)
Final_VI_00 = pd.concat([Final_VI_00, Mortgage_VI])
# Final_VI_01 = Final_VI_01.append(Mortgage_VI)
Final_VI_01 = pd.concat([Final_VI_01, Mortgage_VI])
Final_VI_01.to_csv((f'{output_path}/VI_wNL.csv'), sep=';', decimal=',')

# %% Min/max normalisation VI
Final_VI_00_norm = Final_VI_00.copy(deep=True)
for VI in Final_VI_00.columns:
    max_value = Final_VI_00[VI].max()
    min_value = Final_VI_00[VI].min()
    Final_VI_00_norm[VI] = (Final_VI_00[VI] - min_value) / (max_value - min_value)

Final_VI_00_norm["NR"] = Final_VI_00_norm["NR"].astype(float).fillna(0).round(2)
Final_VI_00_norm["ES"] = Final_VI_00_norm["ES"].astype(float).fillna(0).round(2)
Final_VI_00_norm["RM"] = Final_VI_00_norm["RM"].astype(float).fillna(0).round(2)
Final_VI_00_norm["T"] = Final_VI_00_norm["T"].astype(float).fillna(0).round(2)
Final_VI_00_norm["OPEX"] = Final_VI_00_norm["OPEX"].astype(float).fillna(0).round(2)
Final_VI_00_norm["CAPEX"] = Final_VI_00_norm["CAPEX"].astype(float).fillna(0).round(2)
Final_VI_00_norm["BA"] = Final_VI_00_norm["BA"].astype(float).fillna(0).round(2)
Final_VI_00_norm["WF"] = Final_VI_00_norm["WF"].astype(float).fillna(0).round(2)

# Final_VI_00_norm.dtypes
Final_VI_00_norm

# #%% Min/max normalisation VI
# Final_VI_00_norm = Final_VI_00.copy(deep=True)
# for VI in Final_VI_00.columns:
#     max_value = Final_VI_00[VI].max()
#     min_value = Final_VI_00[VI].min()
#     Final_VI_00_norm[VI] = (Final_VI_00[VI] - min_value) / (max_value - min_value)


Final_VI_00_norm.to_csv((f'{output_path}/VI_norm_{year}.csv'), sep=';', decimal=',')

Final_VI_01_norm = Final_VI_01.copy(deep=True)
for VI in Final_VI_01.columns:
    max_value = Final_VI_01[VI].max()
    min_value = Final_VI_01[VI].min()
    Final_VI_01_norm[VI] = (Final_VI_01[VI] - min_value) / (max_value - min_value)

Final_VI_01_norm.to_csv((f'{output_path}/VI_norm_wNL_{year}.csv'), sep=';', decimal=',')