# Import modules
import pandas as pd
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
import seaborn as sns
from dq import DataQuality

from api_sourcing.dataprocessor import DataProcessor
from settings import input_path, output_path, EU_countries, EU_country_abbreviations, EU_country_full_name, x, y

# define year range
year_start = 2015
year_end = 2019

VI_EU_by_year = {}
VI_EU_all_year = {}
VI_NL_by_year = {}

for year in range(year_start,year_end+1):
        
    # Load data

    # water abstraction - natural resources
    # EUROSTAT_ENV_WAT_ABS = pd.read_excel(input_path / 'water_abstraction.xlsx', sheet_name=None)
    params = {
        "wat_proc": ["ABS_AGR", "ABS_PWS"],
        "wat_src": ["FRW", "FSW"],
        "sinceTimePeriod": "2012",
        "unit": ["MIO_M3", "M3_HAB"],  # "M3_HAB",
        "geo": ["BE", "BG", "CZ", "DK", "DE", "EE", "IE", "EL", "ES", "FR", "HR", "IT", "CY", "LV", "LT", "LU", "HU", "MT", "NL", "AT", "PL", "PT", "RO", "SL", "SK", "FI", "SE", "IS", "NO", "CH", "UK", "BA", "MK", "AL", "RS", "TR", "XK"],
    }
    dataset = "env_wat_abs"

    d = DataProcessor(dataset=dataset, params=params)
    df, meta_data = d.process_data()

    # heatmap section mapping - sector, water process, water sources, unit
    db_map_EUROSTAT = pd.read_excel(input_path / "Data_map.xlsx", sheet_name='Eurostat')

    df = df.rename(columns={"sector": "Water process", "source": "Water sources", "unit": "Unit of measure"})
    df["Water process"] = df["Water process"].map(
        {'Water abstraction by agriculture, forestry, fishing': 'Water abstraction for agriculture', "Water abstraction by public water supply":"Water abstraction for public water supply"})
    EUROSTAT_ENV_WAT_ABS = df.merge(db_map_EUROSTAT, how="left", on=["Water process", "Water sources", "Unit of measure"])
    EUROSTAT_ENV_WAT_ABS = EUROSTAT_ENV_WAT_ABS.dropna()

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

    euro_area_rate = float(e_rates[e_rates['Country'] == 'Euro area']['e_rate'])
    e_rates.loc[e_rates['Country'].isin(euro_area),'e_rate'] = euro_area_rate # fill missing values by the euro area


    mask = EUROSTAT_ENV_WAT_ABS['period']==f'{year}'
    NR_data_all = EUROSTAT_ENV_WAT_ABS.loc[mask, ['country','Values','Heatmap_sector_level_3_cd']]
    NR_data_all = NR_data_all.rename(columns={'country': "Country", 'Values': "Value", 'Heatmap_sector_level_3_cd': "Sector"})


    print(f"NR_data_all \n {NR_data_all}")

    #read database energy
    IEA = pd.read_excel((input_path / "IEA EEI database_Highlights.xlsb"),
                        sheet_name=['Services - Energy', 'Industry - Energy', 'Transport - Energy']) 
    db_map_IEA = pd.read_excel((input_path / "Data_map.xlsx"),
                            sheet_name = 'IEA')


    ES_data_all = pd.DataFrame(columns = ['Country','Value','Sector'])

    # Iterate for each sector and find NR data
    for index,row in db_map_IEA.iterrows():
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

    # remove non EU countries
    def Remove_countries(df_set,EU_countries = EU_countries, EU_country_abbreviations = EU_country_abbreviations):

        common_list = EU_countries
        df_set_new = []

        for df in df_set:
            list = df.Country.unique()

            if 'NL' in list:
                list = [EU_country_abbreviations.get(abbr) for abbr in list]
                
            common_list = [country for country in list if country in common_list]
        
        common_list_full_name = common_list
        common_list_abbr = [EU_country_full_name.get(fullname) for fullname in list]

        for df in df_set:
            list = df.Country.unique()

            if 'NL' in list:
                common_list = common_list_abbr
            else:
                common_list = common_list_full_name

            mask = df["Country"].isin(common_list)
            df_new = df.loc[mask,:].copy()
            df_set_new.append(df_new)

        return common_list_abbr, common_list_full_name, df_set_new

    (common_list_abbr, common_list_full_name, [NR_data_all, ES_data_all, T_data_all, RM_data_all, OPEX_data_all, CAPEX_data_all, BA_data_all, WF_data_all]) = Remove_countries([NR_data_all, ES_data_all, T_data_all,RM_data_all, OPEX_data_all,CAPEX_data_all,BA_data_all,WF_data_all]) #raw material

    dfs_array = [NR_data_all, ES_data_all, T_data_all, RM_data_all, OPEX_data_all, CAPEX_data_all, BA_data_all,WF_data_all]
    strings = ['NR', 'ES', 'T', 'RM', 'OPEX', 'CAPEX', 'BA', 'WF']

    # DQ check 
    count = 0
    df_dict = {}
    for df in dfs_array:
        dq = DataQuality(df)
        df = dq.check_completeness()
        #dq.show_distributions(strings[count], year)
        df_dict[strings[count]] = df
        count += 1

    with pd.ExcelWriter(f'dq_report_{year}.xlsx') as writer:
        for name, df in df_dict.items():
          df.to_excel(writer, sheet_name=name)

    # Get GVA for country x sector from EUKLEMS_INTANPROD dbs and convert to USD from NAC
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
        # nace_r2_code correponds to names of industries/sectors

        merge_group = data_groups | GVA_groups
        group_list = list(merge_group.values())

        # don't know what this means
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
            sum_GVA = select_GVA.groupby(by='Country', as_index=False)['Value'].sum() #it's not sum but just value by country
            # print(f"sum_GVA {sum_GVA}")

            select_VI = VI_data_groups[VI_data_groups[db_group].isin(rel_groups_VI)]
            sum_VI = select_VI.groupby(by='Country', as_index=False)['Value'].sum()
                
            if 'NL' in sum_VI.Country.unique():
                sum_VI['Country'] = [EU_country_abbreviations.get(abbr) for abbr in sum_VI['Country']]

            data_merge = pd.merge(sum_VI, sum_GVA,
                                on='Country',
                                suffixes=['_' + VI, '_GVA'])

            data_merge['Value_norm_' + VI] = (data_merge['Value_' + VI]
                                            / data_merge['Value_GVA'])
            # VI/GVA

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
    print(f"NR_data_all {NR_data_all_norm}")

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

    # apply weighing scheme
    def WeighingScheme(df):
        for VI in df.columns:
            for idx in df.index:
                if VI in ["NR", "ES", "RM", "T"]:
                    df.loc[idx, VI] = df.loc[idx, VI] * 0.25
                else:
                    if VI in ["OPEX", "WF"]:
                        if idx == '12.1.1':
                            df.loc[idx, VI] = df.loc[idx, VI] * 1.5
                        else:
                            df.loc[idx, VI] = df.loc[idx, VI] * 0.33
                    else:
                        if VI == "CAPEX":
                            if idx == '12.1.1':
                                df.loc[idx, VI] = df.loc[idx, VI] * 0.5
                            else:
                                if idx in ['1.11.1', '2.1.1', '2.1.2', '2.2.1', '2.2.2',
                                        '3.1.1', '3.2.1', '4.1.1', '5.1.1', '6.1.1', '7.1.1', '7.1.2', '7.1.3',
                                        '7.2.1', '8.1.1', '8.1.2', '9.1.1', '10.1.1', '10.1.2', '11.1.1']:
                                    df.loc[idx, VI] = df.loc[idx, VI] * 0.33
                                else:
                                    df.loc[idx, VI] = df.loc[idx, VI] * 0.17
                        else:
                            if VI == "BA":
                                df.loc[idx, VI] = df.loc[idx, VI] * 0.17
        df = df.round(2)

        df = df[~df.index.isin(['11.1.1'])] # remove 11.1.1, as it's irrelevant

        return df

    Final_VI_00_norm = WeighingScheme(Final_VI_00_norm)

    VI_EU_by_year[f'df_{year}'] = Final_VI_00_norm
    Final_VI_00_norm_year = Final_VI_00_norm.copy()
    Final_VI_00_norm_year["Year"] = year
    Final_VI_00_norm_year["Sector"] = Final_VI_00_norm_year.index
    VI_EU_all_year[f'df_{year}'] = Final_VI_00_norm_year

    Final_VI = Final_VI_00_norm.to_numpy()

    plt.figure(figsize=(12, 9))
    sns.heatmap(Final_VI, annot=True, fmt='.0%', cmap=sns.color_palette("Reds", as_cmap=True), cbar=True)
    plt.title(f"Heatmap EU 12 countries in {year}")
    plt.xticks([i + 0.5 for i in range(0, 8)], x, fontsize=8, ha='center', rotation=0)
    plt.yticks([i + 0.5 for i in range(0, 37)], y, fontsize=8, rotation=0)
    plt.tick_params(axis='y', which='major', pad=5)
    ax = plt.gca()
    ax.set_position([0.25, 0.1, 0.52, 0.8])
    path = f'{output_path}/plots/VI_eu_{year}.png'
    plt.savefig(path)

    Final_VI_00_norm.to_csv((f'{output_path}/VI_norm_{year}.csv'), sep=';', decimal=',')

    Final_VI_01_norm = Final_VI_01.copy(deep=True)
    for VI in Final_VI_01.columns:
        max_value = Final_VI_01[VI].max()
        min_value = Final_VI_01[VI].min()
        Final_VI_01_norm[VI] = (Final_VI_01[VI] - min_value) / (max_value - min_value)

    Final_VI_01_norm = WeighingScheme(Final_VI_01_norm)

    Final_VI_01_norm.to_csv((f'{output_path}/VI_norm_wNL_{year}.csv'), sep=';', decimal=',')

a = 1

VI_EU_all_year_combined = pd.concat(VI_EU_all_year, ignore_index = True)

# for sector in VI_EU_all_year_combined.Sector:
sector = "1.1.1"
df = VI_EU_all_year_combined

plt.figure(figsize=(10, 7))  # Set the figure size

# Customize line styles and colors
for i, VI in enumerate(VI_EU_by_year[f'df_{year}'].columns):
    plt.plot(range(year_start, year_end + 1), df.loc[df["Sector"] == sector, VI].values,
             label=VI, linewidth=2, linestyle=['-', '--', ':'][i % 3], marker='o')

# Customize axis labels and title
x_ticks = np.arange(year_start, year_end+1, 1)
plt.xticks(x_ticks)
plt.xlabel('Year', fontsize=14)
plt.ylabel('Indicator Values', fontsize=14)
plt.title(f'Trend of Indicator Values Over Years in Sector {sector}', fontsize=16)

# Add grid lines
plt.grid(True, linestyle='--', alpha=0.5)

# Customize legend
plt.legend(loc="upper right", fontsize=12)

# Show the plot
plt.tight_layout()  # Adjust spacing

path = f'{output_path}/plots/VI_eu_trend_{year_start}_{year_end}.png'

plt.savefig(path)

# for year in range(year_start,year_end):
#
#     df_diff = (VI_EU_by_year[f'df_{year+1}']-VI_EU_by_year[f'df_{year}'])/VI_EU_by_year[f'df_{year}']
#
#     df_diff = df_diff.to_numpy()
#
#     plt.figure(figsize=(12, 9))
#
#     # Create a custom gradient from green to red
#     custom_palette = sns.diverging_palette(120, 10, center = "light", as_cmap=True)
#
#     sns.heatmap(df_diff, annot=True, fmt='.0%', cmap=custom_palette, cbar=True)
#     plt.title(f"Heatmap EU 12 countries - trend from {year} to {year+1}")
#     plt.xticks([i + 0.5 for i in range(0, 8)], x, fontsize=8, ha='center', rotation=0)
#     plt.yticks([i + 0.5 for i in range(0, 37)], y, fontsize=8, rotation=0)
#     # Increase the spacing between y-axis tick labels
#     plt.tick_params(axis='y', which='major', pad=5)
#
#     ax = plt.gca()
#
#     # Set the position of the plot (adjust the values as needed)
#     # The arguments are [left, bottom, width, height]
#     ax.set_position([0.25, 0.1, 0.52, 0.8])
#     path = f'{output_path}/plots/VI_eu_diff_{year}_{year+1}.png'
#
#     plt.savefig(path)
