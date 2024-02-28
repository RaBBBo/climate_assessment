import pandas as pd
import openpyxl

class LoadVariables:
    def __init__(self, input_path):
        self.input_path = input_path + "\\"
        self.load_data()

    def load_data(self):
        self.EUROSTAT_ENV_WAT_ABS = pd.read_excel((self.input_path + "water_abstraction.xlsx"), sheet_name = None)
        self.db_map_EUROSTAT = pd.read_excel((self.input_path + "Data_map.xlsx"), sheet_name = 'Eurostat')
        self.EUKLEMS_INTANPROD_naccounts = pd.read_csv((self.input_path + "national accounts.csv"), quotechar = '"', quoting=1, doublequote = True)
        self.EUKLEMS_INTANPROD_caccounts = pd.read_csv((self.input_path + "capital accounts.csv"), quotechar = '"', quoting=1, doublequote = True)
        self.db_map_EUKLEMS = pd.read_excel((self.input_path + "Data_map.xlsx"), sheet_name = 'EUKLEMS')
        self.IEA = pd.read_excel((self.input_path + "IEA EEI database_Highlights.xlsb"), sheet_name=['Services - Energy', 'Industry - Energy', 'Transport - Energy'], engine='pyxlsb') 
        self.db_map_IEA = pd.read_excel((self.input_path + "Data_map.xlsx"), sheet_name = 'IEA')
        self.e_rates = pd.read_csv((self.input_path + "API_PA.NUS.FCRF_DS2_en_csv_v2_4772354.csv"), quotechar = '"', quoting=1, doublequote = True, skiprows=[0,1,2,3], on_bad_lines='skip')
        self.e_rates = self.e_rates.loc[:,['Country Name','2018']]
        self.e_rates = self.e_rates.rename(columns={self.e_rates.columns[0]:'Country', self.e_rates.columns[1]:'e_rate'})
        euro_area = ['Austria', 'Belgium','Cyprus','Estonia','Finland', 'France','Germany','Greece','Ireland','Italy','Latvia', 'Lithuania','Luxembourg','Malta','Netherlands','Portugal', 'Slovakia','Slovenia','Spain']
        euro_area_rate = float(self.e_rates[self.e_rates['Country'] == 'Euro area']['e_rate'])
        self.e_rates.loc[self.e_rates['Country'].isin(euro_area),'e_rate'] = euro_area_rate
