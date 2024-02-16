from api_connect import APIClient
import pandas as pd
from utils import Utils

client = APIClient()
params = {
    "format": "format=JSON",
    "filters": {"time":"2019"}
}
dataset = "env_wat_abs"
k = "DEMO_R_D3DENS"
data = client.get_data(dataset=dataset, params=None)
meta_data = {
    "version":data["version"],
    "class":data["class"],
    "label":data["label"],
    "source":data["source"],
    "updated":data["updated"]
}
series = data["value"]
data_dimensions = {

    "A": data["dimension"]['freq']['category']['label'].get('A', None),
    "Q": data["dimension"]['freq']['category']['label'].get('Q', None),
    "Process": data["dimension"]['wat_proc']['category']['label'],
    "Unit":data["dimension"]['unit']['category']['label'].values(),
    "geo_label":data["dimension"]['geo']['category']['label'],
    "geo_idx":data["dimension"]['geo']['category']['index'],
    "time_idx":data["dimension"]['time']['category']['index']
}




count = len([key for index, key in enumerate(data_dimensions["time_idx"])])
countries_dict = {v: k for k, v in data_dimensions['geo_idx'].items()}
ts_dict = {v: k for k, v in data_dimensions['time_idx'].items()}


df = pd.DataFrame.from_dict(series, orient='index', columns=['Values'])
df = df.reset_index()
df['index'] = df["index"].astype('int64')
df['idx'] = df["index"]/ count
df['idx'] = df['idx'].astype('int64')

multiple = df['idx'].astype('int64').nunique()
dict_map = Utils().make_dict(ts_dict, multiple)

df['country'] = df['idx'].map(countries_dict)
df['period'] = df['index'].map(dict_map)

print(df.head())

