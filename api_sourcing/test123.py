from main import DataProcessor

params = {
    "format": "format=JSON",
    "filters": {"time" : "2019"},
    "sinceTimePeriod" : "sinceTimePeriod = 2012",
    "unit":"unit=MIO_M3"
}
dataset = "env_wat_abs"
d = DataProcessor(dataset=dataset, params=params)
df, meta_data = d.process_data()
