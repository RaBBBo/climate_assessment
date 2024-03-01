from dataprocessor import DataProcessor

params = {
    "wat_proc":["ABS_AGR", "ABS_PWS"],
    "wat_src":["FGW", "FSW"],
    "sinceTimePeriod":"2012",
    "unit":["M3_HAB", "MIO_M3"],
    "geo":["DE","BE","CZ"],
}
dataset = "env_wat_abs"

d = DataProcessor(dataset=dataset, params=params)
df, meta_data = d.process_data()
