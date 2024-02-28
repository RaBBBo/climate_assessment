from reading_data import ReadingModule
from load_variables import LoadVariables
import os

dir = "proxies"
rm = ReadingModule(dir)
files_to_be_read, root = rm.list_file_dirs()

lv = LoadVariables(root)
print(lv.EUROSTAT_ENV_WAT_ABS)
print(lv.db_map_EUROSTAT)
print()
