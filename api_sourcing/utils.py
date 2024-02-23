import pandas as pd
import numpy as np
from typing import List, Dict

class Utils():
    def __init__(self):
        pass

    def make_ts_dict(self, years: List, multiple: int) -> Dict:
        new_dict = {i: years[i % len(years)] for i in range(len(years) * multiple)}
        return new_dict

    def make_sector_dict(self, sectors: List, multiple: int) -> Dict:
        new_dict = {i: sectors[i % len(sectors)] for i in range(len(sectors) * multiple)}
        return new_dict