import pandas as pd
import numpy as np
from typing import List, Dict

class Utils():
    def __init__(self):
        pass

    def make_dict(self, years: List, multiple: int) -> Dict:
        new_dict = {i: years[i % len(years)] for i in range(len(years) * multiple)}
        return new_dict