import pandas as pd
import os
from typing import List

class ReadingModule:
    def __init__(self, file_dir: str):
        self.file_dir = file_dir

    def list_file_dirs(self) -> List:

        list = []
        # Get the current directory
        current_directory = os.getcwd()
        # Get the directory of the Python filepath
        path_to_dir = os.path.join(current_directory, self.file_dir)

        for root, _, files in os.walk(path_to_dir):
            for file in files:
                list.append(os.path.join(root, file))
        return list, path_to_dir
    
    