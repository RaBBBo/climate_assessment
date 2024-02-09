"""
This config file retrieves the root application path to allow other modules to create file paths relative to the project
root in a convenient manner.

The reason to do this is to avoid compatibility issues with certain windows environments where relative paths are not
formed correctly.

Usage example:
import os
import path_configs as pa

os.path.join(pa.ROOT_PATH, "cmlib", "some_file.txt")
"""

import os

ROOT_PATH_GIT = os.path.dirname(__file__)
