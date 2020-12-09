"""
Functions definition for the datamodel component

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import os
import json

# Import exceptions
from eboa.datamodel.errors import EboaResourcesPathNotAvailable

# Auxiliary functions
def get_resources_path():

    eboa_resources_path = None
    if "EBOA_RESOURCES_PATH" in os.environ:
        # Get the path to the resources of the eboa
        eboa_resources_path = os.environ["EBOA_RESOURCES_PATH"]

    else:
        raise EboaResourcesPathNotAvailable("The environment variable EBOA_RESOURCES_PATH is not defined")
    # end if

    return eboa_resources_path

def read_configuration():
    eboa_resources_path = get_resources_path()
    # Get configuration
    with open(eboa_resources_path + "/datamodel.json") as json_data_file:
        config = json.load(json_data_file)
    # end with
    
    if "EBOA_DDBB_HOST" in os.environ:
        config["DDBB_CONFIGURATION"]["host"] = os.environ["EBOA_DDBB_HOST"]
    # end if

    return config
