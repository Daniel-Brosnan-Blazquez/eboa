"""
Functions definition for the datamodel component

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
# Import python utilities
import os
import json

# Import exceptions
from .errors import GsdmResourcesPathNotAvailable

# Auxiliary functions
def get_resources_path():

    gsdm_resources_path = None
    if "GSDM_RESOURCES_PATH" in os.environ:
        # Get the path to the resources of the gsdm
        gsdm_resources_path = os.environ["GSDM_RESOURCES_PATH"]

    else:
        raise GsdmResourcesPathNotAvailable("The environment variable GSDM_RESOURCES_PATH is not defined")
    # end if

    return gsdm_resources_path

def read_configuration():
    gsdm_resources_path = get_resources_path()
    # Get configuration
    with open(gsdm_resources_path + "/" + "config/datamodel.json") as json_data_file:
        config = json.load(json_data_file)

    return config
