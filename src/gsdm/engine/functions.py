"""
Functions definition for the engine component

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
# Import python utilities
import os
import json
from dateutil import parser

# Auxiliary functions
def is_datetime(date):
    """
    Function for the parsing of dates inside json files
    
    :param date: date to be parsed
    :type date: str

    :return: True if date is a correct date, False otherwise
    :rtype: bool
    """
    try:
        parser.parse(date)
    except:
        return False
    else:
        return True

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
    with open(gsdm_resources_path + "/" + "config/engine.json") as json_data_file:
        config = json.load(json_data_file)

    return config

    
