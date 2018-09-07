"""
Functions definition for the engine component

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
# Import python utilities
import os
import json
from dateutil import parser

# Import exceptions
from gsdm.engine.errors import InputError, GsdmResourcesPathNotAvailable

# Import SQLAlchemy utilities
import uuid

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
    """
    Method to obtain the path to the resources of the gsdm
    """
    gsdm_resources_path = None
    if "GSDM_RESOURCES_PATH" in os.environ:
        # Get the path to the resources of the gsdm
        gsdm_resources_path = os.environ["GSDM_RESOURCES_PATH"]

    else:
        raise GsdmResourcesPathNotAvailable("The environment variable GSDM_RESOURCES_PATH is not defined")
    # end if

    return gsdm_resources_path

def read_configuration():
    """
    Method to read the configuration of engine submodule of the gsdm
    """
    gsdm_resources_path = get_resources_path()
    # Get configuration
    with open(gsdm_resources_path + "/" + "config/engine.json") as json_data_file:
        config = json.load(json_data_file)

    return config

def is_valid_date_filters(date_filters, operators):

    if type(date_filters) != list:
        raise InputError("The parameter date_filters must be a list of dictionaries.")
    # end if
    for date_filter in date_filters:
        if type(date_filter) != dict:
            raise InputError("The parameter date_filters must contain dictionaries.")
        # end if
        if len(date_filter.keys()) != 2 or not "date" in date_filter.keys() or not "op" in date_filter.keys():
            raise InputError("Every date_filter should be a dictionary with keys date and op.")
        # end if
        if not date_filter["op"] in operators:
            raise InputError("The specified op is not a valid operator.")
        # end if
        if not is_datetime(date_filter["date"]):
            raise InputError("The specified date is not a valid date.")
        # end if
    # end for

    return True

def is_valid_float_filters(float_filters, operators):

    if type(float_filters) != list:
        raise InputError("The parameter float_filters must be a list of dictionaries.")
    # end if
    for float_filter in float_filters:
        if type(float_filter) != dict:
            raise InputError("The parameter float_filters must contain dictionaries.")
        # end if
        if len(float_filter.keys()) != 2 or not "float" in float_filter.keys() or not "op" in float_filter.keys():
            raise InputError("Every float_filter should be a dictionary with keys float and op.")
        # end if
        if not float_filter["op"] in operators:
            raise InputError("The specified op is not a valid operator.")
        # end if
        try:
            float(float_filter["float"])
        except ValueError:
            raise InputError("The specified float is not a valid float.")
        # end try
    # end for

    return True

def is_valid_string_filters(string_filters, operators):

    if type(string_filters) != list:
        raise InputError("The parameter string_filters must be a list of dictionaries.")
    # end if
    for string_filter in string_filters:
        if type(string_filter) != dict:
            raise InputError("The parameter string_filters must contain dictionaries.")
        # end if
        if len(string_filter.keys()) != 2 or not "str" in string_filter.keys() or not "op" in string_filter.keys():
            raise InputError("Every string_filter should be a dictionary with keys string and op.")
        # end if
        if not string_filter["op"] in operators:
            raise InputError("The specified op is not a valid operator.")
        # end if
        if type(string_filter["str"]) != str:
            raise InputError("The specified str must be a string.")
        # end if
    # end for

    return True

def is_valid_value_filters(value_filters, operators):

    if type(value_filters) != list:
        raise InputError("The parameter value_filters must be a list of dictionaries.")
    # end if
    for value_filter in value_filters:
        if type(value_filter) != dict:
            raise InputError("The parameter value_filters must contain dictionaries.")
        # end if
        if len(value_filter.keys()) != 3 or not "value" in value_filter.keys() or not "op" in value_filter.keys() or not "type" in value_filter.keys():
            raise InputError("Every value_filter should be a dictionary with keys value, type and op.")
        # end if
        if not value_filter["op"] in operators:
            raise InputError("The specified op is not a valid operator.")
        # end if
        if type(value_filter["value"]) != str:
            raise InputError("The specified value must be a string.")
        # end if
        if not type(value_filter["type"]) == str or not value_filter["type"] in ["text", "timestamp", "boolean", "double", "geometry", "object"]:
            raise InputError("The specified type must be a string and allowed values are 'text' or 'timestamp' or 'boolean' or 'double' or 'geometry' or 'object'")
        # end if
    # end for

    return True

def is_valid_values_names_type(values_names_type):

    if type(values_names_type) != list:
        raise InputError("The parameter values_names_type must be a list of dictionaries.")
    # end if
    for value_names_type in values_names_type:
        if type(value_names_type) != dict:
            raise InputError("The parameter values_names_type must contain dictionaries.")
        # end if
        if len(value_names_type.keys()) != 2 or not "names" in value_names_type.keys() or not "type" in value_names_type.keys():
            raise InputError("Every value_names_type should be a dictionary with keys names and type.")
        # end if
        if type(value_names_type["names"]) != list:
            raise InputError("The specified names must be a list of string.")
        # end if
        for value_name in value_names_type["names"]:
            if type(value_name) != str:
                raise InputError("The specified names must contain strings.")
            # end if
        # end for
        if not type(value_names_type["type"]) == str or not value_names_type["type"] in ["text", "timestamp", "boolean", "double", "geometry", "object"]:
            raise InputError("The specified type must be a string and allowed values are 'text' or 'timestamp' or 'boolean' or 'double' or 'geometry' or 'object'")
        # end if


    # end for

    return True

def is_valid_values_name_type_like(values_name_type_like):

    if type(values_name_type_like) != list:
        raise InputError("The parameter values_name_type_like must be a list of dictionaries.")
    # end if
    for value_name_type_like in values_name_type_like:
        if type(value_name_type_like) != dict:
            raise InputError("The parameter value_name_type_like must contain dictionaries.")
        # end if
        if len(value_name_type_like.keys()) != 2 or not "name_like" in value_name_type_like.keys() or not "type" in value_name_type_like.keys():
            raise InputError("Every value_name_type_like should be a dictionary with keys name_like and type.")
        # end if
        if type(value_name_type_like["name_like"]) != str:
            raise InputError("The specified name_like must be a string.")
        # end if
        if not type(value_name_type_like["type"]) == str or not value_name_type_like["type"] in ["text", "timestamp", "boolean", "double", "geometry", "object"]:
            raise InputError("The specified type must be a string and allowed values are 'text' or 'timestamp' or 'boolean' or 'double' or 'geometry' or 'object'")
        # end if
    # end for

    return True

def is_valid_operator_list(operator_list):

    if type(operator_list) != dict:
        raise InputError("The parameter operator_list must be a dictionary.")
    # end if
    if len(operator_list.keys()) != 2 or not "op" in operator_list.keys() or not "list" in operator_list.keys():
        raise InputError("The parameter operator_list should be a dictionary with keys op and list.")
    # end if
    if type(operator_list["op"]) != str or not operator_list["op"] in ["in", "notin"]:
        raise InputError("The specified op must be a string equal to 'in' or 'notin'.")
    # end if
    if type(operator_list["list"]) != list:
        raise InputError("The specified list must be a list of strings.")
    # end if
    for item in operator_list["list"]:
        if type(item) != str and type(item) != uuid.UUID:
            raise InputError("The specified list must contain strings.")
        # end if
    # end for

    return True

def is_valid_operator_like(operator_like):

    if type(operator_like) != dict:
        raise InputError("The parameter operator_like must be a dictionary.")
    # end if
    if len(operator_like.keys()) != 2 or not "op" in operator_like.keys() or not "str" in operator_like.keys():
        raise InputError("The parameter operator_like should be a dictionary with keys op and string.")
    # end if
    if type(operator_like["op"]) != str or not operator_like["op"] in ["like", "notlike"]:
        raise InputError("The specified op must be a string equal to 'like' or 'notlike'.")
    # end if
    if type(operator_like["str"]) != str:
        raise InputError("The specified str must be a string.")
    # end if

    return True
