"""
Functions definition for the engine component

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import os
import json
from dateutil import parser

# Import exceptions
from eboa.engine.errors import InputError, EboaResourcesPathNotAvailable

# Import SQLAlchemy utilities
import uuid

# Import operators
from eboa.engine.operators import arithmetic_operators, text_operators, regex_operators

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

def get_log_path():
    """
    Method to obtain the path to the log of the eboa
    """
    eboa_log_path = None
    if "EBOA_LOG_PATH" in os.environ:
        # Get the path to the log of the eboa
        eboa_log_path = os.environ["EBOA_LOG_PATH"]

    else:
        raise EboaLogPathNotAvailable("The environment variable EBOA_LOG_PATH is not defined")
    # end if

    return eboa_log_path

def get_schemas_path():
    """
    Method to obtain the path to the schemas of the eboa
    """
    eboa_schemas_path = None
    if "EBOA_SCHEMAS_PATH" in os.environ:
        # Get the path to the schemas of the eboa
        eboa_schemas_path = os.environ["EBOA_SCHEMAS_PATH"]

    else:
        raise EboaSchemasPathNotAvailable("The environment variable EBOA_SCHEMAS_PATH is not defined")
    # end if

    return eboa_schemas_path

def get_resources_path():
    """
    Method to obtain the path to the resources of the eboa
    """
    eboa_resources_path = None
    if "EBOA_RESOURCES_PATH" in os.environ:
        # Get the path to the resources of the eboa
        eboa_resources_path = os.environ["EBOA_RESOURCES_PATH"]

    else:
        raise EboaResourcesPathNotAvailable("The environment variable EBOA_RESOURCES_PATH is not defined")
    # end if

    return eboa_resources_path

def read_configuration():
    """
    Method to read the configuration of engine submodule of the eboa
    """
    eboa_resources_path = get_resources_path()
    # Get configuration
    with open(eboa_resources_path + "/engine.json") as json_data_file:
        config = json.load(json_data_file)

    return config

def is_valid_bool_filter(filter):

    if type(filter) != bool:
        raise InputError("The parameter filter must be a boolean value (received filter: {}).".format(filter))
    # end if

    return True

def is_valid_positive_integer(filter):

    try:
        int(filter)
    except ValueError:
        raise InputError("The parameter filter must be an integer (received filter: {}).".format(filter))
    # end try
    
    if int(filter) < 0:
        raise InputError("The parameter filter must be a positive integer (received filter: {}).".format(filter))
    # end if

    return True

def is_valid_order_by(order_by):

    if type(order_by) != dict:
        raise InputError("The parameter order_by must be a dictionary (received order_by: {}).".format(order_by))
    # end if

    if len(order_by.keys()) != 2 or not "field" in order_by.keys() or not "descending" in order_by.keys():
        raise InputError("Every order_by should be a dictionary with keys field and descending (received keys: {}).".format(order_by.keys()))
    # end if

    if type(order_by["field"]) != str:
        raise InputError("The key field inside the dictionary order_by must be a string (received type: {}).".format(order_by["field"]))
    # end if

    if type(order_by["descending"]) != bool:
        raise InputError("The key descending inside the dictionary order_by must be a boolean (received type: {}).".format(order_by["descending"]))
    # end if

    return True

def is_valid_bool_filter_with_op(bool_filter):

    if type(bool_filter) != dict:
        raise InputError("The parameter bool_filter must be a dict (received bool_filter: {}).".format(bool_filter))
    # end if

    if len(bool_filter.keys()) != 2 or not "filter" in bool_filter.keys() or not "op" in bool_filter.keys():
        raise InputError("Every bool_filter should be a dictionary with keys date and op (received keys: {}).".format(bool_filter.keys()))
    # end if

    if not bool_filter["op"] in arithmetic_operators:
        raise InputError("The specified op is not a valid operator (received op: {}).".format(bool_filter["op"]))
    # end if
    try:
        bool(bool_filter["filter"])
    except ValueError:
        raise InputError("The specified bool is not a valid bool (received bool: {}).".format(bool_filter["filter"]))
    # end try

    return True

def is_valid_date_filters(date_filters):

    if type(date_filters) != list:
        raise InputError("The parameter date_filters must be a list of dictionaries (received date_filters: {}).".format(date_filters))
    # end if
    for date_filter in date_filters:
        if type(date_filter) != dict:
            raise InputError("The parameter date_filters must contain dictionaries (received date_filter: {}).".format(date_filter))
        # end if
        if len(date_filter.keys()) != 2 or not "date" in date_filter.keys() or not "op" in date_filter.keys():
            raise InputError("Every date_filter should be a dictionary with keys date and op (received keys: {}).".format(date_filter.keys()))
        # end if
        if not date_filter["op"] in arithmetic_operators:
            raise InputError("The specified op is not a valid operator (received op: {}).".format(date_filter["op"]))
        # end if
        if not is_datetime(date_filter["date"]):
            raise InputError("The specified date is not a valid date (received date: {}).".format(date_filter["date"]))
        # end if
    # end for

    return True

def is_valid_float_filters(float_filters):

    if type(float_filters) != list:
        raise InputError("The parameter float_filters must be a list of dictionaries (received float_filters: {}).".format(float_filters))
    # end if
    for float_filter in float_filters:
        if type(float_filter) != dict:
            raise InputError("The parameter float_filters must contain dictionaries (received float_filter: {}).".format(float_filter))
        # end if
        if len(float_filter.keys()) != 2 or not "float" in float_filter.keys() or not "op" in float_filter.keys():
            raise InputError("Every float_filter should be a dictionary with keys float and op (received keys: {}).".format(float_filter.keys()))
        # end if
        if not float_filter["op"] in arithmetic_operators:
            raise InputError("The specified op is not a valid operator (received op: {}).".format(float_filter["op"]))
        # end if
        try:
            float(float_filter["float"])
        except ValueError:
            raise InputError("The specified float is not a valid float (received float: {}).".format(float_filter["float"]))
        # end try
    # end for

    return True

def is_valid_value_filters(value_filters):

    if type(value_filters) != list:
        raise InputError("The parameter value_filters must be a list of dictionaries (received value_filters: {}).".format(value_filters))
    # end if
    for value_filter in value_filters:
        if type(value_filter) != dict:
            raise InputError("The parameter value_filters must contain dictionaries (received value_filter: {}).".format(value_filter))
        # end if
        if len(value_filter.keys()) > 3:
            raise InputError("Every value_filter should be a dictionary with maximum 3 keys (received keys: {}).".format(value_filter.keys()))
        # end if
        if len(value_filter.keys()) == 3 and (not "name" in value_filter.keys() or not "type" in value_filter.keys() or not "value" in value_filter.keys()):
            raise InputError("Every value_filter should be a dictionary with keys name, type and, if three specified, and value (received keys: {}).".format(value_filter.keys()))
        # end if
        if len(value_filter.keys()) == 2 and (not "name" in value_filter.keys() or not "type" in value_filter.keys()):
            raise InputError("Every value_filter should be a dictionary with at least keys name and type (received keys: {}).".format(value_filter.keys()))
        # end if

        is_valid_text_filter(value_filter["name"])
        if type(value_filter["type"]) != str:
            raise InputError("The specified name must be a string (received type: {}).".format(value_filter["type"]))
        if not value_filter["type"] in ["text", "timestamp", "boolean", "double", "geometry", "object"]:
            raise InputError("The specified type is not in the list of allowed types: 'text' or 'timestamp' or 'boolean' or 'double' or 'geometry' or 'object' (received type: {}).".format(value_filter["type"]))
        # end if
        if "value" in value_filter and value_filter["type"] != "object":
            is_valid_text_filter(value_filter["value"])
        elif "value" in value_filter and value_filter["type"] == "object":
            raise InputError("The object value has no possibility of filtering by value")
        # end if
    # end for

    return True

def is_valid_operator_list(operator_list):

    if type(operator_list) != dict:
        raise InputError("The parameter operator_list must be a dictionary (received operator_list: {}).".format(operator_list))
    # end if
    if len(operator_list.keys()) != 2 or not "op" in operator_list.keys() or not "list" in operator_list.keys():
        raise InputError("The parameter operator_list should be a dictionary with keys op and list (received keys: {}).".format(operator_list.keys()))
    # end if
    if type(operator_list["op"]) != str or not operator_list["op"] in ["in", "notin"]:
        raise InputError("The specified op must be a string equal to 'in' or 'notin' (received op: {}).".format(operator_list["op"]))
    # end if
    if type(operator_list["list"]) != list:
        raise InputError("The specified list must be a list of strings (received list: {}).".format(operator_list["list"]))
    # end if
    for item in operator_list["list"]:
        if type(item) != str and type(item) != uuid.UUID:
            raise InputError("The specified list must contain strings or UUIDs (received content: {}).".format(item))
        # end if
    # end for

    return True

def is_valid_text_filter(text_filter):

    if type(text_filter) != dict:
        raise InputError("The parameter text_filter must be a dictionary (received text_filter: {}).".format(text_filter))
    # end if
    if len(text_filter.keys()) != 2 or not "op" in text_filter.keys() or not "filter" in text_filter.keys():
        raise InputError("The parameter text_filter should be a dictionary with keys op and filter (received keys: {}).".format(text_filter.keys()))
    # end if
    if type(text_filter["op"]) != str or not (text_filter["op"] in arithmetic_operators.keys() or text_filter["op"] in text_operators.keys() or text_filter["op"] in regex_operators.keys()):
        raise InputError("The specified op must be a string inside these list: {} (received op: {}).".format(list(arithmetic_operators.keys()) + list(text_operators.keys()) + list(regex_operators.keys()), text_filter["op"]))
    # end if
    if (text_filter["op"] in ["like", "notlike", "regex"] or text_filter["op"] in arithmetic_operators.keys()) and type(text_filter["filter"]) != str:
        raise InputError("The specified filter must be a string when the op is in the list {} (received filter has type: {}).".format(list(arithmetic_operators.keys()) + ["like", "notlike"], type(text_filter["filter"])))
    # end if
    if text_filter["op"] in ["in", "notin"] and type(text_filter["filter"]) != list:
        raise InputError("The specified filter must be a list when the op is 'in' or 'notin' (received filter has type: {}).".format(type(text_filter["filter"])))
    # end if
    if text_filter["op"] in ["in", "notin"] and type(text_filter["filter"]) == list:
        not_str_filters = [text_filter for text_filter in text_filter["filter"] if type(text_filter) != str and type(text_filter) != uuid.UUID]
        if len(not_str_filters) > 0:
            raise InputError("The specified filter inside the list must be a string when the op is 'in' or 'notin' (received filters are: {}).".format(not_str_filters))
        # end if
    # end if

    return True

