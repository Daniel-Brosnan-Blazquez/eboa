"""
Parsing definition for the engine module

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import exceptions
from eboa.engine.errors import ErrorParsingDictionary

# Import auxiliary functions
from eboa.engine.functions import is_datetime

# Import functions from the eboa parsing module
from eboa.engine.parsing import validate_values, validate_alert_cnf

def validate_data_dictionary(data):
    """
    """
    
    # Check that the operations key exists
    if len(data.keys()) != 1 or not "operations" in data.keys():
        raise ErrorParsingDictionary("operations key does not exist or there are other defined keys in the dictionary")
    # end if

    # Check that the operations key contains a list
    if type(data["operations"]) != list:
        raise ErrorParsingDictionary("operations key does not contain a list")
    # end if

    for item in data["operations"]:
        # Check that the item is dictionary
        if type(item) != dict:
            raise ErrorParsingDictionary("The item inside the operations key is not corresponding to a dictionary")
        # end if
        
        # Check if the dict contains the mode
        if not "mode" in item.keys():
            raise ErrorParsingDictionary("The operation does not contain the mode")
        # end if

        # Check that the mode contains a valid value
        if not item["mode"] in ["insert", "update", "delete"]:
            raise ErrorParsingDictionary("The mode does not correspond to an allowed value")
        # end if

        if item["mode"] == "insert":
            _validate_insert_structure(item)
        # end if

    # end for
    return

def _validate_insert_structure(data):

    check_items = [item in ["mode", "report", "alerts"] for item in data.keys()]
    if False in check_items:
        raise ErrorParsingDictionary("The allowed tags inside the insert operation structure are: mode, report and alerts")
    # end if

    if not "report" in data:
        raise ErrorParsingDictionary("The tag report is mandatory inside the insert operation")
    # end if

    _validate_report(data["report"])

    if "alerts" in data:
        _validate_alerts(data["alerts"])
    # end if

    return

def _validate_report(data):

    if type(data) != dict:
        raise ErrorParsingDictionary("The tag report must be a dictionary")
    # end if
    
    check_items = [item in ["name", "group", "group_description", "path", "compress", "generation_mode", "validity_start", "validity_stop", "triggering_time", "generation_start", "generation_stop", "generator", "generator_version", "ingested", "values"] for item in data.keys()]
    if False in check_items:
        raise ErrorParsingDictionary("The allowed tags inside report structure are: name, group, group_description, path, compress, generation_mode, validity_start, validity_stop, triggering_time, generation_start, generation_stop, generator, generator_version, ingested and values")
    # end if

    # Mandatory tags
    if not "name" in data:
        raise ErrorParsingDictionary("The tag name is mandatory inside report structure")
    # end if
    if not "group" in data:
        raise ErrorParsingDictionary("The tag group is mandatory inside report structure")
    # end if
    if not "group_description" in data:
        raise ErrorParsingDictionary("The tag group_description is mandatory inside report structure")
    # end if
    if not "path" in data:
        raise ErrorParsingDictionary("The tag path is mandatory inside report structure")
    # end if
    if not "compress" in data:
        raise ErrorParsingDictionary("The tag compress is mandatory inside report structure")
    # end if
    if not "generation_mode" in data:
        raise ErrorParsingDictionary("The tag generation_mode is mandatory inside report structure")
    # end if
    if not "validity_start" in data:
        raise ErrorParsingDictionary("The tag validity_start is mandatory inside report structure")
    # end if
    if not "validity_stop" in data:
        raise ErrorParsingDictionary("The tag validity_stop is mandatory inside report structure")
    # end if
    if not "triggering_time" in data:
        raise ErrorParsingDictionary("The tag triggering_time is mandatory inside report structure")
    # end if
    if not "generation_start" in data:
        raise ErrorParsingDictionary("The tag generation_start is mandatory inside report structure")
    # end if
    if not "generation_stop" in data:
        raise ErrorParsingDictionary("The tag generation_stop is mandatory inside report structure")
    # end if
    if not "generator" in data:
        raise ErrorParsingDictionary("The tag generator is mandatory inside report structure")
    # end if
    if not "generator_version" in data:
        raise ErrorParsingDictionary("The tag generator_version is mandatory inside report structure")
    # end if
    
    if not type(data["name"]) == str:
        raise ErrorParsingDictionary("The tag name inside report structure has to be of type string")
    # end if
    if not type(data["group"]) == str:
        raise ErrorParsingDictionary("The tag group inside report structure has to be of type string")
    # end if
    if not type(data["group_description"]) == str:
        raise ErrorParsingDictionary("The tag group_description inside report structure has to be of type string")
    # end if
    if not type(data["path"]) == str:
        raise ErrorParsingDictionary("The tag path inside report structure has to be of type string")
    # end if
    if not type(data["compress"]) == str and not data["compress"].lower() in ["false", "true"]:
        raise ErrorParsingDictionary("The tag compress inside report structure has to have one of the following values: false or true")
    # end if
    if not type(data["generation_mode"]) == str and not data["generation_mode"] in ["MANUAL", "AUTOMATIC"]:
        raise ErrorParsingDictionary("The tag generation_mode inside report structure has to have one of the following values: MANUAL or AUTOMATIC")
    # end if
    if not is_datetime(data["validity_start"]):
        raise ErrorParsingDictionary("The tag validity_start inside report structure has to comply with this pattern AAAA-MM-DDThh:mm:ss[.mmm]")
    # end if
    if not is_datetime(data["validity_stop"]):
        raise ErrorParsingDictionary("The tag validity_stop inside report structure has to comply with this pattern AAAA-MM-DDThh:mm:ss[.mmm]")
    # end if
    if not is_datetime(data["triggering_time"]):
        raise ErrorParsingDictionary("The tag triggering_time inside report structure has to comply with this pattern AAAA-MM-DDThh:mm:ss[.mmm]")
    # end if
    if not is_datetime(data["generation_start"]):
        raise ErrorParsingDictionary("The tag generation_start inside report structure has to comply with this pattern AAAA-MM-DDThh:mm:ss[.mmm]")
    # end if
    if not is_datetime(data["generation_stop"]):
        raise ErrorParsingDictionary("The tag generation_stop inside report structure has to comply with this pattern AAAA-MM-DDThh:mm:ss[.mmm]")
    # end if
    if not type(data["generator"]) == str:
        raise ErrorParsingDictionary("The tag generator inside report structure has to be of type string")
    # end if
    if not type(data["generator_version"]) == str:
        raise ErrorParsingDictionary("The tag generator_version inside report structure has to be of type string")
    # end if

    # Optional tags
    if "ingested" in data and not data["ingested"].lower() in ["false", "true"]:
        raise ErrorParsingDictionary("The tag ingested has to have one of the following values: false or true")
    # end if

    return

def _validate_alerts(data):

    if type(data) != list:
        raise ErrorParsingDictionary("The tag alerts has to be of type list")
    # end if

    for alert in data:
        if type(alert) != dict:
            raise ErrorParsingDictionary("The alert inside the alerts structure have to be of type dict")
        # end if

        check_items = [item in ["message", "generator", "notification_time", "alert_cnf"] for item in alert.keys()]
        if False in check_items:
            raise ErrorParsingDictionary("The allowed tags inside alerts structure are: message, generator, notification_time and alert_cnf")
        # end if

        # Mandatory tags
        if not "message" in alert:
            raise ErrorParsingDictionary("The tag message is mandatory inside alert structure")
        # end if
        if not type(alert["message"]) == str:
            raise ErrorParsingDictionary("The tag message inside alerts structure has to be of type string")
        # end if
        if not "generator" in alert:
            raise ErrorParsingDictionary("The tag generator is mandatory inside alert structure")
        # end if
        if not type(alert["generator"]) == str:
            raise ErrorParsingDictionary("The tag generator inside alerts structure has to be of type string")
        # end if
        if not "notification_time" in alert:
            raise ErrorParsingDictionary("The tag notification_time is mandatory inside alert structure")
        # end if
        if not is_datetime(alert["notification_time"]):
            raise ErrorParsingDictionary("The tag notification_time inside report structure has to comply with this pattern AAAA-MM-DDThh:mm:ss[.mmm]")
        # end if

        validate_alert_cnf(alert["alert_cnf"])

    # end for

    return

