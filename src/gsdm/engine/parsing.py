"""
Parsing definition for the engine module

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
# Import exceptions
from .errors import ErrorParsingDictionary
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

def validate_data_dictionary(data):
    """
    """
    # Check that the operations key exists
    if len(data.keys()) != 1 or not "operations" in data.keys():
        raise ErrorParsingDictionary("Operations key does not exist or there are other defined keys in the dictionary")
    # end if

    # Check that the operations key contains a list
    if type(data["operations"]) != list:
        raise ErrorParsingDictionary("Operations key does not contain a list")
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

    check_items = [item in ["mode", "dim_signature", "source", "explicit_references", "events", "annotations"] for item in data.keys()]
    if False in check_items:
        raise ErrorParsingDictionary("The allowed tags inside the insert operation structure are: mode, dim_signature, source, explicit_references, events and annotations")
    # end if

    # Mandatory tags
    if not "dim_signature" in data:
        raise ErrorParsingDictionary("The tag dim_signature is mandatory inside the insert operation")
    # end if

    if not "source" in data:
        raise ErrorParsingDictionary("The tag source is mandatory inside the insert operation")
    # end if
    _validate_dim_signature(data["dim_signature"])
    _validate_source(data["source"])

    # Optional tags
    if "explicit_references" in data:
        _validate_explicit_references(data["explicit_references"])
    # end if
    if "events" in data:
        _validate_events(data["events"])
    # end if
    if "annotations" in data:
        _validate_annotations(data["annotations"])
    # end if

    return

def _validate_dim_signature(data):

    check_items = [item in ["exec", "name", "version"] for item in data.keys()]
    if False in check_items:
        raise ErrorParsingDictionary("The allowed tags inside dim_signature structure are: exec, name and version")
    # end if

    # Mandatory tags
    if not "exec" in data:
        raise ErrorParsingDictionary("The tag exec is mandatory inside dim_signature structure")
    # end if
    if not "name" in data:
        raise ErrorParsingDictionary("The tag name is mandatory inside dim_signature structure")
    # end if
    if not "version" in data:
        raise ErrorParsingDictionary("The tag version is mandatory inside dim_signature structure")
    # end if
    if not type(data["exec"]) == str:
        raise ErrorParsingDictionary("The tag exec inside dim_signature structure has to be of type string")
    # end if
    if not type(data["name"]) == str:
        raise ErrorParsingDictionary("The tag name inside dim_signature structure has to be of type string")
    # end if
    if not type(data["version"]) == str:
        raise ErrorParsingDictionary("The tag version inside dim_signature structure has to be of type string")
    # end if

    return

def _validate_source(data):

    check_items = [item in ["generation_time", "name", "validity_start", "validity_stop"] for item in data.keys()]
    if False in check_items:
        raise ErrorParsingDictionary("The allowed tags inside source structure are: generation_time, name, validity_start and validity_stop")
    # end if

    # Mandatory tags        
    if not "generation_time" in data:
        raise ErrorParsingDictionary("The tag generation_time is mandatory inside source structure")
    # end if
    if not "name" in data:
        raise ErrorParsingDictionary("The tag name is mandatory inside source structure")
    # end if
    if not "validity_start" in data:
        raise ErrorParsingDictionary("The tag validity_start is mandatory inside source structure")
    # end if
    if not "validity_stop" in data:
        raise ErrorParsingDictionary("The tag validity_stop is mandatory inside source structure")
    # end if
    if not type(data["name"]) == str:
        raise ErrorParsingDictionary("The tag name inside source structure has to be of type string")
    # end if
    parser.parse(data["generation_time"])
    if not is_datetime(data["generation_time"]):
        raise ErrorParsingDictionary("The tag generation_time inside source structure has to comply with this pattern AAAA-MM-DDThh:mm:ss[.mmm]")
    # end if
    if not is_datetime(data["validity_start"]):
        raise ErrorParsingDictionary("The tag validity_start inside source structure has to comply with this pattern AAAA-MM-DDThh:mm:ss[.mmm]")
    # end if
    if not is_datetime(data["validity_stop"]):
        raise ErrorParsingDictionary("The tag validity_stop inside source structure has to comply with this pattern AAAA-MM-DDThh:mm:ss[.mmm]")
    # end if

    return

def _validate_explicit_references(data):

    if type(data) != list:
        raise ErrorParsingDictionary("The tag explicit_references has to be of type list")
    # end if

    for explicit_reference in data:
        if type(explicit_reference) != dict:
            raise ErrorParsingDictionary("The items inside the explicit_references structure have to be of type dict")
        # end if

        check_items = [item in ["group", "name", "links"] for item in explicit_reference.keys()]
        if False in check_items:
            raise ErrorParsingDictionary("The allowed tags inside explicit_references structure are: group, name and links")
        # end if

        # Mandatory tags        
        if not "name" in explicit_reference:
            raise ErrorParsingDictionary("The tag name is mandatory inside explicit_references structure")
        # end if
        if not type(explicit_reference["name"]) == str:
            raise ErrorParsingDictionary("The tag name inside source structure has to be of type string")
        # end if

        # Optional tags
        if "group" in explicit_reference and not type(explicit_reference["name"]) == str:
            raise ErrorParsingDictionary("The tag name inside explicit_references structure has to be of type string")
        # end if
        if "links" in explicit_reference:
            _validate_explicit_reference_links(explicit_reference["links"])
        # end if

    # end for
    return

def _validate_explicit_reference_links(data):

    if type(data) != list:
        raise ErrorParsingDictionary("The tag links for the items inside the explicit_references structure has to be of type list")
    # end if

    for link in data:

        check_items = [item in ["back_ref", "link", "name"] for item in link.keys()]
        if False in check_items:
            raise ErrorParsingDictionary("The allowed tags inside links structure for explicit_references are: back_ref, link and name")
        # end if

        # Mandatory tags
        if not "link" in link:
            raise ErrorParsingDictionary("The tag link is mandatory inside links structure for explicit_references structure")
        # end if
        if not "name" in link:
            raise ErrorParsingDictionary("The tag name is mandatory inside links structure for explicit_references structure")
        # end if
        if not type(link["name"]) == str:
            raise ErrorParsingDictionary("The tag name inside links structure for explicit_references has to be of type string")
        # end if
        if not type(link["link"]) == str:
            raise ErrorParsingDictionary("The tag link inside links structure for explicit_references has to be of type string")
        # end if

        # Optional tags
        if "back_ref" in link and not link["back_ref"] in ["true", "false"]:
            raise ErrorParsingDictionary("The tag back_ref inside links structure for explicit_references has to be of type string and allowed values are 'true' or 'false'")
        # end if

    # end for

    return

def _validate_events(data):

    if type(data) != list:
        raise ErrorParsingDictionary("The tag events has to be of type list")
    # end if

    for event in data:
        if type(event) != dict:
            raise ErrorParsingDictionary("The event inside the events structure have to be of type dict")
        # end if

        check_items = [item in ["explicit_reference", "gauge", "start", "stop", "key", "links", "values"] for item in event.keys()]
        if False in check_items:
            raise ErrorParsingDictionary("The allowed tags inside events structure are: explicit_reference, gauge, start, stop, key, links and values")
        # end if

        # Mandatory tags        
        if not "gauge" in event:
            raise ErrorParsingDictionary("The tag gauge is mandatory inside event structure")
        # end if
        if not "start" in event:
            raise ErrorParsingDictionary("The tag start is mandatory inside event structure")
        # end if
        if not "stop" in event:
            raise ErrorParsingDictionary("The tag stop is mandatory inside event structure")
        # end if
        _validate_gauge(event["gauge"])
        if not is_datetime(event["start"]):
            raise ErrorParsingDictionary("The tag start inside event structure has to comply with this pattern AAAA-MM-DDThh:mm:ss[.mmm]")
        # end if
        if not is_datetime(event["stop"]):
            raise ErrorParsingDictionary("The tag stop inside event structure has to comply with this pattern AAAA-MM-DDThh:mm:ss[.mmm]")
        # end if

        # Optional tags
        if "explicit_reference" in event and not type(event["explicit_reference"]) == str:
            raise ErrorParsingDictionary("The tag explicit_reference inside events structure has to be of type string")
        # end if
        if "key" in event and not type(event["key"]) == str:
            raise ErrorParsingDictionary("The tag key inside events structure has to be of type string")
        # end if
        if "links" in event:
            _validate_event_links(event["links"])
        # end if
        if "values" in event:
            _validate_values(event["values"])
        # end if

    # end for

    return

def _validate_gauge(data):

    if type(data) != dict:
        raise ErrorParsingDictionary("The gauge inside the event structure has to be of type dict")
    # end if

    check_items = [item in ["insertion_type", "name", "system"] for item in data.keys()]
    if False in check_items:
        raise ErrorParsingDictionary("The allowed tags inside gauge structure are: insertion_type, system and name")
    # end if

    # Mandatory tags
    if not "insertion_type" in data:
        raise ErrorParsingDictionary("The tag insertion_type is mandatory inside gauge structure")
    # end if
    if not "name" in data:
        raise ErrorParsingDictionary("The tag name is mandatory inside gauge structure")
    # end if
    if not type(data["name"]) == str:
        raise ErrorParsingDictionary("The tag name inside gauge structure has to be of type string")
    # end if
    if not data["insertion_type"] in ["SIMPLE_UPDATE", "EVENT_KEYS", "ERASE_and_REPLACE"]:
        raise ErrorParsingDictionary("The values allowed for tag insertion_type inside gauge structure are 'SIMPLE_UPDATE', 'EVENT_KEYS' and 'ERASE_and_REPLACE'")
    # end if

    # Optional tags
    if "system" in data and not type(data["system"]) == str:
        raise ErrorParsingDictionary("The tag system inside gauge structure has to be of type string")
    # end if

    return

def _validate_event_links(data):

    if type(data) != list:
        raise ErrorParsingDictionary("The tag links for the items inside the explicit_references structure has to be of type list")
    # end if

    for link in data:

        check_items = [item in ["back_ref", "link", "name", "link_mode"] for item in link.keys()]
        if False in check_items:
            raise ErrorParsingDictionary("The allowed tags inside links structure for explicit_references are: back_ref, link, link_mode and name")
        # end if

        # Mandatory tags
        if not "link" in link:
            raise ErrorParsingDictionary("The tag link is mandatory inside links structure for events structure")
        # end if
        if not "name" in link:
            raise ErrorParsingDictionary("The tag name is mandatory inside links structure for events structure")
        # end if
        if not "link_mode" in link:
            raise ErrorParsingDictionary("The tag link_mode is mandatory inside links structure for events structure")
        # end if
        if not type(link["name"]) == str:
            raise ErrorParsingDictionary("The tag name inside links structure for events has to be of type string")
        # end if
        if not type(link["link"]) == str:
            raise ErrorParsingDictionary("The tag link inside links structure for events has to be of type string")
        # end if
        if not link["link_mode"] in ["by_ref", "by_uuid"]:
            raise ErrorParsingDictionary("The values allowed for tag link_mode inside links structure for events are 'by_ref' and 'by_uuid'")
        # end if

        # Optional tags
        if "back_ref" in link and not link["back_ref"] in ["true", "false"]:
            raise ErrorParsingDictionary("The tag back_ref inside links structure for events has to be of type string and allowed values are true or false")
        # end if

    # end for

    return

def _validate_annotations(data):

    if type(data) != list:
        raise ErrorParsingDictionary("The tag events has to be of type list")
    # end if

    for annotation in data:
        if type(annotation) != dict:
            raise ErrorParsingDictionary("The annotation inside the annotations structure have to be of type dict")
        # end if

        check_items = [item in ["explicit_reference", "annotation_cnf", "values"] for item in annotation.keys()]
        if False in check_items:
            raise ErrorParsingDictionary("The allowed tags inside annotations structure are: explicit_reference, annotation_cnf and values")
        # end if

        # Mandatory tags        
        if not "annotation_cnf" in annotation:
            raise ErrorParsingDictionary("The tag annotation_cnf is mandatory inside annotation structure")
        # end if
        if not "explicit_reference" in annotation:
            raise ErrorParsingDictionary("The tag explicit_reference is mandatory inside annotation structure")
        # end if
        if not type(annotation["explicit_reference"]) == str:
            raise ErrorParsingDictionary("The tag explicit_reference inside annotations structure has to be of type string")
        # end if
        _validate_annotation_cnf(annotation["annotation_cnf"])

        # Optional tags
        if "values" in annotation:
            _validate_values(annotation["values"])
        # end if

    # end for

    return

def _validate_annotation_cnf(data):

    if type(data) != dict:
        raise ErrorParsingDictionary("The annotation_cnf inside the annotation structure has to be of type dict")
    # end if

    check_items = [item in ["name", "system"] for item in data.keys()]
    if False in check_items:
        raise ErrorParsingDictionary("The allowed tags inside annotation_cnf structure are: system and name")
    # end if

    # Mandatory tags
    if not "name" in data:
        raise ErrorParsingDictionary("The tag name is mandatory inside annotation_cnf structure")
    # end if
    if not type(data["name"]) == str:
        raise ErrorParsingDictionary("The tag name inside annotation_cnf structure has to be of type string")
    # end if

    # Optional tags
    if "system" in data and not type(data["system"]) == str:
        raise ErrorParsingDictionary("The tag system inside annotation_cnf structure has to be of type string")
    # end if

    return

def _validate_values(data):

    if type(data) != list:
        raise ErrorParsingDictionary("The tag values has to be of type list")
    # end if

    for value in data:

        check_items = [item in ["name", "type", "value", "values"] for item in value.keys()]
        if False in check_items:
            raise ErrorParsingDictionary("The allowed tags inside values are: name, type, value and values")
        # end if

        # Mandatory tags
        if not "name" in value:
            raise ErrorParsingDictionary("The tag name is mandatory inside values structure")
        # end if
        if not "type" in value:
            raise ErrorParsingDictionary("The tag name is mandatory inside values structure")
        # end if
        if not type(value["name"]) == str:
            raise ErrorParsingDictionary("The tag name inside values structure has to be of type string")
        # end if
        if not type(value["type"]) == str and not value["type"] in ["text", "timestamp", "boolean", "double", "geometry"]:
            raise ErrorParsingDictionary("The tag type inside values structure has to be of type string and allowed values are 'text' or 'timestamp' or 'boolean', 'double', 'geometry'")
        # end if

        if "values" in value and "value" in value:
            raise ErrorParsingDictionary("The tag values cannot contain at the same tag a tag values and a tag value")
        # end if

        if "values" in value:
            _validate_values(value["values"])
        # end if

        if "value" in value and not type(value["value"]) == str:
            raise ErrorParsingDictionary("The tag value inside values structure has to be of type string")
        # end if

    # end for

    return

# def generate_json(self, json_path):
#     """
#     Method to generate a json file from the data managed by the engine

#     :param json_path: path to the json file to be generated
#     :type json_path: str
#     """
#     with open(json_path, "w") as output_file:
#         json.dump(self.data, output_file)

#     return
