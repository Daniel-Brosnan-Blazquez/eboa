"""
Parsing definition for the engine module

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import exceptions
from eboa.engine.errors import ErrorParsingDictionary

# Import auxiliary functions
from eboa.engine.functions import is_datetime

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
        if not item["mode"] in ["insert", "insert_and_erase", "insert_and_erase_with_priority", "insert_and_erase_with_equal_or_lower_priority", "update", "delete"]:
            raise ErrorParsingDictionary("The mode {} does not correspond to an allowed value".format(item["mode"]))
        # end if

        if item["mode"] in ["insert", "insert_and_erase", "insert_and_erase_with_priority", "insert_and_erase_with_equal_or_lower_priority"]:
            _validate_insert_structure(item)
        # end if

    # end for
    return

def _validate_insert_structure(data):

    check_items = [item in ["mode", "dim_signature", "source", "explicit_references", "events", "annotations", "alerts"] for item in data.keys()]
    if False in check_items:
        raise ErrorParsingDictionary("The allowed tags inside the insert operation structure are: mode, dim_signature, source, explicit_references, events, annotations and alerts")
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
    if "alerts" in data:
        _validate_alerts(data["alerts"])
    # end if

    return

def _validate_dim_signature(data):

    if type(data) != dict:
        raise ErrorParsingDictionary("The tag dim_signature must be a dictionary")
    # end if
    
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

    if type(data) != dict:
        raise ErrorParsingDictionary("The tag source must be a dictionary")
    # end if
    
    check_items = [item in ["name", "validity_start", "validity_stop", "reported_validity_start", "reported_validity_stop", "reception_time", "generation_time", "reported_generation_time", "ingested", "processing_duration", "priority", "ingestion_completeness", "alerts"] for item in data.keys()]
    if False in check_items:
        raise ErrorParsingDictionary("The allowed tags inside source structure are: name, validity_start, validity_stop, reported_validity_start, reported_validity_stop, generation_time, reported_generation_time, reception_time, ingested, processing_duration, priority, ingestion_completeness and alerts")
    # end if

    # Mandatory tags        
    if not "generation_time" in data:
        raise ErrorParsingDictionary("The tag generation_time is mandatory inside source structure")
    # end if
    if not "reception_time" in data:
        raise ErrorParsingDictionary("The tag reception_time is mandatory inside source structure")
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
    if not is_datetime(data["generation_time"]):
        raise ErrorParsingDictionary("The tag generation_time inside source structure has to comply with this pattern AAAA-MM-DDThh:mm:ss[.mmm]")
    # end if
    if not is_datetime(data["reception_time"]):
        raise ErrorParsingDictionary("The tag reception_time inside source structure has to comply with this pattern AAAA-MM-DDThh:mm:ss[.mmm]")
    # end if
    if not is_datetime(data["validity_start"]):
        raise ErrorParsingDictionary("The tag validity_start inside source structure has to comply with this pattern AAAA-MM-DDThh:mm:ss[.mmm]")
    # end if
    if not is_datetime(data["validity_stop"]):
        raise ErrorParsingDictionary("The tag validity_stop inside source structure has to comply with this pattern AAAA-MM-DDThh:mm:ss[.mmm]")
    # end if

    # Optional tags
    if "ingested" in data and not type(data["ingested"]) == str and not data["ingested"].lower() in ["false", "true"]:
        raise ErrorParsingDictionary("The tag ingested has to have one of the following values: false or true")
    # end if
    if "processing_duration" in data:
        try:
            float(data["processing_duration"])
        except (ValueError, TypeError):
            raise ErrorParsingDictionary("The tag processing_duration (" + str(data["processing_duration"]) + ") inside source structure has to be convertable to float")
        # end try
    # end if
    if "reported_validity_start" in data and not is_datetime(data["reported_validity_start"]):
        raise ErrorParsingDictionary("The tag reported_validity_start inside source structure has to comply with this pattern AAAA-MM-DDThh:mm:ss[.mmm]")
    # end if
    if "reported_validity_stop" in data and not is_datetime(data["reported_validity_stop"]):
        raise ErrorParsingDictionary("The tag reported_validity_stop inside source structure has to comply with this pattern AAAA-MM-DDThh:mm:ss[.mmm]")
    # end if
    if "reported_generation_time" in data and not is_datetime(data["reported_generation_time"]):
        raise ErrorParsingDictionary("The tag reported_generation_time inside source structure has to comply with this pattern AAAA-MM-DDThh:mm:ss[.mmm]")
    # end if
    if "priority" in data:
        try:
            float(data["priority"])
        except (ValueError, TypeError):
            raise ErrorParsingDictionary("The tag priority (" + str(data["priority"]) + ") inside source structure has to be convertable to float")
        # end try
    # end if
    if "ingestion_completeness" in data:
        _validate_ingestion_completeness(data["ingestion_completeness"])
    # end if
    if "alerts" in data:
        validate_alerts_inside_entity(data["alerts"])
    # end if
    
    return

def _validate_ingestion_completeness(data):

    if type(data) != dict:
        raise ErrorParsingDictionary("The ingestion_completeness tag inside the source structure has to be of type dict")
    # end if

    check_items = [item in ["check", "message"] for item in data.keys()]
    if False in check_items:
        raise ErrorParsingDictionary("The allowed tags inside gauge structure are: check and message")
    # end if

    # Mandatory tags
    if not "check" in data:
        raise ErrorParsingDictionary("The tag check is mandatory inside ingestion_completeness structure")
    # end if
    if not type(data["check"]) == str and not data["check"].lower() in ["false", "true"]:
        raise ErrorParsingDictionary("The tag check inside ingestion_completeness structure has to have one of the following values: false or true")
    # end if
    
    if not "message" in data:
        raise ErrorParsingDictionary("The tag message is mandatory inside ingestion_completeness structure")
    # end if
    if not type(data["message"]) == str:
        raise ErrorParsingDictionary("The tag message inside ingestion_completeness structure has to be of type string")
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

        check_items = [item in ["group", "name", "links", "alerts"] for item in explicit_reference.keys()]
        if False in check_items:
            raise ErrorParsingDictionary("The allowed tags inside explicit_references structure are: group, name, links and alerts")
        # end if

        # Mandatory tags        
        if not "name" in explicit_reference:
            raise ErrorParsingDictionary("The tag name is mandatory inside explicit_references structure")
        # end if
        if not type(explicit_reference["name"]) == str:
            raise ErrorParsingDictionary("The tag name inside source structure has to be of type string")
        # end if

        # Optional tags
        if "group" in explicit_reference and not type(explicit_reference["group"]) == str:
            raise ErrorParsingDictionary("The tag group inside explicit_references structure has to be of type string")
        # end if
        if "links" in explicit_reference:
            _validate_explicit_reference_links(explicit_reference["links"])
        # end if
        if "alerts" in explicit_reference:
            validate_alerts_inside_entity(explicit_reference["alerts"])
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
            raise ErrorParsingDictionary("The tag name inside links structure for explicit_references has to be of type string. Received value: {}. Received type: {}".format(link["name"], type(link["name"])))
        # end if
        if not type(link["link"]) == str:
            raise ErrorParsingDictionary("The tag link inside links structure for explicit_references has to be of type string. Received value: {}. Received type: {}".format(link["link"], type(link["link"])))
        # end if

        # Optional tags
        if "back_ref" in link and not type(link["back_ref"]) == str:
            raise ErrorParsingDictionary("The tag back_ref inside links structure for explicit_references has to be of type string. Received value: {}. Received type: {}".format(link["back_ref"], type(link["back_ref"])))
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

        check_items = [item in ["explicit_reference", "gauge", "start", "stop", "key", "links", "values", "link_ref", "alerts"] for item in event.keys()]
        if False in check_items:
            raise ErrorParsingDictionary("The allowed tags inside events structure are: explicit_reference, gauge, start, stop, key, links, values and alerts")
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
            raise ErrorParsingDictionary("The tag start inside event structure has to comply with this pattern AAAA-MM-DDThh:mm:ss[.mmm] and be a valid date. Received value {}".format(event["start"]))
        # end if
        if not is_datetime(event["stop"]):
            raise ErrorParsingDictionary("The tag stop inside event structure has to comply with this pattern AAAA-MM-DDThh:mm:ss[.mmm] and be a valid date. Received value {}".format(event["start"]))
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
            validate_values(event["values"])
        # end if
        if "link_ref" in event and not type(event["link_ref"]) == str:
            raise ErrorParsingDictionary("The tag link_ref inside events structure has to be of type string")
        # end if
        if "alerts" in event:
            validate_alerts_inside_entity(event["alerts"])
        # end if

    # end for

    return

def _validate_gauge(data):

    if type(data) != dict:
        raise ErrorParsingDictionary("The gauge inside the event structure has to be of type dict")
    # end if

    check_items = [item in ["insertion_type", "name", "system", "description"] for item in data.keys()]
    if False in check_items:
        raise ErrorParsingDictionary("The allowed tags inside gauge structure are: insertion_type, description, system and name")
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
    if not data["insertion_type"] in ["SIMPLE_UPDATE", "EVENT_KEYS", "EVENT_KEYS_with_PRIORITY", "INSERT_and_ERASE", "INSERT_and_ERASE_with_PRIORITY", "INSERT_and_ERASE_with_EQUAL_or_LOWER_PRIORITY", "INSERT_and_ERASE_INTERSECTED_EVENTS_with_PRIORITY", "INSERT_and_ERASE_per_EVENT", "INSERT_and_ERASE_per_EVENT_with_PRIORITY"]:
        raise ErrorParsingDictionary("The values allowed for tag insertion_type inside gauge structure are 'SIMPLE_UPDATE', 'EVENT_KEYS', 'EVENT_KEYS_with_PRIORITY', 'INSERT_and_ERASE', 'INSERT_and_ERASE_with_PRIORITY', 'INSERT_and_ERASE_with_EQUAL_or_LOWER_PRIORITY', 'INSERT_and_ERASE_INTERSECTED_EVENTS_with_PRIORITY', 'INSERT_and_ERASE_per_EVENT' and 'INSERT_and_ERASE_per_EVENT_with_PRIORITY'")
    # end if

    # Optional tags
    if "system" in data and not type(data["system"]) == str:
        raise ErrorParsingDictionary("The tag system inside gauge structure has to be of type string")
    # end if
    if "description" in data and not type(data["description"]) == str:
        raise ErrorParsingDictionary("The tag description inside gauge structure has to be of type string")
    # end if

    return

def _validate_event_links(data):

    if type(data) != list:
        raise ErrorParsingDictionary("The tag links for the items inside the events structure has to be of type list")
    # end if

    for link in data:

        check_items = [item in ["back_ref", "link", "name", "link_mode"] for item in link.keys()]
        if False in check_items:
            raise ErrorParsingDictionary("The allowed tags inside links structure for events are: back_ref, link, link_mode and name")
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
            raise ErrorParsingDictionary("The tag name inside links structure for events has to be of type string. Received value: {}. Received type: {}".format(link["name"], type(link["name"])))
        # end if
        if not type(link["link"]) == str:
            raise ErrorParsingDictionary("The tag link inside links structure for events has to be of type string. Received value: {}. Received type: {}".format(link["link"], type(link["link"])))
        # end if
        if not link["link_mode"] in ["by_ref", "by_uuid"]:
            raise ErrorParsingDictionary("The values allowed for tag link_mode inside links structure for events are 'by_ref' and 'by_uuid'")
        # end if

        # Optional tags
        if "back_ref" in link and not type(link["back_ref"]) == str:
            raise ErrorParsingDictionary("The tag back_ref inside links structure for events has to be of type string. Received value: {}. Received type: {}".format(link["back_ref"], type(link["back_ref"])))
        # end if

    # end for

    return

def _validate_annotations(data):

    if type(data) != list:
        raise ErrorParsingDictionary("The tag annotations has to be of type list")
    # end if

    for annotation in data:
        if type(annotation) != dict:
            raise ErrorParsingDictionary("The annotation inside the annotations structure have to be of type dict")
        # end if

        check_items = [item in ["explicit_reference", "annotation_cnf", "values", "alerts"] for item in annotation.keys()]
        if False in check_items:
            raise ErrorParsingDictionary("The allowed tags inside annotations structure are: explicit_reference, annotation_cnf, values and alerts")
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
            validate_values(annotation["values"])
        # end if
        if "alerts" in annotation:
            validate_alerts_inside_entity(annotation["alerts"])
        # end if

    # end for

    return

def _validate_annotation_cnf(data):

    if type(data) != dict:
        raise ErrorParsingDictionary("The annotation_cnf inside the annotation structure has to be of type dict")
    # end if

    check_items = [item in ["insertion_type", "name", "system", "description"] for item in data.keys()]
    if False in check_items:
        raise ErrorParsingDictionary("The allowed tags inside annotation_cnf structure are: insertion_type, description, system and name")
    # end if

    # Mandatory tags
    if not "name" in data:
        raise ErrorParsingDictionary("The tag name is mandatory inside annotation_cnf structure")
    # end if
    if not type(data["name"]) == str:
        raise ErrorParsingDictionary("The tag name inside annotation_cnf structure has to be of type string")
    # end if

    # Optional tags
    if "insertion_type" in data and not data["insertion_type"] in ["SIMPLE_UPDATE", "INSERT_and_ERASE", "INSERT_and_ERASE_with_PRIORITY"]:
        raise ErrorParsingDictionary("The tag insertion_type inside annotation_cnf structure has to be of type string and allowed values are SIMPLE_UPDATE, INSERT_and_ERASE and INSERT_and_ERASE_with_PRIORITY")
    # end if
    if "system" in data and not type(data["system"]) == str:
        raise ErrorParsingDictionary("The tag system inside annotation_cnf structure has to be of type string")
    # end if
    if "description" in data and not type(data["description"]) == str:
        raise ErrorParsingDictionary("The tag description inside annotation_cnf structure has to be of type string")
    # end if

    return

def validate_values(data):

    if type(data) != list:
        raise ErrorParsingDictionary("The tag values has to be of type list")
    # end if

    for value in data:
        if type(value) != dict:
            raise ErrorParsingDictionary("The values have to be of type dict")
        # end if

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
        if not type(value["type"]) == str or not value["type"] in ["text", "timestamp", "boolean", "double", "geometry", "object"]:
            raise ErrorParsingDictionary("The tag type inside values structure has to be of type string and allowed values are 'text' or 'timestamp' or 'boolean' or 'double' or 'geometry' or 'object'. Provided type is '" + value["type"] + "'. The content of complete value is " + str(value) + ".")
        # end if

        if "values" in value and "value" in value:
            raise ErrorParsingDictionary("The tag values cannot contain at the same time a tag values and a tag value")
        # end if

        if "values" in value:
            validate_values(value["values"])
        # end if

        if "value" in value and value["type"] in ["text", "timestamp", "boolean", "geometry", "object"] and not type(value["value"]) == str:
            raise ErrorParsingDictionary("The tag value ({}) inside values structure has to be of type string. Received type is: {}".format(value["value"], type(value["value"])))
        # end if

        if "value" in value and value["type"] == "double":
            try:
                float(value["value"])
            except (ValueError, TypeError):
                raise ErrorParsingDictionary("The tag value (" + str(value["value"]) + ") inside values structure has to be convertable to float")
            # end try
        # end if

    # end for

    return

def validate_alerts_inside_entity(data):

    if type(data) != list:
        raise ErrorParsingDictionary("The tag alerts has to be of type list")
    # end if

    for alert in data:
        if type(alert) != dict:
            raise ErrorParsingDictionary("The alert inside the alerts structure have to be of type dict")
        # end if

        check_items = [item in ["message", "generator", "notification_time", "alert_cnf", "justification"] for item in alert.keys()]
        if False in check_items:
            raise ErrorParsingDictionary("The allowed tags inside alerts structure are: message, generator, notification_time, alert_cnf and justification")
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
            raise ErrorParsingDictionary("The tag notification_time inside source structure has to comply with this pattern AAAA-MM-DDThh:mm:ss[.mmm]")
        # end if

        validate_alert_cnf(alert["alert_cnf"])

        # Optional tags
        if "justification" in alert and not type(alert["justification"]) == str:
            raise ErrorParsingDictionary("The tag justification inside alerts structure has to be of type string")
        # end if

    # end for

    return

def _validate_alerts(data):

    if type(data) != list:
        raise ErrorParsingDictionary("The tag alerts has to be of type list")
    # end if

    for alert in data:
        if type(alert) != dict:
            raise ErrorParsingDictionary("The alert inside the alerts structure have to be of type dict")
        # end if

        check_items = [item in ["message", "generator", "notification_time", "entity", "alert_cnf", "justification"] for item in alert.keys()]
        if False in check_items:
            raise ErrorParsingDictionary("The allowed tags inside alerts structure are: message, generator, notification_time, entity, alert_cnf and justification")
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
            raise ErrorParsingDictionary("The tag notification_time inside source structure has to comply with this pattern AAAA-MM-DDThh:mm:ss[.mmm]")
        # end if

        if not "entity" in alert:
            raise ErrorParsingDictionary("The tag entity is mandatory inside alert structure")
        # end if
        _validate_alert_entity(alert["entity"])        
        validate_alert_cnf(alert["alert_cnf"])

        # Optional tags
        if "justification" in alert and not type(alert["justification"]) == str:
            raise ErrorParsingDictionary("The tag justification inside alerts structure has to be of type string")
        # end if

    # end for

    return

def _validate_alert_entity(data):

    if type(data) != dict:
        raise ErrorParsingDictionary("The entity tag inside the alert structure has to be of type dict")
    # end if

    check_items = [item in ["reference_mode", "reference", "type"] for item in data.keys()]
    if False in check_items:
        raise ErrorParsingDictionary("The allowed tags inside entity structure are: reference_mode, reference and type")
    # end if

    # Mandatory tags
    if not "reference_mode" in data:
        raise ErrorParsingDictionary("The tag reference_mode is mandatory inside entity structure")
    # end if
    if not data["reference_mode"] in ["by_ref", "by_uuid"]:
        raise ErrorParsingDictionary("The values allowed for tag reference_mode inside entity structure are 'by_ref' and 'by_uuid'")
    # end if
    if not "reference" in data:
        raise ErrorParsingDictionary("The tag reference is mandatory inside entity structure")
    # end if
    if not type(data["reference"]) == str:
        raise ErrorParsingDictionary("The tag reference inside entity structure has to be of type string")
    # end if
    if not "type" in data:
        raise ErrorParsingDictionary("The tag type is mandatory inside entity structure")
    # end if
    if not data["type"] in ["event", "source", "explicit_ref", "annotation"]:
        raise ErrorParsingDictionary("The values allowed for tag type inside entity structure are 'event', 'annotation', 'source' and 'explicit_ref'")
    # end if
    if data["type"] == "annotation" and data["reference_mode"] != "by_uuid":
        raise ErrorParsingDictionary("The alerts associated to annotations are only allowed by using UUDDs")
    # end if
    
    return

def validate_alert_cnf(data):

    if type(data) != dict:
        raise ErrorParsingDictionary("The alert_cnf inside the alert structure has to be of type dict")
    # end if

    check_items = [item in ["name", "severity", "description", "group"] for item in data.keys()]
    if False in check_items:
        raise ErrorParsingDictionary("The allowed tags inside alert_cnf structure are: description, severity and name")
    # end if

    # Mandatory tags
    if not "name" in data:
        raise ErrorParsingDictionary("The tag name is mandatory inside alert_cnf structure")
    # end if
    if not type(data["name"]) == str:
        raise ErrorParsingDictionary("The tag name inside alert_cnf structure has to be of type string")
    # end if
    if not "severity" in data:
        raise ErrorParsingDictionary("The tag name is mandatory inside alert_cnf structure")
    # end if
    severities = ["info", "warning", "minor", "major", "critical", "fatal"]
    if not data["severity"] in severities:
        raise ErrorParsingDictionary("The tag severity inside alert_cnf structure has to be one of the following values {}".format(severities))
    # end if

    # Optional tags
    if "description" in data and not type(data["description"]) == str:
        raise ErrorParsingDictionary("The tag description inside alert_cnf structure has to be of type string")
    # end if
    # Optional tags
    if "group" in data and not type(data["group"]) == str:
        raise ErrorParsingDictionary("The tag group inside alert_cnf structure has to be of type string")
    # end if

    return

def check_filters(filters, expected_filters):
    """
    Method to check if the received filters are matching the expected filters
    
    :param filters: dictionary with the filters to apply to the query
    :type filters: dict
    :param expected_filters: keys expected inside the filters structure
    :type expected_filters: list

    """

    # Check filters
    if filters:
        check_filters = [item in expected_filters for item in filters.keys()]
        if False in check_filters:
            raise ErrorParsingFilters("The allowed tags inside the filters structure are: {}. Received filters are {}".format(expected_filters, filters.keys()))
        # end if
    # end if

    return
