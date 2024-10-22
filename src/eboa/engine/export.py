"""
Export definition for providing formatted extracted data

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
from geoalchemy2.shape import to_shape

import logging
from eboa.logging import Log

# Import EBOA errors
from eboa.engine.errors import ErrorParsingParameters

logging = Log(name = __name__)
logger = logging.logger

########
# Export methods in python dictionary format
########
def export_events(structure, events, include_ers = True, include_annotations = True, include_sources = True, include_alerts = False, group = None):
    """Function to insert the events with DDBB format into a dictionary
    
    :param structure: dictionary where to export the events to
    :type structure: dict
    :param events: list of events
    :type events: list(Event)
    :param include_ers: flag to indicate if the detail of the explicit references has to be included
    :type include_ers: boolean
    :param include_annotations: flag to indicate if the detail of the annotations has to be included
    :type include_annotations: boolean
    :param include_sources: flag to indicate if the detail of the sources has to be included
    :type include_sources: boolean
    :param include_alerts: flag to indicate if the detail of the alerts has to be included
    :type include_alerts: boolean
    :param group: label to group the events
    :type group: str

    Output: The function will insert into the structure the
    corresponding events, leaving the structure like the following:
    structure = {
    "event_groups": {
    group: [uuid1, uuid2, ..., uuidn]
    "events": {
    "uuid1": structure of event associated to uuid1,
    "uuid2": structure of event associated to uuid2,
    ...,
    "uuidn": structure of event associated to uuidn
    }
    }
    }

    """

    if type(structure) != dict:
        raise ErrorParsingParameters("The structure parameter has to have type dict. Received structure is: {}, with type: {}".format(structure, type(structure)))
    # end if
    
    if group != None and type(group) != str:
        raise ErrorParsingParameters("The group parameter has to have type str. Received group is: {}, with type: {}".format(group, type(group)))
    # end if

    if type(include_ers) != bool:
        raise ErrorParsingParameters("The include_ers parameter has to have type bool. Received group is: {}, with type: {}".format(include_ers, type(include_ers)))
    # end if

    if type(include_annotations) != bool:
        raise ErrorParsingParameters("The include_annotations parameter has to have type bool. Received group is: {}, with type: {}".format(include_annotations, type(include_annotations)))
    # end if

    if type(include_alerts) != bool:
        raise ErrorParsingParameters("The include_alerts parameter has to have type bool. Received group is: {}, with type: {}".format(include_alerts, type(include_alerts)))
    # end if

    if group and "event_groups" not in structure:
        structure["event_groups"] = {}
    # end if
    if group and group not in structure["event_groups"]:
        structure["event_groups"][group] = []
    # end if
    if "events" not in structure:
        structure["events"] = {}
    # end if

    if "gauges" not in structure:
        structure["gauges"] = {}
    # end if

    for event in events:

        # Check if the event was already included in the structure
        if str(event.event_uuid) in structure["events"]:
            continue
        # end if

        # Insert the event reference in the group
        if group:
            structure["event_groups"][group].append(str(event.event_uuid))
        # end if

        # Insert the structure of the event
        structure["events"][str(event.event_uuid)] = event.jsonify(include_indexed_values = True)

        # Insert the id in structure of the gauges
        gauge_name = event.gauge.name
        gauge_system = event.gauge.system
        if gauge_name not in structure["gauges"]:
            structure["gauges"][gauge_name] = {}
        # end if
        if gauge_system not in structure["gauges"][gauge_name]:
            structure["gauges"][gauge_name][gauge_system] = []
        # end if
        structure["gauges"][gauge_name][gauge_system].append(str(event.event_uuid))

        # Insert structure of the explicit reference
        if include_ers and event.explicitRef != None:
            export_ers(structure, [event.explicitRef], include_annotations = include_annotations, include_events = False, include_sources = include_sources)

            # Include only this event into the structure of the associated ER
            er_structure = structure["explicit_references"][str(event.explicitRef.explicit_ref_uuid)]
            if "events" not in er_structure:
                er_structure["events"] = {}
            # end if
            name = event.gauge.name
            system = event.gauge.system
            if name not in er_structure["events"]:
                er_structure["events"][name] = []
            # end if
            er_structure["events"][name].append({
                "event_uuid": str(event.event_uuid),
                "name": name,
                "system": system
            })

        # end if

        # Insert structure of the source
        if include_sources:
            export_sources(structure, [event.source], include_alerts = include_alerts)
        # end if

        if include_alerts and len(event.alerts) > 0:
            structure["events"][str(event.event_uuid)]["alerts"] = [alert.get_uuid() for alert in event.alerts]
            export_alerts(structure, event.alerts)
        # end if

    # end for

    return structure

def export_annotations(structure, annotations, include_ers = True, include_events = True, include_sources = True, include_alerts = False, group = None):
    """Function to insert the annotations with DDBB format into a dictionary
    
    :param structure: dictionary where to export the annotations to
    :type structure: dict
    :param annotations: list of annotations
    :type annotations: list(Annotation)
    :param include_ers: flag to indicate if the detail of the explicit references has to be included
    :type include_ers: boolean
    :param include_events: flag to indicate if the detail of the events has to be included
    :type include_events: boolean
    :param include_sources: flag to indicate if the detail of the sources has to be included
    :type include_sources: boolean
    :param include_alerts: flag to indicate if the detail of the alerts has to be included
    :type include_alerts: boolean
    :param group: label to group the annotations
    :type group: str

    Output: The function will insert into the structure the
    corresponding annotations, leaving the structure like the following:
    structure = {
    "annotation_groups": {
    group: [uuid1, uuid2, ..., uuidn]
    "annotations": {
    "uuid1": structure of annotation associated to uuid1,
    "uuid2": structure of annotation associated to uuid2,
    ...,
    "uuidn": structure of annotation associated to uuidn
    }
    }
    }

    """

    if type(structure) != dict:
        raise ErrorParsingParameters("The structure parameter has to have type dict. Received structure is: {}, with type: {}".format(structure, type(structure)))
    # end if
    
    if group != None and type(group) != str:
        raise ErrorParsingParameters("The group parameter has to have type str. Received group is: {}, with type: {}".format(group, type(group)))
    # end if

    if type(include_ers) != bool:
        raise ErrorParsingParameters("The include_ers parameter has to have type bool. Received group is: {}, with type: {}".format(include_ers, type(include_ers)))
    # end if

    if type(include_events) != bool:
        raise ErrorParsingParameters("The include_events parameter has to have type bool. Received group is: {}, with type: {}".format(include_events, type(include_events)))
    # end if

    if type(include_alerts) != bool:
        raise ErrorParsingParameters("The include_alerts parameter has to have type bool. Received group is: {}, with type: {}".format(include_alerts, type(include_alerts)))
    # end if

    if group and "annotation_groups" not in structure:
        structure["annotation_groups"] = {}
    # end if
    if group and group not in structure["annotation_groups"]:
        structure["annotation_groups"][group] = []
    # end if
    if "annotations" not in structure:
        structure["annotations"] = {}
    # end if
        
    for annotation in annotations:

        # Check if the annotation was already included in the structure
        if str(annotation.annotation_uuid) in structure["annotations"]:
            continue
        # end if

        # Insert the annotation reference in the group
        if group:
            structure["annotation_groups"][group].append(str(annotation.annotation_uuid))
        # end if

        # Insert the structure of the annotation
        structure["annotations"][str(annotation.annotation_uuid)] = annotation.jsonify(include_indexed_values = True)

        # Insert structure of the explicit reference
        if include_ers:
            export_ers(structure, [annotation.explicitRef], include_annotations = False, include_events = include_events, include_sources = include_sources)

            # Include only this annotation into the structure of the associated ER
            er_structure = structure["explicit_references"][str(annotation.explicitRef.explicit_ref_uuid)]
            if "annotations" not in er_structure:
                er_structure["annotations"] = {}
            # end if
            name = annotation.annotationCnf.name
            system = annotation.annotationCnf.system
            if name not in er_structure["annotations"]:
                er_structure["annotations"][name] = []
            # end if
            er_structure["annotations"][name].append({
                "annotation_uuid": str(annotation.annotation_uuid),
                "name": name,
                "system": system
            })

        # end if

        # Insert structure of the source
        if include_sources:
            export_sources(structure, [annotation.source], include_alerts = include_alerts)
        # end if

        if include_alerts and len(annotation.alerts) > 0:
            structure["annotations"][str(annotation.annotation_uuid)]["alerts"] = [alert.get_uuid() for alert in annotation.alerts]
            export_alerts(structure, annotation.alerts)
        # end if

    # end for

    return structure

def export_ers(structure, ers, include_annotations = True, include_events = True, include_sources = True, include_alerts = False, group = None):
    """Function to insert the explicit reference with DDBB format into a dictionary
    
    :param structure: dictionary where to export the explicit refereces to
    :type structure: dict
    :param ers: list of explicit refereces
    :type ers: list(ExplicitRef)
    :param include_annotations: flag to indicate if the detail of the annotations has to be included
    :type include_annotations: boolean
    :param include_events: flag to indicate if the detail of the events has to be included
    :type include_events: boolean
    :param include_sources: flag to indicate if the detail of the sources associated to the annotations or events has to be included
    :type include_sources: boolean
    :param include_alerts: flag to indicate if the detail of the alerts has to be included
    :type include_alerts: boolean
    :param group: label to group the ERs
    :type group: str

    Output: The function will insert into the structure the
    corresponding explicit references, leaving the structure like the following:
    structure = {
    "explicit_reference_groups": {
    group: [uuid1, uuid2, ..., uuidn]
    "explicit_references": {
    "uuid1": structure of explicit reference associated to uuid1,
    "uuid2": structure of explicit reference associated to uuid2,
    ...,
    "uuidn": structure of explicit reference associated to uuidn
    }
    }
    }

    """

    if type(structure) != dict:
        raise ErrorParsingParameters("The structure parameter has to have type dict. Received structure is: {}, with type: {}".format(structure, type(structure)))
    # end if
    
    if group != None and type(group) != str:
        raise ErrorParsingParameters("The group parameter has to have type str. Received group is: {}, with type: {}".format(group, type(group)))
    # end if

    if type(include_annotations) != bool:
        raise ErrorParsingParameters("The include_annotations parameter has to have type bool. Received group is: {}, with type: {}".format(include_annotations, type(include_annotations)))
    # end if

    if type(include_events) != bool:
        raise ErrorParsingParameters("The include_events parameter has to have type bool. Received group is: {}, with type: {}".format(include_events, type(include_events)))
    # end if

    if type(include_alerts) != bool:
        raise ErrorParsingParameters("The include_alerts parameter has to have type bool. Received group is: {}, with type: {}".format(include_alerts, type(include_alerts)))
    # end if

    if group and "explicit_reference_groups" not in structure:
        structure["explicit_reference_groups"] = {}
    # end if
    if group and group not in structure["explicit_reference_groups"]:
        structure["explicit_reference_groups"][group] = []
    # end if
    if "explicit_references" not in structure:
        structure["explicit_references"] = {}
    # end if

    for er in ers:

        # Insert the reference in the group
        if group:
            structure["explicit_reference_groups"][group].append(str(er.explicit_ref_uuid))
        # end if

        # Insert the structure of the ER
        if str(er.explicit_ref_uuid) not in structure["explicit_references"]:
            structure["explicit_references"][str(er.explicit_ref_uuid)] = er.jsonify(include_annotations, include_events)
        # end if

        if include_annotations:
            export_annotations(structure, er.annotations, include_ers = False, include_events = False, include_sources = include_sources, include_alerts = include_alerts)
        # end if

        if include_events:
            export_events(structure, er.events, include_ers = False, include_annotations = False, include_sources = include_sources, include_alerts = include_alerts)
        # end if

        if include_alerts and len(er.alerts) > 0:
            structure["explicit_references"][str(er.explicit_ref_uuid)]["alerts"] = [alert.get_uuid() for alert in er.alerts]
            export_alerts(structure, er.alerts)
        # end if


    # end for

    return structure

def export_sources(structure, sources, include_alerts = False, group = None):
    """Function to insert the sources with DDBB format into a dictionary
    
    :param structure: dictionary where to export the sources to
    :type structure: dict
    :param sources: list of sources
    :type sources: list(Source)
    :param include_alerts: flag to indicate if the detail of the alerts has to be included
    :type include_alerts: boolean
    :param group: label to group the sources
    :type group: str

    Output: The function will insert into the structure the
    corresponding sources, leaving the structure like the following:
    structure = {
    "source_groups": {
    group: [uuid1, uuid2, ..., uuidn]
    "sources": {
    "uuid1": structure of source associated to uuid1,
    "uuid2": structure of source associated to uuid2,
    ...,
    "uuidn": structure of source associated to uuidn
    }
    }
    }

    """

    if type(structure) != dict:
        raise ErrorParsingParameters("The structure parameter has to have type dict. Received structure is: {}, with type: {}".format(structure, type(structure)))
    # end if
    
    if group != None and type(group) != str:
        raise ErrorParsingParameters("The group parameter has to have type str. Received group is: {}, with type: {}".format(group, type(group)))
    # end if

    if type(include_alerts) != bool:
        raise ErrorParsingParameters("The include_alerts parameter has to have type bool. Received group is: {}, with type: {}".format(include_alerts, type(include_alerts)))
    # end if

    if group and "source_groups" not in structure:
        structure["source_groups"] = {}
    # end if
    if group and group not in structure["source_groups"]:
        structure["source_groups"][group] = []
    # end if
    if "sources" not in structure:
        structure["sources"] = {}
    # end if

    for source in sources:

        # Check if the source was already included in the structure
        if str(source.source_uuid) in structure["sources"]:
            continue
        # end if
            
        # Insert the source reference in the group
        if group:
            structure["source_groups"][group].append(str(source.source_uuid))
        # end if

        # Insert the structure of the source
        structure["sources"][str(source.source_uuid)] = source.jsonify(include_source_statuses = True)

        if include_alerts and len(source.alerts) > 0:
            structure["sources"][str(source.source_uuid)]["alerts"] = [alert.get_uuid() for alert in source.alerts]
            export_alerts(structure, source.alerts)
        # end if

    # end for

    return structure

def build_values_structure(values, structure, position = 0, parent_level = -1, parent_position = 0, structure_for_searching_values = None):
    """
    """
    indexed_values = index_values(values, structure_for_searching_values = structure_for_searching_values)
    
    if position == 0 and parent_level == -1 and parent_position == 0:
        build_object_structure(values, structure, parent_level, parent_position, indexed_values = indexed_values)
    else:
        if (position, parent_level, parent_position) in indexed_values:
            value = indexed_values[(position, parent_level, parent_position)]
            if str(type(value)) in ["<class 'eboa.datamodel.events.EventObject'>", "<class 'eboa.datamodel.annotations.AnnotationObject'>"]:
                object_entity_structure = {"name": value.name,
                                           "type": "object",
                                           "values": []
                }
                structure.append(object_entity_structure)

                build_object_structure(values, object_entity_structure["values"], parent_level + 1, position, indexed_values = indexed_values)
            else:
                insert_value(values, value, structure)
            # end if
        # end if
    # end if

    return

def build_object_structure(values, structure, parent_level, parent_position, indexed_values = None):

    if indexed_values == None:
        indexed_values = index_values(values)
    # end if
    searched_values = indexed_values[(parent_level, parent_position)]
    insert_values(values, searched_values, structure, parent_level, parent_position, indexed_values)

def insert_values(all_values, values_to_insert, structure, parent_level, parent_position, indexed_values):

    start_iterator = 0
    found_start_iterator = False
    while not found_start_iterator:
        if (start_iterator, parent_level, parent_position) in indexed_values:
            found_start_iterator = True
        else:
            start_iterator += 1
        # end if
    # end while
        
    for iterator in range(start_iterator, start_iterator + len(values_to_insert)):
        insert_value(all_values, indexed_values[(iterator, parent_level, parent_position)], structure, indexed_values = indexed_values)
    # end for

def insert_value(all_values, value, structure, indexed_values = None):

    if str(type(value)) in ["<class 'eboa.datamodel.events.EventBoolean'>", "<class 'eboa.datamodel.annotations.AnnotationBoolean'>"]:
        value_type = "boolean"
    elif str(type(value)) in ["<class 'eboa.datamodel.events.EventDouble'>", "<class 'eboa.datamodel.annotations.AnnotationDouble'>"]:
        value_type = "double"
    elif str(type(value)) in ["<class 'eboa.datamodel.events.EventTimestamp'>", "<class 'eboa.datamodel.annotations.AnnotationTimestamp'>"]:
        value_type = "timestamp"
    elif str(type(value)) in ["<class 'eboa.datamodel.events.EventGeometry'>", "<class 'eboa.datamodel.annotations.AnnotationGeometry'>"]:
        value_type = "geometry"
    elif str(type(value)) in ["<class 'eboa.datamodel.events.EventObject'>", "<class 'eboa.datamodel.annotations.AnnotationObject'>"]:
        value_type = "object"
    else:
        value_type = "text"
    # end if

    if value_type != "object":
        value_content = str(value.value)
        if value_type == "geometry":
            value_content = to_shape(value.value).wkt
        # end if
        elif value_type == "timestamp":
            value_content = value.value.isoformat()
        # end if

        structure.append({"name": value.name,
                          "type": value_type,
                          "value": value_content
        })
    else:
        object_entity_structure = {"name": value.name,
                                   "type": "object",
                                   "values": []
        }
        structure.append(object_entity_structure)

        build_object_structure(all_values, object_entity_structure["values"], value.parent_level + 1, value.position, indexed_values = indexed_values)
    # end if

def index_values(values, structure_for_searching_values = None):

    indexed_values = {}
    for value in values:
        indexed_values[(value.position, value.parent_level, value.parent_position)] = value
        if (value.parent_level, value.parent_position) not in indexed_values:
            indexed_values[(value.parent_level, value.parent_position)] = []
        # end if
        indexed_values[(value.parent_level, value.parent_position)].append(value)

        if structure_for_searching_values != None:
            if value.name not in structure_for_searching_values:
                structure_for_searching_values[value.name] = []
            # end if
            insert_value(values, value, structure_for_searching_values[value.name])
        # end if
    # end for

    return indexed_values

def export_alerts(structure, alerts, group = None):
    """Function to insert the alerts with DDBB format into a dictionary
    
    :param structure: dictionary where to export the alerts to
    :type structure: dict
    :param alerts: list of alerts
    :type alerts: list(Alert)
    :param group: label to group the alerts
    :type group: str

    Output: The function will insert into the structure the
    corresponding alerts, leaving the structure like the following:
    structure = {
    "<alerts_type>_groups": {
    group: [uuid1, uuid2, ..., uuidn]
    },
    "<alerts_type>": {
    "uuid1": structure of alert associated to uuid1,
    "uuid2": structure of alert associated to uuid2,
    ...,
    "uuidn": structure of alert associated to uuidn
    }
    }

    """

    if type(structure) != dict:
        raise ErrorParsingParameters("The structure parameter has to have type dict. Received structure is: {}, with type: {}".format(structure, type(structure)))
    # end if
    
    if group != None and type(group) != str:
        raise ErrorParsingParameters("The group parameter has to have type str. Received group is: {}, with type: {}".format(group, type(group)))
    # end if

    for alert in alerts:

        alert_type = alert.__tablename__

        if alert_type not in structure:
            structure[alert_type] = {}
        # end if

        if alert_type + "_groups" not in structure:
            structure[alert_type + "_groups"] = {}
        # end if

        # Insert the alert reference in the group
        if group:
            alert_group = group
        else:
            alert_group = alert.alertDefinition.group.name
        # end if

        if alert_group not in structure[alert_type + "_groups"]:
            structure[alert_type + "_groups"][alert_group] = []
        # end if

        if alert.get_uuid() not in structure[alert_type + "_groups"][alert_group]:
            structure[alert_type + "_groups"][alert_group].append(alert.get_uuid())
        # end if

        # Insert the structure of the alert
        if alert.get_uuid() not in structure[alert_type]:
            structure[alert_type][alert.get_uuid()] = alert.jsonify()
        # end if

    # end for

    return structure
