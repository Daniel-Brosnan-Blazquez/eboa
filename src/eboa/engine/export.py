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
def export_events(structure, events, include_ers = True, include_annotations = True, group = None):
    """Function to insert the events with DDBB format into a dictionary
    
    :param structure: dictionary where to export the events to
    :type structure: dict
    :param events: list of events
    :type events: list(Event)
    :param include_ers: flag to indicate if the detail of the explicit references has to be included
    :type include_ers: boolean
    :param include_annotations: flag to indicate if the detail of the annotations has to be included
    :type include_annotations: boolean
    :param group: label to group the events
    :type group: str

    Output: The function will insert into the structure the
    corresponding events, leaving the structure like the following:
    structure = {
    "event_groups": {
    group: [event_uuid1, event_uuid2, ..., event_uuidn]
    "events": {
    "event_uuid1": structure of event with event_uuid1,
    "event_uuid2": structure of event with event_uuid1,
    ...,
    "event_uuidn": structure of event with event_uuidn
    }
    }
    }

    """

    if type(structure) != dict:
        raise ErrorParsingParameters("The structure parameter has to have type dict. Received structure is: {}".format(structure))
    # end if
    
    if group != None and type(group) != str:
        raise ErrorParsingParameters("The group parameter has to have type str. Received group is: {}".format(group))
    # end if

    if type(include_ers) != bool:
        raise ErrorParsingParameters("The include_ers parameter has to have type bool. Received group is: {}".format(include_ers))
    # end if

    if type(include_annotations) != bool:
        raise ErrorParsingParameters("The include_annotations parameter has to have type bool. Received group is: {}".format(include_annotations))
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
    
    for event in events:

        # Insert the event reference in the group
        if group:
            structure["event_groups"][group].append(str(event.event_uuid))
        # end if

        # Insert the structure of the event
        if event.event_uuid not in structure["events"]:
            structure["events"][str(event.event_uuid)] = event.jsonify()
        # end if

        # Insert structure of the explicit reference
        if include_ers and event.explicitRef != None:
            export_ers(structure, [event.explicitRef], include_annotations = include_annotations)
        # end if

    # end for

    return structure

def export_annotations(structure, annotations, include_ers = True, include_annotations = True, group = None):
    """Function to insert the annotations with DDBB format into a dictionary
    
    :param structure: dictionary where to export the annotations to
    :type structure: dict
    :param annotations: list of annotations
    :type annotations: list(Annotation)
    :param include_ers: flag to indicate if the detail of the explicit references has to be included
    :type include_ers: boolean
    :param include_annotations: flag to indicate if the detail of the annotations has to be included
    :type include_annotations: boolean
    :param group: label to group the annotations
    :type group: str

    Output: The function will insert into the structure the
    corresponding annotations, leaving the structure like the following:
    structure = {
    "annotation_groups": {
    group: [annotation_uuid1, annotation_uuid2, ..., annotation_uuidn]
    "annotations": {
    "annotation_uuid1": structure of annotation with annotation_uuid1,
    "annotation_uuid2": structure of annotation with annotation_uuid1,
    ...,
    "annotation_uuidn": structure of annotation with annotation_uuidn
    }
    }
    }

    """

    if type(structure) != dict:
        raise ErrorParsingParameters("The structure parameter has to have type dict. Received structure is: {}".format(structure))
    # end if
    
    if group != None and type(group) != str:
        raise ErrorParsingParameters("The group parameter has to have type str. Received group is: {}".format(group))
    # end if

    if type(include_ers) != bool:
        raise ErrorParsingParameters("The include_ers parameter has to have type bool. Received group is: {}".format(include_ers))
    # end if

    if type(include_annotations) != bool:
        raise ErrorParsingParameters("The include_annotations parameter has to have type bool. Received group is: {}".format(include_annotations))
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

        # Insert the annotation reference in the group
        if group:
            structure["annotation_groups"][group].append(str(annotation.annotation_uuid))
        # end if

        # Insert the structure of the annotation
        if annotation.annotation_uuid not in structure["annotations"]:
            structure["annotations"][str(annotation.annotation_uuid)] = annotation.jsonify()
        # end if

        # Insert structure of the explicit reference
        if include_ers:
            export_ers(structure, [annotation.explicitRef], include_annotations = include_annotations)
        # end if

    # end for

    return structure

def export_ers(structure, ers, include_annotations = True, group = None):
    """Function to insert the explicit reference with DDBB format into a dictionary
    
    :param structure: dictionary where to export the explicit refereces to
    :type structure: dict
    :param ers: list of explicit refereces
    :type ers: list(ExplicitRef)
    :param include_annotations: flag to indicate if the detail of the annotations has to be included
    :type include_annotations: boolean
    :param group: label to group the events
    :type group: str

    Output: The function will insert into the structure the
    corresponding explicit references, leaving the structure like the following:
    structure = {
    "explicit_reference_groups": {
    group: [explicit_reference_uuid1, explicit_reference_uuid2, ..., explicit_reference_uuidn]
    "explicit_references": {
    "explicit_reference_uuid1": structure of explicit reference with explicit_reference_uuid1,
    "explicit_reference_uuid2": structure of explicit reference with explicit_reference_uuid2,
    ...,
    "explicit_reference_uuidn": structure of explicit reference with explicit_reference_uuidn
    }
    }
    }

    """

    if type(structure) != dict:
        raise ErrorParsingParameters("The structure parameter has to have type dict. Received structure is: {}".format(structure))
    # end if
    
    if group != None and type(group) != str:
        raise ErrorParsingParameters("The group parameter has to have type str. Received group is: {}".format(group))
    # end if

    if type(include_annotations) != bool:
        raise ErrorParsingParameters("The include_annotations parameter has to have type bool. Received group is: {}".format(include_annotations))
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
            structure["explicit_references"][group].append(str(er.explicit_ref_uuid))
        # end if

        # Insert the structure of the event
        if er.explicit_ref_uuid not in structure["explicit_references"]:
            structure["explicit_references"][str(er.explicit_ref_uuid)] = er.jsonify(include_annotations)
        # end if

    # end for

    return structure

def build_values_structure(values, structure, position = 0, parent_level = -1, parent_position = 0):
    """
    """
    if position == 0 and parent_level == -1 and parent_position == 0:
        build_object_structure(values, structure, parent_level, parent_position)
    else:
        value = [value for value in values if value.parent_level == parent_level and value.parent_position == parent_position and value.position == position]
        if len(value) > 0:
            value = value[0]
            if str(type(value)) in ["<class 'eboa.datamodel.events.EventObject'>", "<class 'eboa.datamodel.annotations.AnnotationObject'>"]:
                object_entity_structure = {"name": value.name,
                                           "type": "object",
                                           "values": []
                }
                structure.append(object_entity_structure)

                build_object_structure(values, object_entity_structure["values"], parent_level + 1, position)
            else:
                insert_values(values, [value], structure)
            # end if
        # end if
    # end if

    return

def build_object_structure(values, structure, parent_level, parent_position):

    searched_values = [value for value in values if value.parent_level == parent_level and value.parent_position == parent_position]
    insert_values(values, searched_values, structure)

def insert_values(all_values, values_to_insert, structure):

    sorted_values = sorted(values_to_insert, key=lambda x: x.position)
    for value in sorted_values:
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

            build_object_structure(all_values, object_entity_structure["values"], value.parent_level + 1, value.position)
        # end if
    # end for

