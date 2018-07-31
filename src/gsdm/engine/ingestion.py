"""
Ingestion helpers definition

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
# Import python utilities
from dateutil import parser
import copy

def insert_event_for_ingestion(event, source, list_of_events):
    """
    Method to insert an event into a list for ingestion so that some checks are performed before
    :param event: dictionary with the structure of the event to insert
    :type event: dict
    :param source: dictionary with the structure of the source to insert
    :type source: dict
    :param list_of_events: list of events
    :type parent: list
    """
    # Discard events that are not inside the validity period
    if parser.parse(event["start"]) >= parser.parse(source["validity_stop"]) or parser.parse(event["stop"]) <= parser.parse(source["validity_start"]):
        return
    elif parser.parse(event["start"]) < parser.parse(source["validity_start"]) and parser.parse(event["stop"]) <= parser.parse(source["validity_stop"]) and parser.parse(event["stop"]) > parser.parse(source["validity_start"]):
        event["start"] = source["validity_start"]
    elif parser.parse(event["stop"]) > parser.parse(source["validity_stop"]) and parser.parse(event["start"]) < parser.parse(source["validity_stop"]) and parser.parse(event["start"]) >= parser.parse(source["validity_start"]):
        event["stop"] = source["validity_stop"]
    elif parser.parse(event["start"]) < parser.parse(source["validity_start"]) or parser.parse(event["stop"]) > parser.parse(source["validity_stop"]):
        event_before_validity = copy.deepcopy(event)
        event_before_validity["stop"] = validity_start
        event["start"] = validity_stop
        list_of_events.append(event_before_validity)
    # end if
    list_of_events.append(event)
    return
