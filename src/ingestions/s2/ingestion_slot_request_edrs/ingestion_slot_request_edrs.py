"""
Ingestion module for the SRA (Slot request for unit A) files of Sentinel-2

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import os
import argparse
from dateutil import parser
import datetime
import json
from tempfile import mkstemp

# Import xml parser
from lxml import etree

# Import engine
import eboa.engine.engine as eboa_engine
from eboa.engine.engine import Engine

# Import query
from eboa.engine.query import Query

# Import ingestion helpers
import eboa.engine.ingestion as ingestion

# Import query
from eboa.engine.query import Query

# Import debugging
from eboa.debugging import debug

# Import logging
from eboa.logging import Log

logging_module = Log()
logger = logging_module.logger

version = "1.0"

def process_file(file_path):
    """
    Function to process the file and insert its relevant information
    into the DDBB of the eboa
    
    :param file_path: path to the file to be processed
    :type file_path: str
    """
    list_of_events = []
    list_of_explicit_references = []
    file_name = os.path.basename(file_path)

    # Remove namespaces
    (_, new_file_path) = new_file = mkstemp()
    ingestion.remove_namespaces(file_path, new_file_path)

    # Parse file
    parsed_xml = etree.parse(new_file_path)
    xpath_xml = etree.XPathEvaluator(parsed_xml)

    satellite = file_name[0:3]
    generation_time = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Source/Creation_Date")[0].text.split("=")[1]
    validity_start = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Start")[0].text.split("=")[1]
    validity_stop = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Stop")[0].text.split("=")[1]
    edrs = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Mission")[0].text

    source = {
        "name": file_name,
        "generation_time": generation_time,
        "validity_start": validity_start,
        "validity_stop": validity_stop
    }

    query = Query()

    # Extract the slots
    slots = xpath_xml("/Earth_Explorer_File/Data_Block/List_of_Sessions/Session")
    for slot in slots:
        start = slot.xpath("Start_Time")[0].text.split("=")[1]
        stop = slot.xpath("Stop_Time")[0].text.split("=")[1]
        session_id = slot.xpath("Session_ID")[0].text
        sentinel = slot.xpath("LEO_Satellite_ID")[0].text
        orbit_node = slot.xpath("LEO_Absolute_Orbit")
        orbit = -1
        if orbit_node:
            orbit = orbit_node[0].text
        # end if

        # Get the associated planned playback in the NPPF
        playbacks = query.get_linked_events_join(gauge_names = {"filter": "PLANNED_PLAYBACK_CORRECTION", "op": "like"},
                                                 gauge_systems = {"filter": sentinel, "op": "like"},
                                                 start_filters = [{"date": start, "op": ">"}],
                                                 stop_filters = [{"date": stop, "op": "<"}],
                                                 link_names = {"filter": ["TIME_CORRECTION"], "op": "in"},
                                                 return_prime_events = False)

        status = "NO_MATCHED_PLAYBACK"
        links = []
        if len(playbacks["linked_events"]) > 0:
            for playback in playbacks["linked_events"]:
                # Get the planned playback mean
                planned_playback_mean_uuids = [link.event_uuid_link for link in playback.eventLinks if link.name == "PLANNED_PLAYBACK_TYPE"]
                if len(planned_playback_mean_uuids) > 0:
                    planned_playback_mean_uuid = planned_playback_mean_uuids[0]
                
                    planned_playback_mean = query.get_events(event_uuids = {"op": "in", "filter": [planned_playback_mean_uuid]})[0]

                    if planned_playback_mean.gauge.name == "PLANNED_PLAYBACK_MEAN_OCP":
                        status = "MATCHED_PLAYBACK"
                        links.append({
                            "link": str(playback.event_uuid),
                            "link_mode": "by_uuid",
                            "name": "SLOT_REQUEST_EDRS",
                            "back_ref": "PLANNED_PLAYBACK"
                        })
                    # end if
                # end if
            # end for
        # end if

        # Associate the explicit reference to the group EDRS_SESSION_IDs
        explicit_reference = {
            "name": session_id,
            "group": "EDRS_SESSION_IDs"
        }
        list_of_explicit_references.append(explicit_reference)
        slot_event = {
            "explicit_reference": session_id,
            "gauge": {
                "insertion_type": "INSERT_and_ERASE",
                "name": "SLOT_REQUEST_EDRS",
                "system": sentinel
            },
            "links": links,
            "start": start,
            "stop": stop,
            "values": [{
                "name": "slot_request_information",
                "type": "object",
                "values": [
                    {"name": "session_id",
                     "type": "text",
                     "value": session_id},
                    {"name": "edrs_unit",
                     "type": "text",
                     "value": edrs},
                    {"name": "orbit",
                     "type": "double",
                     "value": str(orbit)},
                    {"name": "satellite",
                     "type": "text",
                     "value": sentinel},
                    {"name": "status",
                     "type": "text",
                     "value": status}
                ]
            }]
        }

        # Insert slot_event
        ingestion.insert_event_for_ingestion(slot_event, source, list_of_events)
    # end for

    # Build the xml
    data = {"operations": [{
        "mode": "insert_and_erase",
        "dim_signature": {
            "name": "SLOT_REQUEST_EDRS",
            "exec": os.path.basename(__file__),
            "version": version
        },
        "source": source,
        "explicit_references": list_of_explicit_references,
        "events": list_of_events
    }]}

    return data

def insert_data_into_DDBB(data, filename, engine):
    # Treat data
    returned_value = engine.treat_data(data, filename)
    if returned_value == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]:
        logger.error("The file {} could not be validated".format(filename))
    # end if

    return returned_value

def command_process_file(file_path, output_path = None):
    # Process file
    data = process_file(file_path)

    engine = Engine()
    # Validate data
    filename = os.path.basename(file_path)

    returned_value = 0
    # Treat data
    if output_path == None:
        returned_value = insert_data_into_DDBB(data, filename, engine)
    else:
        with open(output_path, "w") as write_file:
            json.dump(data, write_file, indent=4)
    # end if
    
    return returned_value

if __name__ == "__main__":
    """
    This will be not here. After all the needs are identified, the ingestions a priori will have only the method command_process_file and this method will be called from outside
    """
    args_parser = argparse.ArgumentParser(description='Process MPL_SPs.')
    args_parser.add_argument('-f', dest='file_path', type=str, nargs=1,
                             help='path to the file to process', required=True)
    args_parser.add_argument('-o', dest='output_path', type=str, nargs=1,
                             help='path to the output file', required=False)
    args = args_parser.parse_args()
    file_path = args.file_path[0]
    output_path = None
    if args.output_path != None:
        output_path = args.output_path[0]
    # end if

    # Before calling to the processor there should be a validation of
    # the file following a schema. Schema not available for ORBPREs

    returned_value = command_process_file(file_path, output_path)
    
    logger.info("The ingestion has been performed and the exit status is {}".format(returned_value))
