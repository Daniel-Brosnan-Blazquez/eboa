#!/usr/bin/env python3
"""
Ingestion module for the Station schedule files of Sentinel-2

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

@debug
def _generate_station_schedule_events(xpath_xml, source, engine, query, list_of_events):
    """
    Method to generate the events of the station schedule files

    :param xpath_xml: source of information that was xpath evaluated
    :type xpath_xml: XPathEvaluator
    :param source: information of the source
    :type xpath_xml: dict
    :param list_of_events: list to store the events to be inserted into the eboa
    :type list_of_events: list
    """

    satellite = source["name"][0:3]
    station = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/File_Type")[0].text[6:]
    # schedulings
    schedulings = xpath_xml("/Earth_Explorer_File/Data_Block/SCHEDULE/ACQ")
    for schedule in schedulings:

        data_start = schedule.xpath("Data_Start")[0].text.split("=")[1]
        data_stop = schedule.xpath("Data_Stop")[0].text.split("=")[1]

        acquisition_start = schedule.xpath("Acquisition_Start")[0].text.split("=")[1]
        acquisition_stop = schedule.xpath("Acquisition_Stop")[0].text.split("=")[1]

        playbacks = query.get_linked_events(gauge_names = {"filter": "PLANNED_PLAYBACK_CORRECTION", "op": "like"},
                                            gauge_systems = {"filter": satellite, "op": "like"},
                                            start_filters = [{"date": data_start, "op": ">"}],
                                            stop_filters = [{"date": data_stop, "op": "<"}],
                                            link_names = {"filter": "TIME_CORRECTION", "op": "like"},
                                            return_prime_events = False)

        status = "MATCHED_PLAYBACK"
        links = []
        if len(playbacks["linked_events"]) == 0:
            status = "NO_MATCHED_PLAYBACK"
        else:
            for playback in playbacks["linked_events"]:
                links.append({
                    "link": str(playback.event_uuid),
                    "link_mode": "by_uuid",
                    "name": "STATION_SCHEDULE",
                    "back_ref": "PLANNED_PLAYBACK"
                })
                value = {
                    "name": "station_schedule",
                    "type": "object",
                    "values": [{"name": "station",
                                "type": "text",
                                "value": station}]
                }
                engine.insert_event_values(playback.event_uuid, value)
                planned_playback_correction_uuid = [event_link.event_uuid_link for event_link in playback.eventLinks if event_link.name == "TIME_CORRECTION"][0]
                engine.insert_event_values(planned_playback_correction_uuid, value)
            # end for
        # end if

        # TODO: This could be a place to create an alert as the Station schedule would not cover correctly the planned playbacks

        orbit = schedule.xpath("Orbit_Number")[0].text
        # Station schedule event
        station_schedule_event = {
            "gauge": {
                "insertion_type": "INSERT_and_ERASE",
                "name": "STATION_SCHEDULE",
                "system": station
            },
            "links": links,
            "start": data_start,
            "stop": data_stop,
            "values": [{
                "name": "schedule_information",
                "type": "object",
                "values": [
                    {"name": "orbit",
                     "type": "double",
                     "value": str(orbit)},
                    {"name": "satellite",
                     "type": "text",
                     "value": satellite},
                    {"name": "station",
                     "type": "text",
                     "value": station},
                    {"name": "status",
                     "type": "text",
                     "value": status},
                    {"name": "acquisition_start",
                     "type": "timestamp",
                     "value": acquisition_start},
                    {"name": "acquisition_stop",
                     "type": "timestamp",
                     "value": acquisition_stop},
                    {"name": "delta_start",
                     "type": "double",
                     "value": str((parser.parse(data_start) - parser.parse(acquisition_start)).total_seconds())},
                    {"name": "delta_stop",
                     "type": "double",
                     "value": str((parser.parse(data_stop) - parser.parse(acquisition_stop)).total_seconds())},
                ]
            }]
        }

        # Insert station_schedule_event
        ingestion.insert_event_for_ingestion(station_schedule_event, source, list_of_events)

    # end for

    return

def process_file(file_path, engine, query):
    """Function to process the file and insert its relevant information
    into the DDBB of the eboa
    
    :param file_path: path to the file to be processed
    :type file_path: str
    """
    list_of_events = []
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
    station = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/File_Type")[0].text[6:]

    source = {
        "name": file_name,
        "generation_time": generation_time,
        "validity_start": validity_start,
        "validity_stop": validity_stop
    }

    # Generate station schedule events
    _generate_station_schedule_events(xpath_xml, source, engine, query, list_of_events)

    # Build the xml
    data = {"operations": [{
        "mode": "insert_and_erase",
        "dim_signature": {
            "name": "STATION_SCHEDULE_" + station + "_" + satellite,
            "exec": os.path.basename(__file__),
            "version": version
        },
        "source": source,
        "events": list_of_events
    }]}

    os.remove(new_file_path)

    return data

def insert_data_into_DDBB(data, filename, engine):
    # Treat data
    returned_value = engine.treat_data(data, filename)
    if returned_value == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]:
        logger.error("The file {} could not be validated".format(filename))
    # end if

    return returned_value

def command_process_file(file_path, output_path = None):
    engine = Engine()
    query = Query()
    # Process file
    data = process_file(file_path, engine, query)

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
