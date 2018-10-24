"""
Ingestion module for the DFEP schedule files of Sentinel-2

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import os
import argparse
from dateutil import parser
import datetime
import json

# Import xml parser
from lxml import etree

# Import engine
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
def _generate_dfep_schedule_events(xpath_xml, source, list_of_events):
    """
    Method to generate the events of the dfep schedule files

    :param xpath_xml: source of information that was xpath evaluated
    :type xpath_xml: XPathEvaluator
    :param source: information of the source
    :type xpath_xml: dict
    :param list_of_events: list to store the events to be inserted into the eboa
    :type list_of_events: list
    """

    satellite = source["name"][0:3]
    station = xpath_xml("/Earth_Explorer_File/Data_Block/sched/station/@name")[0]
    # schedulings
    schedulings = xpath_xml("/Earth_Explorer_File/Data_Block/sched/station/acq[action = 'ADD']")
    # Get planning events to correct their timings
    query = Query()

    for schedule in schedulings:

        start = schedule.xpath("start")[0].text
        stop = schedule.xpath("stop")[0].text

        playbacks = query.get_linked_events_join(gauge_name_like = {"str": "CORRECTION_PLAYBACK_TYPE%", "op": "like"}, gauge_systems = {"list": [satellite], "op": "in"}, start_filters = [{"date": start, "op": ">"}], stop_filters = [{"date": stop, "op": "<"}], link_names = {"list": ["TIME_CORRECTION"], "op": "in"}, return_prime_events = False)

        status = "MATCHED_PLAYBACK"
        links = []
        if len(playbacks["linked_events"]) == 0:
            status = "NO_MATCHED_PLAYBACK"
        else:
            for playback in playbacks["linked_events"]:
                links.append({
                    "link": str(playback.event_uuid),
                    "link_mode": "by_uuid",
                    "name": "DFEP_SCHEDULE",
                    "back_ref": "true"
                })
            # end for
        # end if

        # TODO: This could be a place to create an alert as the DFEP schedule would not cover correctly the planned playbacks

        orbit = schedule.xpath("@id")[0].split("_")[1]
        # DFEP schedule event
        dfep_schedule_event = {
            "key": satellite + "-" + str(orbit),
            "gauge": {
                "insertion_type": "ERASE_and_REPLACE",
                "name": "DFEP_SCHEDULE",
                "system": station
            },
            "links": links,
            "start": start,
            "stop": stop,
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
                     "value": status}
                ]
            }]
        }

        # Insert dfep_schedule_event
        ingestion.insert_event_for_ingestion(dfep_schedule_event, source, list_of_events)

    # end for

    return

def process_file(file_path):
    """Function to process the file and insert its relevant information
    into the DDBB of the eboa
    
    :param file_path: path to the file to be processed
    :type file_path: str
    """
    list_of_events = []
    file_name = os.path.basename(file_path)

    # Parse file
    parsed_xml = etree.parse(file_path)
    xpath_xml = etree.XPathEvaluator(parsed_xml)

    satellite = file_name[0:3]
    generation_time = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Source/Creation_Date")[0].text.split("=")[1]
    validity_start = xpath_xml("/Earth_Explorer_File/Data_Block/sched/station/acq[action = 'DELETE_RANGE']/start")[0].text
    validity_stop = xpath_xml("/Earth_Explorer_File/Data_Block/sched/station/acq[action = 'DELETE_RANGE']/stop")[0].text
    station = xpath_xml("/Earth_Explorer_File/Data_Block/sched/station/@name")[0]

    source = {
        "name": file_name,
        "generation_time": generation_time,
        "validity_start": validity_start,
        "validity_stop": validity_stop
    }

    # Generate dfep schedule events
    _generate_dfep_schedule_events(xpath_xml, source, list_of_events)

    # Build the xml
    data = {"operations": [{
        "mode": "insert_and_erase",
        "dim_signature": {
            "name": "DFEP_SCHEDULE_" + station + "_" + satellite,
            "exec": os.path.basename(__file__),
            "version": version
        },
        "source": source,
        "events": list_of_events
    }]}

    return data

def insert_data_into_DDBB(data, filename, engine):
    # Treat data
    returned_value = engine.treat_data(data, filename)
    if returned_value == engine.exit_codes["FILE_NOT_VALID"]["status"]:
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
    args_parser = argparse.ArgumentParser(description='Process MPL_FSs.')
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
