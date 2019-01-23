"""
Ingestion module for the REP_PASS_E files of Sentinel-2

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
from eboa.engine.engine import Engine

# Import ingestion helpers
import eboa.engine.ingestion as ingestion
import ingestions.s2.functions as functions
import ingestions.s2.xpath_functions as xpath_functions

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
def _generate_received_data_information(xpath_xml, source, list_of_events):
    """
    Method to generate the events for the idle operation of the satellite
    :param xpath_xml: source of information that was xpath evaluated
    :type xpath_xml: XPathEvaluator
    :param source: information of the source
    :type xpath_xml: dict
    :param list_of_events: list to store the events to be inserted into the eboa
    :type list_of_events: list

    :return: status -> COMPLETE: there are no ISP gaps; INCOMPLETE: there are ISP gaps
    :rtype: str

    """

    status = "COMPLETE"

    # Obtain the satellite
    satellite = source["name"][0:3]

    # Obtain link session ID
    session_id = xpath_xml("/Earth_Explorer_File/Data_Block/input_files/input_file[1]")[0].text[7:31]

    # Obtain channel
    channel = xpath_xml("/Earth_Explorer_File/Data_Block/child::*[contains(name(),'data_')]")[0].tag[6:7]

    # Obtain downlink orbit
    downlink_orbit = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Variable_Header/Downlink_Orbit")[0].text

    vcids = xpath_xml("/Earth_Explorer_File/Data_Block/*[contains(name(),'data_C')]/Status[@VCID = 4 or @VCID = 5 or @VCID = 6 or @VCID = 20 or @VCID = 21 or @VCID = 22]")
    for vcid in vcids:
        vcid_number = vcid.get("VCID")
        downlink_mode = functions.get_vcid_mode(vcid_number)
        # Obtain the sensing segment received (EFEP reports only give information about the start date of the first and last scenes)
        sensing_starts = vcid.xpath("ISP_Status/Status/SensStartTime")
        sensing_starts_in_iso_8601 = [functions.three_letter_to_iso_8601(sensing_start.text) for sensing_start in sensing_starts]
        
        # Sort list
        sensing_starts_in_iso_8601.sort()
        sensing_start = sensing_starts_in_iso_8601[0]

        sensing_stops = vcid.xpath("ISP_Status/Status/SensStopTime")
        sensing_stops_in_iso_8601 = [functions.three_letter_to_iso_8601(sensing_stop.text) for sensing_stop in sensing_stops]
        
        # Sort list
        sensing_stops_in_iso_8601.sort()
        sensing_stop = sensing_stops_in_iso_8601[-1]

        # APID configuration
        apid_conf = functions.get_vcid_apid_configuration(vcid_number)
        
        # Obtain complete missing APIDs
        complete_missing_apids = vcid.xpath("ISP_Status/Status[number(NumPackets) = 0 and number(@APID) >= number($min_apid) and number(@APID) <= number($max_apid)]", min_apid = apid_conf["min_apid"], max_apid = apid_conf["max_apid"])
        for apid in complete_missing_apids:
            status = "INCOMPLETE"
            apid_number = apid.get("APID")
            band_detector = functions.get_band_detector(apid_number)
            isp_gap_event = {
                "explicit_reference": session_id,
                "key": session_id + channel,
                "gauge": {
                    "insertion_type": "EVENT_KEYS",
                    "name": "ISP-GAP",
                    "system": "EDRS"
                },
                "start": sensing_start,
                "stop": sensing_stop,
                "values": [{
                    "name": "values",
                    "type": "object",
                    "values": [
                        {"name": "impact",
                         "type": "text",
                         "value": "COMPLETE"},
                        {"name": "band",
                         "type": "text",
                         "value": band_detector["band"]},
                        {"name": "detector",
                         "type": "double",
                         "value": band_detector["detector"]},
                        {"name": "downlink_orbit",
                         "type": "double",
                         "value": downlink_orbit},
                        {"name": "satellite",
                         "type": "text",
                         "value": satellite},
                        {"name": "reception_station",
                         "type": "text",
                         "value": "EPAE"},
                        {"name": "vcid",
                         "type": "double",
                         "value": vcid_number},
                        {"name": "downlink_mode",
                         "type": "text",
                         "value": downlink_mode},
                        {"name": "apid",
                         "type": "double",
                         "value": apid_number}
                   ]
                }]
            }

            # Insert isp_gap_event
            list_of_events.append(isp_gap_event)

        # end for

        # Obtain ISP gaps at the beggining
        isp_missing_at_begin_apids = vcid.xpath("ISP_Status/Status[number(NumPackets) > 0 and number(@APID) >= number($min_apid) and number(@APID) <= number($max_apid) and not(three_letter_to_iso_8601(string(SensStartTime)) = $sensing_start)]", min_apid = apid_conf["min_apid"], max_apid = apid_conf["max_apid"], sensing_start = sensing_start)
        for apid in isp_missing_at_begin_apids:
            status = "INCOMPLETE"
            apid_number = apid.get("APID")
            band_detector = functions.get_band_detector(apid_number)

            isp_gap_event = {
                "explicit_reference": session_id,
                "key": session_id + channel,
                "gauge": {
                    "insertion_type": "EVENT_KEYS",
                    "name": "ISP-GAP",
                    "system": "EDRS"
                },
                "start": sensing_start,
                "stop": functions.three_letter_to_iso_8601(apid.xpath("SensStartTime")[0].text),
                "values": [{
                    "name": "values",
                    "type": "object",
                    "values": [
                        {"name": "impact",
                         "type": "text",
                         "value": "AT_THE_BEGINNING"},
                        {"name": "band",
                         "type": "text",
                         "value": band_detector["band"]},
                        {"name": "detector",
                         "type": "double",
                         "value": band_detector["detector"]},
                        {"name": "downlink_orbit",
                         "type": "double",
                         "value": downlink_orbit},
                        {"name": "satellite",
                         "type": "text",
                         "value": satellite},
                        {"name": "reception_station",
                         "type": "text",
                         "value": "EPAE"},
                        {"name": "vcid",
                         "type": "double",
                         "value": vcid_number},
                        {"name": "downlink_mode",
                         "type": "text",
                         "value": downlink_mode},
                        {"name": "apid",
                         "type": "double",
                         "value": apid_number}
                   ]
                }]
            }

            # Insert isp_gap_event
            list_of_events.append(isp_gap_event)

        # end for

        # Obtain ISP gaps at the end
        isp_missing_at_end_apids = vcid.xpath("ISP_Status/Status[number(NumPackets) > 0 and number(@APID) >= number($min_apid) and number(@APID) <= number($max_apid) and not(three_letter_to_iso_8601(string(SensStopTime)) = $sensing_stop)]", min_apid = apid_conf["min_apid"], max_apid = apid_conf["max_apid"], sensing_stop = sensing_stop)
        for apid in isp_missing_at_end_apids:
            status = "INCOMPLETE"
            apid_number = apid.get("APID")
            band_detector = functions.get_band_detector(apid_number)

            isp_gap_event = {
                "explicit_reference": session_id,
                "key": session_id + channel,
                "gauge": {
                    "insertion_type": "EVENT_KEYS",
                    "name": "ISP-GAP",
                    "system": "EDRS"
                },
                "start": functions.three_letter_to_iso_8601(apid.xpath("SensStopTime")[0].text),
                "stop": sensing_stop,
                "values": [{
                    "name": "values",
                    "type": "object",
                    "values": [
                        {"name": "impact",
                         "type": "text",
                         "value": "AT_THE_END"},
                        {"name": "band",
                         "type": "text",
                         "value": band_detector["band"]},
                        {"name": "detector",
                         "type": "double",
                         "value": band_detector["detector"]},
                        {"name": "downlink_orbit",
                         "type": "double",
                         "value": downlink_orbit},
                        {"name": "satellite",
                         "type": "text",
                         "value": satellite},
                        {"name": "reception_station",
                         "type": "text",
                         "value": "EPAE"},
                        {"name": "vcid",
                         "type": "double",
                         "value": vcid_number},
                        {"name": "downlink_mode",
                         "type": "text",
                         "value": downlink_mode},
                        {"name": "apid",
                         "type": "double",
                         "value": apid_number}
                   ]
                }]
            }

            # Insert isp_gap_event
            list_of_events.append(isp_gap_event)

        # end for

        isp_validity_event = {
            "explicit_reference": session_id,
            "key": session_id + channel,
            "gauge": {
                "insertion_type": "EVENT_KEYS",
                "name": "ISP-VALIDITY",
                "system": "EDRS"
            },
            "start": sensing_start,
            "stop": sensing_stop,
            "values": [{
                "name": "values",
                "type": "object",
                "values": [
                    {"name": "status",
                     "type": "text",
                     "value": status},
                    {"name": "downlink_orbit",
                     "type": "double",
                     "value": downlink_orbit},
                    {"name": "satellite",
                     "type": "text",
                     "value": satellite},
                    {"name": "reception_station",
                     "type": "text",
                     "value": "EPAE"},
                    {"name": "vcid",
                     "type": "double",
                     "value": vcid_number},
                    {"name": "downlink_mode",
                     "type": "text",
                     "value": downlink_mode},
                    {"name": "num_packets",
                     "type": "double",
                     "value": vcid.xpath("ISP_Status/Summary/NumPackets")[0].text},
                    {"name": "num_frames",
                     "type": "double",
                     "value": vcid.xpath("NumFrames")[0].text}
               ]
            }]
        }

        # Insert isp_gap_event
        list_of_events.append(isp_validity_event)

    # end for
    
    return status

@debug
def _generate_pass_information(xpath_xml, source, list_of_annotations, list_of_explicit_references, status):
    """
    Method to generate the events for the idle operation of the satellite
    :param xpath_xml: source of information that was xpath evaluated
    :type xpath_xml: XPathEvaluator
    :param source: information of the source
    :type xpath_xml: dict
    :param list_of_annotations: list to store the annotations to be inserted into the eboa
    :type list_of_annotations: list
    :param list_of_explicit_references: list to store the annotations to be inserted into the eboa
    :type list_of_explicit_references: list
    """

    # Obtain the satellite
    satellite = source["name"][0:3]

    # Obtain link session ID
    session_id = xpath_xml("/Earth_Explorer_File/Data_Block/input_files/input_file[1]")[0].text[7:31]

    # Obtain channel
    channel = xpath_xml("/Earth_Explorer_File/Data_Block/child::*[contains(name(),'data_')]")[0].tag[6:7]

    # Obtain downlink orbit
    downlink_orbit = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Variable_Header/Downlink_Orbit")[0].text

    # Associate the explicit reference to the group EDRS_SESSION_IDs
    explicit_reference = {
        "name": session_id,
        "group": "EDRS_LINK_SESSION_IDs"
    }

    list_of_explicit_references.append(explicit_reference)

    # Link session
    session_id_annotation = {
        "explicit_reference": session_id,
        "annotation_cnf": {
            "name": "LINK-SESSION-ID-CH" + channel,
            "system": "EDRS"
        },
        "values": [{
            "name": "link_session_information",
            "type": "object",
            "values": [
                {"name": "session_id",
                 "type": "text",
                 "value": session_id},
                {"name": "satellite",
                 "type": "text",
                 "value": satellite},
                {"name": "reception_station",
                 "type": "text",
                 "value": "EPAE"}
            ]
        }]
    }

    list_of_annotations.append(session_id_annotation)

    # Downlink orbit
    downlink_orbit_annotation = {
        "explicit_reference": session_id,
        "annotation_cnf": {
            "name": "DOWNLINK-ORBIT-CH" + channel,
            "system": "EDRS"
        },
        "values": [{
            "name": "downlink_orbit_information",
            "type": "object",
            "values": [
                {"name": "downlink_orbit",
                 "type": "double",
                 "value": downlink_orbit},
                {"name": "satellite",
                 "type": "text",
                 "value": satellite},
                {"name": "reception_station",
                 "type": "text",
                 "value": "EPAE"}
            ]
        }]
    }

    list_of_annotations.append(downlink_orbit_annotation)

    return

def process_file(file_path):
    """
    Function to process the file and insert its relevant information
    into the DDBB of the eboa
    
    :param file_path: path to the file to be processed
    :type file_path: str
    """
    list_of_events = []
    list_of_annotations = []
    list_of_explicit_references = []
    file_name = os.path.basename(file_path)

    # Remove namespaces
    (_, new_file_path) = new_file = mkstemp()
    ingestion.remove_namespaces(file_path, new_file_path)

    # Parse file
    parsed_xml = etree.parse(new_file_path)

    # Register functions for using in XPATH
    ns = etree.FunctionNamespace(None)
    ns["three_letter_to_iso_8601"] = xpath_functions.three_letter_to_iso_8601
    xpath_xml = etree.XPathEvaluator(parsed_xml)

    satellite = file_name[0:3]
    generation_time = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Source/Creation_Date")[0].text.split("=")[1]
    validity_start = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Start")[0].text.split("=")[1]
    validity_stop = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Stop")[0].text.split("=")[1]

    source = {
        "name": file_name,
        "generation_time": generation_time,
        "validity_start": validity_start,
        "validity_stop": validity_stop
    }

    # Extract the information of the received data
    status = _generate_received_data_information(xpath_xml, source, list_of_events)

    # Extract the information of the pass
    _generate_pass_information(xpath_xml, source, list_of_annotations, list_of_explicit_references, status)

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
        "events": list_of_events,
        "annotations": list_of_annotations
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
