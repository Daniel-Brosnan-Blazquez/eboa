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
import ingestions.functions.date_functions as date_functions
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
def _generate_received_data_information(xpath_xml, source, engine, query, list_of_events, list_of_planning_operations):
    """
    Method to generate the events for the idle operation of the satellite
    :param xpath_xml: source of information that was xpath evaluated
    :type xpath_xml: XPathEvaluator
    :param source: information of the source
    :type source: dict
    :param engine: object to access the engine of the EBOA
    :type engine: Engine
    :param query: object to access the query interface of the EBOA
    :type query: Query
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

    # Completeness operations for the completeness analysis of the plan
    completeness_planning_operation = {
        "mode": "insert",
        "dim_signature": {
            "name": "RECEPTION_" + satellite,
            "exec": "planning_" + os.path.basename(__file__),
            "version": version
        },
        "events": []
    }

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

        # ISP validity event
        raw_isp_validity_event_link_ref = "RAW_ISP_VALIDITY_" + vcid_number
        
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
                "links": [
                    {
                        "link": raw_isp_validity_event_link_ref,
                        "link_mode": "by_ref",
                        "name": "RAW_ISP_VALIDITY",
                        "back_ref": "true"
                    }],
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
                "links": [
                    {
                        "link": raw_isp_validity_event_link_ref,
                        "link_mode": "by_ref",
                        "name": "RAW_ISP_VALIDITY",
                        "back_ref": "true"
                    }],
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
                "links": [
                    {
                        "link": raw_isp_validity_event_link_ref,
                        "link_mode": "by_ref",
                        "name": "RAW_ISP_VALIDITY",
                        "back_ref": "true"
                    }],
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

        raw_isp_validity_event = {
            "link_ref": raw_isp_validity_event_link_ref,
            "explicit_reference": session_id,
            "key": session_id + channel,
            "gauge": {
                "insertion_type": "EVENT_KEYS",
                "name": "RAW-ISP-VALIDITY",
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

        # Insert raw_isp_validity_event
        list_of_events.append(raw_isp_validity_event)

        # Obtain the planned imaging events from the corrected events which record type corresponds to the downlink mode and are intersecting the segment of the RAW-ISP-VALIDTY
        record_type = downlink_mode
        corrected_planned_imagings = query.get_events_join(gauge_name_like = {"str": "PLANNED_CUT_IMAGING_%_CORRECTION", "op": "like"}, gauge_systems = {"list": [satellite], "op": "in"}, values_name_type_like = [{"name_like": "record_type", "type": "text", "op": "like"}], value_filters = [{"value": record_type, "type": "text", "op": "=="}], start_filters = [{"date": sensing_stop, "op": "<"}], stop_filters = [{"date": sensing_start, "op": ">"}])
        
        # If there are no found planned imaging events, the MSI will not be linked to the plan and so it will be unexpected
        
        # If there are found planned imaging events, the MSI will be linked to the plan and its segment will be removed from the completeness
        if len(corrected_planned_imagings) > 0:
            corrected_planned_imagings_sorted = sorted(corrected_planned_imagings, key=lambda event: event.start)
            start_period = corrected_planned_imagings_sorted[0].start
            stop_period = corrected_planned_imagings_sorted[-1].stop

            for corrected_planned_imaging in corrected_planned_imagings:
                value = {
                    "name": "completeness_began",
                    "type": "object",
                    "values": []
                }
                exit_status = engine.insert_event_values(corrected_planned_imaging.event_uuid, value)
                if exit_status["inserted"] == True:
                    planned_imaging_uuid = [event_link.event_uuid_link for event_link in corrected_planned_imaging.eventLinks if event_link.name == "PLANNED_EVENT"][0]

                    # Insert the linked COMPLETENESS event for the automatic completeness check
                    if not "source" in completeness_planning_operation:
                        completeness_planning_operation["source"] = {
                            "name": source["name"],
                            "generation_time": str(corrected_planned_imaging.source.generation_time),
                            "validity_start": str(start_period),
                            "validity_stop": str(stop_period)
                        }
                    # end if
                    completeness_planning_operation["events"].append({
                        "gauge": {
                            "insertion_type": "SIMPLE_UPDATE",
                            "name": "PLANNING_COMPLETENESS",
                            "system": satellite
                        },
                        "start": str(corrected_planned_imaging.start),
                        "stop": str(corrected_planned_imaging.stop),
                        "links": [
                            {
                                "link": str(planned_imaging_uuid),
                                "link_mode": "by_uuid",
                                "name": "PLANNED_IMAGING",
                                "back_ref": "COMPLETENESS"
                            }],
                        "values": [{
                            "name": "details",
                            "type": "object",
                            "values": [
                                {"name": "status",
                                 "type": "text",
                                 "value": "MISSING"}
                            ]
                        }]
                    })
                # end if
            # end for

            # Build the ISP-VALIDITY events
            raw_isp_validity_date_segments = date_functions.convert_input_events_to_date_segments([raw_isp_validity_event])
            planning_date_segments = date_functions.convert_eboa_events_to_date_segments(corrected_planned_imagings)
            isp_validity_valid_segments = date_functions.intersect_timelines(raw_isp_validity_date_segments, planning_date_segments)

            # Obtain the unexpected segments
            isp_validity_valid_coverage = {
                "id": "isp_validity_valid_coverage",
                "start": isp_validity_valid_segments[0]["start"],
                "stop": isp_validity_valid_segments[-1]["stop"]
            }

            isp_validity_unexpected_segments = date_functions.difference_timelines(raw_isp_validity_date_segments, [isp_validity_valid_coverage])

            # Insert the valid segments
            for isp_validity_valid_segment in isp_validity_valid_segments:
                corrected_planned_imaging = [event for event in corrected_planned_imagings if event.event_uuid == isp_validity_valid_segment["id2"]][0]
                planned_imaging_uuid = [event_link.event_uuid_link for event_link in corrected_planned_imaging.eventLinks if event_link.name == "PLANNED_EVENT"][0]

                # ISP validity event
                isp_validity_event_link_ref = "ISP_VALIDITY_" + vcid_number + "_" + str(isp_validity_valid_segment["start"])
                isp_validity_event = {
                    "link_ref": isp_validity_event_link_ref,
                    "explicit_reference": session_id,
                    "key": session_id + channel,
                    "gauge": {
                        "insertion_type": "EVENT_KEYS",
                        "name": "ISP-VALIDITY",
                        "system": "EDRS"
                    },
                    "links": [
                        {
                            "link": str(planned_imaging_uuid),
                            "link_mode": "by_uuid",
                            "name": "PLANNED_IMAGING",
                            "back_ref": "ISP-VALIDITY"
                        },{
                            "link": str(isp_validity_valid_segment["id1"]),
                            "link_mode": "by_ref",
                            "name": "RAW_ISP_VALIDITY",
                            "back_ref": "ISP-VALIDITY"
                        }],
                    "start": str(isp_validity_valid_segment["start"]),
                    "stop": str(isp_validity_valid_segment["stop"]),
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
                             "value": downlink_mode}
                        ]
                    }]
                }
                # Insert isp_validity_event
                list_of_events.append(isp_validity_event)

                isp_validity_completeness_event = {
                    "explicit_reference": session_id,
                    "key": session_id + channel,
                    "gauge": {
                        "insertion_type": "ERASE_and_REPLACE",
                        "name": "PLANNING_COMPLETENESS",
                        "system": satellite
                    },
                    "links": [
                        {
                            "link": isp_validity_event_link_ref,
                            "link_mode": "by_ref",
                            "name": "ISP-VALIDITY",
                            "back_ref": "COMPLETENESS"
                        }],
                    "start": str(isp_validity_valid_segment["start"]),
                    "stop": str(isp_validity_valid_segment["stop"]),
                    "values": [{
                        "name": "details",
                        "type": "object",
                        "values": [
                            {"name": "status",
                             "type": "text",
                             "value": "RECEIVED"}
                        ]
                    }]
                }

                # Insert isp_validity_event
                list_of_events.append(isp_validity_completeness_event)
            # end for
        # end if
    # end for

    # Insert completeness operation for the completeness analysis of the plan
    if "source" in completeness_planning_operation:
        list_of_planning_operations.append(completeness_planning_operation)
    # end if

    return status

@debug
def _generate_pass_information(xpath_xml, source, engine, query, list_of_annotations, list_of_explicit_references, status):
    """
    Method to generate the events for the idle operation of the satellite
    :param xpath_xml: source of information that was xpath evaluated
    :type xpath_xml: XPathEvaluator
    :param source: information of the source
    :type source: dict
    :param engine: object to access the engine of the EBOA
    :type engine: Engine
    :param query: object to access the query interface of the EBOA
    :type query: Query
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

def process_file(file_path, engine, query):
    """
    Function to process the file and insert its relevant information
    into the DDBB of the eboa
    
    :param file_path: path to the file to be processed
    :type file_path: str
    :param engine: object to access the engine of the EBOA
    :type engine: Engine
    :param query: object to access the query interface of the EBOA
    :type query: Query
    """
    list_of_events = []
    list_of_planning_operations = []
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
    # Set the validity start to be the first sensing received to avoid error ingesting
    sensing_starts = xpath_xml("/Earth_Explorer_File/Data_Block/*[contains(name(),'data_C')]/Status/ISP_Status/Status/SensStartTime")
    sensing_starts_in_iso_8601 = [functions.three_letter_to_iso_8601(sensing_start.text) for sensing_start in sensing_starts]

    # Sort list
    sensing_starts_in_iso_8601.sort()
    validity_start = sensing_starts_in_iso_8601[0]
    validity_stop = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Stop")[0].text.split("=")[1]

    source = {
        "name": file_name,
        "generation_time": generation_time,
        "validity_start": validity_start,
        "validity_stop": validity_stop
    }

    # Extract the information of the received data
    status = _generate_received_data_information(xpath_xml, source, engine, query, list_of_events, list_of_planning_operations)

    # Extract the information of the pass
    _generate_pass_information(xpath_xml, source, engine, query, list_of_annotations, list_of_explicit_references, status)

    

    # Build the xml
    data = {}
    data["operations"] = list_of_planning_operations
    data["operations"].append({
        "mode": "insert",
        "dim_signature": {
            "name": "RECEPTION_" + satellite,
            "exec": os.path.basename(__file__),
            "version": version
        },
        "source": source,
        "explicit_references": list_of_explicit_references,
        "events": list_of_events,
        "annotations": list_of_annotations
    })

    return data

def insert_data_into_DDBB(data, filename, engine):
    # Treat data
    returned_value = engine.treat_data(data, filename)
    if returned_value == engine.exit_codes["FILE_NOT_VALID"]["status"]:
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
