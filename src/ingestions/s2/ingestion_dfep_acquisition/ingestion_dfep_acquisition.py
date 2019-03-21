"""
Ingestion module for the REP_PASS_E files of Sentinel-2

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
import pdb
# Import python utilities
import os
import argparse
import datetime
import json
from tempfile import mkstemp
from dateutil import parser

# Import xml parser
from lxml import etree

# Import engine
import eboa.engine.engine as eboa_engine
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
def _generate_acquisition_data_information(xpath_xml, source, engine, query, list_of_events, list_of_planning_operations):
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

    :return: status -> COMPLETE: there were no gaps during the acquisition; INCOMPLETE: there were gaps during the acquisition
    :rtype: str

    """
    
    general_status = "COMPLETE"

    # Obtain the satellite
    satellite = source["name"][0:3]

    # Obtain the station
    station = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Source/System")[0].text

    # Obtain link session ID
    session_id = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/File_Type")[0].text + "_" + xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Start")[0].text.split("UTC=",1)[1]

    # Obtain downlink orbit
    downlink_orbit = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Variable_Header/Downlink_Orbit")[0].text

    # Completeness operation for the playback completeness analysis of the plan
    playback_planning_completeness_operation = {
        "mode": "insert",
        "dim_signature": {
            "name": "RECEPTION_" + satellite,
            "exec": "playback_planning_completeness_" + os.path.basename(__file__),
            "version": version
        },
        "events": []
    }

    playback_planning_completeness_generation_times = []

    vcids = xpath_xml("/Earth_Explorer_File/Data_Block/*[contains(name(),'data_C')]/Status[NumFrames > 0 and (@VCID = 2 or @VCID = 3 or @VCID = 4 or @VCID = 5 or @VCID = 6 or @VCID = 20 or @VCID = 21 or @VCID = 22)]")
    for vcid in vcids:
        # Obtain channel
        channel = vcid.xpath("..")[0].tag[6:7]
    
        vcid_number = vcid.get("VCID")
        downlink_mode = functions.get_vcid_mode(vcid_number)
        # Acquisition segment
        acquisition_start = functions.three_letter_to_iso_8601(vcid.xpath("AcqStartTime")[0].text)
        acquisition_stop = functions.three_letter_to_iso_8601(vcid.xpath("AcqStopTime")[0].text)

        # Reference to the playback validity event
        playback_validity_event_link_ref = "PLAYBACK_" + downlink_mode + "_VALIDITY_" + vcid_number

        status = "COMPLETE"

        gaps = vcid.xpath("Gaps/Gap")
        for gap in gaps:
            general_status = "INCOMPLETE"
            status = "INCOMPLETE"
            start = functions.three_letter_to_iso_8601(gap.xpath("PreAcqTime")[0].text)
            stop = functions.three_letter_to_iso_8601(gap.xpath("PostAcqTime")[0].text)
            estimated_lost = gap.xpath("EstimatedLost")[0].text
            pre_counter = gap.xpath("PreCounter")[0].text
            post_counter = gap.xpath("PostCounter")[0].text
            gap_event = {
                "explicit_reference": session_id,
                "key": session_id + "_CHANNEL_" + channel,
                "gauge": {
                    "insertion_type": "EVENT_KEYS",
                    "name": "PLAYBACK_" + downlink_mode + "_GAP",
                    "system": station
                },
                "start": start,
                "stop": stop,
                "links": [
                    {
                        "link": playback_validity_event_link_ref,
                        "link_mode": "by_ref",
                        "name": "PLAYBACK_GAP",
                        "back_ref": "PLAYBACK_VALIDITY"
                    }],
                "values": [{
                    "name": "values",
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
                         "value": station},
                        {"name": "channel",
                         "type": "double",
                         "value": channel},
                        {"name": "vcid",
                         "type": "double",
                         "value": vcid_number},
                        {"name": "downlink_mode",
                         "type": "text",
                         "value": downlink_mode},
                        {"name": "estimated_lost",
                         "type": "double",
                         "value": estimated_lost},
                        {"name": "pre_counter",
                         "type": "double",
                         "value": pre_counter},
                        {"name": "post_counter",
                         "type": "double",
                         "value": post_counter}
                   ]
                }]
            }

            # Insert gap_event
            list_of_events.append(gap_event)

        # end for

        matching_status = "NO_MATCHED_PLANNED_PLAYBACK"
        links_playback_validity = []
        links_playback_completeness = []
        query_start = acquisition_start
        # Obtain planned playbacks
        if downlink_mode in ["NOMINAL", "NRT"]:
            corrected_planned_playback_gauge_names = ["PLANNED_PLAYBACK_TYPE_" + downlink_mode + "_CORRECTION", "PLANNED_PLAYBACK_TYPE_REGULAR_CORRECTION"]
        elif downlink_mode in ["SAD", "HKTM"]:
            # The SAD and HKTM playbacks have no duration because the duration is not set in the NPPF and can be modified, so in order to get the information from DDBB, the start value of the query is moved to start -11 seconds (correct margin as per mission specification there must be always 11 seconds between playbacks)
            query_start = str(parser.parse(acquisition_start) + datetime.timedelta(seconds=-11))
            corrected_planned_playback_gauge_names = ["PLANNED_PLAYBACK_TYPE_" + downlink_mode + "_CORRECTION", "PLANNED_PLAYBACK_TYPE_HKTM_SAD_CORRECTION"]
        else:
            corrected_planned_playback_gauge_names = ["PLANNED_PLAYBACK_TYPE_" + downlink_mode + "_CORRECTION"]
        # end if
        corrected_planned_playbacks = query.get_events_join(gauge_names = {"list": corrected_planned_playback_gauge_names, "op": "in"}, gauge_systems = {"list": [satellite], "op": "in"}, start_filters = [{"date": acquisition_stop, "op": "<"}], stop_filters = [{"date": query_start, "op": ">"}])

        # Initialize the completeness
        for corrected_planned_playback in corrected_planned_playbacks:
            matching_status = "MATCHED_PLANNED_PLAYBACK"

            planned_playback_uuid = [event_link.event_uuid_link for event_link in corrected_planned_playback.eventLinks if event_link.name == "PLANNED_EVENT"][0]

            planned_playback_event = query.get_events(event_uuids = {"op": "in", "list": [planned_playback_uuid]})
            playback_planning_completeness_generation_times.append(planned_playback_event[0].source.generation_time)

            links_playback_validity.append({
                "link": str(planned_playback_uuid),
                "link_mode": "by_uuid",
                "name": "PLAYBACK_VALIDITY",
                "back_ref": "PLANNED_PLAYBACK"
            })
            links_playback_completeness.append({
                "link": str(planned_playback_uuid),
                "link_mode": "by_uuid",
                "name": "PLAYBACK_COMPLETENESS",
                "back_ref": "PLANNED_PLAYBACK"
            })
            if downlink_mode == "SAD":
                channels = ["2"]
            elif downlink_mode == "HKTM":
                # HKTM
                channels = ["1"]
            else:
                channels = ["1","2"]
            # end if
            for data_channel in channels:
                value = {
                    "name": "playback_completeness_began_channel_" + data_channel,
                    "type": "object",
                    "values": []
                }
                exit_status = engine.insert_event_values(planned_playback_uuid, value)

                if exit_status["inserted"] == True:

                    # Insert the linked COMPLETENESS event for the automatic completeness check
                    planning_event_values = corrected_planned_playback.get_structured_values()
                    planning_event_values[0]["values"] = planning_event_values[0]["values"] + [
                        {"name": "status",
                         "type": "text",
                         "value": "MISSING"}
                    ]

                    if downlink_mode == "SAD":
                        start = corrected_planned_playback.start + datetime.timedelta(seconds=3)
                        stop = start + datetime.timedelta(seconds=2)
                    elif downlink_mode == "HKTM":
                        # HKTM
                        channels = ["1"]
                        start = corrected_planned_playback.start + datetime.timedelta(seconds=2)
                        stop = start
                    else:
                        channels = ["1","2"]
                        start = corrected_planned_playback.start + datetime.timedelta(seconds=3)
                        stop = corrected_planned_playback.stop - datetime.timedelta(seconds=3)
                    # end if
                    

                    playback_planning_completeness_operation["events"].append({
                        "gauge": {
                            "insertion_type": "SIMPLE_UPDATE",
                            "name": "PLANNED_PLAYBACK_" + downlink_mode + "_COMPLETENESS_CHANNEL_" + data_channel,
                            "system": satellite
                        },
                        "start": str(start),
                        "stop": str(stop),
                        "links": [
                            {
                                "link": str(planned_playback_uuid),
                                "link_mode": "by_uuid",
                                "name": "PLAYBACK_COMPLETENESS",
                                "back_ref": "PLANNED_PLAYBACK"
                            }],
                        "values": planning_event_values
                    })
                # end if
            # end for
        # end for

        playback_validity_event = {
            "link_ref": playback_validity_event_link_ref,
            "explicit_reference": session_id,
            "key": session_id,
            "gauge": {
                "insertion_type": "EVENT_KEYS",
                "name": "PLAYBACK_" + downlink_mode + "_VALIDITY_" + vcid_number,
                "system": station
            },
            "links": links_playback_validity,
            "start": acquisition_start,
            "stop": acquisition_stop,
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
                     "value": station},
                    {"name": "channel",
                     "type": "double",
                     "value": channel},
                    {"name": "vcid",
                     "type": "double",
                     "value": vcid_number},
                    {"name": "downlink_mode",
                     "type": "text",
                     "value": downlink_mode},
                    {"name": "matching_plan_status",
                     "type": "text",
                     "value": matching_status}
                ]
            }]
        }
        # Insert playback_validity_event
        list_of_events.append(playback_validity_event)

        completeness_status = "RECEIVED"
        if status != "COMPLETE":
            completeness_status = status
        # end if                    

        playback_completeness_event = {
            "explicit_reference": session_id,
            "key": session_id + "_CHANNEL_" + channel,
            "gauge": {
                "insertion_type": "INSERT_and_ERASE_per_EVENT",
                "name": "PLANNED_PLAYBACK_" + downlink_mode + "_COMPLETENESS_CHANNEL_" + channel,
                "system": satellite
            },
            "links": links_playback_completeness,
            "start": acquisition_start,
            "stop": acquisition_stop,
            "values": [{
                "name": "details",
                "type": "object",
                "values": [
                    {"name": "status",
                     "type": "text",
                     "value": completeness_status},
                    {"name": "downlink_orbit",
                     "type": "double",
                     "value": downlink_orbit},
                    {"name": "satellite",
                     "type": "text",
                     "value": satellite},
                    {"name": "reception_station",
                     "type": "text",
                     "value": station},
                    {"name": "downlink_mode",
                     "type": "text",
                     "value": downlink_mode},
                ]
            }]
        }

        # Insert playback_completeness_event
        list_of_events.append(playback_completeness_event)
    # end for

    # Insert completeness operation for the completeness analysis of the plan
    if len(playback_planning_completeness_operation["events"]) > 0:
        playback_planning_completeness_event_starts = [event["start"] for event in playback_planning_completeness_operation["events"]]
        playback_planning_completeness_event_starts.sort()
        playback_planning_completeness_event_stops = [event["stop"] for event in playback_planning_completeness_operation["events"]]
        playback_planning_completeness_event_stops.sort()

        playback_planning_completeness_generation_times.sort()
        generation_time = playback_planning_completeness_generation_times[0]

        playback_planning_completeness_operation["source"] = {
            "name": source["name"],
            "generation_time": str(generation_time),
            "validity_start": str(playback_planning_completeness_event_starts[0]),
            "validity_stop": str(playback_planning_completeness_event_stops[-1])
        }

        list_of_planning_operations.append(playback_planning_completeness_operation)
    # end if

    return general_status

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

    # Obtain the station
    station = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Source/System")[0].text

    # Obtain link session ID
    session_id = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/File_Type")[0].text + "_" + xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Start")[0].text.split("UTC=",1)[1]

    # Obtain downlink orbit
    downlink_orbit = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Variable_Header/Downlink_Orbit")[0].text

    # Completeness operation for the isp completeness analysis of the plan
    isp_planning_completeness_operation = {
        "mode": "insert",
        "dim_signature": {
            "name": "RECEPTION_" + satellite,
            "exec": "isp_planning_completeness_" + os.path.basename(__file__),
            "version": version
        },
        "events": []
    }

    isp_planning_completeness_generation_times = []

    vcids = xpath_xml("/Earth_Explorer_File/Data_Block/*[contains(name(),'data_C')]/Status[number(NumFrames) > 0 and (@VCID = 4 or @VCID = 5 or @VCID = 6)]")
    for vcid in vcids:

        # Iterator and timeline for the ISP gaps
        isp_gap_iterator = 0
        timeline_isp_gaps = []

        vcid_number = vcid.get("VCID")

        downlink_mode = functions.get_vcid_mode(vcid_number)
        # Obtain the sensing segment received (EFEP reports only give information about the start date of the first and last scenes)
        sensing_starts = vcid.xpath("ISP_Status/Status/SensStartTime")
        sensing_starts_in_iso_8601 = [functions.three_letter_to_iso_8601(sensing_start.text) for sensing_start in sensing_starts]
        
        # Sort list
        sensing_starts_in_iso_8601.sort()
        sensing_start = sensing_starts_in_iso_8601[0]
        corrected_sensing_start = functions.convert_from_gps_to_utc(sensing_start)

        sensing_stops = vcid.xpath("ISP_Status/Status/SensStopTime")
        sensing_stops_in_iso_8601 = [functions.three_letter_to_iso_8601(sensing_stop.text) for sensing_stop in sensing_stops]
        
        # Sort list
        sensing_stops_in_iso_8601.sort()
        sensing_stop = sensing_stops_in_iso_8601[-1]
        # Add 1 scene at the end
        corrected_sensing_stop = str(functions.convert_from_datetime_gps_to_datetime_utc(parser.parse(sensing_stop)) + datetime.timedelta(seconds=3.608))

        # The data comes in pairs of VCIDs
        # 4 - 20
        # 5 - 21
        # 6 - 22
        # Get all the APIDs which contain data
        apids = xpath_xml("/Earth_Explorer_File/Data_Block/*[contains(name(),'data_C')]/Status[number(@VCID) = $vcid_number or number(@VCID) = $corresponding_vcid_number]/ISP_Status/Status[NumPackets > 0]", vcid_number = int(vcid_number), corresponding_vcid_number = int(vcid_number) + 16)

        # Obtain sensing segments per apid
        # Obtain sensing gaps per apid (the gap is clean, PreCounter = threshold and PostCounter = 0)
        sensing_gaps_per_apid = {}

        timelines_of_sensing_gaps = []
        for apid in apids:
            apid_number = apid.get("APID")
            sensing_gaps_per_apid[apid_number] = []
            band_detector = functions.get_band_detector(apid_number)
            counter_threshold = functions.get_counter_threshold(band_detector["band"])
            sensing_gaps = apid.xpath("Gaps/Gap[dates_difference(three_letter_to_iso_8601(string(PostSensTime)),three_letter_to_iso_8601(string(PreSensTime))) > 4 and number(PreCounter) = $counter_threshold and number(PostCounter) = 0]", counter_threshold=counter_threshold)
            for sensing_gap in sensing_gaps:
                sensing_gaps_per_apid[apid_number].append({
                    "id": apid_number,
                    "start": parser.parse(functions.convert_from_gps_to_utc(functions.three_letter_to_iso_8601(sensing_gap.xpath("string(PreSensTime)")))),
                    "stop": parser.parse(functions.convert_from_gps_to_utc(functions.three_letter_to_iso_8601(sensing_gap.xpath("string(PostSensTime)")))),
                })
            # end for
            if len(sensing_gaps_per_apid[apid_number]) > 0:
                timelines_of_sensing_gaps.append(sensing_gaps_per_apid[apid_number])
            # end if
        # end for

        # Obtain the sensing gaps common to all apids
        sensing_gaps = date_functions.intersect_many_timelines(timelines_of_sensing_gaps)

        # Received datablocks
        covered_sensing = {
            "id": "covered_sensing",
            "start": parser.parse(corrected_sensing_start),
            "stop": parser.parse(corrected_sensing_stop)
        }
        received_datablocks = [segment for segment in date_functions.difference_timelines([covered_sensing], sensing_gaps) if segment["id"] == "covered_sensing"]

        # Create ISP gaps for complete missing scenes
        for apid_number in functions.get_apid_numbers():
            gaps = []
            if str(apid_number) in sensing_gaps_per_apid:
                gaps = date_functions.intersect_timelines(received_datablocks, sensing_gaps_per_apid[str(apid_number)])
            else:
                gaps = received_datablocks
            # end if
            for gap in gaps:
                status = "INCOMPLETE"
                band_detector = functions.get_band_detector(apid_number)

                isp_gap_event = {
                    "link_ref": "ISP_GAP_" + str(isp_gap_iterator),
                    "explicit_reference": session_id,
                    "key": session_id,
                    "gauge": {
                        "insertion_type": "EVENT_KEYS",
                        "name": "ISP_GAP",
                        "system": station
                    },
                    "start": str(gap["start"]),
                    "stop": str(gap["stop"]),
                    "values": [{
                        "name": "values",
                        "type": "object",
                        "values": [
                            {"name": "impact",
                             "type": "text",
                             "value": "COMPLETE_SCENES_BAND"},
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
                             "value": station},
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

                # Insert segment for associating the ISP gap
                timeline_isp_gaps.append({
                    "id": "ISP_GAP_" + str(isp_gap_iterator),
                    "start": gap["start"],
                    "stop": gap["stop"]
                })

                isp_gap_iterator += 1    
            # end for
        # end for
        
        # Create ISP gaps for gaps at the beginning of the APIDs (StartCounter != 0)
        apids_with_gaps_at_the_beginning = xpath_xml("/Earth_Explorer_File/Data_Block/*[contains(name(),'data_C')]/Status[number(@VCID) = $vcid_number or number(@VCID) = $corresponding_vcid_number]/ISP_Status/Status[NumPackets > 0 and not(StartCounter = 0)]", vcid_number = int(vcid_number), corresponding_vcid_number = int(vcid_number) + 16)
        for apid in apids_with_gaps_at_the_beginning:
            apid_number = apid.get("APID")            
            status = "INCOMPLETE"
            band_detector = functions.get_band_detector(apid_number)

            counter_threshold = functions.get_counter_threshold(band_detector["band"])
            start = parser.parse(functions.convert_from_gps_to_utc(functions.three_letter_to_iso_8601(apid.xpath("string(SensStartTime)"))))

            missing_packets = int(apid.xpath("string(StartCounter)"))

            seconds_gap = (missing_packets / counter_threshold) * 3.608
            
            stop = start + datetime.timedelta(seconds=seconds_gap)
            
            isp_gap_event = {
                "link_ref": "ISP_GAP_" + str(isp_gap_iterator),
                "explicit_reference": session_id,
                "key": session_id,
                "gauge": {
                    "insertion_type": "EVENT_KEYS",
                    "name": "ISP_GAP",
                    "system": station
                },
                "start": str(start),
                "stop": str(stop),
                "values": [{
                    "name": "values",
                    "type": "object",
                    "values": [
                        {"name": "impact",
                         "type": "text",
                         "value": "AT_BEGINNING"},
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
                         "value": station},
                        {"name": "vcid",
                         "type": "double",
                         "value": vcid_number},
                        {"name": "downlink_mode",
                         "type": "text",
                         "value": downlink_mode},
                        {"name": "apid",
                         "type": "double",
                         "value": apid_number},
                        {"name": "missing_packets",
                         "type": "double",
                         "value": missing_packets}
                   ]
                }]
            }

            # Insert isp_gap_event
            list_of_events.append(isp_gap_event)

            # Insert segment for associating the ISP gap
            timeline_isp_gaps.append({
                "id": "ISP_GAP_" + str(isp_gap_iterator),
                "start": start,
                "stop": stop
            })

            isp_gap_iterator += 1    
        # end for

        # Create ISP gaps for gaps smaller than a scene
        gaps_smaller_than_scene = xpath_xml("/Earth_Explorer_File/Data_Block/*[contains(name(),'data_C')]/Status[number(@VCID) = $vcid_number or number(@VCID) = $corresponding_vcid_number]/ISP_Status/Status[NumPackets > 0]/Gaps/Gap[(not (PreCounter = get_counter_threshold_from_apid(string(../../@APID))) and not (PostCounter = 0)) or not (PostCounter = 0)]", vcid_number = int(vcid_number), corresponding_vcid_number = int(vcid_number) + 16)

        for gap in gaps_smaller_than_scene:
            status = "INCOMPLETE"
            apid_number = gap.xpath("../../@APID")[0]
            band_detector = functions.get_band_detector(apid_number)

            counter_threshold = functions.get_counter_threshold(band_detector["band"])
            scene_start = parser.parse(functions.convert_from_gps_to_utc(functions.three_letter_to_iso_8601(gap.xpath("string(PreSensTime)"))))

            counter_start_value = int(gap.xpath("string(PreCounter)"))
            counter_start = counter_start_value
            if counter_start == counter_threshold:
                counter_start = -1
            # end if

            counter_stop = int(gap.xpath("string(PostCounter)"))

            missing_packets = counter_stop - counter_start

            seconds_from_scene_start_to_gap_start = (counter_start / counter_threshold) * 3.608
            seconds_from_scene_start_to_gap_stop = (counter_stop / counter_threshold) * 3.608
            
            start = scene_start + datetime.timedelta(seconds=seconds_from_scene_start_to_gap_start)
            stop = scene_start + datetime.timedelta(seconds=seconds_from_scene_start_to_gap_stop)
            
            isp_gap_event = {
                "link_ref": "ISP_GAP_" + str(isp_gap_iterator),
                "explicit_reference": session_id,
                "key": session_id,
                "gauge": {
                    "insertion_type": "EVENT_KEYS",
                    "name": "ISP_GAP",
                    "system": station
                },
                "start": str(start),
                "stop": str(stop),
                "values": [{
                    "name": "values",
                    "type": "object",
                    "values": [
                        {"name": "impact",
                         "type": "text",
                         "value": "SMALLER_THAN_A_SCENE"},
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
                         "value": station},
                        {"name": "vcid",
                         "type": "double",
                         "value": vcid_number},
                        {"name": "downlink_mode",
                         "type": "text",
                         "value": downlink_mode},
                        {"name": "apid",
                         "type": "double",
                         "value": apid_number},
                        {"name": "counter_start",
                         "type": "double",
                         "value": counter_start_value},
                        {"name": "counter_stop",
                         "type": "double",
                         "value": counter_stop},
                        {"name": "missing_packets",
                         "type": "double",
                         "value": missing_packets}
                   ]
                }]
            }

            # Insert isp_gap_event
            list_of_events.append(isp_gap_event)

            # Insert segment for associating the ISP gap
            timeline_isp_gaps.append({
                "id": "ISP_GAP_" + str(isp_gap_iterator),
                "start": start,
                "stop": stop
            })

            isp_gap_iterator += 1    
        # end for

        # Merge timeline of isp gaps
        merged_timeline_isp_gaps = date_functions.merge_timeline(date_functions.sort_timeline_by_start(timeline_isp_gaps))

        # Obtain the planned imaging events from the corrected events which record type corresponds to the downlink mode and are intersecting the segment of the RAW_ISP_VALIDTY
        if downlink_mode != "RT":
            corrected_planned_imagings = query.get_events_join(gauge_name_like = {"str": "PLANNED_CUT_IMAGING_%_CORRECTION", "op": "like"}, gauge_systems = {"list": [satellite], "op": "in"}, values_name_type_like = [{"name_like": "record_type", "type": "text", "op": "like"}], value_filters = [{"value": downlink_mode, "type": "text", "op": "=="}], start_filters = [{"date": corrected_sensing_stop, "op": "<"}], stop_filters = [{"date": corrected_sensing_start, "op": ">"}])
        else:
            corrected_planned_imagings = query.get_events_join(gauge_name_like = {"str": "PLANNED_CUT_IMAGING_%_CORRECTION", "op": "like"}, gauge_systems = {"list": [satellite], "op": "in"}, start_filters = [{"date": corrected_sensing_stop, "op": "<"}], stop_filters = [{"date": corrected_sensing_start, "op": ">"}])
        # end if

        corrected_planned_imagings_segments = date_functions.convert_eboa_events_to_date_segments(corrected_planned_imagings)

        # Initialize the completeness
        for corrected_planned_imaging in corrected_planned_imagings:
            for channel in ["1","2"]:
                value = {
                    "name": "isp_completeness_began_channel_" + channel,
                    "type": "object",
                    "values": []
                }
                planned_imaging_uuid = [event_link.event_uuid_link for event_link in corrected_planned_imaging.eventLinks if event_link.name == "PLANNED_EVENT"][0]
                exit_status = engine.insert_event_values(planned_imaging_uuid, value)
                if exit_status["inserted"] == True:

                    planned_imaging_event = query.get_events(event_uuids = {"op": "in", "list": [planned_imaging_uuid]})
                    isp_planning_completeness_generation_times.append(planned_imaging_event[0].source.generation_time)
                    
                    # Insert the linked COMPLETENESS event for the automatic completeness check
                    planning_event_values = corrected_planned_imaging.get_structured_values()
                    planning_event_values[0]["values"] = planning_event_values[0]["values"] + [
                        {"name": "status",
                         "type": "text",
                         "value": "MISSING"}
                    ]

                    start = corrected_planned_imaging.start + datetime.timedelta(seconds=4)
                    stop = corrected_planned_imaging.stop - datetime.timedelta(seconds=4)

                    isp_planning_completeness_operation["events"].append({
                        "gauge": {
                            "insertion_type": "SIMPLE_UPDATE",
                            "name": "PLANNED_IMAGING_ISP_COMPLETENESS_CHANNEL_" + channel,
                            "system": satellite
                        },
                        "start": str(start),
                        "stop": str(stop),
                        "links": [
                            {
                                "link": str(planned_imaging_uuid),
                                "link_mode": "by_uuid",
                                "name": "ISP_COMPLETENESS",
                                "back_ref": "PLANNED_IMAGING"
                            }],
                        "values": planning_event_values
                    })
                # end if
            # end for
        # end for

        # Create the ISP_VALIDITY events
        for received_datablock in received_datablocks:
            sensing_orbit = ""
            links_isp_validity = []
            links_isp_completeness = []
            intersected_planned_imagings_segments = date_functions.intersect_timelines([received_datablock], corrected_planned_imagings_segments)
            matching_status = "NO_MATCHED_PLANNED_IMAGING"
            if len(intersected_planned_imagings_segments) > 0:
                matching_status = "MATCHED_PLANNED_IMAGING"
                intersected_planned_imagings_segment = intersected_planned_imagings_segments[0]
                corrected_planned_imaging = [event for event in corrected_planned_imagings if event.event_uuid == intersected_planned_imagings_segment["id2"]][0]
                planned_imaging_uuid = [event_link.event_uuid_link for event_link in corrected_planned_imaging.eventLinks if event_link.name == "PLANNED_EVENT"][0]
                sensing_orbit_values = query.get_event_values_interface(value_type="double", values_names_type=[{"op": "in", "names": ["start_orbit"], "type": "double"}], event_uuids = {"op": "in", "list": [planned_imaging_uuid]})
                sensing_orbit = str(sensing_orbit_values[0].value)
                links_isp_validity.append({
                    "link": str(planned_imaging_uuid),
                    "link_mode": "by_uuid",
                    "name": "ISP-VALIDITY",
                    "back_ref": "PLANNED_IMAGING"
                })
                links_isp_completeness.append({
                    "link": str(planned_imaging_uuid),
                    "link_mode": "by_uuid",
                    "name": "ISP_COMPLETENESS",
                    "back_ref": "PLANNED_IMAGING"
                })
            # end if
            
            # ISP validity event reference
            isp_validity_event_link_ref = "ISP_VALIDITY_" + vcid_number + "_" + str(received_datablock["start"])

            isp_gaps_intersected = date_functions.intersect_timelines([received_datablock], merged_timeline_isp_gaps)

            isp_validity_status = "COMPLETE"
            if len(isp_gaps_intersected) > 0:
                isp_validity_status = "INCOMPLETE"
            # end if

            links_isp_validity.append({
                "link": "PLAYBACK_" + downlink_mode + "_VALIDITY_" + vcid_number,
                "link_mode": "by_ref",
                "name": "ISP_VALIDITY",
                "back_ref": "PLAYBACK-VALIDITY"
            })

            for isp_gap_intersected in isp_gaps_intersected:
                for id in isp_gap_intersected["id2"]:
                    links_isp_validity.append({
                        "link": id,
                        "link_mode": "by_ref",
                        "name": "ISP-VALIDITY",
                        "back_ref": "ISP-GAP"
                    })
                # end for
            # end for

            isp_validity_event = {
                "link_ref": isp_validity_event_link_ref,
                "explicit_reference": session_id,
                "key": session_id,
                "gauge": {
                    "insertion_type": "EVENT_KEYS",
                    "name": "ISP_VALIDITY",
                    "system": station
                },
                "links": links_isp_validity,
                "start": str(received_datablock["start"]),
                "stop": str(received_datablock["stop"]),
                "values": [{
                    "name": "values",
                    "type": "object",
                    "values": [
                        {"name": "status",
                         "type": "text",
                         "value": isp_validity_status},
                        {"name": "downlink_orbit",
                         "type": "double",
                         "value": downlink_orbit},
                        {"name": "satellite",
                         "type": "text",
                         "value": satellite},
                        {"name": "reception_station",
                         "type": "text",
                         "value": station},
                        {"name": "downlink_mode",
                         "type": "text",
                         "value": downlink_mode},
                        {"name": "matching_plan_status",
                         "type": "text",
                         "value": matching_status},
                        {"name": "sensing_orbit",
                         "type": "text",
                         "value": sensing_orbit}
                    ]
                }]
            }
            # Insert isp_validity_event
            list_of_events.append(isp_validity_event)

            completeness_status = "RECEIVED"
            if isp_validity_status != "COMPLETE":
                completeness_status = isp_validity_status
            # end if                    
            
            links_isp_completeness.append({
                "link": isp_validity_event_link_ref,
                "link_mode": "by_ref",
                "name": "COMPLETENESS",
                "back_ref": "ISP-VALIDITY"
            })

            for channel in ["1","2"]:
                isp_validity_completeness_event = {
                    "explicit_reference": session_id,
                    "key": session_id + "_CHANNEL_" + channel,
                    "gauge": {
                        "insertion_type": "INSERT_and_ERASE_per_EVENT",
                        "name": "PLANNED_IMAGING_ISP_COMPLETENESS_CHANNEL_" + channel,
                        "system": satellite
                    },
                    "links": links_isp_completeness,
                    "start": str(received_datablock["start"]),
                    "stop": str(received_datablock["stop"]),
                    "values": [{
                        "name": "details",
                        "type": "object",
                        "values": [
                            {"name": "status",
                             "type": "text",
                             "value": completeness_status},
                            {"name": "downlink_orbit",
                             "type": "double",
                             "value": downlink_orbit},
                            {"name": "satellite",
                             "type": "text",
                             "value": satellite},
                            {"name": "reception_station",
                             "type": "text",
                             "value": station},
                            {"name": "downlink_mode",
                             "type": "text",
                             "value": downlink_mode},
                            {"name": "sensing_orbit",
                             "type": "text",
                             "value": sensing_orbit}
                        ]
                    }]
                }

                # Insert isp_validity_event
                list_of_events.append(isp_validity_completeness_event)

            # end for
        # end for

    # end for

    # Insert completeness operation for the completeness analysis of the plan
    if len(isp_planning_completeness_operation["events"]) > 0:
        isp_planning_completeness_event_starts = [event["start"] for event in isp_planning_completeness_operation["events"]]
        isp_planning_completeness_event_starts.sort()
        isp_planning_completeness_event_stops = [event["stop"] for event in isp_planning_completeness_operation["events"]]
        isp_planning_completeness_event_stops.sort()

        isp_planning_completeness_generation_times.sort()
        generation_time = isp_planning_completeness_generation_times[0]

        isp_planning_completeness_operation["source"] = {
            "name": source["name"],
            "generation_time": str(generation_time),
            "validity_start": str(isp_planning_completeness_event_starts[0]),
            "validity_stop": str(isp_planning_completeness_event_stops[-1])
        }

        list_of_planning_operations.append(isp_planning_completeness_operation)
    # end if

    return status

@debug
def _generate_pass_information(xpath_xml, source, engine, query, list_of_annotations, list_of_explicit_references, isp_status, acquisition_status):
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

    # Obtain the station
    station = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Source/System")[0].text

    # Obtain link session ID
    session_id = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/File_Type")[0].text + "_" + xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Start")[0].text.split("UTC=",1)[1]

    # Obtain downlink orbit
    downlink_orbit = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Variable_Header/Downlink_Orbit")[0].text

    # Associate the explicit reference to the group STATION_LINK_SESSION_IDs
    explicit_reference = {
        "name": session_id,
        "group": "STATION_LINK_SESSION_IDs"
    }

    # Link session
    link_details_annotation = {
        "explicit_reference": session_id,
        "annotation_cnf": {
            "name": "LINK_DETAILS",
            "system": station
        },
        "values": [{
            "name": "link_details",
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
                 "value": station},
                {"name": "isp_status",
                 "type": "text",
                 "value": isp_status},
                {"name": "acquisition_status",
                 "type": "text",
                 "value": acquisition_status}
            ]
        }]
    }

    list_of_annotations.append(link_details_annotation)

    return

@debug
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
    ns["dates_difference"] = xpath_functions.dates_difference
    ns["get_counter_threshold_from_apid"] = xpath_functions.get_counter_threshold_from_apid
    xpath_xml = etree.XPathEvaluator(parsed_xml)

    satellite = file_name[0:3]
    generation_time = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Source/Creation_Date")[0].text.split("=")[1]
    # Set the validity start to be the first sensing received to avoid error ingesting
    sensing_starts = xpath_xml("/Earth_Explorer_File/Data_Block/*[contains(name(),'data_C')]/Status[@VCID = 2 or @VCID = 4 or @VCID = 5 or @VCID = 6 or @VCID = 20 or @VCID = 21 or @VCID = 22]/ISP_Status/Status/SensStartTime")

    acquisition_starts = xpath_xml("/Earth_Explorer_File/Data_Block/*[contains(name(),'data_C')]/Status/ISP_Status/Status/AcqStartTime")

    if len(sensing_starts) > 0:
        # Set the validity start to be the first sensing timing acquired to avoid error ingesting
        sensing_starts_in_iso_8601 = [functions.three_letter_to_iso_8601(sensing_start.text) for sensing_start in sensing_starts]

        # Sort list
        sensing_starts_in_iso_8601.sort()
        corrected_sensing_start = functions.convert_from_gps_to_utc(sensing_starts_in_iso_8601[0])
        validity_start = corrected_sensing_start
    elif len(acquisition_starts) > 0:
        # Set the validity start to be the first acquisition timing registered to avoid error ingesting
        acquisition_starts_in_iso_8601 = [functions.three_letter_to_iso_8601(acquisition_start.text) for acquisition_start in acquisition_starts]

        # Sort list
        acquisition_starts_in_iso_8601.sort()
        validity_start = acquisition_starts_in_iso_8601[0]
    else:
        validity_start = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Start")[0].text.split("=")[1]
    # end if

    acquisition_stops = xpath_xml("/Earth_Explorer_File/Data_Block/*[contains(name(),'data_C')]/Status/ISP_Status/Status/AcqStopTime")
    if len(acquisition_stops) > 0:
        # Set the validity stop to be the last acquisition timing registered to avoid error ingesting
        acquisition_stops_in_iso_8601 = [functions.three_letter_to_iso_8601(acquisition_stop.text) for acquisition_stop in acquisition_stops]

        # Sort list
        acquisition_stops_in_iso_8601.sort()
        validity_stop = acquisition_stops_in_iso_8601[-1]
    else:
        validity_stop = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Stop")[0].text.split("=")[1]
    # end if

    source = {
        "name": file_name,
        "generation_time": generation_time,
        "validity_start": validity_start,
        "validity_stop": validity_stop
    }

    # Extract the information of the received data
    isp_status = _generate_received_data_information(xpath_xml, source, engine, query, list_of_events, list_of_planning_operations)

    # Extract the information of the received data
    acquisition_status = _generate_acquisition_data_information(xpath_xml, source, engine, query, list_of_events, list_of_planning_operations)

    # Extract the information of the pass
    _generate_pass_information(xpath_xml, source, engine, query, list_of_annotations, list_of_explicit_references, isp_status, acquisition_status)

    

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
