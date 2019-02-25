"""
Ingestion module for the NPPF files of Sentinel-2

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
import eboa.engine.engine as eboa_engine
from eboa.engine.engine import Engine

# Import ingestion helpers
import eboa.engine.ingestion as ingestion

# Import debugging
from eboa.debugging import debug

# Import logging
from eboa.logging import Log

logging_module = Log()
logger = logging_module.logger

version = "1.0"
imaging_modes={
    "MPMSSCAL": "SUN_CAL",
    "MPMSDASC": "DARK_CAL_CSM_OPEN",
    "MPMSDCLO": "DARK_CAL_CSM_CLOSE",
    "MPMSIVIC": "VICARIOUS_CAL",
    "MPMSNOBS": "NOMINAL",
    "MPMSIRAW": "RAW",
    "MPMSIDTS": "TEST"
}

record_types={
    "MPMMRNOM": "NOMINAL",
    "MPMMRNRT": "NRT"
}

playback_types={
    "MPMMPNOM": "NOMINAL",
    "MPMMPREG": "REGULAR",
    "MPMMPBRT": "RT",
    "MPMMPBHK": "HKTM",
    "MPMMPBSA": "SAD",
    "MPMMPBHS": "HKTM_SAD",
    "MPMMPNRT": "NRT"
}

playback_means={
    "MPXBSBOP": "XBAND",
    "MPG1STRT": "OCP",
    "MPG2STRT": "OCP",
    "MPG3STRT": "OCP"
}

@debug
def _generate_record_events(xpath_xml, source, list_of_events):
    """
    Method to generate the events for the MSI operations
    :param xpath_xml: source of information that was xpath evaluated
    :type xpath_xml: XPathEvaluator
    :param source: information of the source
    :type xpath_xml: dict
    :param list_of_events: list to store the events to be inserted into the eboa
    :type list_of_events: list
    
    Conceptual design of what is expected given the following inputs
    RECORD                  |--NRT--|
    RECORD          |--NOM--|       |--NOM--|
    IMAGING          |-------IMAGING-------|
    
    RESULT:
    RECORD EVENT 1  |--NOM--|
    RECORD EVENT 2          |--NRT--|
    RECORD EVENT 3                  |--NOM--|
    CUT_IMG EV 1     |------|
    CUT_IMG EV 2            |-------|
    CUT_IMG EV 3                    |------|
    IMAGING EVENT 1  |-------IMAGING-------|

    RECORD events and CUT_IMAGING events are linked by RECORD_OPERATION and IMAGING_OPERATION links
    IMAGING events and CUT_IMAGING events are linked by COMPLETE_IMAGING_OPERATION link (with back_ref)
    """

    satellite = source["name"][0:3]
    # Recording operations
    record_operations = xpath_xml("/Earth_Explorer_File/Data_Block/List_of_EVRQs/EVRQ[RQ/RQ_Name='MPMMRNOM' or RQ/RQ_Name='MPMMRNRT']")

    for record_operation in record_operations:
        # Record start information
        record_start = record_operation.xpath("RQ/RQ_Execution_Time")[0].text.split("=")[1]
        record_start_orbit = record_operation.xpath("RQ/RQ_Absolute_orbit")[0].text
        record_start_angle = record_operation.xpath("RQ/RQ_Deg_from_ANX")[0].text
        record_start_request = record_operation.xpath("RQ/RQ_Name")[0].text
        record_start_scn_dup = record_operation.xpath("RQ/List_of_RQ_Parameters/RQ_Parameter[RQ_Parameter_Name = 'SCN_DUP']/RQ_Parameter_Value")

        # Record stop information
        record_operation_stop = record_operation.xpath("following-sibling::EVRQ[RQ/RQ_Name='MPMMRSTP' or RQ/RQ_Name='MPMMRNRT' or RQ/RQ_Name='MPMMRNOM'][1]")[0]
        record_stop_orbit = record_operation_stop.xpath("RQ/RQ_Absolute_orbit")[0].text
        record_stop_angle = record_operation_stop.xpath("RQ/RQ_Deg_from_ANX")[0].text
        record_stop = record_operation_stop.xpath("RQ/RQ_Execution_Time")[0].text.split("=")[1]
        record_stop_request = record_operation_stop.xpath("RQ/RQ_Name")[0].text
        record_stop_scn_dup = record_operation_stop.xpath("RQ/List_of_RQ_Parameters/RQ_Parameter[RQ_Parameter_Name = 'SCN_DUP']/RQ_Parameter_Value")

        record_type = record_types[record_operation.xpath("RQ/RQ_Name")[0].text]

        following_imaging_operation = record_operation.xpath("following-sibling::EVRQ[RQ/RQ_Name='MPMSSCAL' or RQ/RQ_Name='MPMSDASC' or RQ/RQ_Name='MPMSDCLO' or RQ/RQ_Name='MPMSIVIC' or RQ/RQ_Name='MPMSNOBS' or RQ/RQ_Name='MPMSIRAW' or RQ/RQ_Name='MPMSIDTS' or RQ/RQ_Name='MPMSIMID' or RQ/RQ_Name='MPMSIDSB' or RQ/RQ_Name='MPMMRSTP'][1]")[0]
        if following_imaging_operation.xpath("RQ[RQ_Name='MPMSIMID' or RQ_Name='MPMSIDSB' or RQ_Name='MPMMRSTP']"):
            cut_imaging_start_operation = record_operation
            cut_imaging_stop_operation = following_imaging_operation
            imaging_start_operation = record_operation.xpath("preceding-sibling::EVRQ[RQ/RQ_Name='MPMSSCAL' or RQ/RQ_Name='MPMSDASC' or RQ/RQ_Name='MPMSDCLO' or RQ/RQ_Name='MPMSIVIC' or RQ/RQ_Name='MPMSNOBS' or RQ/RQ_Name='MPMSIRAW' or RQ/RQ_Name='MPMSIDTS'][1]")[0]
            imaging_start = imaging_start_operation.xpath("RQ/RQ_Execution_Time")[0].text.split("=")[1]
            cut_imaging_start_request = imaging_start_operation.xpath("RQ/RQ_Name")[0].text
        else:
            cut_imaging_start_operation = following_imaging_operation
            cut_imaging_start_request = cut_imaging_start_operation.xpath("RQ/RQ_Name")[0].text
            cut_imaging_stop_operation = record_operation.xpath("following-sibling::EVRQ[(RQ/RQ_Name='MPMSIMID' or RQ/RQ_Name='MPMSIDSB' or RQ/RQ_Name='MPMMRSTP')][1]")[0]
            imaging_start = following_imaging_operation.xpath("RQ/RQ_Execution_Time")[0].text.split("=")[1]
        # end if

        # Imaging start information
        cut_imaging_start = cut_imaging_start_operation.xpath("RQ/RQ_Execution_Time")[0].text.split("=")[1]
        cut_imaging_start_orbit = cut_imaging_start_operation.xpath("RQ/RQ_Absolute_orbit")[0].text
        cut_imaging_start_angle = cut_imaging_start_operation.xpath("RQ/RQ_Deg_from_ANX")[0].text

        cut_imaging_mode = imaging_modes[cut_imaging_start_request]

        # Imaging stop information
        cut_imaging_stop = cut_imaging_stop_operation.xpath("RQ/RQ_Execution_Time")[0].text.split("=")[1]
        if cut_imaging_mode == "SUN_CAL":
            cut_imaging_stop = cut_imaging_start
        # end if
        cut_imaging_stop_orbit = cut_imaging_stop_operation.xpath("RQ/RQ_Absolute_orbit")[0].text
        cut_imaging_stop_angle = cut_imaging_stop_operation.xpath("RQ/RQ_Deg_from_ANX")[0].text
        cut_imaging_stop_request = cut_imaging_stop_operation.xpath("RQ/RQ_Name")[0].text

        record_link_id = "record_" + record_start

        # Record event
        record_event = {
            "link_ref": record_link_id,
            "gauge": {
                "insertion_type": "ERASE_and_REPLACE",
                "name": "PLANNED_RECORD_" + record_type,
                "system": satellite
            },
            "start": record_start,
            "stop": record_stop,
            "values": [{
                "name": "values",
                "type": "object",
                "values": [
                    {"name": "start_request",
                     "type": "text",
                     "value": record_start_request},
                    {"name": "stop_request",
                     "type": "text",
                     "value": record_stop_request},
                    {"name": "start_orbit",
                     "type": "double",
                     "value": record_start_orbit},
                    {"name": "start_angle",
                     "type": "double",
                     "value": record_start_angle},
                    {"name": "stop_orbit",
                     "type": "double",
                     "value": record_stop_orbit},
                    {"name": "stop_angle",
                     "type": "double",
                     "value": record_stop_angle},
                    {"name": "satellite",
                     "type": "text",
                     "value": satellite}
                ]
            }]
        }

        # Include parameters
        if len(record_start_scn_dup) == 1:
            record_event["values"][0]["values"].append(
                    {"name": "start_scn_dup",
                     "type": "double",
                     "value": record_start_scn_dup[0].text},
            )
        # end if

        if len(record_stop_scn_dup) == 1:
            record_event["values"][0]["values"].append(
                    {"name": "stop_scn_dup",
                     "type": "double",
                     "value": record_stop_scn_dup[0].text}
            )
        # end if

        # Insert record_event
        ingestion.insert_event_for_ingestion(record_event, source, list_of_events)

        cut_imaging_link_id = "cut_imaging_" + cut_imaging_start
        imaging_link_id = "imaging_" + imaging_start

        # Imaging event
        cut_imaging_event = {
            "link_ref": cut_imaging_link_id,
            "gauge": {
                "insertion_type": "ERASE_and_REPLACE",
                "name": "PLANNED_CUT_IMAGING_" + cut_imaging_mode,
                "system": satellite
            },
            "start": cut_imaging_start,
            "stop": cut_imaging_stop,
            "links": [
                {
                    "link": record_link_id,
                    "link_mode": "by_ref",
                    "name": "PLANNED_RECORD_OPERATION",
                    "back_ref": "PLANNED_IMAGING_OPERATION"
                },
                {
                    "link": imaging_link_id,
                    "link_mode": "by_ref",
                    "name": "PLANNED_COMPLETE_IMAGING_OPERATION",
                    "back_ref": "PLANNED_CUT_IMAGING_OPERATION"
                }
            ],
            "values": [{
                "name": "values",
                "type": "object",
                "values": [
                    {"name": "start_request",
                     "type": "text",
                     "value": cut_imaging_start_request},
                    {"name": "stop_request",
                     "type": "text",
                     "value": cut_imaging_stop_request},
                    {"name": "start_orbit",
                     "type": "double",
                     "value": cut_imaging_start_orbit},
                    {"name": "start_angle",
                     "type": "double",
                     "value": cut_imaging_start_angle},
                    {"name": "stop_orbit",
                     "type": "double",
                     "value": cut_imaging_stop_orbit},
                    {"name": "stop_angle",
                     "type": "double",
                     "value": cut_imaging_stop_angle},
                    {"name": "satellite",
                     "type": "text",
                     "value": satellite},
                    {"name": "record_type",
                     "type": "text",
                     "value": record_type}
                ]
            }]
        }

        # Insert imaging_event
        ingestion.insert_event_for_ingestion(cut_imaging_event, source, list_of_events)

    # end for

    # Imaging operations
    imaging_operations = xpath_xml("/Earth_Explorer_File/Data_Block/List_of_EVRQs/EVRQ[RQ/RQ_Name='MPMSSCAL' or RQ/RQ_Name='MPMSDASC' or RQ/RQ_Name='MPMSDCLO' or RQ/RQ_Name='MPMSIVIC' or RQ/RQ_Name='MPMSNOBS' or RQ/RQ_Name='MPMSIRAW' or RQ/RQ_Name='MPMSIDTS']")

    for imaging_operation in imaging_operations:
        # Imaging start information
        imaging_start = imaging_operation.xpath("RQ/RQ_Execution_Time")[0].text.split("=")[1]
        imaging_start_orbit = imaging_operation.xpath("RQ/RQ_Absolute_orbit")[0].text
        imaging_start_angle = imaging_operation.xpath("RQ/RQ_Deg_from_ANX")[0].text
        imaging_start_request = imaging_operation.xpath("RQ/RQ_Name")[0].text

        imaging_mode = imaging_modes[imaging_start_request]

        # Imaging stop information
        imaging_stop_operation = imaging_operation.xpath("following-sibling::EVRQ[(RQ/RQ_Name='MPMSIMID' or RQ/RQ_Name='MPMSIDSB' or RQ/RQ_Name='MPMMRSTP')][1]")[0]
        imaging_stop = imaging_stop_operation.xpath("RQ/RQ_Execution_Time")[0].text.split("=")[1]
        if imaging_mode == "SUN_CAL":
            imaging_stop = imaging_start
        # end if
        imaging_stop_orbit = imaging_stop_operation.xpath("RQ/RQ_Absolute_orbit")[0].text
        imaging_stop_angle = imaging_stop_operation.xpath("RQ/RQ_Deg_from_ANX")[0].text
        imaging_stop_request = imaging_stop_operation.xpath("RQ/RQ_Name")[0].text

        imaging_link_id = "imaging_" + imaging_start

        # Imaging event
        imaging_event = {
            "link_ref": imaging_link_id,
            "gauge": {
                "insertion_type": "ERASE_and_REPLACE",
                "name": "PLANNED_IMAGING_" + imaging_mode,
                "system": satellite
            },
            "start": imaging_start,
            "stop": imaging_stop,
            "values": [{
                "name": "values",
                "type": "object",
                "values": [
                    {"name": "start_request",
                     "type": "text",
                     "value": imaging_start_request},
                    {"name": "stop_request",
                     "type": "text",
                     "value": imaging_stop_request},
                    {"name": "start_orbit",
                     "type": "double",
                     "value": imaging_start_orbit},
                    {"name": "start_angle",
                     "type": "double",
                     "value": imaging_start_angle},
                    {"name": "stop_orbit",
                     "type": "double",
                     "value": imaging_stop_orbit},
                    {"name": "stop_angle",
                     "type": "double",
                     "value": imaging_stop_angle},
                    {"name": "satellite",
                     "type": "text",
                     "value": satellite}
                ]
            }]
        }

        # Insert imaging_event
        ingestion.insert_event_for_ingestion(imaging_event, source, list_of_events)

    # end for

    return

@debug
def _generate_idle_events(xpath_xml, source, list_of_events):
    """
    Method to generate the events for the idle operation of the satellite
    :param xpath_xml: source of information that was xpath evaluated
    :type xpath_xml: XPathEvaluator
    :param source: information of the source
    :type xpath_xml: dict
    :param list_of_events: list to store the events to be inserted into the eboa
    :type list_of_events: list
    """

    satellite = source["name"][0:3]

    # Idle operations
    idle_operations = xpath_xml("/Earth_Explorer_File/Data_Block/List_of_EVRQs/EVRQ[RQ/RQ_Name='MPMSIMID' or RQ/RQ_Name='MPMSSBID']")

    for idle_operation in idle_operations:
        # Idle start information
        idle_start = idle_operation.xpath("RQ/RQ_Execution_Time")[0].text.split("=")[1]
        idle_start_orbit = idle_operation.xpath("RQ/RQ_Absolute_orbit")[0].text
        idle_start_angle = idle_operation.xpath("RQ/RQ_Deg_from_ANX")[0].text
        idle_start_request = idle_operation.xpath("RQ/RQ_Name")[0].text

        # Idle stop information
        idle_operation_stop = idle_operation.xpath("following-sibling::EVRQ[RQ/RQ_Name='MPMSSCAL' or RQ/RQ_Name='MPMSDASC' or RQ/RQ_Name='MPMSDCLO' or RQ/RQ_Name='MPMSIVIC' or RQ/RQ_Name='MPMSNOBS' or RQ/RQ_Name='MPMSIRAW' or RQ/RQ_Name='MPMSIDTS' or RQ/RQ_Name='MPMSIDSB'][1]")
        if len(idle_operation_stop) == 1:
            idle_stop_orbit = idle_operation_stop[0].xpath("RQ/RQ_Absolute_orbit")[0].text
            idle_stop_angle = idle_operation_stop[0].xpath("RQ/RQ_Deg_from_ANX")[0].text
            idle_stop = idle_operation_stop[0].xpath("RQ/RQ_Execution_Time")[0].text.split("=")[1]
            idle_stop_request = idle_operation_stop[0].xpath("RQ/RQ_Name")[0].text
            values = [
                {"name": "start_request",
                 "type": "text",
                 "value": idle_start_request},
                {"name": "stop_request",
                 "type": "text",
                 "value": idle_stop_request},
                {"name": "start_orbit",
                 "type": "double",
                 "value": idle_start_orbit},
                {"name": "start_angle",
                 "type": "double",
                 "value": idle_start_angle},
                {"name": "stop_orbit",
                 "type": "double",
                 "value": idle_stop_orbit},
                {"name": "stop_angle",
                 "type": "double",
                 "value": idle_stop_angle},
                {"name": "satellite",
                 "type": "text",
                 "value": satellite}
            ]
        else:
            idle_stop_orbit = None
            idle_stop_angle = None
            idle_stop = source["validity_stop"]
            idle_stop_request = None
            values = [
                {"name": "start_request",
                 "type": "text",
                 "value": idle_start_request},
                {"name": "start_orbit",
                 "type": "double",
                 "value": idle_start_orbit},
                {"name": "start_angle",
                 "type": "double",
                 "value": idle_start_angle},
                {"name": "satellite",
                 "type": "text",
                 "value": satellite}
            ]
        # end if

        # Idle event
        idle_event = {
            "gauge": {
                "insertion_type": "ERASE_and_REPLACE",
                "name": "PLANNED_IDLE",
                "system": satellite
            },
            "start": idle_start,
            "stop": idle_stop,
            "values": [{
                "name": "values",
                "type": "object",
                "values": values},
                {"name": "satellite",
                 "type": "text",
                 "value": satellite}]
        }

        # Insert idle_event
        ingestion.insert_event_for_ingestion(idle_event, source, list_of_events)

    # end for

    return

@debug
def _generate_playback_events(xpath_xml, source, list_of_events):
    """
    Method to generate the events for the idle operation of the satellite
    :param xpath_xml: source of information that was xpath evaluated
    :type xpath_xml: XPathEvaluator
    :param source: information of the source
    :type xpath_xml: dict
    :param list_of_events: list to store the events to be inserted into the eboa
    :type list_of_events: list

    Conceptual design of what is expected given the following inputs
    PLAYBACK MEAN      |------------XBAND------------|
    PLAYBACK MEAN                           |------------OCP-----------|
    PLAYBACK TYPES      |--NOM--||SAD|   |NOM||SAD|     |--NOM--||SAD|
    
    RESULT:
    PB MEAN EVENT 1    |------------XBAND------------|
    PB TY EVS LINKED    |--NOM--||SAD|   |NOM||SAD|
    PB MEAN EVENT 2                         |------------OCP----------
    PB TY EVS LINKED                                    |--NOM--||SAD|

    PB MEAN events and PB TY events are linked by PLAYBACK_OPERATION link (with back_ref)
    """

    satellite = source["name"][0:3]

    # Playback operations
    playback_operations = xpath_xml("/Earth_Explorer_File/Data_Block/List_of_EVRQs/EVRQ[RQ/RQ_Name='MPXBSBOP' or RQ/RQ_Name='MPG1STRT' or RQ/RQ_Name='MPG2STRT' or RQ/RQ_Name='MPG3STRT']")

    for playback_operation in playback_operations:
        # Playback start information
        playback_start = playback_operation.xpath("RQ/RQ_Execution_Time")[0].text.split("=")[1]
        playback_start_orbit = playback_operation.xpath("RQ/RQ_Absolute_orbit")[0].text
        playback_start_angle = playback_operation.xpath("RQ/RQ_Deg_from_ANX")[0].text
        playback_start_request = playback_operation.xpath("RQ/RQ_Name")[0].text

        playback_mean = playback_means[playback_start_request]

        # Playback stop information
        if playback_mean == "XBAND":
            playback_operation_stop = playback_operation.xpath("following-sibling::EVRQ[RQ/RQ_Name='MPXBOPSB'][1]")[0]
        else:
            playback_operation_stop = playback_operation.xpath("following-sibling::EVRQ[RQ/RQ_Name='MPOCPRY2'][1]")[0]
        # end if
        playback_stop_orbit = playback_operation_stop.xpath("RQ/RQ_Absolute_orbit")[0].text
        playback_stop_angle = playback_operation_stop.xpath("RQ/RQ_Deg_from_ANX")[0].text
        playback_stop = playback_operation_stop.xpath("RQ/RQ_Execution_Time")[0].text.split("=")[1]
        playback_stop_request = playback_operation_stop.xpath("RQ/RQ_Name")[0].text

        playback_mean_link_id = "playback_mean_" + playback_stop

        # Playback event
        playback_event = {
            "link_ref": playback_mean_link_id,
            "gauge": {
                "insertion_type": "ERASE_and_REPLACE",
                "name": "PLANNED_PLAYBACK_MEAN_" + playback_mean,
                "system": satellite
            },
            "start": playback_start,
            "stop": playback_stop,
            "values": [{
                "name": "values",
                "type": "object",
                "values": [
                    {"name": "start_request",
                     "type": "text",
                     "value": playback_start_request},
                    {"name": "stop_request",
                     "type": "text",
                     "value": playback_stop_request},
                    {"name": "start_orbit",
                     "type": "double",
                     "value": playback_start_orbit},
                    {"name": "start_angle",
                     "type": "double",
                     "value": playback_start_angle},
                    {"name": "stop_orbit",
                     "type": "double",
                     "value": playback_stop_orbit},
                    {"name": "stop_angle",
                     "type": "double",
                     "value": playback_stop_angle},
                    {"name": "satellite",
                     "type": "text",
                     "value": satellite}
                ]
            }]
        }

        # Insert playback_event
        ingestion.insert_event_for_ingestion(playback_event, source, list_of_events)

    # end for


    # Associate the playback types to the playback means
    playback_type_start_operations = xpath_xml("/Earth_Explorer_File/Data_Block/List_of_EVRQs/EVRQ[RQ/RQ_Name='MPMMPNOM' or RQ/RQ_Name='MPMMPREG' or RQ/RQ_Name='MPMMPBRT' or RQ/RQ_Name='MPMMPBHK' or RQ/RQ_Name='MPMMPBSA' or RQ/RQ_Name='MPMMPBHS' or RQ/RQ_Name='MPMMPNRT']")

    for playback_type_start_operation in playback_type_start_operations:

        # Playback_Type start information
        playback_type_start = playback_type_start_operation.xpath("RQ/RQ_Execution_Time")[0].text.split("=")[1]
        playback_type_start_orbit = playback_type_start_operation.xpath("RQ/RQ_Absolute_orbit")[0].text
        playback_type_start_angle = playback_type_start_operation.xpath("RQ/RQ_Deg_from_ANX")[0].text
        playback_type_start_request = playback_type_start_operation.xpath("RQ/RQ_Name")[0].text

        playback_type = playback_types[playback_type_start_request]

        if playback_type in ["HKTM", "SAD", "HKTM_SAD"]:
            playback_type_stop_operation = playback_type_start_operation
        else:
            playback_type_stop_operation = playback_type_start_operation.xpath("following-sibling::EVRQ[RQ/RQ_Name='MPMMPSTP'][1]")[0]
        # end if

        # Playback_Type stop information
        playback_type_stop = playback_type_stop_operation.xpath("RQ/RQ_Execution_Time")[0].text.split("=")[1]
        playback_type_stop_orbit = playback_type_stop_operation.xpath("RQ/RQ_Absolute_orbit")[0].text
        playback_type_stop_angle = playback_type_stop_operation.xpath("RQ/RQ_Deg_from_ANX")[0].text
        playback_type_stop_request = playback_type_stop_operation.xpath("RQ/RQ_Name")[0].text

        playback_mean_stop = playback_type_start_operation.xpath("following-sibling::EVRQ[RQ/RQ_Name='MPXBOPSB' or RQ/RQ_Name='MPOCPRY2'][1]")[0].xpath("RQ/RQ_Execution_Time")[0].text.split("=")[1]
        playback_mean_link_id = "playback_mean_" + playback_mean_stop

        # Playback_Type event
        playback_type_event = {
            "gauge": {
                "insertion_type": "ERASE_and_REPLACE",
                "name": "PLANNED_PLAYBACK_TYPE_" + playback_type,
                "system": satellite
            },
            "start": playback_type_start,
            "stop": playback_type_stop,
            "links": [
                {
                    "link": playback_mean_link_id,
                    "link_mode": "by_ref",
                    "name": "PLANNED_PLAYBACK_MEAN",
                    "back_ref": "PLANNED_PLAYBACK_TYPE"
                }
            ],
            "values": [{
                "name": "values",
                "type": "object",
                "values": [
                    {"name": "start_request",
                     "type": "text",
                     "value": playback_type_start_request},
                    {"name": "stop_request",
                     "type": "text",
                     "value": playback_type_stop_request},
                    {"name": "start_orbit",
                     "type": "double",
                     "value": playback_type_start_orbit},
                    {"name": "start_angle",
                     "type": "double",
                     "value": playback_type_start_angle},
                    {"name": "stop_orbit",
                     "type": "double",
                     "value": playback_type_stop_orbit},
                    {"name": "stop_angle",
                     "type": "double",
                     "value": playback_type_stop_angle},
                    {"name": "satellite",
                     "type": "text",
                     "value": satellite}
                ]
            }]
        }

        parameters = []
        playback_type_event["values"][0]["values"].append(
            {"name": "parameters",
             "type": "object",
             "values": parameters},
        )
        if playback_type == "HKTM_SAD":
            parameters.append(
                {"name": "MEM_FRHK",
                 "type": "double",
                 "value": playback_type_start_operation.xpath("RQ/List_of_RQ_Parameters/RQ_Parameter[RQ_Parameter_Name = 'MEM_FRHK']/RQ_Parameter_Value")[0].text},
            )
            parameters.append(
                {"name": "MEM_FSAD",
                 "type": "double",
                 "value": playback_type_start_operation.xpath("RQ/List_of_RQ_Parameters/RQ_Parameter[RQ_Parameter_Name = 'MEM_FSAD']/RQ_Parameter_Value")[0].text},
            )
        # end if
        if playback_type in ["HKTM", "SAD", "NOMINAL", "REGULAR", "NRT", "RT"]:
            parameters.append(
                {"name": "MEM_FREE",
                 "type": "double",
                 "value": playback_type_start_operation.xpath("RQ/List_of_RQ_Parameters/RQ_Parameter[RQ_Parameter_Name = 'MEM_FREE']/RQ_Parameter_Value")[0].text},
            )
        # end if
        if playback_type in ["NOMINAL", "REGULAR", "NRT"]:
            parameters.append(
                {"name": "SCN_DUP",
                 "type": "double",
                 "value": playback_type_stop_operation.xpath("RQ/List_of_RQ_Parameters/RQ_Parameter[RQ_Parameter_Name = 'SCN_DUP']/RQ_Parameter_Value")[0].text},
            )
            parameters.append(
                {"name": "SCN_RWD",
                 "type": "double",
                 "value": playback_type_stop_operation.xpath("RQ/List_of_RQ_Parameters/RQ_Parameter[RQ_Parameter_Name = 'SCN_RWD']/RQ_Parameter_Value")[0].text},
            )
        # end if
        if playback_type == "RT":
            parameters.append(
                {"name": "SCN_DUP_START",
                 "type": "double",
                 "value": playback_type_start_operation.xpath("RQ/List_of_RQ_Parameters/RQ_Parameter[RQ_Parameter_Name = 'SCN_DUP']/RQ_Parameter_Value")[0].text},
            )
            parameters.append(
                {"name": "SCN_DUP_STOP",
                 "type": "double",
                 "value": playback_type_stop_operation.xpath("RQ/List_of_RQ_Parameters/RQ_Parameter[RQ_Parameter_Name = 'SCN_DUP']/RQ_Parameter_Value")[0].text},
            )
            parameters.append(
                {"name": "SCN_RWD",
                 "type": "double",
                 "value": playback_type_stop_operation.xpath("RQ/List_of_RQ_Parameters/RQ_Parameter[RQ_Parameter_Name = 'SCN_RWD']/RQ_Parameter_Value")[0].text},
            )
        # end if

        # Insert playback_type_event
        ingestion.insert_event_for_ingestion(playback_type_event, source, list_of_events)

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
    parsed_xml = etree.parse(file_path)
    xpath_xml = etree.XPathEvaluator(parsed_xml)

    satellite = file_name[0:3]
    generation_time = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Source/Creation_Date")[0].text.split("=")[1]
    validity_start = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Start")[0].text.split("=")[1]
    validity_stop = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Stop")[0].text.split("=")[1]
    deletion_queue = xpath_xml("/Earth_Explorer_File/Data_Block/List_of_EVRQs/EVRQ[RQ/RQ_Name='MGSYQDEL']")
    if len(deletion_queue) == 1:
        validity_start = deletion_queue[0].xpath("RQ/RQ_Execution_Time")[0].text.split("=")[1]
    # end if

    source = {
        "name": file_name,
        "generation_time": generation_time,
        "validity_start": validity_start,
        "validity_stop": validity_stop
    }

    # Generate record events
    _generate_record_events(xpath_xml, source, list_of_events)

    # Generate playback events
    _generate_playback_events(xpath_xml, source, list_of_events)

    # Generate idle events
    _generate_idle_events(xpath_xml, source, list_of_events)

    # Build the xml
    data = {"operations": [{
        "mode": "insert_and_erase",
        "dim_signature": {
            "name": "NPPF_" + satellite,
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
    This will be not here. After all the needs are identified, the ingestions a priori will have only the method process_file and this method will be called from outside
    """
    args_parser = argparse.ArgumentParser(description='Process NPPFs.')
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
    # the file following a schema. Schema not available for NPPFs

    returned_value = command_process_file(file_path, output_path)
    
    logger.info("The ingestion has been performed and the exit status is {}".format(returned_value))
