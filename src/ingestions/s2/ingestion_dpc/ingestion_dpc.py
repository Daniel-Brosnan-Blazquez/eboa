"""
Ingestion module for the DPC files of Sentinel-2

Written by DEIMOS Space S.L. (femd)

module eboa
"""

# Import python utilities
import os
import argparse
from dateutil import parser
import datetime
import json
import sys
import pdb
from tempfile import mkstemp

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

def L0_L1B_processing(satellite, source, engine, query, granule_timeline,list_of_events,ds_output, granule_timeline_per_detector, processing_planning_completeness_operation, processing_received_completeness_operation, level, system):
    """
    Method to generate the events for the levels L0 and L1B
    :param satellite: corresponding satellite
    :type source: str
    :param source: information of the source
    :type source: dict
    :param engine: object to access the engine of the EBOA
    :type engine: Engine
    :param query: object to access the query interface of the EBOA
    :type query: Query
    :param granule_timeline: list of granule segments to be processed
    :type granule_timeline: list
    :param list_of_events: list to store the events to be inserted into the eboa
    :type list_of_events: list
    :param ds_output: datastrip
    :type ds_output: str
    :param granule_timeline_per_detector: dict containing the granule segments per detector
    :type granule_timeline_per_detector: dict
    :param processing_planning_completeness_operation: operation containing the events used to determine planning completeness
    :type processing_planning_completeness_operation: dict
    :param processing_received_completeness_operation: operation containing the events used to determine received imaging completeness
    :type processing_received_completeness_operation: dict
    :param level: level of the outputs being processed
    :type level: str
    :param system: center where data has been processed
    :type system: str

    :return: None

    """
    granule_timeline_sorted = date_functions.sort_timeline_by_start(granule_timeline)
    datablocks = date_functions.merge_timeline(granule_timeline_sorted)
    #Obtain the gaps from the processing per detector
    data_gaps = {}
    for detector in granule_timeline_per_detector:
        granule_timeline_per_detector[detector] = date_functions.sort_timeline_by_start(granule_timeline_per_detector[detector])
        granule_timeline_per_detector[detector] = date_functions.merge_timeline(granule_timeline_per_detector[detector])
        if detector not in data_gaps:
            data_gaps[detector] = {}
        #end if
        data_gaps[detector] = date_functions.difference_timelines(granule_timeline_per_detector[detector],datablocks)
    #end for

    #Obtain the gaps existing during the reception per detector
    if len(datablocks) > 0:
        isp_gaps = query.get_linked_events_join(gauge_name_like = {"str": "ISP_GAP", "op": "like"},
        gauge_systems = {"list": [satellite], "op": "in"},
        start_filters = [{"date": str(datablocks[-1]["stop"]), "op": "<"}],
        stop_filters = [{"date": str(datablocks[0]["start"]), "op": ">"}],
        link_names = {"list": ["TIME_CORRECTION"], "op": "in"},
        return_prime_events = False)["linked_events"]
        data_isp_gaps = {}
        for gap in isp_gaps:
            if gap.detector not in data_isp_gaps:
                data_isp_gaps[gap.detector] = {}
            #end if
            data_isp_gaps[gap.detector].append(gap)
        #end for

        for detector in data_isp_gaps:
            data_isp_gaps[detector] = date_functions.convert_eboa_events_to_date_segments(data_isp_gaps[detector])
            data_isp_gaps[detector] = date_functions.merge_timeline(data_isp_gaps[detector])
        #end for

        for datablock in datablocks:
            db_tl = []
            db_tl.append(datablock)
            status = "COMPLETE"
            gaps_datablock = {}
            isp_gaps_datablock = {}

            for detector in data_gaps:
                if detector not in gaps_datablock:
                    gaps_datablock[detector] = {}
                #end if
                gaps_datablock[detector] = date_functions.intersect_timelines(data_gaps[detector],datablocks)
                #If gaps in the datablock, the status is incomplete
                if(len(gaps_datablock[detector]) > 0):
                    status="INCOMPLETE"
                #end if
            #end for
            #Obtain the isp_gaps for this datablock
            for detector in data_isp_gaps:
                if detector not in isp_gaps_datablock:
                    isp_gaps_datablock = {}
                #end if
                isp_gaps_datablock[detector] = date_functions.intersect_timelines(data_isp_gaps[detector],datablock)
            #end for


            #Obtain the planned imaging
            planned_imaging = query.get_linked_events_join(gauge_name_like = {"str": "PLANNED_CUT_IMAGING_%_CORRECTION", "op": "like"},
            gauge_systems = {"list": [satellite], "op": "in"},
            start_filters = [{"date": str(datablock["stop"]), "op": "<"}],
            stop_filters = [{"date": str(datablock["start"]), "op": ">"}],
            link_names = {"list": ["TIME_CORRECTION"], "op": "in"},
            return_prime_events = False)["linked_events"]
            #Planning completeness
            if len(planned_imaging) is not 0:
                planned_imaging_timeline = date_functions.convert_eboa_events_to_date_segments(planned_imaging)
                start_period = planned_imaging[0].start
                stop_period = planned_imaging[0].stop
                value = {
                    "name": "processing_completeness_" + level+ "_began",
                    "type": "object",
                    "values": []
                }
                planned_imaging_uuid = planned_imaging[0].event_uuid
                exit_status = engine.insert_event_values(planned_imaging_uuid, value)
                if exit_status["inserted"] == True:
                    # Insert the linked COMPLETENESS event for the automatic completeness check
                    planning_event_values = planned_imaging[0].get_structured_values()
                    planning_event_values[0]["values"] = planning_event_values[0]["values"] + [
                        {"name": "status",
                         "type": "text",
                         "value": "MISSING"}
                    ]

                    processing_planning_completeness_operation["events"].append({
                        "gauge": {
                                "insertion_type": "SIMPLE_UPDATE",
                            "name": "PLANNED_IMAGING_" + level + "_COMPLETENESS",
                            "system": satellite
                        },
                        "start": str(planned_imaging[0].start),
                        "stop": str(planned_imaging[0].stop),
                        "links": [
                            {
                                "link": str(planned_imaging_uuid),
                                "link_mode": "by_uuid",
                                "name": "PROCESSING_COMPLETENESS",
                                "back_ref": "PLANNED_IMAGING"
                            }],
                        "values": planning_event_values
                    })

                    event_link_ref = "PROCESSING_VALIDITY_" + datablock["start"].isoformat()
                    #Create gap events
                    for detector in data_gaps:
                        for gap in gaps_datablock[detector]:
                            gap_source = "processing"
                            #Check if the gap already existed in the received imaging
                            if detector in isp_gaps_datablock and gap in date_functions.intersect_timelines(gaps_datablock[detector],isp_gaps_datablock[detector]):
                                gap_source = "reception"
                            #end if

                            event_gap = {
                                "explicit_reference": ds_output,
                                "gauge": {
                                    "insertion_type": "SIMPLE_UPDATE",
                                    "name": "PROCESSING_GAP_" + level,
                                    "system": system
                                },
                                "links": [{
                                         "link": str(planned_imaging_uuid),
                                         "link_mode": "by_uuid",
                                         "name": "PROCESSING_GAP",
                                         "back_ref": "PLANNED_IMAGING"
                                         },{
                                         "link": event_link_ref,
                                         "link_mode": "by_ref",
                                         "name": "PROCESSING_GAP",
                                         "back_ref": "PROCESSING_VALIDITY"
                                         }
                                     ],
                                 "start": str(gap["start"]),
                                 "stop": str(gap["stop"]),
                                 "values": [{
                                     "name": "details",
                                     "type": "object",
                                     "values": [{
                                        "type": "text",
                                        "value": detector,
                                        "name": "detector"
                                        },{
                                        "type": "text",
                                        "value": gap_source,
                                        "name": "source"
                                    }]
                                 }]
                            }
                            list_of_events.append(event_gap)
                        #end for
                    #end for

                    datablock_completeness_event = {
                        "explicit_reference": ds_output,
                        "gauge": {
                            "insertion_type": "INSERT_and_ERASE_per_EVENT",
                            "name": "PLANNED_IMAGING_" + level + "_COMPLETENESS",
                            "system": satellite
                        },
                        "links": [{
                                     "link": str(planned_imaging_uuid),
                                     "link_mode": "by_uuid",
                                     "name": "PROCESSING_COMPLETENESS",
                                     "back_ref": "PLANNED_IMAGING"
                                 },
                                 {
                                     "link": event_link_ref,
                                     "link_mode": "by_ref",
                                     "name": "COMPLETENESS",
                                     "back_ref": "PROCESSING_VALIDITY"
                                 }
                             ],
                         "start": str(datablock["start"]),
                         "stop": str(datablock["stop"]),
                         "values": [{
                             "name": "details",
                             "type": "object",
                             "values": [{
                                "type": "text",
                                "value": status,
                                "name": "status"
                             }]
                         }]
                    }
                    list_of_events.append(datablock_completeness_event)

                    validity_event = {
                        "link_ref": event_link_ref,
                        "explicit_reference": ds_output,
                        "gauge": {
                            "insertion_type": "SIMPLE_UPDATE",
                            "name": "PROCESSING_VALIDITY_" + level,
                            "system": system
                        },
                        "links": [{
                                 "link": str(planned_imaging_uuid),
                                 "link_mode": "by_uuid",
                                 "name": "PROCESSING_VALIDITY",
                                 "back_ref": "PLANNED_IMAGING"
                                 }],
                         "start": str(datablock["start"]),
                         "stop": str(datablock["stop"]),
                         "values": [{
                             "name": "details",
                             "type": "object",
                             "values": [{
                                "type": "text",
                                "value": status,
                                "name": "status"
                             }],
                         }]
                    }
                    list_of_events.append(validity_event)
                #end if
            #end if

            #Obtain received imaging events
            received_imaging = query.get_events_join(gauge_name_like = {"str":"ISP_VALIDITY", "op":"like"},
            start_filters = [{"date": str(datablock["stop"]), "op": "<"}],
            stop_filters = [{"date": str(datablock["start"]), "op": ">"}])
            #Received Imaging Completeness
            if len(received_imaging) is not 0:
                received_imaging_timeline = date_functions.convert_eboa_events_to_date_segments(received_imaging)
                start_period =received_imaging[0].start
                stop_period = received_imaging[0].stop
                value = {
                    "name": "received_imaging_completeness_" + level+ "_began",
                    "type": "object",
                    "values": []
                }
                received_imaging_uuid = received_imaging[0].event_uuid
                exit_status = engine.insert_event_values(received_imaging_uuid, value)
                if exit_status["inserted"] == True:
                    # Insert the linked COMPLETENESS event for the automatic completeness check
                    received_event_values = received_imaging[0].get_structured_values()
                    received_event_values[0]["values"] = received_event_values[0]["values"] + [
                        {"name": "status",
                         "type": "text",
                         "value": "MISSING"}
                    ]

                    processing_received_completeness_operation["events"].append({
                        "gauge": {
                                "insertion_type": "SIMPLE_UPDATE",
                            "name": "RECEIVED_IMAGING_" + level + "_COMPLETENESS",
                            "system": satellite
                        },
                        "start": str(received_imaging[0].start),
                        "stop": str(received_imaging[0].stop),
                        "links": [
                            {
                                "link": str(received_imaging_uuid),
                                "link_mode": "by_uuid",
                                "name": "PROCESSING_COMPLETENESS",
                                "back_ref": "RECEIVED_IMAGING"
                            }],
                        "values": received_event_values
                    })

                    event_link_ref = "PROCESSING_VALIDITY_" + datablock["start"].isoformat()

                    datablock_completeness_event = {
                        "explicit_reference": ds_output,
                        "gauge": {
                            "insertion_type": "INSERT_and_ERASE_per_EVENT",
                            "name": "RECEIVED_IMAGING_" + level + "_COMPLETENESS",
                            "system": satellite
                        },
                        "links": [{
                                     "link": str(received_imaging_uuid),
                                     "link_mode": "by_uuid",
                                     "name": "PROCESSING_COMPLETENESS",
                                     "back_ref": "RECEIVED_IMAGING"
                                 },
                                 {
                                     "link": event_link_ref,
                                     "link_mode": "by_ref",
                                     "name": "COMPLETENESS",
                                     "back_ref": "PROCESSING_VALIDITY"
                                 }
                             ],
                         "start": str(datablock["start"]),
                         "stop": str(datablock["stop"]),
                         "values": [{
                             "name": "details",
                             "type": "object",
                             "values": [{
                                "type": "text",
                                "value": status,
                                "name": "status"
                             }]
                         }]
                    }
                    list_of_events.append(datablock_completeness_event)
                #end if
            #end if
        #end for
    #end if
#end if

def L1C_L2A_processing(satellite, source, engine, query, list_of_events, processing_validity_events,ds_output, processing_planning_completeness_operation, processing_received_completeness_operation, level, system):
    """
    Method to generate the events for the levels L1C and L2A
    :param satellite: corresponding satellite
    :type source: str
    :param source: information of the source
    :type source: dict
    :param engine: object to access the engine of the EBOA
    :type engine: Engine
    :param query: object to access the query interface of the EBOA
    :type query: Query
    :param list_of_events: list to store the events to be inserted into the eboa
    :type list_of_events: list
    :param processing_validity_events: dict containing the events linked to the sensing date from the datablock analysed
    :type processing_validity_events: dict
    :param ds_output: datastrip
    :type ds_output: str
    :param processing_planning_completeness_operation: operation containing the events used to determine completeness
    :type processing_planning_completeness_operation: dict
    :param processing_received_completeness_operation: operation containing the events used to determine received imaging completeness
    :type processing_received_completeness_operation: dict
    :param level: level of the outputs being processed
    :type level: str
    :param system: center where data has been processed
    :type system: str

    :return: None

    """
    gaps = []
    planned_imagings = []
    planned_cut_imagings = []
    received_imagings = []
    status = "COMPLETE"
    #Classify the events obtained from the datatrip linked events
    if len(processing_validity_events["prime_events"]) > 0:
        processing_validity_event = processing_validity_events["prime_events"][0]

        for event in processing_validity_events["linked_events"]:
            if event.gauge.name.startswith("PROCESSING_GAP"):
                gaps.append(event)
            #end if
            elif event.gauge.name.startswith("PLANNED_IMAGING"):
                planned_imagings.append(event)
            #end elif
            elif event.gauge.name.startswith("PLANNED_CUT_IMAGING"):
                planned_cut_imagings.append(event)
            #end elif
            elif event.gauge.name.startswith("RECEIVED_IMAGING"):
                received_imagings.append(event)
        #end for

        #If gaps, status is incomplete
        if len(gaps) > 0:
            status = "INCOMPLETE"
        #end if

        #Planned completeness
        if len(planned_cut_imagings) is not 0:
            planned_imaging_timeline = date_functions.convert_eboa_events_to_date_segments(planned_cut_imagings)
            start_period = planned_cut_imagings[0].start
            stop_period = planned_cut_imagings[0].stop
            value = {
                "name": "processing_completeness_" + level + "_began",
                "type": "object",
                "values": []
            }
            planned_imaging_uuid = planned_cut_imagings[0].event_uuid
            exit_status = engine.insert_event_values(planned_imaging_uuid, value)
            if exit_status["inserted"] == True:
                planning_event_values = planned_cut_imagings[0].get_structured_values()
                planning_event_values[0]["values"] = planning_event_values[0]["values"] + [
                    {"name": "status",
                     "type": "text",
                     "value": "MISSING"}
                ]

                processing_planning_completeness_operation["events"].append({
                    "gauge": {
                            "insertion_type": "SIMPLE_UPDATE",
                        "name": "PLANNED_IMAGING_" + level + "_COMPLETENESS",
                        "system": satellite
                    },
                    "start": str(planned_cut_imagings[0].start),
                    "stop": str(planned_cut_imagings[0].stop),
                    "links": [
                        {
                            "link": str(planned_imaging_uuid),
                            "link_mode": "by_uuid",
                            "name": "PROCESSING_COMPLETENESS",
                            "back_ref": "PLANNED_IMAGING"
                        }],
                    "values": planning_event_values
                })

                event_link_ref = "PROCESSING_VALIDITY_" + processing_validity_event.start.isoformat()

                for gap in gaps:
                    event_gap = {
                        "explicit_reference": ds_output,
                        "gauge": {
                            "insertion_type": "SIMPLE_UPDATE",
                            "name": "PROCESSING_GAP_" + level,
                            "system": system
                        },
                        "links": [{
                                 "link": str(planned_imaging_uuid),
                                 "link_mode": "by_uuid",
                                 "name": "PROCESSING_GAP",
                                 "back_ref": "PLANNED_IMAGING"
                                 },{
                                 "link": event_link_ref,
                                 "link_mode": "by_ref",
                                 "name": "PROCESSING_GAP",
                                 "back_ref": "PROCESSING_VALIDITY"
                                 }
                             ],
                         "start": str(gap.start),
                         "stop": str(gap.start),
                         "values": gap.get_structured_values()
                    }
                    list_of_events.append(event_gap)
                #end for
                datablock_completeness_event = {
                    "explicit_reference": ds_output,
                    "gauge": {
                        "insertion_type": "INSERT_and_ERASE_per_EVENT",
                        "name": "PLANNED_IMAGING_" + level + "_COMPLETENESS",
                        "system": satellite
                    },
                    "links": [{
                                 "link": str(planned_imaging_uuid),
                                 "link_mode": "by_uuid",
                                 "name": "PROCESSING_COMPLETENESS",
                                 "back_ref": "PLANNED_IMAGING"
                             },
                             {
                                 "link": event_link_ref,
                                 "link_mode": "by_ref",
                                 "name": "COMPLETENESS",
                                 "back_ref": "PROCESSING_VALIDITY"
                             }
                         ],
                     "start": processing_validity_event.start.isoformat(),
                     "stop": processing_validity_event.stop.isoformat(),
                     "values": [{
                         "name": "details",
                         "type": "object",
                         "values": [{
                            "type": "text",
                            "value": status,
                            "name": "status"
                         }]
                    }]
                }
                list_of_events.append(datablock_completeness_event)

                validity_event = {
                    "link_ref": event_link_ref,
                    "explicit_reference": ds_output,
                    "gauge": {
                        "insertion_type": "SIMPLE_UPDATE",
                        "name": "PROCESSING_VALIDITY_" + level,
                        "system": system
                    },
                    "links": [{
                             "link": str(planned_imaging_uuid),
                             "link_mode": "by_uuid",
                             "name": "PROCESSING_VALIDITY",
                             "back_ref": "PLANNED_IMAGING"
                             }],
                    "start": processing_validity_event.start.isoformat(),
                    "stop": processing_validity_event.stop.isoformat(),
                     "values": planned_cut_imagings[0].get_structured_values()
                }
                list_of_events.append(validity_event)
            #end if
        #end if

        #Received Imaging Completeness
        if len(received_imagings) is not 0:
            received_imaging_timeline = date_functions.convert_eboa_events_to_date_segments(received_imagings)
            start_period = received_imagings[0].start
            stop_period = received_imagings[0].stop
            value = {
                "name": "received_completeness_" + level + "_began",
                "type": "object",
                "values": []
            }
            received_imaging_uuid = received_imagings[0].event_uuid
            exit_status = engine.insert_event_values(received_imaging_uuid, value)
            if exit_status["inserted"] == True:
                received_event_values = received_imagings[0].get_structured_values()
                received_event_values[0]["values"] = received_event_values[0]["values"] + [
                    {"name": "status",
                     "type": "text",
                     "value": "MISSING"}
                ]

                processing_received_completeness_operation["events"].append({
                    "gauge": {
                            "insertion_type": "SIMPLE_UPDATE",
                        "name": "RECEIVED_IMAGING_" + level + "_COMPLETENESS",
                        "system": satellite
                    },
                    "start": str(received_imagings[0].start),
                    "stop": str(received_imagings[0].stop),
                    "links": [
                        {
                            "link": str(received_imaging_uuid),
                            "link_mode": "by_uuid",
                            "name": "PROCESSING_COMPLETENESS",
                            "back_ref": "PLANNED_IMAGING"
                        }],
                    "values": received_event_values
                })

                event_link_ref = "PROCESSING_VALIDITY_" + processing_validity_event.start.isoformat()

                isp_completeness_event = {
                    "explicit_reference": ds_output,
                    "gauge": {
                        "insertion_type": "INSERT_and_ERASE_per_EVENT",
                        "name": "RECEIVED_IMAGING_" + level + "_COMPLETENESS",
                        "system": satellite
                    },
                    "links": [{
                                 "link": str(received_imaging_uuid),
                                 "link_mode": "by_uuid",
                                 "name": "PROCESSING_COMPLETENESS",
                                 "back_ref": "RECEIVED_IMAGING"
                             },
                             {
                                 "link": event_link_ref,
                                 "link_mode": "by_ref",
                                 "name": "COMPLETENESS",
                                 "back_ref": "PROCESSING_VALIDITY"
                             }
                         ],
                     "start": processing_validity_event.start.isoformat(),
                     "stop": processing_validity_event.stop.isoformat(),
                     "values": [{
                         "name": "details",
                         "type": "object",
                         "values": [{
                            "type": "text",
                            "value": status,
                            "name": "status"
                         }]
                    }]
                }
                list_of_events.append(isp_completeness_event)
            #end if
        #end if

    #end if


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
    file_name = os.path.basename(file_path)

    # Remove namespaces
    (_, new_file_path) = new_file = mkstemp()
    ingestion.remove_namespaces(file_path, new_file_path)

    # Parse file
    parsed_xml = etree.parse(new_file_path)
    xpath_xml = etree.XPathEvaluator(parsed_xml)

    list_of_explicit_references = []
    list_of_annotations = []
    list_of_events = []
    list_of_timelines = []
    list_of_configuration_events = []
    list_of_configuration_explicit_references = []
    list_of_operations = []
    flag_query = False

    #Obtain the satellite
    satellite = file_name[0:3]
    #Obtain the station
    system = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Source/System")[0].text
    #Obtain the creation date
    creation_date = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Source/Creation_Date")[0].text.split("=")[1]
    #Obtain the validity start
    validity_start = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Start")[0].text.split("=")[1]
    #Obtain the validity stop
    validity_stop = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Stop")[0].text.split("=")[1]
    #Obtain the workplan current status
    workplan_current_status = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SUPERVISION_INFO/WORKPLAN_CURRENT_STATUS")[0].text
    #Obtain the workplan message
    workplan_message = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SUPERVISION_INFO/WORKPLAN_MESSAGE")[0].text
    #Obtain the workplan start datetime
    workplan_start_datetime = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SUPERVISION_INFO/WORKPLAN_START_DATETIME")[0].text
    #Obtain the workplan end datetime
    workplan_end_datetime = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SUPERVISION_INFO/WORKPLAN_END_DATETIME")[0].text
    #Obtain a list of the mrfs
    mrf_list = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SYNTHESIS_INFO/Product_Report/List_Of_MRFs/MRF")
    #Obtain a list of the steps
    steps_list = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/DATA/STEP_INFO")
    #Source for the main operation
    source = {
        "name": file_name,
        "generation_time": creation_date,
        "validity_start": validity_start,
        "validity_stop": validity_stop
    }


    # Completeness operations for the completeness analysis of the plan
    processing_planning_completeness_operation = {
        "mode": "insert",
        "dim_signature": {
            "name": "PROCESSING_" + satellite,
            "exec": "processing_planning_" + os.path.basename(__file__),
            "version": version
        },
        "events": []
    }
    # Completeness operations for the completeness analysis of the received imaging
    processing_received_completeness_operation = {
        "mode": "insert",
        "dim_signature": {
            "name": "PROCESSING_" + satellite,
            "exec": "processing_received_" + os.path.basename(__file__),
            "version": version
        },
        "events": []
    }

    ##Use outputs filtered by datastrip
    #Loop through each output node that contains a datastrip (excluding the auxiliary data)
    for output_msi in xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SYNTHESIS_INFO/Product_Report/*[contains(name(),'Output_Products') and boolean(child::DATA_STRIP_ID)]") :
        granule_timeline_per_detector = {}
        granule_timeline = []
        #Obtain the datastrip
        ds_output = output_msi.find("DATA_STRIP_ID").text
        #Obtain the sensing date from the datastrip
        output_sensing_date = ds_output[41:57]
        #Obtain the baseline from the datastrip
        baseline = ds_output[58:]
        #Obtain the production level from the datastrip
        level = ds_output[13:16].replace("_","")

        #Obtain the input datastrip if exists
        input = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SYNTHESIS_INFO/Product_Report/Input_Products/DATA_STRIP_ID")
        if len(input) > 0:
            ds_input = input[0].text
        #end if

        baseline_annotation = {
        "explicit_reference": ds_output,
        "annotation_cnf": {
            "name": "PRODUCTION-BASELINE",
            "system": system
            },
        "values": [{
            "name": "baseline_information",
            "type": "object",
            "values": [
                {"name": "baseline",
                 "type": "text",
                 "value": baseline
                 }]
            }]
        }
        list_of_annotations.append(baseline_annotation)

        explicit_reference = {
           "group": level + "_DS",
           "links": [{
               "back_ref": "INPUT-DATASTRIP",
               "link": "OUTPUT-DATASTRIP",
               "name": ds_input
               }
           ],
           "name": ds_output
        }
        list_of_explicit_references.append(explicit_reference)

        #Loop over each granule in the ouput
        for granule in output_msi.xpath("*[name() = 'GRANULES_ID' and contains(text(),'_GR_')]"):
            #Obtain the granule id
            granule_t = granule.text
            level_gr = granule_t[13:16].replace("_","")
            granule_sensing_date = granule_t[42:57]
            detector = granule_t[59:61]

            #Create a segment for each granule with a margin calculated to get whole scenes
            start= parser.parse(granule_sensing_date)
            stop = start + datetime.timedelta(seconds=5)
            granule_segment = {
                "start": start,
                "stop": stop,
                "id": granule_t
            }

            #Create a dictionary containing all the granules for each detector
            granule_timeline.append(granule_segment)
            if detector not in granule_timeline_per_detector:
                granule_timeline_per_detector[detector] = []
            #end if
            granule_timeline_per_detector[detector].append(granule_segment)
            explicit_reference = {
                "group": level_gr + "_GR",
                "links": [{
                    "back_ref": "DATASTRIP",
                    "link": "GRANULE",
                    "name": ds_output
                    }
                ],
                "name": granule_t
            }
            list_of_explicit_references.append(explicit_reference)
        #end for

        #Loop over each tile in the output
        for tile in output_msi.xpath("*[name() = 'GRANULES_ID' and contains(text(),'_TL_')]"):
            #Obtain the tile id
            tile_t = tile.text
            level_tl = tile_t[13:16]
            level_tl.replace("_","")

            explicit_reference = {
                "group": level_tl + "_TL",
                "links": [{
                    "back_ref": "DATASTRIP",
                    "link": "GRANULE",
                    "name": ds_output
                    }
                ],
                "name": tile_t
            }
            list_of_explicit_references.append(explicit_reference)
        #end for

        #Loop over each TCI in the ouput
        for true_color in output_msi.xpath("*[name() = 'GRANULES_ID' and contains(text(),'_TC_')]"):
            #Obtain the true color imaging id
            true_color_t = true_color.text
            level_tc = true_color_t[13:16]
            level_tc.replace("_","")

            explicit_reference = {
                "group": level_tc + "_TC",
                "links": [{
                    "back_ref": "DATASTRIP",
                    "link": "GRANULE",
                    "name": ds_output
                    }
                ],
                "name": true_color_t
            }
            list_of_explicit_references.append(explicit_reference)
        #end_for

        #If the query has not been executed yet
        if flag_query is False:
            processing_validity_events = query.get_linked_events_join(gauge_names = {"list": ["PROCESSING_VALIDITY_L1B","PROCESSING_VALIDITY_L0"], "op": "in"},
            gauge_systems = {"list": [system], "op": "in"},
            explicit_ref_like = {"str": "%" + output_sensing_date + "%", "op": "like"},
            return_prime_events = True)
        #end if
        if level == "L1B" or level == "L0":
            L0_L1B_processing(satellite, source, engine, query, granule_timeline,list_of_events,ds_output,granule_timeline_per_detector, processing_planning_completeness_operation, processing_received_completeness_operation, level, system)
        #end if
        elif (level == "L1C" or level == "L2A") and flag_query is False:
            flag_query = True
            L1C_L2A_processing(satellite, source, engine, query, list_of_events, processing_validity_events, ds_output, processing_planning_completeness_operation, processing_received_completeness_operation, level, system)
        #end elif

        if len(processing_planning_completeness_operation["events"]) > 0:
            completeness_event_starts = [event["start"] for event in processing_planning_completeness_operation["events"]]
            completeness_event_starts.sort()
            completeness_event_stops = [event["stop"] for event in processing_planning_completeness_operation["events"]]
            completeness_event_stops.sort()

            #Source for the completeness planning operation adjusting the validity to the events
            processing_planning_completeness_operation["source"] = {
            "name": source["name"],
            "generation_time": source["generation_time"],
            "validity_start": str(completeness_event_starts[0]),
            "validity_stop": str(completeness_event_stops[-1])
            }

            list_of_operations.append(processing_planning_completeness_operation)
        #end if

        if len(processing_received_completeness_operation["events"]) > 0:
            completeness_event_starts = [event["start"] for event in processing_received_completeness_operation["events"]]
            completeness_event_starts.sort()
            completeness_event_stops = [event["stop"] for event in processing_received_completeness_operation["events"]]
            completeness_event_stops.sort()

            #Source for the completeness received imaging operation adjusting the validity to the events
            processing_received_completeness_operation["source"] = {
                "name": source["name"],
                "generation_time": source["generation_time"],
                "validity_start": str(completeness_event_starts[0]),
                "validity_stop": str(completeness_event_stops[-1])
            }

            list_of_operations.append(processing_received_completeness_operation)
        #end if

        event_timeliness = {
            "explicit_reference": ds_output,
            "gauge": {
                "insertion_type": "SIMPLE_UPDATE",
                "name": "TIMELINESS",
                "system": system
            },
            "start": steps_list[0].find("PROCESSING_START_DATETIME").text[:-1],
            "stop": steps_list[-1].find("PROCESSING_END_DATETIME").text[:-1],
            "links": [],
            "values": [{
                "name": "details",
                "type": "object",
                "values": []
            }]
        }
        list_of_events.append(event_timeliness)

        #Steps
        for step in steps_list:
            if step.find("EXEC_STATUS").text == 'COMPLETED':
                event_step = {
                    "explicit_reference": ds_output,
                    "gauge": {
                        "insertion_type": "SIMPLE_UPDATE",
                        "name": "STEP-INFO",
                        "system": system
                    },
                    "start": step.find("PROCESSING_START_DATETIME").text[:-1],
                    "stop": step.find("PROCESSING_END_DATETIME").text[:-1],
                    "links": [],
                    "values": [{
                        "name": "details",
                        "type": "object",
                        "values": [{
                                   "name": "id",
                                   "type": "text",
                                   "value": step.get("id")
                                   },{
                                   "name": "exec_mode",
                                   "type": "text",
                                   "value": step.find("SUBSYSTEM_INFO/STEP_REPORT/GENERAL_INFO/EXEC_MODE").text
                        }]
                    }]
                }
                list_of_events.append(event_step)
            #end if
        #end for

        for mrf in mrf_list:
            explicit_reference = {
                "group": "MISSION_CONFIGURATION",
                "links": [{
                    "back_ref": "DATASTRIP",
                    "link": "CONFIGURATION",
                    "name": ds_output
                    }
                ],
                "name": mrf.find("Id").text
            }
            list_of_configuration_explicit_references.append(explicit_reference)
        #end for
    #end for

    for mrf in mrf_list:
        #Only if the mrf does not exist in the DB
        mrfsDB = query.get_events_join(explicit_ref_like = {"op": "like", "str": mrf.find("Id").text})
        if len(mrfsDB) is 0:
            #If the date is correct, else the date is set to a maximum value
            try:
                stop = str(parser.parse(mrf.find("ValidityStop").text[:-1]))
            #end if
            except:
                stop = str(datetime.datetime.max)
            #end except
            event_mrf={
                "key":mrf.find("Id").text,
                "explicit_reference": mrf.find("Id").text,
                "gauge": {
                    "insertion_type": "EVENT_KEYS",
                    "name": "MRF-VALIDITY",
                    "system": system
                },
                "start": mrf.find("ValidityStart").text[:-1],
                "stop": stop,
                "links": [],
                "values": [{
                    "name": "details",
                    "type": "object",
                    "values": [{
                          "name": "generation_time",
                          "type": "timestamp",
                          "value": mrf.find("Id").text[25:40]
                          }]
                    }]
                }
            list_of_configuration_events.append(event_mrf)
        #end if
    #end for

    #Adjust the validity period to the events in the operation
    if len(list_of_events) > 0:
        event_starts = [event["start"] for event in list_of_events]
        event_starts.sort()
        if source["validity_start"] > event_starts[0]:
            source["validity_start"] = parser.parse(event_starts[0]).isoformat()
        #end if
        event_stops = [event["stop"] for event in list_of_events]
        event_stops.sort()
        if source["validity_stop"] < event_stops[-1]:
            source["validity_stop"] = parser.parse(event_stops[-1]).isoformat()
        #end if
     #end if

    list_of_operations.append({
        "mode": "insert",
        "dim_signature": {
              "name": "PROCESSING_" + satellite,
              "exec": os.path.basename(__file__),
              "version": version
        },
        "source": source,
        "annotations": list_of_annotations,
        "explicit_references": list_of_explicit_references,
        "events": list_of_events,
        })
    list_of_operations.append({
        "mode": "insert",
        "dim_signature": {
              "name": "PROCESSING_"  + satellite,
              "exec": "configuration_" + os.path.basename(__file__),
              "version": version
        },
        "source": {
            "name": file_name,
            "generation_time": creation_date,
            "validity_start": str(datetime.datetime.min),
            "validity_stop": str(datetime.datetime.max)
        },
        "explicit_references": list_of_configuration_explicit_references,
        "events": list_of_configuration_events,
    })

    data = {"operations": list_of_operations}
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
    # the file following a schema. Schema not available for STNACQs

    returned_value = command_process_file(file_path, output_path)

    logger.info("The ingestion has been performed and the exit status is {}".format(returned_value))
