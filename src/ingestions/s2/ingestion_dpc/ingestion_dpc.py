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
    satellite = file_name[0:3]
    system = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Source/System")[0].text

    creation_date = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Source/Creation_Date")[0].text.split("=")[1]
    validity_start = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Start")[0].text.split("=")[1]
    validity_stop = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Stop")[0].text.split("=")[1]

    workplan_current_status = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SUPERVISION_INFO/WORKPLAN_CURRENT_STATUS")[0].text
    workplan_message = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SUPERVISION_INFO/WORKPLAN_MESSAGE")[0].text
    workplan_start_datetime = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SUPERVISION_INFO/WORKPLAN_START_DATETIME")[0].text
    workplan_end_datetime = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SUPERVISION_INFO/WORKPLAN_END_DATETIME")[0].text

    mrf_list = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SYNTHESIS_INFO/Product_Report/List_Of_MRFs/MRF")
    steps_list = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/DATA/STEP_INFO")
    source = {
        "name": file_name,
        "generation_time": creation_date,
        "validity_start": validity_start,
        "validity_stop": validity_stop
    }

    #COMPLETENESS
    # Completeness operations for the production completeness analysis of the plan
    processing_planning_completeness_operation = {
        "mode": "insert",
        "dim_signature": {
            "name": "PROCESSING_" + satellite,
            "exec": "processing_planning_" + os.path.basename(__file__),
            "version": version
        },
        "events": []
    }

    #MSI_OUTPUTS (DATABLOCK)
    ##Use outputs filtered by datastrip
    for output_msi in xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SYNTHESIS_INFO/Product_Report/*[contains(name(),'Output_Products') and boolean(child::DATA_STRIP_ID)]") :
        granule_timeline_per_detector = {}
        granule_timeline = []
        ds_output = output_msi.find("DATA_STRIP_ID").text
        output_sensing_date = ds_output[41:57]
        baseline = ds_output[58:]
        level = ds_output[13:16]

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

        #RELATIONSHIPS
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

        for granule in output_msi.xpath("*[name() = 'GRANULES_ID' and contains(text(),'_GR_')]"):

            granule_t = granule.text
            level_gr = granule_t[13:16].replace("_","")
            granule_sensing_date = granule_t[42:57]
            detector = granule_t[59:61]
            start= parser.parse(granule_sensing_date)
            stop = start + datetime.timedelta(seconds=5)
            granule_segment = {
                "start": start,
                "stop": stop,
                "id": granule_t
            }
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

        # for granule in output_msi.xpath("*[name() = 'GRANULES_ID' and contains(text(),'_TL_')]"):
        #
        #     granule_t = granule.text
        #     level_gr = granule_t[13:16]
        #     level_gr,replace("_","")
        #     SI_start= parser.parse(output_sensing_date[1:])
        #     SI_stop = SI_start + datetime.timedelta(seconds=5)
        # #end for
        #
        # for granule in output_msi.xpath("*[name() = 'GRANULES_ID' and contains(text(),'_TC_')]"):
        #
        #     granule_t = granule.text
        #     level_gr = granule_t[13:16]
        #     level_gr,replace("_","")
        #     SI_start= parser.parse(output_sensing_date[1:])
        #     SI_stop = SI_start + datetime.timedelta(seconds=5)
        # #end_for
        # #END RELATIONSHIPS
        if(len(granule_timeline) > 0):
            granule_timeline_sorted = date_functions.sort_timeline_by_start(granule_timeline)
            for detector in granule_timeline_per_detector:
                granule_timeline_per_detector[detector] = date_functions.sort_timeline_by_start(granule_timeline_per_detector[detector])
                granule_timeline_per_detector[detector] = date_functions.merge_timeline(granule_timeline_per_detector[detector])
            #end for
            datablocks = date_functions.merge_timeline(granule_timeline_sorted)

            status = "todo"
            for datablock in datablocks:

                planned_imaging = query.get_linked_events_join(gauge_name_like = {"str": "PLANNED_CUT_IMAGING_%_CORRECTION", "op": "like"},
                gauge_systems = {"list": [satellite], "op": "in"},
                start_filters = [{"date": str(datablock["stop"]), "op": "<"}],
                stop_filters = [{"date": str(datablock["start"]), "op": ">"}],
                link_names = {"list": ["TIME_CORRECTION"], "op": "in"},
                return_prime_events = False)["linked_events"]

                if planned_imaging is not None:
                    planned_imaging_timeline = date_functions.convert_eboa_events_to_date_segments(planned_imaging)
                    start_period = planned_imaging[0].start
                    stop_period = planned_imaging[0].stop

                    value = {
                        "name": "processing_completeness_began",
                        "type": "object",
                        "values": []
                    }
                    planned_imaging_uuid = planned_imaging[0].event_uuid
                    exit_status = engine.insert_event_values(planned_imaging_uuid, value)

                    if exit_status["inserted"] == True:
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
                                    "name": "PLANNED_PROCESSING",
                                    "back_ref": "COMPLETENESS"
                                }],
                            "values": planning_event_values
                        })

                        event_link_ref = "DATABLOCK_COMPLETENESS_" + datablock["start"].isoformat() + "_" + datablock["stop"].isoformat()
                        datablock_completeness_event = {
                            "link_ref": event_link_ref,
                            "explicit_reference": ds_output,
                            "gauge": {
                                "insertion_type": "INSERT_and_ERASE_per_EVENT",
                                "name": "PLANNED_IMAGING_" + level + "_COMPLETENESS",
                                "system": system
                            },
                            "links": [{
                                     "link": str(planned_imaging_uuid),
                                     "link_mode": "by_uuid",
                                     "name": "PROCESSING_VALIDITY",
                                     "back_ref": "PLANNED_IMAGING"
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
                        datablock_event = {
                            "explicit_reference": ds_output,
                            "gauge": {
                                "insertion_type": "SIMPLE_UPDATE",
                                "name": "DATABLOCK_" + level,
                                "system": system
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
                                     "name": "PROCESSING_VALIDITY",
                                     "back_ref": "COMPLETENESS"
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
                                 }],
                             }]
                        }
                        list_of_events.append(datablock_event)
                    #end if
                #end if
            #end for
        #end if

        if len(processing_planning_completeness_operation["events"]) > 0:
            completeness_event_starts = [event["start"] for event in processing_planning_completeness_operation["events"]]
            completeness_event_starts.sort()
            completeness_event_stops = [event["stop"] for event in processing_planning_completeness_operation["events"]]
            completeness_event_stops.sort()

            processing_planning_completeness_operation["source"] = {
                "name": source["name"],
                "generation_time": source["generation_time"],
                "validity_start": str(completeness_event_starts[0]),
                "validity_stop": str(completeness_event_stops[-1])
            }

            list_of_operations.append(processing_planning_completeness_operation)
        #end if

        #TIMELINESS
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
                #duplicate if product L1A & L1B
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
        #END TIMELINESS

    #end for

    #CONFIGURATION
    for mrf in mrf_list:
        ## Change query to use the query for events
        mrfsDB = query.get_explicit_refs(explicit_ref_like = {"op": "like", "str": mrf.find("Id").text})
        if len(mrfsDB) is 0:
            try:
                stop = str(parser.parse(mrf.find("ValidityStop").text[:-1]))
            except:
                stop = str(datetime.datetime.max)
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
    #end for
    #END CONFIGURATION

    if len(list_of_events) > 0:
        event_starts = [event["start"] for event in list_of_events]
        event_starts.sort()
        if source["validity_start"] > event_starts[0]:
            source["validity_start"] = event_starts[0]
        #end if
        event_stops = [event["stop"] for event in list_of_events]
        event_stops.sort()
        if source["validity_stop"] < event_stops[-1]:
            source["validity_stop"] = event_stops[-1]
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
            #"granule_timeline_per_detector": granule_timeline_per_detector}
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
