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
    test = []
    satellite = file_name[0:3]
    system = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Source/System")[0].text

    creation_date = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Source/Creation_Date")[0].text.split("=")[1]
    validity_start = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Start")[0].text.split("=")[1]
    validity_stop = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Stop")[0].text.split("=")[1]

    workplan_current_status = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SUPERVISION_INFO/WORKPLAN_CURRENT_STATUS")[0].text
    workplan_message = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SUPERVISION_INFO/WORKPLAN_MESSAGE")[0].text
    workplan_start_datetime = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SUPERVISION_INFO/WORKPLAN_START_DATETIME")[0].text
    workplan_end_datetime = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SUPERVISION_INFO/WORKPLAN_END_DATETIME")[0].text

    output = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SYNTHESIS_INFO/Product_Report/*[contains(name(),'Output_Products')]/DATA_STRIP_ID")
    if len(output) > 0:
        ds_output = output[0].text
    else:
        ds_output = ""
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
    completeness_processing_operation = {
        "mode": "insert",
        "dim_signature": {
            "name": "PROCESSING_" + system + "_" + satellite,
            "exec": "processing_planning_" + os.path.basename(__file__),
            "version": version
        },
        "events": []
    }

    #MSI_OUTPUTS (DATABLOCK)

    for output_msi in xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SYNTHESIS_INFO/Product_Report/*[contains(name(),'Output_Products') and boolean(child::DATA_STRIP_ID)]") :
        granule_timeline = []
        ds_output = output_msi.find("DATA_STRIP_ID").text
        output_sensing_date = ds_output[41:57]
        baseline = ds_output[58:]
        level = ds_output[13:16]

        input = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SYNTHESIS_INFO/Product_Report/Input_Products/DATA_STRIP_ID")
        if len(input) > 0:
            ds_input = input[0].text

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
               "back_ref": "OUTPUT-DATASTRIP",
               "link": "INPUT-DATASTRIP",
               #XPATH CAN BE USED
               "name": ds_input
               }
           ],
           "name": ds_output
        }
        list_of_explicit_references.append(explicit_reference)

        for granule in output_msi.xpath("*[name() = 'GRANULES_ID' and contains(text(),'_GR_')]"):

            granule_t = granule.text
            level_gr = granule_t[13:16]
            level_gr.replace("_","")
            output_sensing_date = granule_t[42:57]
            SI_start= parser.parse(output_sensing_date)
            SI_stop = SI_start + datetime.timedelta(seconds=5)
            granule_segment = {
                "start": SI_start,
                "start_str": str(SI_start),
                "stop": SI_stop,
                "stop_str": str(SI_stop),
                "id": granule_t
            }
            granule_timeline.append(granule_segment)

            explicit_reference = {
                "group": level_gr + "_GR",
                "links": [{
                    "back_ref": "DATASTRIP",
                    "link": "GRANULE",
                    "name": granule_t
                    }
                ],
                "name": ds_output
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
            corrected_planned_processing = query.get_events_join(gauge_name_like = {"str": "PLANNED_CUT_IMAGING_%_CORRECTION", "op": "like"},
            gauge_systems = {"list": [satellite], "op": "in"},
            start_filters = [{"date": granule_timeline_sorted[-1]["stop_str"], "op": "<"}],
            stop_filters = [{"date": granule_timeline_sorted[0]["start_str"], "op": ">"}])

            if len(corrected_planned_processing) > 0:
                corrected_planned_processing_sorted = sorted(corrected_planned_processing, key=lambda event: event.start)
                corrected_planned_processing_sorted_timeline = date_functions.convert_eboa_events_to_date_segments(corrected_planned_processing_sorted)
                timeline_intersected = date_functions.intersect_timelines(granule_timeline_sorted,corrected_planned_processing_sorted_timeline)
                start_period = corrected_planned_processing_sorted[0].start
                stop_period = corrected_planned_processing_sorted[-1].stop

                for corrected_planned_process in corrected_planned_processing:
                    value = {
                        "name": "processing_completeness_began",
                        "type": "object",
                        "values": []
                    }
                    exit_status = engine.insert_event_values(corrected_planned_process.event_uuid, value)

                    if exit_status["inserted"] == True:
                        planned_processing_uuid = [event_link.event_uuid_link for event_link in corrected_planned_process.eventLinks if event_link.name == "PLANNED_EVENT"][0]
                        planning_event_values = corrected_planned_process.get_structured_values()
                        planning_event_values[0]["values"] = planning_event_values[0]["values"] + [
                            {"name": "status",
                             "type": "text",
                             "value": "MISSING"}
                        ]

                        completeness_processing_operation["events"].append({
                            "gauge": {
                                    "insertion_type": "SIMPLE_UPDATE",
                                "name": "PLANNED_PROCESSING_COMPLETENESS",
                                "system": satellite
                            },
                            "start": str(corrected_planned_process.start),
                            "stop": str(corrected_planned_process.stop),
                            "links": [
                                {
                                    "link": str(planned_processing_uuid),
                                    "link_mode": "by_uuid",
                                    "name": "PLANNED_PROCESSING",
                                    "back_ref": "COMPLETENESS"
                                }],
                            "values": planning_event_values
                        })

                    for segment_intersected in timeline_intersected:
                        datablock_completeness_event = {
                            "explicit_reference": ds_output,
                            "gauge": {
                                "insertion_type": "INSERT_and_ERASE_per_EVENT",
                                "name": "PLANNED_PROCESSING_COMPLETENESS",
                                "system": system
                            },
                            "links": [{
                                     "link": str(planned_processing_uuid),
                                     "link_mode": "by_uuid",
                                     "name": "PLANNED_PROCESSING",
                                     "back_ref": "COMPLETENESS"
                                     }#, NEEDED?
                                 # {
                                 #     "link": event_link_ref,
                                 #     "link_mode": "by_ref",
                                 #     "name": "PRODUCTION-VALIDITY",
                                 #     "back_ref": "COMPLETENESS"
                                 #}
                                 ],
                             "start": str(segment_intersected["start"]),
                             "stop": str(segment_intersected["stop"]),
                             "values": [{
                                 "name": "details",
                                 "type": "object",
                                 "values": []
                             }]
                        }
                        list_of_events.append(datablock_completeness_event)
                    #end for
                #end if

                     # Insert completeness operation for the completeness analysis of the plan
                    if "source" in completeness_processing_operation:
                        list_of_planning_operations.append(completeness_processing_operation)
                     # end if

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
            "links": "",
            "values": [{
                "name": "details",
                "type": "object",
                "values": []
            }]
        }

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
                    "links": "",
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
        #END TIMELINESS

    #end for

    #CONFIGURATION
    for mrf in mrf_list:
        explicit_reference = {
            "group": "MISSION CONFIGURATION",
            "links": [{
                "back_ref": "DATASTRIP",
                "link": "CONFIGURATION",
                "name": mrf.find("Id").text
                }
            ],
            "name": ds_output
        }
        list_of_explicit_references.append(explicit_reference)

        mrfsDB = query.get_explicit_refs(explicit_ref_like = {"op": "like", "str": mrf.find("Id").text})
        if len(mrfsDB) is 0:
            event_mrf={
                "key":mrf.find("Id").text,
                "explicit_reference": mrf.find("Id").text,
                "gauge": {
                    "insertion_type": "EVENT_KEYS",
                    "name": "MRF-VALIDITY",
                    "system": system
                },
                "start": mrf.find("ValidityStart").text[:-1],
                "stop": mrf.find("ValidityStop").text[:-1],
                "links": "",
                "values": [{
                          "name": "generation_time",
                          "type": "date",
                          "value": mrf.find("Id").text.split("_")[6]
                          }]
                }
            list_of_events.append(event_mrf)
    #end for
    #END CONFIGURATION

    data = {"operations": [{
        "mode": "insert",
        "dim_signature": {
              "name": "PROCESSING_" + system + "_" + satellite,
              "exec": os.path.basename(__file__),
              "version": version
        },
        "source": source,
        "annotations": list_of_annotations,
        "explicit_references": list_of_explicit_references,
        "events": list_of_events,
        },
        completeness_processing_operation],
        "test":test}
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
    # the file following a schema. Schema not available for STNACQs

    returned_value = command_process_file(file_path, output_path)

    logger.info("The ingestion has been performed and the exit status is {}".format(returned_value))
