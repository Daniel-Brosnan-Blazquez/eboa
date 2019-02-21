# Import python utilities
import os
import argparse
from dateutil import parser
import datetime
import json
import sys
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
def get_planning_events(satellite, validity_start, validity_stop, list_of_events):
    """
    """
    # Get planning events to correct their timings
    query = Query()

    planning_gauges = query.get_gauges_join(dim_signatures = {"list": ["NPPF_" + satellite], "op": "in"})

    planning_events = query.get_events(gauge_uuids = {"list": [gauge.gauge_uuid for gauge in planning_gauges], "op": "in"}, start_filters = [{"date": validity_start, "op": ">"}], stop_filters = [{"date": validity_stop, "op": "<"}])

    return planning_events

def get_received_events(satellite, validity_start, validity_stop, list_of_events):
    """
    """
    # Get planning events to correct their timings
    query = Query()

    planning_gauges = query.get_gauges_join(dim_signatures = {"list": ["RECEPTION_" + satellite], "op": "in"})

    planning_events = query.get_events(gauge_uuids = {"list": [gauge.gauge_uuid for gauge in planning_gauges], "op": "in"}, start_filters = [{"date": validity_start, "op": ">"}], stop_filters = [{"date": validity_stop, "op": "<"}])

    return planning_events

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

    satellite = file_name[0:3]
    system = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Source/System")[0].text

    creation_date = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Source/Creation_Date")[0].text.split("=")[1]
    validity_start = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Start")[0].text.split("=")[1]
    validity_stop = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Stop")[0].text.split("=")[1]

    workplan_current_status = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SUPERVISION_INFO/WORKPLAN_CURRENT_STATUS")[0].text
    workplan_message = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SUPERVISION_INFO/WORKPLAN_MESSAGE")[0].text
    workplan_start_datetime = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SUPERVISION_INFO/WORKPLAN_START_DATETIME")[0].text
    workplan_end_datetime = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SUPERVISION_INFO/WORKPLAN_END_DATETIME")[0].text

    ds_input = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SYNTHESIS_INFO/Product_Report/Input_Products/DATA_STRIP_ID")[0].text
    input_ds_sensing_aux =  xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SYNTHESIS_INFO/Product_Report/Input_Products/DATA_STRIP_ID")[0].text[42:57]
    input_ds_sensing = input_ds_sensing_aux[:4] + "-" + input_ds_sensing_aux[4:6] + "-" + input_ds_sensing_aux[6:11] + ":" + input_ds_sensing_aux[11:13] + ":" + input_ds_sensing_aux[13:15]
    granules_input = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SYNTHESIS_INFO/Product_Report/Input_Products/GRANULES_ID")

    outputs = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SYNTHESIS_INFO/Product_Report/child::*[contains(name(),'Output_Products')]")

    mrf_list = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SYNTHESIS_INFO/Product_Report/List_Of_MRFs/MRF")
    mrf_id =  xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SYNTHESIS_INFO/Product_Report/List_Of_MRFs/MRF/Id")
    mrf_validity_start = xpath_xml ("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SYNTHESIS_INFO/Product_Report/List_Of_MRFs/MRF/ValidityStart")
    mrf_validity_stop = xpath_xml ("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SYNTHESIS_INFO/Product_Report/List_Of_MRFs/MRF/ValidityStop")

    steps_id = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/DATA/STEP_INFO/@id")
    steps_start = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/DATA/STEP_INFO/PROCESSING_START_DATETIME")
    steps_stop = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/DATA/STEP_INFO/PROCESSING_END_DATETIME")
    exec_statuses = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/DATA/STEP_INFO/EXEC_STATUS")
    steps_list = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/DATA/STEP_INFO")

    explicit_reference = {
        "group": "SENSING-ID",
        "links": [{
            "back_ref": "true",
            "link": "SENSING-DATASTRIP",
            "name": input_ds_sensing
            }
        ],
        "name": ds_input
    }
    list_of_explicit_references.append(explicit_reference)

    #INPUT_PRODUCTS
    for granule in granules_input:
        granule_t = granule.text
        #datetime format
        SI_start= datetime.datetime(int(granule_t[42:46]),int(granule_t[46:48]),int(granule_t[48:50]),int(granule_t[51:53]),int(granule_t[53:55]),int(granule_t[55:57]))
        SI_stop = SI_start + datetime.timedelta(0,5)
        #change to ISO Format
        SI_iso_start = SI_start.isoformat()
        SI_iso_stop = SI_stop.isoformat()

        detector = granule_t[59:61]

        event_input_granule={
            "explicit_reference": ds_input,
            "gauge": {
                "insertion_type": "SIMPLE_UPDATE",
                "name": "INPUT-GRANULE",
                "system": system
            },
            "start": SI_iso_start,
            "stop": SI_iso_stop,
            "links": "",
            "values": [{
                "name": "details",
                "type": "object",
                "values": [{
                    "name": "detector",
                    "type": "text",
                    "value": detector
                  },{
                    "name": "id",
                    "type": "text",
                    "value": granule_t
                  }]
            }]}
        list_of_events.append(event_input_granule)
    #end for

    #OUTPUT_PRODUCTS (and steps due to duplicity)
    for output in outputs:
        index = outputs.index(output)
        ds_output = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SYNTHESIS_INFO/Product_Report/child::*[contains(name(),'Output_Products')]/DATA_STRIP_ID")[outputs.index(output)].text
        output_sensing_date_aux = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SYNTHESIS_INFO/Product_Report/child::*[contains(name(),'Output_Products')]/DATA_STRIP_ID")[outputs.index(output)].text[42:57]
        output_sensing_date = output_sensing_date_aux[:4] + "-" + output_sensing_date_aux[4:6] + "-" + output_sensing_date_aux[6:11] + ":" + output_sensing_date_aux[11:13] + ":" + output_sensing_date_aux[13:15]
        baseline = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SYNTHESIS_INFO/Product_Report/child::*[contains(name(),'Output_Products')]/DATA_STRIP_ID")[outputs.index(output)].text[58:]
        granules_output = output.findall('GRANULES_ID')

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
                     "value": baseline}
                ]
            }]
        }
        list_of_annotations.append(baseline_annotation)

        for granule in granules_output:
            granule_t = granule.text

            if "GR" in granule_t[9:19]:
                SI_start= datetime.datetime(int(granule_t[42:46]),int(granule_t[46:48]),int(granule_t[48:50]),int(granule_t[51:53]),int(granule_t[53:55]),int(granule_t[55:57]))
                SI_stop = SI_start + datetime.timedelta(0,5)
                #change to ISO Format
                SI_iso_start = SI_start.isoformat()
                SI_iso_stop = SI_stop.isoformat()

                detector = granule_t[59:61]

                event_output_granule={
                    "explicit_reference": ds_output,
                    "gauge": {
                        "insertion_type": "SIMPLE_UPDATE",
                        "name": "OUTPUT-GRANULE",
                        "system": system
                    },
                    "start": SI_iso_start,
                    "stop": SI_iso_stop,
                    "links": "",
                    "values": [{
                        "name": "details",
                        "type": "object",
                        "values": [{
                            "name": "detector",
                            "type": "text",
                            "value": detector
                          },{
                            "name": "id",
                            "type": "text",
                            "value": granule_t
                          }]
                    }]
                }
            #end if
            else:
                tile_Id_x = granule_t[41:48]
                tile_Id_y = granule_t[49:55]
                event_output_granule={
                    "explicit_reference": ds_output,
                    "gauge": {
                        "insertion_type": "SIMPLE_UPDATE",
                        "name": "OUTPUT-TILE",
                        "system": system
                    },
                    "start": output_sensing_date,
                    "stop": output_sensing_date,
                    "links": "",
                    "values": [{
                        "name": "details",
                        "type": "object",
                        "values": [{
                            "name": "id",
                            "type": "text",
                            "value": granule_t
                          },{
                            "name": "tile_id_x",
                            "type": "text",
                            "value": tile_Id_x
                          },{
                            "name": "tile_id_y",
                            "type": "text",
                            "value": tile_Id_y
                          }]
                    }]
                }
            #end else
            list_of_events.append(event_output_granule)
        #end for

        #Steps
        for step in steps_list:
            if exec_statuses[steps_list.index(step)].text == 'COMPLETED':
                #duplicate if product L1A & L1B
                event_step = {
                    "explicit_reference": ds_output,
                    "gauge": {
                        "insertion_type": "SIMPLE_UPDATE",
                        "name": "STEP-INFO",
                        "system": system
                    },
                    "start": steps_start[steps_list.index(step)].text[:-1],
                    "stop": steps_stop[steps_list.index(step)].text[:-1],
                    "links": "",
                    "values": [{
                        "name": "details",
                        "type": "object",
                        "values": []
                    }]
                }
                list_of_events.append(event_step)
            #end if
        #end for

        explicit_reference = {
            "group": "OUTPUT-PRODUCTS",
            "links": [{
                "back_ref": "true",
                "link": "OUTPUT-DATASTRIP",
                "name": ds_output
                }
            ],
            "name": granule_t
        }
        list_of_explicit_references.append(explicit_reference)

        explicit_reference = {
            "group": "PRODUCTION-DS",
            "links": [{
                "back_ref": "true",
                "link": "INPUT-OUTPUT",
                "name": ds_output
                }
            ],
            "name": ds_input
        }
        list_of_explicit_references.append(explicit_reference)

        explicit_reference = {
            "group": "SENSING-ID",
            "links": [{
                "back_ref": "true",
                "link": "SENSING-DATASTRIP",
                "name": output_sensing_date
                }
            ],
            "name": ds_output
        }
        list_of_explicit_references.append(explicit_reference)
    #end for

    #MRF
    for mrf in mrf_list:
        event_mrf={
            "explicit_reference": mrf_id[mrf_list.index(mrf)].text,
            "gauge": {
                "insertion_type": "SIMPLE_UPDATE",
                "name": "MRF-VALIDITY",
                "system": system
            },
            "start": mrf_validity_start[mrf_list.index(mrf)].text[:-1],
            "stop": mrf_validity_stop[mrf_list.index(mrf)].text[:-1],
            "links": "",
            "values": []
            }
        list_of_events.append(event_mrf)

        ##########################################################
        #Duplicate if multiple outputs ??????????????????????????
        ##########################################################

        explicit_reference = {
            "group": "MRFs",
            "links": [{
                "back_ref": "true",
                "link": "MRF-DATASTRIP",
                "name": ds_output
                }
            ],
            "name": mrf_id[mrf_list.index(mrf)].text
        }
        list_of_explicit_references.append(explicit_reference)
    #end for

    source = {
        "name": file_name,
        "generation_time": creation_date,
        "validity_start": validity_start,
        "validity_stop": validity_stop
    }

    data = {"operations": [{
        "mode": "insert",
        "dim_signature": {
              "name": "DPC-OPERATION_" + system + "_"+ satellite,
              "exec": os.path.basename(__file__),
              "version": version
        },
        "source": source,
        "annotations": list_of_annotations,
        "explicit_references": list_of_explicit_references,
        "events": list_of_events,
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
