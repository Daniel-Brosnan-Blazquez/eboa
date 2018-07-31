"""
Ingestion module for the NPPF files of Sentinel-2

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
# Import python utilities
import os
import argparse
from dateutil import parser
import datetime

# Import xml parser
from lxml import etree

# Import engine
from gsdm.engine.engine import Engine

# Import ingestion helpers
import gsdm.engine.ingestion as ingestion

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

def process_file(file_path):
    """Function to process the file and insert its relevant information
    into the DDBB of the gsdm
    
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

    record_operations = xpath_xml("/Earth_Explorer_File/Data_Block/List_of_EVRQs/EVRQ[RQ/RQ_Name='MPMMRNOM' or RQ/RQ_Name='MPMMRNRT']")

    for record_operation in record_operations:
        # Record start information
        record_start = record_operation.xpath("RQ/RQ_Execution_Time")[0].text.split("=")[1]
        record_start_orbit = record_operation.xpath("RQ/RQ_Absolute_orbit")[0].text
        record_start_angle = record_operation.xpath("RQ/RQ_Deg_from_ANX")[0].text
        record_start_request = record_operation.xpath("RQ/RQ_Name")[0].text
        record_start_scn_dup = record_operation.xpath("RQ/List_of_RQ_Parameters/RQ_Parameter[RQ_Parameter_Name = 'SCN_DUP']/RQ_Parameter_Value")

        # Record stop information
        record_operation_stop = record_operation.xpath("following-sibling::EVRQ[RQ/RQ_Name='MPMMRSTP' or RQ/RQ_Name='MPMMRNRT'][1]")[0]
        record_stop_orbit = record_operation_stop.xpath("RQ/RQ_Absolute_orbit")[0].text
        record_stop_angle = record_operation_stop.xpath("RQ/RQ_Deg_from_ANX")[0].text
        record_stop = record_operation_stop.xpath("RQ/RQ_Execution_Time")[0].text.split("=")[1]
        record_stop_request = record_operation_stop.xpath("RQ/RQ_Name")[0].text
        record_stop_scn_dup = record_operation_stop.xpath("RQ/List_of_RQ_Parameters/RQ_Parameter[RQ_Parameter_Name = 'SCN_DUP']/RQ_Parameter_Value")

        record_type = record_types[record_operation.xpath("RQ/RQ_Name")[0].text]

        following_imaging_operation = record_operation.xpath("following-sibling::EVRQ[RQ/RQ_Name='MPMSSCAL' or RQ/RQ_Name='MPMSDASC' or RQ/RQ_Name='MPMSDCLO' or RQ/RQ_Name='MPMSIVIC' or RQ/RQ_Name='MPMSNOBS' or RQ/RQ_Name='MPMSIRAW' or RQ/RQ_Name='MPMSIDTS' or RQ/RQ_Name='MPMSIMID' or RQ/RQ_Name='MPMSIDSB' or RQ/RQ_Name='MPMMRSTP'][1]")[0]
        if following_imaging_operation.xpath("RQ[RQ_Name='MPMSIMID' or RQ_Name='MPMSIDSB' or RQ_Name='MPMMRSTP']"):
            imaging_start_operation = record_operation.xpath("preceding-sibling::EVRQ[RQ/RQ_Name='MPMSSCAL' or RQ/RQ_Name='MPMSDASC' or RQ/RQ_Name='MPMSDCLO' or RQ/RQ_Name='MPMSIVIC' or RQ/RQ_Name='MPMSNOBS' or RQ/RQ_Name='MPMSIRAW' or RQ/RQ_Name='MPMSIDTS'][1]")[0]
            imaging_stop_operation = following_imaging_operation
        else:
            imaging_start_operation = following_imaging_operation
            imaging_stop_operation = record_operation.xpath("following-sibling::EVRQ[(RQ/RQ_Name='MPMSIMID' or RQ/RQ_Name='MPMSIDSB' or RQ/RQ_Name='MPMMRSTP')][1]")[0]
        # end if

        # Imaging start information
        imaging_start = imaging_start_operation.xpath("RQ/RQ_Execution_Time")[0].text.split("=")[1]
        imaging_start_orbit = imaging_start_operation.xpath("RQ/RQ_Absolute_orbit")[0].text
        imaging_start_angle = imaging_start_operation.xpath("RQ/RQ_Deg_from_ANX")[0].text
        imaging_start_request = imaging_start_operation.xpath("RQ/RQ_Name")[0].text

        imaging_mode = imaging_modes[imaging_start_request]

        # Imaging stop information
        imaging_stop = imaging_stop_operation.xpath("RQ/RQ_Execution_Time")[0].text.split("=")[1]
        if imaging_mode == "SUN_CAL":
            imaging_stop = (parser.parse(imaging_start) + datetime.timedelta(seconds=50)).isoformat()
        # end if
        imaging_stop_orbit = imaging_stop_operation.xpath("RQ/RQ_Absolute_orbit")[0].text
        imaging_stop_angle = imaging_stop_operation.xpath("RQ/RQ_Deg_from_ANX")[0].text
        imaging_stop_request = imaging_stop_operation.xpath("RQ/RQ_Name")[0].text

        event_link_id = "event_link_" + record_start

        # Record event
        record_event = {
            "gauge": {
                "insertion_type": "ERASE_and_REPLACE",
                "name": "record",
                "system": satellite
            },
            "start": record_start,
            "stop": record_start,
            "links": [
                {
                    "link": event_link_id,
                    "link_mode": "by_ref",
                    "name": "IMAGING_OPERATION"
                }
            ],
            "values": [{
                "name": "record_values",
                "type": "object",
                "values": [
                    {"name": "record_type",
                     "type": "text",
                     "value": record_type},
                    {"name": "record_start_request",
                     "type": "text",
                     "value": record_start_request},
                    {"name": "record_stop_request",
                     "type": "text",
                     "value": record_stop_request},
                    {"name": "record_start_orbit",
                     "type": "double",
                     "value": record_start_orbit},
                    {"name": "record_start_angle",
                     "type": "double",
                     "value": record_start_angle},
                    {"name": "record_stop_orbit",
                     "type": "double",
                     "value": record_stop_orbit},
                    {"name": "record_stop_angle",
                     "type": "double",
                     "value": record_stop_angle}
                ]
            }]
        }

        # Include parameters
        if len(record_start_scn_dup) == 1:
            record_event["values"][0]["values"].append(
                    {"name": "record_start_scn_dup",
                     "type": "double",
                     "value": record_start_scn_dup[0].text},
            )
        # end if

        if len(record_stop_scn_dup) == 1:
            record_event["values"][0]["values"].append(
                    {"name": "record_stop_scn_dup",
                     "type": "double",
                     "value": record_stop_scn_dup[0].text}
            )
        # end if

        # Insert record_event
        ingestion.insert_event_for_ingestion(record_event, source, list_of_events)

        # Imaging event
        imaging_event = {
            "gauge": {
                "insertion_type": "ERASE_and_REPLACE",
                "name": "imaging",
                "system": satellite
            },
            "start": imaging_start,
            "stop": imaging_stop,
            "links": [
                {
                    "link": event_link_id,
                    "link_mode": "by_ref",
                    "name": "RECORD_OPERATION"
                }
            ],
            "values": [{
                "name": "imaging_values",
                "type": "object",
                "values": [
                    {"name": "imaging_mode",
                     "type": "text",
                     "value": imaging_mode},
                    {"name": "imaging_start_request",
                     "type": "text",
                     "value": imaging_start_request},
                    {"name": "imaging_stop_request",
                     "type": "text",
                     "value": imaging_stop_request},
                    {"name": "imaging_start_orbit",
                     "type": "double",
                     "value": imaging_start_orbit},
                    {"name": "imaging_start_angle",
                     "type": "double",
                     "value": imaging_start_angle},
                    {"name": "imaging_stop_orbit",
                     "type": "double",
                     "value": imaging_stop_orbit},
                    {"name": "imaging_stop_angle",
                     "type": "double",
                     "value": imaging_stop_angle}
                ]
            }]
        }

        # Insert imaging_event
        ingestion.insert_event_for_ingestion(imaging_event, source, list_of_events)

    # end for

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
                {"name": "idle_start_request",
                 "type": "text",
                 "value": idle_start_request},
                {"name": "idle_stop_request",
                 "type": "text",
                 "value": idle_stop_request},
                {"name": "idle_start_orbit",
                 "type": "double",
                 "value": idle_start_orbit},
                {"name": "idle_start_angle",
                 "type": "double",
                 "value": idle_start_angle},
                {"name": "idle_stop_orbit",
                 "type": "double",
                 "value": idle_stop_orbit},
                {"name": "idle_stop_angle",
                 "type": "double",
                 "value": idle_stop_angle}
            ]
        else:
            idle_stop_orbit = None
            idle_stop_angle = None
            idle_stop = validity_stop
            idle_stop_request = None
            values = [
                {"name": "idle_start_request",
                 "type": "text",
                 "value": idle_start_request},
                {"name": "idle_start_orbit",
                 "type": "double",
                 "value": idle_start_orbit},
                {"name": "idle_start_angle",
                 "type": "double",
                 "value": idle_start_angle}
            ]
        # end if

        # Idle event
        idle_event = {
            "gauge": {
                "insertion_type": "ERASE_and_REPLACE",
                "name": "idle",
                "system": satellite
            },
            "start": idle_start,
            "stop": idle_stop,
            "values": [{
                "name": "idle_values",
                "type": "object",
                "values": values
            }]
        }

        # Insert idle_event
        ingestion.insert_event_for_ingestion(idle_event, source, list_of_events)

    # end for

    # Build the xml
    data = {"operations": [{
        "mode": "insert",
        "dim_signature": {
            "name": "NPPF_" + satellite,
            "exec": os.path.basename(__file__),
            "version": version
        },
        "source": source,
        "events": list_of_events
    }]}

    return data

if __name__ == "__main__":
    """
    This will be not here. After all the needs are identified, the ingestions a priori will have only the method process_file and this method will be called from outside
    """
    args_parser = argparse.ArgumentParser(description='Process NPPFs.')
    args_parser.add_argument('-f', dest='file_path', type=str, nargs=1,
                        help='path to the file to process', required=True)
    args = args_parser.parse_args()

    # Before calling to the processor there should be a validation of
    # the file following a schema. Schema not available for NPPFs

    # Process file
    file_path = args.file_path[0]
    data = process_file(file_path)

    engine = Engine()
    # Validate data
    filename = os.path.basename(file_path)
    returned_value = engine.validate_data(data, filename)

    if returned_value == engine.exit_codes["FILE_NOT_VALID"]["status"]:
        print("The file could not be validated")
    else:
        # Treat data
        returned_value = engine.treat_data(data)
    # end if
    
    print(returned_value)
