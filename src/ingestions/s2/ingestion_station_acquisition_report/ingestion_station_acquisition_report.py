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

    satellite = file_name[0:3]
    generation_time = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Source/Creation_Date")[0].text.split("=")[1]
    validity_start = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Start")[0].text.split("=")[1]
    validity_stop = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Stop")[0].text.split("=")[1]
    system = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Source/System")[0].text

    acq_id = xpath_xml("/Earth_Explorer_File/Data_Block/StationAcquisitionReport/StationDownlinkDetails/Acq_Id")[0].text
    start = xpath_xml("/Earth_Explorer_File/Data_Block/StationAcquisitionReport/StationDownlinkDetails/DownlinkStartTime")[0].text.split("=")[1]
    stop = xpath_xml("/Earth_Explorer_File/Data_Block/StationAcquisitionReport/StationDownlinkDetails/DownlinkEndTime")[0].text.split("=")[1]
    downlink_status = xpath_xml("/Earth_Explorer_File/Data_Block/StationAcquisitionReport/StationDownlinkDetails/DownlinkStatus")[0].text

    if downlink_status != "OK":
        caracterized_downlink_status = "NOK"
    else:
        caracterized_downlink_status = "OK"
    # end if

    comments = xpath_xml("/Earth_Explorer_File/Data_Block/StationAcquisitionReport/StationDownlinkDetails/Comments")[0].text
    if comments is None:
        comments=" "
    #end if

    antenna_id = xpath_xml("/Earth_Explorer_File/Data_Block/StationAcquisitionReport/StationDownlinkDetails/AntennaId")[0].text
    orbit,support_number = acq_id.split("_")


    source = {
        "name": file_name,
        "generation_time": generation_time,
        "validity_start": validity_start,
        "validity_stop": validity_stop
    }

    playbacks = query.get_linked_events_join(gauge_name_like = {"str":"PLANNED_PLAYBACK_TYPE_%_CORRECTION","op":"like"},
                                         gauge_systems = {"list":[satellite],"op":"in"},
                                         start_filters = [{"date":start,"op":">"}], stop_filters = [{"date":stop, "op":"<"}],
                                         link_names = {"list":["TIME_CORRECTION"],"op":"in"}, return_prime_events = False)

    links = []
    status = "MATCHED_PLAYBACK"
    if len(playbacks["linked_events"]) == 0:
        status = "NO_MATCHED_PLAYBACK"
    else:
        for playback in playbacks["linked_events"]:
            links.append({
                "link": str(playback.event_uuid),
                "link_mode": "by_uuid",
                "name": "STATION_ACQUISITION_REPORT",
                "back_ref": "PLANNED_PLAYBACK"
            })
        # end for
    # end if

    # Build the xml
    data = {"operations": [{
        "mode": "insert",
        "dim_signature": {
            "name": "STATION_REPORT_" + satellite + "_"+ system,
            "exec": os.path.basename(__file__),
            "version": version
        },
        "source": source,
        "events": [{
            "key": "STATION_REPORT_" + satellite + "_" + system + acq_id,
            "gauge": {
                "insertion_type": "EVENT_KEYS",
                "name": "STATION_REPORT",
                "system": satellite
            },
            "start": start,
            "stop": stop,
            "values": [{
                "name": "details",
                "type": "object",
                "values": [{
                    "name": "downlink_status",
                    "type": "text",
                    "value": downlink_status
                    },
                    {
                    "name": "caracterized_downlink_status",
                    "type": "text",
                    "value": caracterized_downlink_status
                    },
                    {
                    "name": "comments",
                    "type": "text",
                    "value": comments
                    },
                    {
                    "name": "antenna_id",
                    "type": "text",
                    "value": antenna_id
                    },{
                    "name": "satellite",
                    "type": "text",
                    "value": satellite
                    },{
                    "name": "orbit",
                    "type": "double",
                    "value": orbit
                    },{
                    "name": "support_number",
                    "type": "double",
                    "value": support_number
                    },{
                    "name": "status",
                    "type": "text",
                    "value": status
                    }]
                }],
            "links": links
        }],
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
    #print(json.dumps(data,indent = 4))
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
