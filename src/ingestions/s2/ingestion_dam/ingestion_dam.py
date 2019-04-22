"""
Ingestion module for the REP_OPDAM files of Sentinel-2

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
from tempfile import mkstemp

# Import xml parser
from lxml import etree

# Import engine
import eboa.engine.engine as eboa_engine
from eboa.engine.engine import Engine

# Import ingestion helpers
import eboa.engine.ingestion as ingestion
import ingestions.s2.functions as functions
import ingestions.functions.functions as ingestion_helpers
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

    # Obtain the station
    system = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Source/System")[0].text
    # Obtain the creation date from the file name as the annotation creation date is not always correct
    creation_date = file_name[25:40]
    # Obtain the validity start
    validity_start = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Start")[0].text.split("=")[1]
    # Obtain the validity stop
    validity_stop = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Stop")[0].text.split("=")[1]

    # Source for the main operation
    source= {
        "name": file_name,
        "generation_time": creation_date,
        "validity_start": validity_start,
        "validity_stop": validity_stop
    }


    for product in xpath_xml("/Earth_Explorer_File/Data_Block/IngestedProducts/IngestedProduct"):
        #Obtain the product ID
        product_id = product.xpath("product_id")[0].text
        # Obtain the datastrip ID
        datastrip_id = product.xpath("parent_id")[0].text
        # Obtain the satellite
        satellite = datastrip_id[0:3]
        # Obtain the baseline
        baseline = datastrip_id[58:]
        # Obtain the production level from the datastrip
        level = datastrip_id[13:16].replace("_","")
        # Obtain the sensing identifier
        sensing_identifier = datastrip_id[41:57]
        # Obtain the datatake ID
        datatake_id = product.xpath("datatake_id")[0].text
        # Obtain the cataloging_time
        cataloging_time = product.xpath("insertion_time")[0].text


        datatake_exists = len(query.get_explicit_refs(annotation_cnf_names = {"filter": "DATATAKE", "op": "like"},
        annotation_value_filters = [{"name": {"str": "datatake_identifier", "op": "like"}, "type": "text", "value": {"op": "like", "value": datatake_id}}])) > 0
        cataloging_annotation = {
            "explicit_reference" : product_id,
            "annotation_cnf": {
                "name": "CATALOGING_TIME",
                "system": system
                },
            "values": [{
                "name": "details",
                "type": "object",
                "values": [
                    {"name": "cataloging_time",
                     "type": "timestamp",
                     "value": cataloging_time
                     }]
            }]
        }
        list_of_annotations.append(cataloging_annotation)

        if not datatake_exists:

            # Insert the datatake_annotation
            datatake_annotation = {
            "explicit_reference": datastrip_id,
            "annotation_cnf": {
                "name": "DATATAKE",
                "system": satellite
                },
            "values": [{
                "name": "details",
                "type": "object",
                "values": [
                    {"name": "datatake_identifier",
                     "type": "text",
                     "value": datatake_id
                     }]
                }]
            }
            list_of_annotations.append(datatake_annotation)

            baseline_annotation = {
            "explicit_reference": datastrip_id,
            "annotation_cnf": {
                "name": "BASELINE",
                "system": system
                },
            "values": [{
                "name": "details",
                "type": "object",
                "values": [
                    {"name": "baseline",
                     "type": "text",
                     "value": baseline
                     }]
                }]
            }
            list_of_annotations.append(baseline_annotation)

            datastrip_sensing_explicit_ref= {
                "group": level + "_DS",
                "links": [{
                    "back_ref": "SENSING_IDENTIFIER",
                    "link": "DATASTRIP",
                    "name": sensing_identifier
                    }
                ],
                "name": datastrip_id
            }
            list_of_explicit_references.append(datastrip_sensing_explicit_ref)

            # sensing_identifier_annotation = {
            # "explicit_reference": datastrip_id,
            # "annotation_cnf": {
            #     "name": "SENSING_IDENTIFIER",
            #     "system": system
            #     },
            # "values": [{
            #     "name": "details",
            #     "type": "object",
            #     "values": [
            #         {"name": "sensing_identifier",
            #          "type": "text",
            #          "value": sensing_identifier
            #         }]
            #     }]
            # }
            # list_of_annotations.append(sensing_identifier_annotation)

            for granule in product.xpath("product_id[contains(text(),'_GR')]"):
            #if '_GR' in product_id:
                # Insert the granule explicit reference
                granule_explicit_reference = {
                    "group": level + "_GR",
                    "links": [{
                        "back_ref": "DATASTRIP",
                        "link": "GRANULE",
                        "name": datastrip_id
                        }
                    ],
                    "name": product_id
                }
                list_of_explicit_references.append(granule_explicit_reference)
            # end if

            for tile in product.xpath("product_id[contains(text(),'_TL')]"):
            #if '_TL' in product_id:
                # Insert the tile explicit reference
                tile_explicit_reference = {
                    "group": level + "_TL",
                    "links": [{
                        "back_ref": "DATASTRIP",
                        "link": "TILE",
                        "name": datastrip_id
                        }
                    ],
                    "name": product_id
                }
                list_of_explicit_references.append(tile_explicit_reference)
            # end if
        #end if
    # end for

    data = {"operations": [{
        "mode": "insert",
        "dim_signature": {
              "name": "CATALOGING",
              "exec": os.path.basename(__file__),
              "version": version
        },
        "source": source,
        "annotations": list_of_annotations,
        "explicit_references": list_of_explicit_references,
        }]}

    os.remove(new_file_path)

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
