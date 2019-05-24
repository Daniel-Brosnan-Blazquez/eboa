#!/usr/bin/env python3
"""
Ingestion module for the REP_OPDHUS files of Sentinel-2

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

    list_of_annotations = []
    list_of_datastrips = []

    system = "DHUS"
    # Obtain the creation date from the file name as the annotation creation date is not always correct
    creation_date = file_name[25:40]
    # Obtain the validity start
    validity_start = creation_date
    # Obtain the validity stop
    validity_stop = creation_date

    # Source for the main operation
    source= {
        "name": file_name,
        "generation_time": creation_date,
        "validity_start": validity_start,
        "validity_stop": validity_stop
    }

    for tile in xpath_xml("/Earth_Explorer_File/Data_Block/Products/Product/PDI[contains(text(),'_TL')]"):
            #Obtain the product ID
            tile_id = tile.text
            product_name = str(tile.xpath("../@name")[0])

            tile_dhus_dissemination_annotation = {
                "explicit_reference" : tile_id,
                "annotation_cnf": {
                    "name": "DHUS_DISSEMINATION_TIME",
                    "system": system
                    },
                "values": [{
                    "name": "details",
                    "type": "object",
                    "values": [
                        {"name": "dhus_dissemination_time",
                         "type": "timestamp",
                         "value": creation_date
                         }]
                }]
            }
            list_of_annotations.append(tile_dhus_dissemination_annotation)

            tile_user_product_annotation = {
                "explicit_reference" : tile_id,
                "annotation_cnf": {
                    "name": "USER_PRODUCT",
                    "system": system
                    },
                "values": [{
                    "name": "details",
                    "type": "object",
                    "values": [
                        {"name": "product_name",
                         "type": "text",
                         "value": product_name
                         }]
                }]
            }
            list_of_annotations.append(tile_user_product_annotation)
    #end for

    for datastrip in xpath_xml("/Earth_Explorer_File/Data_Block/Products/Product/PDI[contains(text(),'_DS')]"):
        if datastrip.text not in list_of_datastrips:
            list_of_datastrips.append(datastrip.text)

            datastrip_dhus_dissemination_annotation = {
            "explicit_reference" : datastrip.text,
                "annotation_cnf": {
                    "name": "DHUS_DISSEMINATION_TIME",
                    "system": system
                    },
                "values": [{
                    "name": "details",
                    "type": "object",
                    "values": [
                        {"name": "dhus_dissemination_time",
                         "type": "timestamp",
                         "value": creation_date
                         }]
                }]
            }
            list_of_annotations.append(datastrip_dhus_dissemination_annotation)
        #end if
    #end for


    data = {"operations": [{
        "mode": "insert",
        "dim_signature": {
              "name": "DHUS_DISSEMINATION",
              "exec": os.path.basename(__file__),
              "version": version
        },
        "source": source,
        "annotations": list_of_annotations
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
