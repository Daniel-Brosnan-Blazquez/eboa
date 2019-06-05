#!/usr/bin/env python3
"""
Ingestion script for helping inserting data to EBOA

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import argparse
from importlib import import_module
import sys
import os
from dateutil import parser
import copy
from lxml import etree
import json

# Import logging
from eboa.logging import Log

# Import engine
import eboa.engine.engine as eboa_engine
from eboa.engine.engine import Engine
from eboa.engine.query import Query

logging_module = Log()
logger = logging_module.logger

def insert_data_into_DDBB(data, filename, engine):
    # Treat data
    returned_statuses = engine.treat_data(data, filename)
    for returned_status in returned_statuses:
        if returned_status["status"] == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]:
            logger.error("The file {} could not be validated".format(filename))
        # end if
    # end for

    return returned_statuses

def command_process_file(processor, file_path, output_path = None):

    # Process file
    try:
        processor_module = import_module(processor)
    except ImportError as e:
        logger.error("The specified processor {} for processing the file {} does not exist. Return error: {}".format(processor, file_path, str(e)))
        exit(-1)
    # end try

    engine = Engine()
    query = Query()

    try:
        data = processor_module.process_file(file_path, engine, query)
    except Exception as e:
        logger.info("The ingestion has ended unexpectedly with the following error: {}".format(str(e)))
        exit(-1)
    # end try

    # Validate data
    filename = os.path.basename(file_path)

    returned_statuses = [{
        "source": filename,
        "dim_signature": None,
        "processor": None,
        "status": eboa_engine.exit_codes["OK"]["status"]
    }]

    # Treat data
    if output_path == None:
        returned_statuses = insert_data_into_DDBB(data, filename, engine)
    else:
        with open(output_path, "w") as write_file:
            json.dump(data, write_file, indent=4)
    # end if

    failures = [returned_status for returned_status in returned_statuses if not returned_status["status"] in [eboa_engine.exit_codes["OK"]["status"], eboa_engine.exit_codes["SOURCE_ALREADY_INGESTED"]["status"]]]
    successes = [returned_status for returned_status in returned_statuses if returned_status["status"] in [eboa_engine.exit_codes["OK"]["status"], eboa_engine.exit_codes["SOURCE_ALREADY_INGESTED"]["status"]]]

    if len(failures) == 0:
        for success in successes:
            logger.info("The ingestion of the file {} has been performed correctly for the DIM signature {} using the processor {}".format(filename,
                                                                                                                                           success["dim_signature"],
                                                                                                                                           success["processor"]))
        # end for

    else:
        for failure in failures:
            logger.info("The ingestion of the file {} has been performed correctly for the DIM signature {} using the processor {} with status {}".format(filename,
                                                                                                                                                          failure["dim_signature"],
                                                                                                                                                          failure["processor"],
                                                                                                                                                          failure["status"]))
        # end for
        for success in successes:
            logger.info("The ingestion of the file {} has been performed correctly for the DIM signature {} using the processor {}".format(filename,
                                                                                                                                           success["dim_signature"],
                                                                                                                                           success["processor"]))
        # end for                    
    # end if

    # Remove the entry associated to the notification of pending ingestions
    query.get_sources(names = {"filter": os.path.basename(file_path), "op": "like"}, processors = {"filter": "", "op": "like"}, processor_version_filters = [{"str": "", "op": "=="}], delete = True)
    logger.info("The associated alert for notifying about the pending ingestion of the file {} is going to be deleted from DDBB".format(filename))
        
    engine.close_session()
    query.close_session()
    
    return returned_statuses

def main():
    args_parser = argparse.ArgumentParser(description="Process NPPFs.")
    args_parser.add_argument("-p", dest="processor", type=str, nargs=1,
                        help="processor module", required=True)
    args_parser.add_argument("-f", dest="file_path", type=str, nargs=1,
                        help="path to the file to process", required=True)
    args_parser.add_argument("-o", dest="output_path", type=str, nargs=1,
                             help="path to the output file", required=False)
    args = args_parser.parse_args()
    file_path = args.file_path[0]

    # Check if file exists
    if not os.path.isfile(file_path):
        logger.error("The specified file {} does not exist".format(file_path))
        exit(-1)
    # end if

    processor = args.processor[0]
    output_path = None
    if args.output_path != None:
        output_path = args.output_path[0]
    # end if

    returned_statuses = command_process_file(processor, file_path, output_path)
    
    exit(0)

if __name__ == "__main__":

    main()
