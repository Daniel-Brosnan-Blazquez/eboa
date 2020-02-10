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
import traceback
import datetime

# Import auxiliary functions
from eboa.engine.functions import is_datetime, read_configuration

# Import logging
from eboa.logging import Log

# Import engine
import eboa.engine.engine as eboa_engine
from eboa.engine.engine import Engine
from eboa.engine.query import Query

logging_module = Log(name = os.path.basename(__file__))
logger = logging_module.logger

config = read_configuration()

def insert_data_into_DDBB(data, filename, engine, processing_duration):
    # Treat data
    returned_statuses = engine.treat_data(data, filename, processing_duration = processing_duration)

    for returned_status in returned_statuses:
        if returned_status["status"] == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]:
            logger.error("The file {} could not be validated".format(filename))
        # end if
    # end for

    return returned_statuses

def command_process_file(processor, file_path, reception_time, output_path = None):

    filename = os.path.basename(file_path)
    # Process file
    try:
        processor_module = import_module(processor)
    except ImportError as e:
        logger.error("The specified processor {} for processing the file {} does not exist. Returned error: {}".format(processor, file_path, str(e)))
        # Log status
        query_log_status = Query()
        sources = query_log_status.get_sources(names = {"filter": filename, "op": "=="}, dim_signatures = {"filter": "PENDING_SOURCES", "op": "=="})
        if len(sources) > 0:
            eboa_engine.insert_source_status(query_log_status.session, sources[0], eboa_engine.exit_codes["PROCESSOR_DOES_NOT_EXIST"]["status"], error = True, message = eboa_engine.exit_codes["PROCESSOR_DOES_NOT_EXIST"]["message"].format(file_path, processor, str(e)))
        # end if
        query_log_status.close_session()
                
        return -1
    # end try

    engine = Engine()
    query = Query()

    processing_duration = None

    # Try the ingestion max_number_of_retries + 1
    max_number_of_retries = config["INGESTION_RETRIES"]
    number_of_retries = 0
    while number_of_retries <= max_number_of_retries:
        success = True

        try:
            start = datetime.datetime.now()
            data = processor_module.process_file(file_path, engine, query, reception_time)
            stop = datetime.datetime.now()
            processing_duration = stop - start
        except Exception as e:
            logger.error("The processing of the file {} has ended unexpectedly with the following error: {}".format(file_path, str(e)))
            logger.error(traceback.print_exc(file=sys.stdout))
            traceback.print_exc(file=sys.stdout)
            # Log status
            query_log_status = Query()
            sources = query_log_status.get_sources(names = {"filter": filename, "op": "=="}, dim_signatures = {"filter": "PENDING_SOURCES", "op": "=="})
            if len(sources) > 0:
                eboa_engine.insert_source_status(query_log_status.session, sources[0], eboa_engine.exit_codes["PROCESSING_ENDED_UNEXPECTEDLY"]["status"], error = True, message = eboa_engine.exit_codes["PROCESSING_ENDED_UNEXPECTEDLY"]["message"].format(filename, processor, str(e)))
            # end if
            query_log_status.close_session()

            return -1
        # end try

        # Validate data
        returned_statuses = [{
            "source": filename,
            "dim_signature": None,
            "processor": None,
            "status": eboa_engine.exit_codes["OK"]["status"]
        }]

        # Treat data
        if output_path == None:
            try:
                returned_statuses = insert_data_into_DDBB(data, filename, engine, processing_duration)
            except Exception as e:
                success = False

                logger.error("The insertion of the data related to file {} has ended unexpectedly with the following error: {}".format(filename, str(e)))
                if number_of_retries >= max_number_of_retries:
                    logger.error("The ingestion of the file {} has exceeded the number of retries {}. Previously related data ingested has been removed".format(filename, max_number_of_retries))
                    # Log status
                    query_log_status = Query()
                    sources = query_log_status.get_sources(names = {"filter": filename, "op": "=="}, dim_signatures = {"filter": "PENDING_SOURCES", "op": "=="})
                    if len(sources) > 0:
                        eboa_engine.insert_source_status(query_log_status.session, sources[0], eboa_engine.exit_codes["INGESTION_ENDED_UNEXPECTEDLY"]["status"], error = True, message = eboa_engine.exit_codes["INGESTION_ENDED_UNEXPECTEDLY"]["message"].format(filename, processor, str(e)))
                    # end if
                    query_log_status.close_session()

                    return -1
                else:
                    query_remove = Query()
                    query_remove.get_sources(names = {"filter": filename, "op": "=="}, dim_signatures = {"filter": "PENDING_SOURCES", "op": "!="}, delete = True)
                    query_remove.close_session()

                    logger.error("The ingestion of the file {} is going to be retried. Previously related data ingested has been removed".format(filename))
                # end if
            # end try
        else:
            with open(output_path, "w") as write_file:
                json.dump(data, write_file, indent=4)
        # end if

        if success:
            break
        # end if
        engine.close_session()
        query.close_session()

        engine = Engine()
        query = Query()

        number_of_retries += 1
    # end while

    failures = [returned_status for returned_status in returned_statuses if not returned_status["status"] in [eboa_engine.exit_codes["OK"]["status"], eboa_engine.exit_codes["SOURCE_ALREADY_INGESTED"]["status"]]]
    successes = [returned_status for returned_status in returned_statuses if returned_status["status"] in [eboa_engine.exit_codes["OK"]["status"], eboa_engine.exit_codes["SOURCE_ALREADY_INGESTED"]["status"]]]

    for failure in failures:
        logger.error("The ingestion of the file {} has failed for the DIM signature {} using the processor {} with status {}".format(filename,
                                                                                                                                                      failure["dim_signature"],
                                                                                                                                                      failure["processor"],
                                                                                                                                                      failure["status"]))
    # end for
    for success in successes:
        logger.info("The ingestion of the file {} has been performed correctly for the DIM signature {} using the processor {}".format(filename,
                                                                                                                                       success["dim_signature"],
                                                                                                                                       success["processor"]))
    # end for                    

    if output_path == None and len(failures) == 0:
        # Remove the entry associated to the notification of pending ingestions
        query.get_sources(names = {"filter": os.path.basename(file_path), "op": "=="}, dim_signatures = {"filter": "PENDING_RECEIVED_SOURCES_BY_DEC", "op": "=="}, delete = True)
        query.get_sources(names = {"filter": os.path.basename(file_path), "op": "=="}, dim_signatures = {"filter": "PENDING_SOURCES", "op": "=="}, delete = True)
        logger.info("The associated alert for notifying about the pending ingestion of the file {} is going to be deleted from DDBB".format(filename))
    # end if
        
    engine.close_session()
    query.close_session()
    
    return returned_statuses

def main(file_path, processor, output_path = None, reception_time = None):

    filename = os.path.basename(file_path)
    # Check if file exists
    if not os.path.isfile(file_path):
        logger.error("The specified file {} does not exist".format(file_path))
        # Log status
        query_log_status = Query()
        sources = query_log_status.get_sources(names = {"filter": filename, "op": "=="}, dim_signatures = {"filter": "PENDING_SOURCES", "op": "=="})
        if len(sources) > 0:
            eboa_engine.insert_source_status(query_log_status.session, sources[0], eboa_engine.exit_codes["FILE_DOES_NOT_EXIST"]["status"], error = True, message = eboa_engine.exit_codes["FILE_DOES_NOT_EXIST"]["message"].format(filename))
        # end if
        query_log_status.close_session()

        return -1
    # end if

    logger.info("Received file {}".format(file_path))
    
    returned_statuses = command_process_file(processor, file_path, reception_time, output_path)

    if returned_statuses == -1:
        return -1
    # end if
    
    return 0

if __name__ == "__main__":
    args_parser = argparse.ArgumentParser(description="Process NPPFs.")
    args_parser.add_argument("-p", dest="processor", type=str, nargs=1,
                        help="processor module", required=True)
    args_parser.add_argument("-f", dest="file_path", type=str, nargs=1,
                        help="path to the file to process", required=True)
    args_parser.add_argument("-t", dest="reception_time", type=str, nargs=1,
                             help="reception time of the file", required=False)
    args_parser.add_argument("-o", dest="output_path", type=str, nargs=1,
                             help="path to the output file", required=False)
    args = args_parser.parse_args()

    file_path = args.file_path[0]

    processor = args.processor[0]

    output_path = None
    if args.output_path != None:
        output_path = args.output_path[0]
    # end if

    reception_time = datetime.datetime.now().isoformat()
    if args.reception_time != None and is_datetime(args.reception_time[0]):
        reception_time = parser.parse(args.reception_time[0]).isoformat()
    # end if
    
    returned_value = main(file_path, processor, output_path, reception_time)

    exit(returned_value)
    
