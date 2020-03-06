#!/usr/bin/env python3
"""
Reporting script for helping generating reports using the BOA

Written by DEIMOS Space S.L. (dibb)

module rboa
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
from shutil import move

# Import logging
from eboa.logging import Log

# Import engine
import rboa.engine.engine as rboa_engine
from rboa.engine.engine import Engine
from eboa.engine.query import Query

logging_module = Log(name = os.path.basename(__file__))
logger = logging_module.logger

def insert_data_into_DDBB(data, report_name, engine):
    # Treat data
    returned_statuses = engine.treat_data(data, report_name)

    for returned_status in returned_statuses:
        if returned_status["status"] == rboa_engine.exit_codes["FILE_NOT_VALID"]["status"]:
            logger.error("The metadata of the report {} could not be validated".format(report_name))
        # end if
    # end for

    return returned_statuses

def command_generate_reporting(report_name, processor, generator, generation_mode, begin, end, output_path = None, parameters = None):

    # Import the processor module
    try:
        processor_module = import_module(processor)
    except ImportError as e:
        logger.error("The specified processor {} for generating the report {} does not exist. Return error: {}".format(processor, report_name, str(e)))
        # Log status
        query_log_status = Query()
        reports = query_log_status.get_reports(names = {"filter": report_name, "op": "=="}, report_groups = {"filter": "PENDING_GENERATION", "op": "=="})
        if len(reports) > 0:
            rboa_engine.insert_report_status(query_log_status.session, reports[0], rboa_engine.exit_codes["PROCESSOR_DOES_NOT_EXIST"]["status"], error = True, message = rboa_engine.exit_codes["PROCESSOR_DOES_NOT_EXIST"]["message"].format(report_name, processor, str(e)))
        # end if
        query_log_status.close_session()
                
        exit(-1)
    # end try

    # Prepare metadata
    creation_date = datetime.datetime.now().isoformat()
    metadata = {"operations": [{
        "mode": "insert",
        "report": {"name": report_name,
                   "group": "",
                   "group_description": "",
                   "path": "",
                   "compress": "true",
                   "generation_mode": generation_mode,
                   "validity_start": begin,
                   "validity_stop": end,
                   "triggering_time": creation_date,
                   "generation_start": "",
                   "generation_stop": "",
                   "generator": generator,
                   "generator_version": ""
        }
    }]
    }
    
    generation_duration = None
    html_file_path = None
    
    try:
        generation_start = datetime.datetime.now()
        if parameters != None:
            html_file_path = processor_module.generate_report(begin, end, metadata, parameters)
        else:
            html_file_path = processor_module.generate_report(begin, end, metadata)
        # end if
        generation_stop = datetime.datetime.now()
    except Exception as e:
        logger.error("The generation of the report {} has ended unexpectedly with the following error: {}".format(report_name, str(e)))
        traceback.print_exc(file=sys.stdout)
        # Log status
        query_log_status = Query()
        reports = query_log_status.get_reports(names = {"filter": report_name, "op": "=="}, report_groups = {"filter": "PENDING_GENERATION", "op": "=="})
        if len(reports) > 0:
            rboa_engine.insert_report_status(query_log_status.session, reports[0], rboa_engine.exit_codes["GENERATION_ENDED_UNEXPECTEDLY"]["status"], error = True, message = rboa_engine.exit_codes["GENERATION_ENDED_UNEXPECTEDLY"]["message"].format(report_name, processor, str(e)))
            # end if
        query_log_status.close_session()

        exit(-1)
    # end try

    # Validate data
    if html_file_path == None:
        # Log status
        log = rboa_engine.exit_codes["HTML_FILE_NOT_GENERATED"]["message"].format(report_name, processor)
        logger.error(log)
        query_log_status = Query()
        reports = query_log_status.get_reports(names = {"filter": report_name, "op": "=="}, report_groups = {"filter": "PENDING_GENERATION", "op": "=="})
        if len(reports) > 0:
            rboa_engine.insert_report_status(query_log_status.session, reports[0], rboa_engine.exit_codes["HTML_FILE_NOT_GENERATED"]["status"], error = True, message = log)
            # end if
        query_log_status.close_session()

        exit(-1)
    elif output_path == None:

        metadata["operations"][0]["report"]["generation_start"] = generation_start.isoformat()
        metadata["operations"][0]["report"]["generation_stop"] = generation_stop.isoformat()
        metadata["operations"][0]["report"]["path"] = html_file_path

        try:
            engine_rboa = Engine()
            returned_statuses = insert_data_into_DDBB(metadata, report_name, engine_rboa)
            engine_rboa.close_session()
    
            failures = [returned_status for returned_status in returned_statuses if not returned_status["status"] in [rboa_engine.exit_codes["OK"]["status"]]]
            successes = [returned_status for returned_status in returned_statuses if returned_status["status"] in [rboa_engine.exit_codes["OK"]["status"]]]

            for failure in failures:
                logger.error("The ingestion of the metadata of the report {} has failed using the generator {} with status {}".format(report_name,
                                                                                                                                      processor,
                                                                                                                                      failure["status"]))
            # end for
            for success in successes:
                logger.info("The ingestion of the metadata of the report {} has been performed correctly using the processor {}".format(report_name,
                                                                                                                                               success["generator"]))
            # end for                    

            if output_path == None and len(failures) == 0:
                query = Query()
                # Remove the entry associated to the notification of pending ingestions
                query.get_reports(names = {"filter": report_name, "op": "=="}, report_groups = {"filter": "PENDING_GENERATION", "op": "=="}, delete = True)
                logger.info("The associated alert for notifying about the pending generation of the report {} is going to be deleted from DDBB".format(report_name))
                query.close_session()
            # end if

            os.remove(html_file_path)

        except Exception as e:
            logger.error("The insertion of the metadata related to the generation of the report {} has ended unexpectedly with the following error: {}".format(report_name, str(e)))
            # Log status
            traceback.print_exc(file=sys.stdout)
            query_log_status = Query()
            reports = query_log_status.get_reports(names = {"filter": report_name, "op": "=="}, report_groups = {"filter": "PENDING_GENERATION", "op": "=="})
            if len(reports) > 0:
                rboa_engine.insert_report_status(query_log_status.session, reports[0], rboa_engine.exit_codes["METADATA_INGESTION_ENDED_UNEXPECTEDLY"]["status"], error = True, message = rboa_engine.exit_codes["METADATA_INGESTION_ENDED_UNEXPECTEDLY"]["message"].format(report_name, processor, str(e)))
            # end if
            query_log_status.close_session()

            exit(-1)
        # end try
    else:
        move(html_file_path, output_path)
        logger.info("The generated file {} has been moved to {}".format(html_file_path, output_path))
    # end if

    return

if __name__ == "__main__":
    args_parser = argparse.ArgumentParser(description="Process RBOA reporting generation.")
    args_parser.add_argument("-n", dest="report_name", type=str, nargs=1,
                        help="report name", required=True)
    args_parser.add_argument("-p", dest="processor", type=str, nargs=1,
                        help="processor module", required=True)
    args_parser.add_argument("-a", dest="arguments", type=str, nargs="+",
                             help="parameters to the generator", required=False)
    args_parser.add_argument("-g", dest="generator", type=str, nargs=1,
                        help="generator module", required=True)
    args_parser.add_argument("-m", dest="generation_mode", type=str, nargs=1,
                        help="generator mode", required=True)
    args_parser.add_argument("-b", dest="begin", type=str, nargs=1,
                             help="start date of the reporting period", required=True)
    args_parser.add_argument("-e", dest="end", type=str, nargs=1,
                             help="stop date of the reporting period", required=True)
    args_parser.add_argument("-o", dest="output_path", type=str, nargs=1,
                             help="path to the output file", required=False)

    args = args_parser.parse_args()

    report_name = args.report_name[0]
    processor = args.processor[0]
    generator = args.generator[0]    
    generation_mode = args.generation_mode[0]
    begin = args.begin[0]
    end = args.end[0]
    output_path = None
    if args.output_path != None:
        output_path = args.output_path[0]
    # end if
    parameters = None
    if len(args.arguments) > 0:
        parameters = {}
    # end if
    for argument in args.arguments:
        name = argument.split("=")[0]
        value = argument.split("=")[1]
        parameters[name] = value
    # end for

    command_generate_reporting(report_name, processor, generator, generation_mode, begin, end, output_path, parameters)

    exit(0)
    
