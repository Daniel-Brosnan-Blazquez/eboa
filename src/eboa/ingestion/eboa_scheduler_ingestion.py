#!/usr/bin/env python3
"""
Ingestion script for helping inserting data to EBOA when commanded from the BOA scheduler

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

# Import eboa_ingestion module
from eboa.ingestion import eboa_ingestion

logging_module = Log(name = os.path.basename(__file__))
logger = logging_module.logger

config = read_configuration()

if __name__ == "__main__":
    args_parser = argparse.ArgumentParser(description="Process NPPFs.")
    args_parser.add_argument("-p", dest="processor", type=str, nargs=1,
                             help="processor module", required=True)
    args_parser.add_argument("-f", dest="file_path", type=str, nargs=1,
                             help="path to the file to process", required=True)
    args_parser.add_argument("-s", dest="schema_path", type=str, nargs=1,
                             help="path to the schema to check the file (only for XML files)", required=False)
    args_parser.add_argument("-b", dest="begin", type=str, nargs=1,
                             help="start date of the reporting period", required=True)
    args_parser.add_argument("-e", dest="end", type=str, nargs=1,
                             help="stop date of the reporting period", required=True)
    args_parser.add_argument("-o", dest="output_path", type=str, nargs=1,
                             help="path to the output file", required=False)
    args = args_parser.parse_args()

    file_path = args.file_path[0]

    processor = args.processor[0]

    # Path to the schema if specified
    schema_path = None
    if args.schema_path != None:
        schema_path = args.schema_path[0]
    # end if

    # Path to the output if specified
    output_path = None
    if args.output_path != None:
        output_path = args.output_path[0]
    # end if

    reception_time = datetime.datetime.now().isoformat()
    if args.begin != None and is_datetime(args.begin[0]):
        reception_time = parser.parse(args.begin[0]).isoformat()
    # end if
    
    returned_value = eboa_ingestion.main(file_path, processor, output_path, reception_time, schema_path)

    exit(returned_value)
    
