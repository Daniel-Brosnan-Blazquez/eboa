#!/usr/bin/env python3
"""
RBOA reporting tool

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
from importlib import import_module
import shutil
import os
from dateutil import parser
import datetime
import shlex
from subprocess import Popen, PIPE

# Import engine functions
from eboa.engine.functions import get_resources_path, get_schemas_path

# Import xml parser
from lxml import etree
import eboa.triggering.xpath_functions as xpath_functions

# Import engine
import rboa.engine.engine as rboa_engine
from rboa.engine.engine import Engine
from eboa.engine.query import Query

# Import logging
import argparse
from eboa.logging import Log

# Import debugging
from eboa.debugging import debug

# Import errors
from rboa.triggering.errors import ReportingConfigCannotBeRead, ReportingConfigDoesNotPassSchema, WrongReportingPeriod

logging_module = Log(name = os.path.basename(__file__))
logger = logging_module.logger

on_going_ingestions_folder = get_resources_path() + "/on_going_ingestions/"

def get_reporting_conf():
    schema_path = get_schemas_path() + "/reporting_generators_schema.xsd"
    parsed_schema = etree.parse(schema_path)
    schema = etree.XMLSchema(parsed_schema)
    # Get configuration
    try:
        reporting_xml = etree.parse(get_resources_path() + "/reporting_generators.xml")
    except etree.XMLSyntaxError as e:
        log = "The reporting configuration file ({}) cannot be read".format(get_resources_path() + "/reporting_generators.xml")
        logger.error(log)
        raise ReportingConfigCannotBeRead(log)
    # end try

    valid = schema.validate(reporting_xml)
    if not valid:
        log = "The reporting configuration file ({}) does not pass the schema ({})".format(get_resources_path() + "/reporting_generators.xml", get_schemas_path() + "/reporting_generators_schema.xsd")
        logger.error(log)
        raise ReportingConfigDoesNotPassSchema(log)
    # end if

    reporting_xpath = etree.XPathEvaluator(reporting_xml)

    return reporting_xpath

def execute_generator(generator, generation_mode, begin, end, output_path = None):

    # Register needed xpath functions
    ns = etree.FunctionNamespace(None)
    ns["match"] = xpath_functions.match

    # Check configuration
    reporting_xpath = get_reporting_conf()

    generator_xpaths = reporting_xpath("/reporting_generators/generator[@name = $generator]", generator = generator)

    if len(generator_xpaths) > 0:
        generator_xpath = generator_xpaths[0]
    else:
        logger.error("The generator '{}' is not registered in the configuration".format(generator))
        exit(-1)
    # end if
    
    report_name_format = generator_xpath.xpath("name_format")[0].text

    creation_date = datetime.datetime.now().isoformat().split(".")[0].replace(":", "").replace("-", "")
    validity_start = begin.split(".")[0].replace(":", "").replace("-", "")
    validity_stop = end.split(".")[0].replace(":", "").replace("-", "")
    
    report_name = report_name_format.replace("%C", creation_date).replace("%B", validity_start).replace("%E", validity_stop)

    command = generator_xpath.xpath("command")[0].text

    # Add generator
    command += " -g '" + generator + "'"
    
    # Execute the associated command
    add_report_name_parameter = generator_xpath.xpath("command/@add_report_name_parameter")
    add_report_name_parameter_flag = True
    if len(add_report_name_parameter) > 0:
        if add_report_name_parameter[0].lower() == "false":
            add_report_name_parameter_flag = False
        # end if
    # end if
    if add_report_name_parameter_flag:
        command += " -n " + report_name
    # end if

    add_begin_end_parameters = generator_xpath.xpath("command/@add_begin_end_parameters")
    add_begin_end_parameters_flag = True
    if len(add_begin_end_parameters) > 0:
        if add_begin_end_parameters[0].lower() == "false":
            add_begin_end_parameters_flag = False
        # end if
    # end if
    if add_begin_end_parameters_flag:
        command += " -b " + begin + " -e " + end
    # end if

    add_generation_mode_parameter = generator_xpath.xpath("command/@add_generation_mode_parameter")
    add_generation_mode_parameter_flag = True
    if len(add_generation_mode_parameter) > 0:
        if add_generation_mode_parameter[0].lower() == "false":
            add_generation_mode_parameter_flag = False
        # end if
    # end if
    if add_generation_mode_parameter_flag:
        command += " -m " + generation_mode
    # end if

    # Add parameters
    parameters = generator_xpath.xpath("parameters/parameter")
    if len(parameters) > 0:
        command += " -a "
        for parameter in parameters:
            name = parameter.get("name")
            value = parameter.text
            command += name + "=" + "'" + value + "' "
        # end for
    # end if
    
    if output_path:
        command += " -o " + "'" + output_path + "'"
    # end if

    logger.info("The command {} is going to be executed".format(command))
    
    if output_path == None:
        engine_rboa = Engine()
        
        # Insert an associated alert for checking pending ingestions
        data = {"operations": [{
            "mode": "insert",
            "report": {"name": report_name,
                       "group": "PENDING_GENERATION",
                       "group_description": "Group of reports pending of generation",
                       "path": "",
                       "compress": "false",
                       "generation_mode": generation_mode,
                       "validity_start": validity_start,
                       "validity_stop": validity_stop,
                       "triggering_time": creation_date,
                       "generation_start": creation_date,
                       "generation_stop": creation_date,
                       "generator": generator,
                       "generator_version": "",
                       "ingested": "false",
                       "values": [
                           {"type": "text",
                            "name": "command",
                            "value": command}
                       ]
            },
            "alerts": [{
                "message": "The report {} using generator {} is going to be produced".format(report_name, generator),
                "generator": os.path.basename(__file__),
                "notification_time": (datetime.datetime.now() + datetime.timedelta(hours=2)).isoformat(),
                "alert_cnf": {
                    "name": "PENDING_REPORT_GENERATION",
                    "severity": "fatal",
                    "description": "Alert refers to the pending generation of the report",
                    "group": "REPORTING_CONTROL"
                }
            }]
        }]
        }
        engine_rboa.treat_data(data)
        
        engine_rboa.close_session()
    # end if

    command_split = shlex.split(command)
    program = Popen(command_split, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    output, error = program.communicate()
    return_code = program.returncode
    if return_code != 0:
        logger.error("The execution of the command {} has ended unexpectedly with the following error: {}".format(command, str(error)))
        exit(-1)
    # end if

    logger.info("The command {} has been successfully executed".format(command))
    
    return

def main():

    args_parser = argparse.ArgumentParser(description='RBOA reporting generation tool.')
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
    generator = args.generator[0]
    generation_mode = args.generation_mode[0]
    begin = args.begin[0]
    end = args.end[0]

    output_path = None
    if args.output_path != None:
        output_path = args.output_path[0]
        # Check if file exists
        if not os.path.isdir(os.path.dirname(output_path)):
            logger.error("The specified path to the output file {} does not exist".format(output_path))
            exit(-1)
        # end if
    # end if

    if parser.parse(begin) > parser.parse(end):
        log = "The generator {} has been triggered with a begin value {} greater than the end value".format(begin, end)
        logger.error(log)
        raise WrongReportingPeriod(log)
    # end if

    execute_generator(generator, generation_mode, begin, end, output_path)

    exit(0)

if __name__ == "__main__":

    main()
