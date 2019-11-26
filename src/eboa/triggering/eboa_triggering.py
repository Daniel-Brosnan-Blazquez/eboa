#!/usr/bin/env python3
"""
Triggering definition

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import os
import shlex
from subprocess import Popen, PIPE
from oslo_concurrency import lockutils
import datetime
import errno

# Import engine functions
from eboa.engine.functions import get_resources_path, get_schemas_path

# Import xml parser
from lxml import etree
import eboa.triggering.xpath_functions as xpath_functions

# Import engine
import eboa.engine.engine as eboa_engine
from eboa.engine.engine import Engine
from eboa.engine.query import Query

# Import logging
import argparse
from eboa.logging import Log

# Import debugging
from eboa.debugging import debug

# Import errors
from eboa.triggering.errors import TriggeringConfigCannotBeRead, TriggeringConfigDoesNotPassSchema, FileDoesNotMatchAnyRule

logging_module = Log(name = os.path.basename(__file__))
logger = logging_module.logger

on_going_ingestions_folder = get_resources_path() + "/on_going_ingestions/"

def get_triggering_conf():
    schema_path = get_schemas_path() + "/triggering_schema.xsd"
    parsed_schema = etree.parse(schema_path)
    schema = etree.XMLSchema(parsed_schema)
    # Get configuration
    try:
        triggering_xml = etree.parse(get_resources_path() + "/triggering.xml")
    except etree.XMLSyntaxError as e:
        logger.error("The triggering configuration file ({}) cannot be read".format(get_resources_path() + "/triggering.xml"))
        raise TriggeringConfigCannotBeRead("The triggering configuration file ({}) cannot be read".format(get_resources_path() + "/triggering.xml"))
    # end try

    valid = schema.validate(triggering_xml)
    if not valid:
        logger.error("The triggering configuration file ({}) does not pass the schema ({})".format(get_resources_path() + "/triggering.xml", get_schemas_path() + "/triggering_schema.xsd"))
        raise TriggeringConfigDoesNotPassSchema("The triggering configuration file ({}) does not pass the schema ({})".format(get_resources_path() + "/triggering.xml", get_schemas_path() + "/triggering_schema.xsd"))
    # end if

    triggering_xpath = etree.XPathEvaluator(triggering_xml)

    return triggering_xpath

@debug
def block_process(dependency, file_name):
    """
    Method to block the execution of the process on a mutex
    
    :param dependecy: dependency where to block the process
    :type dependency: str
    """
    @lockutils.synchronized(dependency, lock_file_prefix="eboa-triggering-", external=True, lock_path="/dev/shm", fair=True)
    def block_on_dependecy():
        logger.debug("The triggering of the file {} has been unblocked by the dependency: {}".format(file_name, dependency))
        return
    # end def

    try:
        number_of_files_being_processed = len(os.listdir(on_going_ingestions_folder + dependency))
        while number_of_files_being_processed > 0:
            block_on_dependecy()
            number_of_files_being_processed = len(os.listdir(on_going_ingestions_folder + dependency))
        # end while
    except FileNotFoundError:
        # There were not files ingested yet associated to the dependency
        pass
    # end try
    return

@debug
def execute_command(command):
    """
    Method to execute the commands while blocking other processes which would have dependencies
    
    :param source_type: lock to use for the mutex
    :type source_type: str
    :param command: command to execute inside the mutex
    :type command: str
    """
    command_split = shlex.split(command)
    program = Popen(command_split, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    output, error = program.communicate()
    return_code = program.returncode
    if return_code != 0:
        logger.error("The execution of the command {} has ended unexpectedly with the following error: {}".format(command, str(error)))
        exit(-1)
    # end if
    
    return

@debug
def block_and_execute_command(source_type, command, file_name, dependencies, dependencies_on_this):
    """
    Method to execute the commands while blocking other processes which would have dependencies
    
    :param source_type: lock to use for the mutex
    :type source_type: str
    :param command: command to execute inside the mutex
    :type command: str
    """
    newpid = os.fork()
    if newpid == 0:
        for dependency in dependencies:
            logger.info("The triggering of the file {} has a dependency on: {}".format(file_name, dependency.text))
            # Block process on the related mutex depending on the configuration
            block_process(dependency.text, file_name)
        # end for

        logger.info("The following command is going to be triggered: {}".format(command))
        logger.info("The execution of the command {} will block triggerings depending on: {}".format(command, source_type))

        execute_command(command)
        logger.info("The triggering of the file {} has been executed".format(file_name))
    
    else:

        try:
            os.makedirs(on_going_ingestions_folder + source_type)
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
            # end if
            pass
        # end try

        open(on_going_ingestions_folder + source_type + "/" + file_name,"w+")
        
        @debug
        @lockutils.synchronized(source_type, lock_file_prefix="eboa-triggering-", external=True, lock_path="/dev/shm", fair=True)
        def blocking_on_command():
            logger.info("The triggering of the file {} will block the triggering depending on: {}".format(file_name, source_type))
            os.waitpid(newpid, 0)
        # end def

        if len(dependencies_on_this) > 0:
            blocking_on_command()
        else:
            os.waitpid(newpid, 0)
        # end if

        try:
            os.remove(on_going_ingestions_folder + source_type + "/" + file_name)
        except FileNotFoundError:
            pass
        # end try

    # end if

    return

@debug
def triggering(file_path, reception_time, engine_eboa, output_path = None):
    """
    Method to trigger commands depending on the matched configuration to the file_path
    
    :param file_path: path to the input file
    :type file_path: str
    :param reception_time: date of reception of the file
    :type reception_time: str
    :param output_path: path to the output file
    :type output_path: str
    """

    file_name = os.path.basename(file_path)

    # Register needed xpath functions
    ns = etree.FunctionNamespace(None)
    ns["match"] = xpath_functions.match

    # Check configuration
    triggering_xpath = get_triggering_conf()

    matching_rules_without_skip = triggering_xpath("/triggering_rules/rule[match(source_mask, $file_name) and (not(boolean(@skip)) or @skip = 'false')]", file_name = file_name)
    matching_rules_with_skip = triggering_xpath("/triggering_rules/rule[match(source_mask, $file_name) and @skip = 'true']", file_name = file_name)

    logger.info("Found {} rule/s for the file {} with tool execution".format(len(matching_rules_without_skip), file_name))

    if len(matching_rules_without_skip) > 0:
        # File register into the configuration
        for rule in matching_rules_without_skip:
            dependencies = None
            if output_path == None:
                dependencies = rule.xpath("dependencies/source_type")
            # end if
            
            # Execute the associated tool entering on its specific mutex depending on its source type
            add_file_path = rule.xpath("tool/command/@add_file_path")
            add_file_path_content = True
            if len(add_file_path) > 0:
                if add_file_path[0].lower() == "false":
                    add_file_path_content = False
                # end if
            # end if
            if add_file_path_content:
                command = rule.xpath("tool/command")[0].text + " " + file_path + " -t " + reception_time
            else:
                command = rule.xpath("tool/command")[0].text + " -t " + reception_time
            # end if

            if output_path:
                command = command + " -o " + output_path
            # end if
            source_type = rule.xpath("source_type")[0].text

            # Get dependencies on this type of triggering
            dependencies_on_this = triggering_xpath("/triggering_rules/rule/dependencies[source_type = $source_type]", source_type = source_type)

            logger.info("Found {} dependecy/ies on the triggering of the file {}".format(len(dependencies_on_this), file_name))

            if output_path:
                execute_command(command)
            else:
                block_and_execute_command(source_type, command, file_name, dependencies, dependencies_on_this)
            # end if
            
        # end for
    elif len(matching_rules_with_skip) > 0:
        logger.info("Found {} rule/s for the file {} with no tool execution".format(len(matching_rules_with_skip), file_name))

        # Insert an associated alert for checking pending ingestions
        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "SOURCES_NOT_PROCESSED",
                              "exec": "",
                              "version": ""},
            "source": {"name": os.path.basename(file_path),
                       "reception_time": reception_time,
                       "generation_time": reception_time,
                       "validity_start": datetime.datetime.now().isoformat(),
                       "validity_stop": datetime.datetime.now().isoformat(),
                       "ingested": "false"}
        }]
        }
        engine_eboa.treat_data(data)
        
        # Remove the metadata indicating the pending ingestion
        query_remove = Query()
        query_remove.get_sources(names = {"filter": file_name, "op": "=="}, dim_signatures = {"filter": "PENDING_SOURCES", "op": "=="}, delete = True)

        query_remove.close_session()

    else:
        # File not register into the configuration
        query_log_status = Query()
        sources = query_log_status.get_sources(names = {"filter": file_name, "op": "=="}, dim_signatures = {"filter": "PENDING_SOURCES", "op": "=="})
        if len(sources) > 0:
            eboa_engine.insert_source_status(query_log_status.session, sources[0], eboa_engine.exit_codes["FILE_DOES_NOT_HAVE_A_TRIGGERING_RULE"]["status"], error = True, message = eboa_engine.exit_codes["FILE_DOES_NOT_HAVE_A_TRIGGERING_RULE"]["message"].format(file_path))
        # end if
        query_log_status.close_session()

        # Register the associated alert
        logger.error("The file {} does not match with any configured rule in {}".format(file_name, get_resources_path() + "/triggering.xml"))
        raise FileDoesNotMatchAnyRule("The file {} does not match with any configured rule in {}".format(file_name, get_resources_path() + "/triggering.xml"))
    # end if

    return

def main():

    args_parser = argparse.ArgumentParser(description='EBOA triggering.')
    args_parser.add_argument('-f', dest='file_path', type=str, nargs=1,
                             help='path to the file to process', required=True)
    args_parser.add_argument("-o", dest="output_path", type=str, nargs=1,
                             help="path to the output file", required=False)
    args_parser.add_argument("-r", "--remove_input",
                             help="remove input file when triggering finished", action="store_true")
    
    args = args_parser.parse_args()
    file_path = args.file_path[0]
    
    logger.info("Received file {}".format(file_path))
    
    output_path = None
    if args.output_path != None:
        output_path = args.output_path[0]
        # Check if file exists
        if not os.path.isdir(os.path.dirname(output_path)):
            logger.error("The specified path to the output file {} does not exist".format(output_path))
            exit(-1)
        # end if
    # end if

    reception_time = datetime.datetime.now().isoformat()

    if output_path:
        # Check if file exists
        if not os.path.isfile(file_path):
            logger.error("The specified file {} does not exist".format(file_path))
            exit(-1)
        # end if

        engine_eboa = Engine()
        triggering(file_path, reception_time, engine_eboa, output_path)
        if args.remove_input:
            try:
                logger.info("The received file {} is going to be removed".format(file_path))
                os.remove(file_path)
            except FileNotFoundError:
                pass
            # end try
        # end if
    else:

        # Check if file exists
        if not os.path.isfile(file_path):
            logger.error("The specified file {} does not exist".format(file_path))
            query_log_status = Query()
            sources = query_log_status.get_sources(names = {"filter": os.path.basename(file_path), "op": "=="}, dim_signatures = {"filter": "PENDING_SOURCES", "op": "=="})
            if len(sources) > 0:
                eboa_engine.insert_source_status(query_log_status.session, sources[0], eboa_engine.exit_codes["FILE_DOES_NOT_EXIST"]["status"], error = True, message = eboa_engine.exit_codes["FILE_DOES_NOT_EXIST"]["message"].format(file_path))
            # end if
            query_log_status.close_session()

            exit(-1)
        # end if
        
        newpid = os.fork()
        result = 0
        if newpid == 0:
            engine_eboa = Engine()

            # Insert an associated alert for checking pending ingestions
            data = {"operations": [{
                "mode": "insert",
                "dim_signature": {"name": "PENDING_SOURCES",
                                  "exec": "",
                                  "version": ""},
                "source": {"name": os.path.basename(file_path),
                           "reception_time": reception_time,
                           "generation_time": reception_time,
                           "validity_start": datetime.datetime.now().isoformat(),
                           "validity_stop": datetime.datetime.now().isoformat(),
                           "ingested": "false"},
                "alerts": [{
                    "message": "The input {} has been received to be ingested".format(file_path),
                    "generator": os.path.basename(__file__),
                    "notification_time": (datetime.datetime.now() + datetime.timedelta(hours=2)).isoformat(),
                    "alert_cnf": {
                        "name": "PENDING_INGESTION_OF_SOURCE",
                        "severity": "fatal",
                        "description": "Alert refers to the pending ingestion of the relative input",
                        "group": "INGESTION_CONTROL"
                    },
                    "entity": {
                        "reference_mode": "by_ref",
                        "reference": os.path.basename(file_path),
                        "type": "source"
                    }
                }]
            }]
            }
            engine_eboa.treat_data(data)

            result = triggering(file_path, reception_time, engine_eboa)
            if args.remove_input:
                try:
                    os.remove(file_path)
                    logger.info("The received file {} is going to be removed".format(file_path))
                except FileNotFoundError:
                    pass
                # end try
            # end if

            engine_eboa.close_session()

        # end if
    # end if

    exit(0)    

if __name__ == "__main__":

    main()
