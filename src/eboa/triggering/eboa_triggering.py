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
        error_message = f"The triggering configuration file ({get_resources_path() + '/triggering.xml'}) cannot be read"
        logger.error(error_message)
        raise TriggeringConfigCannotBeRead(error_message)
    # end try

    valid = schema.validate(triggering_xml)
    if not valid:
        error_message = f"The triggering configuration file ({get_resources_path() + '/triggering.xml'}) does not pass the schema ({get_schemas_path() + '/triggering_schema.xsd'})"
        logger.error(error_message)
        raise TriggeringConfigDoesNotPassSchema(error_message)
    # end if

    triggering_xpath = etree.XPathEvaluator(triggering_xml)

    # Register needed xpath functions
    ns = etree.FunctionNamespace(None)
    ns["match"] = xpath_functions.match

    logger.info(f"Triggering configuration file ({get_resources_path() + '/triggering.xml'}) read successfully")
    
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
        logger.info(f"The triggering of the file {file_name} has been unblocked by the dependency: {dependency}")
        return
    # end def

    try:
        number_of_files_being_processed = len(os.listdir(on_going_ingestions_folder + dependency))
        while number_of_files_being_processed > 0:
            logger.info(f"The triggering of the file {file_name} will be blocked by the dependency: {dependency}. The number of files with source_type {dependency} being processed is {number_of_files_being_processed}")
            block_on_dependecy()
            number_of_files_being_processed = len(os.listdir(on_going_ingestions_folder + dependency))
        # end while
    except FileNotFoundError:
        # There were not files ingested yet associated to the dependency
        pass
    # end try

    logger.info(f"There are no files with source_type {dependency} blocking the triggering of the file {file_name}")
    
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
    exit_code = 0
    command_split = shlex.split(command)
    program = Popen(command_split, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    output, error = program.communicate()
    return_code = program.returncode
    error_message = ""
    if return_code != 0:
        logger.error(f"The execution of the command {command} has ended unexpectedly with the following error: {str(error.decode('UTF-8'))}")
        exit_code = -1
        error_message = str(error.decode("UTF-8"))
    else:
        logger.info(f"The execution of the command {command} has ended successfully")
    # end if

    return exit_code, error_message

@debug
def block_and_execute_command(source_type, command, file_name, dependencies, dependencies_on_this, test):
    """
    Method to execute the commands while blocking other processes which would have dependencies
    
    :param source_type: lock to use for the mutex
    :type source_type: str
    :param command: command to execute inside the mutex
    :type command: str
    """

    exit_code = 0
    
    newpid = -1
    if not test:
        newpid = os.fork()
    # end if
    if test or newpid == 0:
        for dependency in dependencies:
            logger.info(f"The triggering of the file {file_name} has a dependency on: {dependency.text}")
            # Block process on the related mutex depending on the configuration
            block_process(dependency.text, file_name)
        # end for

        logger.info(f"The following command is going to be triggered: {command}")
        logger.info(f"The execution of the command {command} will block triggerings depending on: {source_type}")

        exit_code, error_message = execute_command(command)
        if exit_code == 0:
            logger.info(f"The triggering of the file {file_name} has been executed")
        else:
            # Execution of triggering command failed
            logger.error(eboa_engine.exit_codes["TRIGGERING_COMMAND_ENDED_UNEXPECTEDLY"]["message"].format(file_name, error_message))
            query_log_status = Query()
            sources = query_log_status.get_sources(names = {"filter": file_name, "op": "=="}, dim_signatures = {"filter": "PENDING_SOURCES", "op": "=="})
            if len(sources) > 0:
                eboa_engine.insert_source_status(query_log_status.session, sources[0], eboa_engine.exit_codes["TRIGGERING_COMMAND_ENDED_UNEXPECTEDLY"]["status"], error = True, message = eboa_engine.exit_codes["TRIGGERING_COMMAND_ENDED_UNEXPECTEDLY"]["message"].format(file_name, error_message))
            # end if
            query_log_status.close_session()
        # end if
    
    else:

        try:
            logger.info(f"Creating folder {on_going_ingestions_folder + source_type}")
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
            logger.info(f"The triggering of the file {file_name} will block the triggering/s depending on: {source_type}".format(file_name, source_type))
            os.waitpid(newpid, 0)
        # end def

        if len(dependencies_on_this) > 0:
            logger.info(f"There are {len(dependencies_on_this)} rules depending on this source_type ({source_type}). This triggering will block them untill the ingestion has finished")
            blocking_on_command()
        else:
            logger.info(f"There are no dependency/ies on this source_type ({source_type})")
            os.waitpid(newpid, 0)
        # end if

        try:
            os.remove(on_going_ingestions_folder + source_type + "/" + file_name)
            logger.info(f"File {on_going_ingestions_folder + source_type + '/' + file_name} has been removed to unblock triggering/s depending on this source_type ({source_type})")
        except FileNotFoundError:
            pass
        # end try

        # Set the exit code to mark this thread as the parent and avoid processing the rest of rules
        exit_code = -1000

        logger.info(f"Main process for the triggering of file {file_name} has ended successfully")

    # end if

    return exit_code

@debug
def triggering(file_path, reception_time, engine_eboa, test, output_path = None, remove_input = False):
    """
    Method to trigger commands depending on the matched configuration to the file_path
    
    :param file_path: path to the input file
    :type file_path: str
    :param reception_time: date of reception of the file
    :type reception_time: str
    :param output_path: path to the output file
    :type output_path: str
    :param remove_input: flag to indicate if the input has to be removed
    :type remove_input: bool
    """

    exit_code = 0
    
    file_name = os.path.basename(file_path)

    # Check configuration
    triggering_xpath = get_triggering_conf()

    matching_rules = triggering_xpath("/triggering_rules/rule[match(source_mask, $file_name)]", file_name = file_name)

    confirm_removal_input = True    
    if len(matching_rules) > 0:
        logger.info(f"Found {len(matching_rules)} rule/s for the file {file_name}")
        for rule in matching_rules:
            skip = rule.get("skip")
            if skip == "true":
                logger.info(f"Found a rule for the file {file_name} with no tool execution")

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
                logger.info(f"Found a rule for the file {file_name} with tool execution")

                # Check if the PENDING_SOURCES object is still available
                query_pending = Query()
                sources = query_pending.get_sources(names = {"filter": file_name, "op": "=="}, dim_signatures = {"filter": "PENDING_SOURCES", "op": "=="})
                if len(sources) == 0:
                    logger.info(f"Recreating the alert for the expectancy of the ingestion of the file {file_name}")
                    # If the PENDING_SOURCES object is not available anymore, create it
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
                            "message": f"The input {file_path} has been received to be ingested",
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
                # end if
                query_pending.close_session()
                
                # File register into the configuration
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

                logger.info(f"Found {len(dependencies_on_this)} dependecy/ies on the triggering of the file {file_name}")

                if output_path:
                    logger.info(f"The following command is going to be triggered: {command}")
                    exit_code, error_message = execute_command(command)
                    if exit_code == 0:
                        logger.info(f"The triggering of the file {file_name} has been executed")
                    else:
                        # Execution of triggering command failed
                        logger.error(eboa_engine.exit_codes["TRIGGERING_COMMAND_ENDED_UNEXPECTEDLY"]["message"].format(file_path, error_message))
                        query_log_status = Query()
                        sources = query_log_status.get_sources(names = {"filter": file_name, "op": "=="}, dim_signatures = {"filter": "PENDING_SOURCES", "op": "=="})
                        if len(sources) > 0:
                            eboa_engine.insert_source_status(query_log_status.session, sources[0], eboa_engine.exit_codes["TRIGGERING_COMMAND_ENDED_UNEXPECTEDLY"]["status"], error = True, message = eboa_engine.exit_codes["TRIGGERING_COMMAND_ENDED_UNEXPECTEDLY"]["message"].format(file_path, error_message))
                        # end if
                        query_log_status.close_session()
                    # end if
                else:
                    exit_code = block_and_execute_command(source_type, command, file_name, dependencies, dependencies_on_this, test)

                    # Check if this thread is the parent thread and shall just end
                    if exit_code == -1000:
                        confirm_removal_input = False
                        exit_code = 0
                        break
                    # end if
                # end if
            # end if
        # end for
    else:
        exit_code = -1
        # File not register into the configuration
        query_log_status = Query()
        sources = query_log_status.get_sources(names = {"filter": file_name, "op": "=="}, dim_signatures = {"filter": "PENDING_SOURCES", "op": "=="})
        if len(sources) > 0:
            eboa_engine.insert_source_status(query_log_status.session, sources[0], eboa_engine.exit_codes["FILE_DOES_NOT_HAVE_A_TRIGGERING_RULE"]["status"], error = True, message = eboa_engine.exit_codes["FILE_DOES_NOT_HAVE_A_TRIGGERING_RULE"]["message"].format(file_path))
        # end if
        query_log_status.close_session()

        # Register the associated alert
        error_message = f"The file {file_name} does not match with any configured rule in {get_resources_path() + '/triggering.xml'}"
        logger.error(error_message)
        print("\nWARNING: " + error_message)
    # end if

    # Remove input if indicated and there are no more rules applying
    if remove_input and confirm_removal_input:
        try:
            os.remove(file_path)
            logger.info(f"The received file {file_path} has been removed")
        except FileNotFoundError:
            pass
        # end try
    # end if
    
    return exit_code

def main(file_path, output_path = None, remove_input = False, test = False):
    
    logger.info(f"Received file {file_path}")

    exit_code = 0
    
    reception_time = datetime.datetime.now().isoformat()

    if output_path != None:
        # Check if file exists
        if not os.path.isfile(file_path):
            logger.error(f"The specified file {file_path} does not exist")
            exit(-1)
        # end if

        engine_eboa = Engine()

        triggering(file_path, reception_time, engine_eboa, test, output_path)
        if remove_input:
            try:
                os.remove(file_path)
                logger.info(f"The received file {file_path} has been removed")
            except FileNotFoundError:
                pass
            # end try
        # end if
    else:

        # Check if file exists
        if not os.path.isfile(file_path):
            logger.error(f"The specified file {file_path} does not exist")
            exit_code = -1
        # end if

        newpid = -1
        if not test:
            newpid = os.fork()
        # end if
        if test or newpid == 0:
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
                    "message": f"The input {file_path} has been received to be ingested",
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

            # Check if file exists
            if not os.path.isfile(file_path):
                logger.error(f"The specified file {file_path} does not exist and will be marked in the DDBB")
                query_log_status = Query()
                sources = query_log_status.get_sources(names = {"filter": os.path.basename(file_path), "op": "=="}, dim_signatures = {"filter": "PENDING_SOURCES", "op": "=="})
                if len(sources) > 0:
                    eboa_engine.insert_source_status(query_log_status.session, sources[0], eboa_engine.exit_codes["FILE_DOES_NOT_EXIST"]["status"], error = True, message = eboa_engine.exit_codes["FILE_DOES_NOT_EXIST"]["message"].format(file_path))
                # end if
                query_log_status.close_session()
                engine_eboa.close_session()

                exit(-1)
            # end if


            exit_code = triggering(file_path, reception_time, engine_eboa, test, remove_input = remove_input)

            engine_eboa.close_session()

        # end if
    # end if

    exit(exit_code)    

if __name__ == "__main__":

    args_parser = argparse.ArgumentParser(description='EBOA triggering.')
    args_parser.add_argument('-f', dest='file_path', type=str, nargs=1,
                             help='path to the file to process', required=True)
    args_parser.add_argument("-o", dest="output_path", type=str, nargs=1,
                             help="path to the output file", required=False)
    args_parser.add_argument("-r", "--remove_input",
                             help="remove input file when triggering finished", action="store_true")
    
    args = args_parser.parse_args()

    # File path
    file_path = args.file_path[0]

    # Output path
    output_path = None
    if args.output_path != None:
        output_path = args.output_path[0]
        # Check if file exists
        if not os.path.isdir(os.path.dirname(output_path)):
            logger.error(f"The specified path to the output file {output_path} does not exist")
            exit(-1)
        # end if
    # end if

    # Remove input
    remove_input = False
    remove_input = args.remove_input
    
    main(file_path, output_path, remove_input)
