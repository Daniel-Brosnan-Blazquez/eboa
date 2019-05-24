"""
Triggering definition

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import os
from oslo_concurrency import lockutils

# Import engine functions
from eboa.engine.functions import get_resources_path, get_schemas_path

# Import xml parser
from lxml import etree
import eboa.triggering.xpath_functions as xpath_functions

# Import logging
import argparse
from eboa.logging import Log

# Import debugging
from eboa.debugging import debug

# Import errors
from eboa.triggering.errors import TriggeringConfigCannotBeRead, TriggeringConfigDoesNotPassSchema, FileDoesNotMatchAnyRule

logging_module = Log()
logger = logging_module.logger

# Set the synchronized module
synchronized = lockutils.synchronized_with_prefix('eboa-triggering-')

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

    triggering_xpath = etree.XPathEvaluator(triggering_xml)

    valid = schema.validate(triggering_xml)
    if not valid:
        logger.error("The triggering configuration file ({}) does not pass the schema ({})".format(get_resources_path() + "/triggering.xml", get_schemas_path() + "/triggering_schema.xsd"))
        raise TriggeringConfigCannotBeRead("The triggering configuration file ({}) does not pass the schema ({})".format(get_resources_path() + "/triggering.xml", get_schemas_path() + "/triggering_schema.xsd"))
    # end if

    return triggering_xpath

@debug
def block_process(dependency):
    """
    Method to block the execution of the process on a mutex
    
    :param dependecy: dependency where to block the process
    :type dependency: str
    """
    @synchronized(dependency, external=True, lock_path="/dev/shm")
    def block_on_dependecy():
        return
    # end def

    block_on_dependecy()
    
    return

@debug
def execute_command(source_type, command):
    """
    Method to execute the commands while blocking other processes which would have dependencies
    
    :param source_type: lock to use for the mutex
    :type source_type: str
    :param command: command to execute inside the mutex
    :type command: str
    """
    @synchronized(source_type, external=True, lock_path="/dev/shm")
    def blocking_on_command(command):
        os.system(command)
    # end def

    blocking_on_command(command)

    return

@debug
def triggering(file_path):
    """
    Method to trigger commands depending on the matched configuration to the file_path
    
    :param file_path: path to the input file
    :type file_path: str
    """

    file_name = os.path.basename(file_path)

    # Register needed xpath functions
    ns = etree.FunctionNamespace(None)
    ns["match"] = xpath_functions.match

    # Check configuration
    triggering_xpath = get_triggering_conf()

    matching_rules = triggering_xpath("/triggering_rules/rule[match(source_mask, $file_name)]", file_name = file_name)

    logger.info("Found {} rules for the file {}".format(len(matching_rules), file_name))

    if len(matching_rules) > 0:
        # File register into the configuration
        for rule in matching_rules:
            dependecies = rule.xpath("dependencies/source_type")
            for dependency in dependecies:
                logger.info("The triggering of the file {} has a dependency on: {}".format(file_name, dependency.text))
                # Block process on the related mutex depending on the configuration
                block_process(dependency.text)
            # end for

            # Execute the associated tool entering on its specific mutex depending on its source type
            add_file_path = rule.xpath("tool/command/@add_file_path")
            add_file_path_content = True
            if len(add_file_path) > 0:
                if add_file_path[0].lower() == "false":
                    add_file_path_content = False
                # end if
            # end if
            if add_file_path_content:
                command = rule.xpath("tool/command")[0].text + " " + file_path
            else:
                command = rule.xpath("tool/command")[0].text
            # end if
            logger.info("The following command is going to be triggered: {}".format(command))
            source_type = rule.xpath("source_type")[0].text
            logger.info("The execution of the command {} will block triggerings depending on: {}".format(command, source_type))
            execute_command(source_type, command)
            
            # Register an alert if the tool didn't succeed
        # end for
    else:
        # File not register into the configuration
        # Register the associated alert
        logger.error("The file {} does not match with any configured rule in {}".format(file_name, get_resources_path() + "/triggering.xml"))
        raise FileDoesNotMatchAnyRule("The file {} does not match with any configured rule in {}".format(file_name, get_resources_path() + "/triggering.xml"))
    # end if

    return

if __name__ == "__main__":

    args_parser = argparse.ArgumentParser(description='EBOA triggering.')
    args_parser.add_argument('-f', dest='file_path', type=str, nargs=1,
                             help='path to the file to process', required=True)
    args = args_parser.parse_args()
    file_path = args.file_path[0]

    logger.info("Received file {}".format(file_path))

    newpid = os.fork()
    result = 0
    if newpid == 0:
        result = triggering(file_path)
    # end if

    exit(0)
