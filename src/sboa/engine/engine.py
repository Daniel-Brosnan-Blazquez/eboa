"""
Engine definition

Written by DEIMOS Space S.L. (dibb)

module sboa
"""

# Import python utilities
import datetime
import uuid
import random
import os
from dateutil import parser
import tarfile
from shutil import copyfile
import errno
import json

# Import xml parser
from lxml import etree

# Import SQLalchemy entities
from sqlalchemy.exc import IntegrityError, InternalError
from sqlalchemy.sql import func
from sqlalchemy.orm import scoped_session

# Import eboa functions
from eboa.engine.functions import get_resources_path, get_schemas_path

# Import debugging
from eboa.debugging import debug, race_condition

# Import datamodel
from sboa.datamodel.base import Session
from sboa.datamodel.rules import Rule, Task, Triggering

# Import auxiliary functions
from eboa.engine.common_functions import insert_values, insert_alert_groups, insert_alert_cnfs

# Import auxiliary functions
from rboa.engine.functions import get_rboa_archive_path

# Import BOA scheduler
import sboa.scheduler.boa_scheduler_functions as boa_scheduler

# Import logging
from sboa.logging import Log

logging = Log(name = __name__)
logger = logging.logger

archive_path = get_rboa_archive_path()

exit_codes = {
    "OK": {
        "status": 0,
        "message": "The scheduler configuration file ({}) has been loaded"
    },
    "FILE_NOT_READABLE": {
        "status": 1,
        "message": "The scheduler configuration could not be loaded. The scheduler configuration file ({}) cannot be read"
    },
    "FILE_NOT_VALID": {
        "status": 2,
        "message": "The scheduler configuration could not be loaded. The scheduler configuration file ({}) does not pass the schema verification ({})"
    },
    "CONFIG_FILE_DOES_NOT_EXIST": {
        "status": 3,
        "message": "The scheduler configuration could not be loaded. The scheduler configuration file ({}) does not exist"
    },
    "T0_IS_NOT_DATETIME": {
        "status": 4,
        "message": "The scheduler configuration could not be loaded. The T0 parameter ({}) is not an instance of datetime"
    },
    "RULES_AND_TASKS_GENERATED": {
        "status": 5,
        "message": "Rules and tasks have been generated"
    },
    "DATE_IS_NOT_DATETIME": {
        "status": 6,
        "message": "Triggering date is not a valid datetime for the task {}"
    },
    "TASK_UUID_DOES_NOT_EXIST": {
        "status": 7,
        "message": "The task uuid provided does not correspond to any task stored"
    },
    "OK_TRIGGERING": {
        "status": 0,
        "message": "The triggering for the task ({}) has been registered"
    },
    "DUPLICATED_RULE_NAMES": {
        "status": 8,
        "message": "The scheduler configuration could not be loaded. There are names of rules duplicated. List of rule names {} / list of unique rule names {}"
    },
    "DUPLICATED_TASK_NAMES": {
        "status": 9,
        "message": "The scheduler configuration could not be loaded. There are names of tasks duplicated. List of task names {} / list of unique task names {}"
    },
    "BOA_SCHEDULER_COULD_NOT_START": {
        "status": 10,
        "message": "BOA scheduler could not start. Output of command was {}, output error {} and return code {}"
    }    
}

class Engine():
    """Class for communicating with the engine of the sboa module

    Provides access to the logic for inserting, deleting and updating
    the information stored into the DDBB related to the scheduler
    """

    def __init__(self):
        """
        Instantiation method
        """
        self.Scoped_session = scoped_session(Session)
        self.session = self.Scoped_session()
    
        return

    #####################
    # INSERTION METHODS #
    #####################
    @debug
    def insert_configuration(self, t0 = datetime.datetime.now().date(), configuration_path = get_resources_path() + "/scheduler.xml", start_scheduler = False):

        # Switch off scheduler
        boa_scheduler.stop_scheduler()

        list_rules = []
        list_tasks = []
        returned_status = self.generate_rules_and_tasks(list_rules, list_tasks, t0, configuration_path)

        if returned_status["status"] != exit_codes["RULES_AND_TASKS_GENERATED"]["status"]:
            return returned_status
        # end if

        # Remove previous agenda
        self.session.query(Rule).delete(synchronize_session=False)
        
        # Bulk insert mappings
        self.session.bulk_insert_mappings(Rule, list_rules)
        self.session.bulk_insert_mappings(Task, list_tasks)
        self.session.commit()

        # Switch on scheduler
        if start_scheduler:
            command_status = boa_scheduler.command_start_scheduler()

            if command_status["return_code"] != 0:
                message = exit_codes["BOA_SCHEDULER_COULD_NOT_START"]["message"].format(command_status["output"], command_status["error"], command_status["return_code"])
                logger.error(message)
                return {"status": exit_codes["BOA_SCHEDULER_COULD_NOT_START"]["status"], "message": message}
            # end if
        # end if            
        
        message = exit_codes["OK"]["message"].format(configuration_path)
        logger.info(message)
        return {"status": exit_codes["OK"]["status"], "message": message}

    # end def

    @debug
    def generate_rules_and_tasks(self, list_rules, list_tasks, t0 = datetime.datetime.now().date(), configuration_path = get_resources_path() + "/scheduler.xml"):

        # Check if file exists
        if not isinstance(t0, datetime.date):
            message = exit_codes["T0_IS_NOT_DATETIME"]["message"].format(t0)
            logger.error(message)
            return {"status": exit_codes["T0_IS_NOT_DATETIME"]["status"], "message": message}
        # end if

        # Check if file exists
        if not os.path.isfile(configuration_path):
            message = exit_codes["CONFIG_FILE_DOES_NOT_EXIST"]["message"].format(configuration_path)
            logger.error(message)
            return {"status": exit_codes["CONFIG_FILE_DOES_NOT_EXIST"]["status"], "message": message}
        # end if
        
        # Get schema
        schema_path = get_schemas_path() + "/sboa_schema.xsd"
        parsed_schema = etree.parse(schema_path)
        schema = etree.XMLSchema(parsed_schema)
        
        # Get configuration
        try:
            scheduler_xml = etree.parse(configuration_path)
        except etree.XMLSyntaxError as e:

            logger.error(exit_codes["FILE_NOT_READABLE"]["message"].format(configuration_path))
            return {"status": exit_codes["FILE_NOT_READABLE"]["status"], "message": message}
        # end try
        
        # Check configuration against the schema
        valid = schema.validate(scheduler_xml)
        if not valid:
            logger.error(exit_codes["FILE_NOT_VALID"]["message"].format(configuration_path, get_schemas_path() + "/sboa_schema.xsd"))
            return {"status": exit_codes["FILE_NOT_VALID"]["status"], "message": message}
        # end if

        # Obtain the xpath evaluator
        scheduler_xpath = etree.XPathEvaluator(scheduler_xml)

        # Check uniqueness of names
        rule_names = [rule for rule in scheduler_xpath("/rules/rule[not(boolean(@skip)) and not(@skip = 'true')]/@name")]
        unique_rule_names = set(rule_names)
        if len(rule_names) != len(unique_rule_names):
            message = exit_codes["DUPLICATED_RULE_NAMES"]["message"].format(rule_names, unique_rule_names)
            logger.error(message)
            return {"status": exit_codes["DUPLICATED_RULE_NAMES"]["status"], "message": message}
        # end if

        task_names = [task.xpath("@name")[0] for task in scheduler_xpath("/rules/rule/tasks/task[not(boolean(@skip)) and not(@skip = 'true')]")]
        unique_task_names = set(task_names)
        if len(task_names) != len(unique_task_names):
            message = exit_codes["DUPLICATED_TASK_NAMES"]["message"].format(task_names, unique_task_names)
            logger.error(message)
            return {"status": exit_codes["DUPLICATED_TASK_NAMES"]["status"], "message": message}
        # end if
        
        for rule in scheduler_xpath("/rules/rule[not(boolean(@skip)) and not(@skip = 'true')]"):
            # Get metadata of the rule
            name = rule.xpath("@name")[0]
            periodicity = rule.xpath("periodicity")[0].text
            window_delay = rule.xpath("window_delay")[0].text
            window_size = rule.xpath("window_size")[0].text
            date_specific = rule.xpath("date_specific")
            rule_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))

            # Stablish triggering time
            if len(date_specific) > 0:
                triggering_time = date_specific[0].xpath("date")[0].text
            else:
                time = rule.xpath("date/time")[0].text
                weekday = rule.xpath("date/weekday")
                if len(weekday) > 0:
                    weekday_literal = weekday[0].text
                    if weekday_literal == "monday":
                        weekday_number = 1
                    elif weekday_literal == "tuesday":
                        weekday_number = 2
                    elif weekday_literal == "wednesday":
                        weekday_number = 3
                    elif weekday_literal == "thursday":
                        weekday_number = 4
                    elif weekday_literal == "friday":
                        weekday_number = 5
                    elif weekday_literal == "saturday":
                        weekday_number = 6
                    else:
                        weekday_number = 7
                    # end if
                    current_weekday = t0.isoweekday()
                    if current_weekday > weekday_number:
                        sum_days = 7 + weekday_number - current_weekday
                    else:
                        sum_days = weekday_number - current_weekday
                    # end if

                    triggering_date = datetime.datetime(t0.year, t0.month, t0.day).date() + datetime.timedelta(days=sum_days)
                    
                    triggering_time = triggering_date.isoformat() + "T" + time
                else:
                    triggering_time = datetime.datetime(t0.year, t0.month, t0.day).date().isoformat() + "T" + time
                # end if

            # end if

            list_rules.append(dict(rule_uuid = rule_uuid, name = name, periodicity = periodicity, window_delay = window_delay, window_size = window_size))

            for task in rule.xpath("tasks/task[not(boolean(@skip)) and not(@skip = 'true')]"):
                name = task.xpath("@name")[0]
                command = task.xpath("command")[0].text

                add_window_arguments = True
                add_window_arguments_xml = task.xpath("@add_window_arguments")
                if len(add_window_arguments_xml) > 0 and add_window_arguments_xml[0] == "false":
                    add_window_arguments = False
                # end if
                    
                task_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                list_tasks.append(dict(task_uuid = task_uuid, name = name, command = command, triggering_time = triggering_time, add_window_arguments = add_window_arguments, rule_uuid = rule_uuid))
            # end for
        # end for
        return {"status": exit_codes["RULES_AND_TASKS_GENERATED"]["status"], "message": exit_codes["RULES_AND_TASKS_GENERATED"]["message"]}
    # end def

    @debug
    def insert_triggering(self, date, task_uuid):

        task = self.session.query(Task).filter(Task.task_uuid == task_uuid).first()
        if not task:
            logger.error(exit_codes["TASK_UUID_DOES_NOT_EXIST"]["message"])
            returned_information = {
                "triggering_uuid": "",
                "status": exit_codes["TASK_UUID_DOES_NOT_EXIST"]["status"]
            }
            return returned_information
        # end if

        if not isinstance(date, datetime.datetime):
            # Log the error
            logger.error(exit_codes["DATE_IS_NOT_DATETIME"]["message"].format(task.name))
            returned_information = {
                "triggering_uuid": "",
                "status": exit_codes["DATE_IS_NOT_DATETIME"]["status"]
            }
            return returned_information
        # end if

        id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))

        triggering = Triggering(id, date, False, task.task_uuid)
        
        self.session.add(triggering)
        self.session.commit()
        logger.info(exit_codes["OK_TRIGGERING"]["message"].format(task.name))        
        returned_information = {
            "triggering_uuid": id,
            "status": exit_codes["OK_TRIGGERING"]["status"]
        }
        return returned_information

    @debug
    def triggering_done(self, triggering_uuid):

        triggering = self.session.query(Triggering).filter(Triggering.triggering_uuid == triggering_uuid).first()
        triggering.triggered = True
        self.session.commit()

        return

    @debug
    def set_triggering_time(self, task_uuid, triggering_time):

        task = self.session.query(Task).filter(Task.task_uuid == task_uuid).first()
        task.triggering_time = triggering_time
        self.session.commit()

        return

    def close_session (self):
        """
        Method to close the session
        """
        self.session.close()
        return
    # end def
