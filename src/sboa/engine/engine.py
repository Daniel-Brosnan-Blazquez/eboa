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

# Import parsing module
import rboa.engine.parsing as parsing

# Import auxiliary functions
from eboa.engine.common_functions import insert_values, insert_alert_groups, insert_alert_cnfs

# Import auxiliary functions
from rboa.engine.functions import get_rboa_archive_path

# Import logging
from eboa.logging import Log

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
        "message": "The scheduler configuration file ({}) cannot be read"
    },
    "FILE_NOT_VALID": {
        "status": 2,
        "message": "The scheduler configuration file ({}) does not pass the schema verification ({})"
    },
    "CONFIG_FILE_DOES_NOT_EXIST": {
        "status": 3,
        "message": "The scheduler configuration file ({}) does not exist"
    },
    "T0_IS_NOT_DATETIME": {
        "status": 4,
        "message": "The T0 parameter ({}) is not an instance of datetime"
    }
}

class Engine():
    """Class for communicating with the engine of the rboa module

    Provides access to the logic for inserting, deleting and updating
    the information stored into the DDBB related to reports
    """

    def __init__(self, data = None):
        """
        Instantiation method

        :param data: data provided to be treat by the engine (default None)
        :type data: dict
        """
        if data == None:
            data = {}
        # end if
        self.data = data
        self.Scoped_session = scoped_session(Session)
        self.session = self.Scoped_session()
        self.session_progress = self.Scoped_session()
        self.operation = None
    
        return

    #####################
    # INSERTION METHODS #
    #####################
    def insert_configuration(self, t0 = datetime.datetime.now().date(), configuration_path = get_resources_path() + "/scheduler.xml"):

        # Check if file exists
        if not isinstance(t0, datetime.date):
            logger.error(exit_codes["T0_IS_NOT_DATETIME"]["message"].format(t0))
            return exit_codes["T0_IS_NOT_DATETIME"]["status"]
        # end if

        # Check if file exists
        if not os.path.isfile(configuration_path):
            logger.error(exit_codes["CONFIG_FILE_DOES_NOT_EXIST"]["message"].format(configuration_path))
            return exit_codes["CONFIG_FILE_DOES_NOT_EXIST"]["status"]
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
            return exit_codes["FILE_NOT_READABLE"]["status"]
        # end try
        
        # Check configuration against the schema
        valid = schema.validate(scheduler_xml)
        if not valid:
            logger.error(exit_codes["FILE_NOT_VALID"]["message"].format(configuration_path, get_schemas_path() + "/sboa_schema.xsd"))
            return exit_codes["FILE_NOT_VALID"]["status"]
        # end if

        # Obtain the xpath evaluator
        scheduler_xpath = etree.XPathEvaluator(scheduler_xml)

        # Remove previous agenda
        self.session.query(Rule).delete(synchronize_session=False)
        
        list_rules = []
        list_tasks = []
        for rule in scheduler_xpath("/rules/rule[not(boolean(@skip)) and not(@skip = true)]"):
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
                        weekday_number = 1
                    elif weekday_literal == "wednesday":
                        weekday_number = 1
                    elif weekday_literal == "thursday":
                        weekday_number = 1
                    elif weekday_literal == "friday":
                        weekday_number = 1
                    elif weekday_literal == "saturday":
                        weekday_number = 1
                    else:
                        weekday_number = 6
                    # end if
                    current_weekday = t0.isoweekday()
                    if current_weekday > weekday_number:
                        sum_days = 7 + weekday_number - current_weekday
                    else:
                        sum_days = weekday_number - current_weekday
                    # end if

                    triggering_date = t0.date() + datetime.timedelta(days=sum_days)
                    
                    triggering_time = triggering_date.isoformat() + "T" + time
                else:
                    triggering_time = t0.date().isoformat() + "T" + time
                # end if

            # end if

            list_rules.append(dict(rule_uuid = rule_uuid, name = name, periodicity = periodicity, window_delay = window_delay, window_size = window_size, triggering_time = triggering_time))

            for task in rule.xpath("tasks/task"):
                name = task.xpath("@name")[0]
                command = task.xpath("command")[0].text
                task_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                list_tasks.append(dict(task_uuid = task_uuid, name = name, command = command, rule_uuid = rule_uuid))
            # end for
        # end for

        # Bulk insert mappings
        self.session.bulk_insert_mappings(Rule, list_rules)
        self.session.bulk_insert_mappings(Task, list_tasks)
        self.session.commit()
        
        logger.error(exit_codes["OK"]["message"].format(configuration_path))
        return exit_codes["OK"]["status"]

    # end def

    def close_session (self):
        """
        Method to close the session
        """
        self.session.close()
        return
    # end def
