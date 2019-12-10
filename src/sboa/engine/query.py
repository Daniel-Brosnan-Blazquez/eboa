"""
Engine definition

Written by DEIMOS Space S.L. (dibb)

module sboa
"""
# Import python utilities
import datetime
from datetime import timedelta
from lxml import etree
import uuid

# Import SQLalchemy entities
from sqlalchemy import extract
from sqlalchemy.orm import scoped_session

# Import datamodel
from sboa.datamodel.base import Session, engine, Base
from sboa.datamodel.rules import Rule, Task, Triggering

# Import exceptions
from eboa.engine.errors import InputError

# Import auxiliary functions
import eboa.engine.functions as functions

# Import logging
from sboa.logging import Log

# Import query printing facilities
from eboa.engine.query import log_query

# Import operators
from eboa.engine.operators import arithmetic_operators, text_operators

logging = Log(name = __name__)
logger = logging.logger

class Query():

    def __init__(self, session = None):
        """
        Class for querying the information stored into DDBB

        :param session: opened session
        :type session: sqlalchemy.orm.sessionmaker
        """
        if session == None:
            Scoped_session = scoped_session(Session)
            self.session = Scoped_session()
        else:
            self.session = session
        # end if

        return

    def clear_db(self):
        for table in reversed(Base.metadata.sorted_tables):
            engine.execute(table.delete())
        # end for

    def get_rules(self, rule_uuids = None, names = None, window_size_filters = None, order_by = None, limit = None, offset = None):
        """
        Method to obtain the rule entities filtered by the received parameters

        :param rule_uuids: list of rule identifiers
        :type rule_uuids: text_filter
        :param names: name filters
        :type names: text_filter
        :param window_size_filters: list of window size filters
        :type window_size_filters: float_filters

        :return: found rules
        :rtype: list
        """
        params = []

        # Rule UUIDs
        if rule_uuids != None:
            functions.is_valid_text_filter(rule_uuids)
            if rule_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[rule_uuids["op"]]
                params.append(op(Rule.rule_uuid, rule_uuids["filter"]))
            else:
                filter = eval('Rule.rule_uuid.' + text_operators[rule_uuids["op"]])
                params.append(filter(rule_uuids["filter"]))
            # end if
        # end if

        # Names
        if names != None:
            functions.is_valid_text_filter(names)
            if names["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[names["op"]]
                params.append(op(Rule.name, names["filter"]))
            else:
                filter = eval('Rule.name.' + text_operators[names["op"]])
                params.append(filter(names["filter"]))
            # end if
        # end if

        # window size filters
        if window_size_filters != None:
            functions.is_valid_float_filters(window_size_filters)
            for window_size_filter in window_size_filters:
                op = arithmetic_operators[window_size_filter["op"]]
                params.append(op(Rule.window_size, window_size_filter["float"]))
            # end for
        # end if

        query = self.session.query(Rule).filter(*params)

        # Order by
        if order_by != None:
            functions.is_valid_order_by(order_by)
            if order_by["descending"]:
                order_by_statement = eval("Rule." + order_by["field"] + ".desc()")
            else:
                order_by_statement = eval("Rule." + order_by["field"])
            # end if
            query = query.order_by(order_by_statement)
        # end if

        # Limit
        if limit != None:
            functions.is_valid_positive_integer(limit)
            query = query.limit(limit)
        # end if

        # Offset
        if offset != None:
            functions.is_valid_positive_integer(offset)
            query = query.offset(offset)
        # end if
        
        log_query(query)
        rules = query.all()

        return rules

    def get_tasks(self, task_uuids = None, names = None, triggering_time_filters = None, order_by = None, limit = None, offset = None):
        """
        Method to obtain the task entities filtered by the received parameters

        :param task_uuids: list of task identifiers
        :type task_uuids: text_filter
        :param names: name filters
        :type names: text_filter

        :return: found tasks
        :rtype: list
        """
        params = []
        
        # Task UUIDs
        if task_uuids != None:
            functions.is_valid_text_filter(task_uuids)
            if task_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[task_uuids["op"]]
                params.append(op(Task.task_uuid, task_uuids["filter"]))
            else:
                filter = eval('Task.task_uuid.' + text_operators[task_uuids["op"]])
                params.append(filter(task_uuids["filter"]))
            # end if
        # end if

        # Names
        if names != None:
            functions.is_valid_text_filter(names)
            if names["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[names["op"]]
                params.append(op(Task.name, names["filter"]))
            else:
                filter = eval('Task.name.' + text_operators[names["op"]])
                params.append(filter(names["filter"]))
            # end if
        # end if

        # triggering_time filters
        if triggering_time_filters != None:
            functions.is_valid_date_filters(triggering_time_filters)
            for triggering_time_filter in triggering_time_filters:
                op = arithmetic_operators[triggering_time_filter["op"]]
                params.append(op(Task.triggering_time, triggering_time_filter["date"]))
            # end for
        # end if

        query = self.session.query(Task).filter(*params)

        # Order by
        if order_by != None:
            functions.is_valid_order_by(order_by)
            if order_by["descending"]:
                order_by_statement = eval("Task." + order_by["field"] + ".desc()")
            else:
                order_by_statement = eval("Task." + order_by["field"])
            # end if
            query = query.order_by(order_by_statement)
        # end if

        # Limit
        if limit != None:
            functions.is_valid_positive_integer(limit)
            query = query.limit(limit)
        # end if

        # Offset
        if offset != None:
            functions.is_valid_positive_integer(offset)
            query = query.offset(offset)
        # end if
        
        log_query(query)
        tasks = query.all()

        return tasks


    def get_triggerings(self, triggering_uuids = None, date_filters = None, triggered = None, task_names = None, order_by = None, limit = None, offset = None):
        """
        Method to obtain the triggering entities filtered by the received parameters

        :param triggering_uuids: list of triggering identifiers
        :type triggering_uuids: text_filter
        :param date_filters: list of triggering time filters
        :type date_filters: date_filters
        :param task_names: name of tasks filters
        :type task_names: text_filter

        :return: found triggerings
        :rtype: list
        """
        params = []
        tables = []
        
        # Triggering UUIDs
        if triggering_uuids != None:
            functions.is_valid_text_filter(triggering_uuids)
            if triggering_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[triggering_uuids["op"]]
                params.append(op(Triggering.triggering_uuid, triggering_uuids["filter"]))
            else:
                filter = eval('Triggering.triggering_uuid.' + text_operators[triggering_uuids["op"]])
                params.append(filter(triggering_uuids["filter"]))
            # end if
        # end if

        # date filters
        if date_filters != None:
            functions.is_valid_date_filters(date_filters)
            for date_filter in date_filters:
                op = arithmetic_operators[date_filter["op"]]
                params.append(op(Triggering.date, date_filter["date"]))
            # end for
        # end if

        # triggered filter
        if triggered != None:
            functions.is_valid_bool_filter(triggered)
            params.append(Triggering.triggered == triggered)
        # end if
        
        # Names
        if task_names != None:
            functions.is_valid_text_filter(task_names)
            if task_names["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[task_names["op"]]
                params.append(op(Task.name, task_names["filter"]))
            else:
                filter = eval('Task.name.' + text_operators[task_names["op"]])
                params.append(filter(task_names["filter"]))
            # end if
            tables.append(Task)
        # end if

        query = self.session.query(Triggering)
        for table in set(tables):
            query = query.join(table)
        # end for

        query = query.filter(*params)

        # Order by
        if order_by != None:
            functions.is_valid_order_by(order_by)
            if order_by["descending"]:
                order_by_statement = eval("Triggering." + order_by["field"] + ".desc()")
            else:
                order_by_statement = eval("Triggering." + order_by["field"])
            # end if
            query = query.order_by(order_by_statement)
        # end if

        # Limit
        if limit != None:
            functions.is_valid_positive_integer(limit)
            query = query.limit(limit)
        # end if

        # Offset
        if offset != None:
            functions.is_valid_positive_integer(offset)
            query = query.offset(offset)
        # end if
        
        log_query(query)
        triggerings = query.all()

        return triggerings

    def close_session (self):
        """
        Method to close the session
        """
        self.session.close()
        return
