"""
Engine definition

Written by DEIMOS Space S.L. (jubv)

module uboa
"""

# Import SQLalchemy entities
from sqlalchemy.orm import scoped_session

# Import datamodel
from uboa.datamodel.base import Session, engine, Base
from uboa.datamodel.users import RoleUser, Role, User

# Import auxiliary functions
import eboa.engine.functions as functions

# Import operators
from eboa.engine.operators import arithmetic_operators, text_operators

# Import logging
from sboa.logging import Log

# Import query printing facilities
from eboa.engine.query import log_query

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

    def get_users(self, user_uuids = None, emails = None, usernames = None, groups = None, last_login_at_filters = None, current_login_at_filters = None, last_login_ip_filters = None, current_login_ip_filters = None, login_counts = None, active = None, fs_uniquifiers = None, confirmed_at_filters = None, order_by = None, limit = None, offset = None):
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

        # User UUIDs
        if user_uuids != None:
            functions.is_valid_text_filter(user_uuids)
            if user_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[user_uuids["op"]]
                params.append(op(User.user_uuid, user_uuids["filter"]))
            else:
                filter = eval('User.user_uuid.' + text_operators[user_uuids["op"]])
                params.append(filter(user_uuids["filter"]))
            # end if
        # end if

        # Emails
        if emails != None:
            functions.is_valid_text_filter(emails)
            if emails["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[emails["op"]]
                params.append(op(User.email, emails["filter"]))
            else:
                filter = eval('User.email.' + text_operators[emails["op"]])
                params.append(filter(emails["filter"]))
            # end if
        # end if
        
        # Usernames
        if usernames != None:
            functions.is_valid_text_filter(usernames)
            if usernames["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[usernames["op"]]
                params.append(op(User.username, usernames["filter"]))
            else:
                filter = eval('User.username.' + text_operators[usernames["op"]])
                params.append(filter(usernames["filter"]))
            # end if
        # end if

        # Groups
        if groups != None:
            functions.is_valid_text_filter(groups)
            if groups["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[groups["op"]]
                params.append(op(User.group, groups["filter"]))
            else:
                filter = eval('User.group.' + text_operators[groups["op"]])
                params.append(filter(groups["filter"]))
            # end if
        # end if

        # last_login_at filters
        if last_login_at_filters != None:
            functions.is_valid_date_filters(last_login_at_filters)
            for last_login_at_filter in last_login_at_filters:
                op = arithmetic_operators[last_login_at_filter["op"]]
                params.append(op(User.last_login_at, last_login_at_filter["date"]))
            # end for
        # end if

        # current_login_at filters
        if current_login_at_filters != None:
            functions.is_valid_date_filters(current_login_at_filters)
            for current_login_at_filter in current_login_at_filters:
                op = arithmetic_operators[current_login_at_filter["op"]]
                params.append(op(User.current_login_at, current_login_at_filter["date"]))
            # end for
        # end if

        # last_login_ip filters
        if last_login_ip_filters != None:
            functions.is_valid_text_filter(last_login_ip_filters)
            if last_login_ip_filters["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[last_login_ip_filters["op"]]
                params.append(op(User.last_login_ip, last_login_ip_filters["filter"]))
            else:
                filter = eval('User.last_login_ip.' + text_operators[last_login_ip_filters["op"]])
                params.append(filter(last_login_ip_filters["filter"]))
            # end if
        # end if

        # current_login_ip filters
        if current_login_ip_filters != None:
            functions.is_valid_text_filter(current_login_ip_filters)
            if current_login_ip_filters["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[current_login_ip_filters["op"]]
                params.append(op(User.current_login_ip, current_login_ip_filters["filter"]))
            else:
                filter = eval('User.current_login_ip.' + text_operators[current_login_ip_filters["op"]])
                params.append(filter(current_login_ip_filters["filter"]))
            # end if
        # end if

        # Login counts
        if login_counts != None:
            functions.is_valid_positive_integer(login_counts)
            for login_count in login_counts:
                op = arithmetic_operators[login_count["op"]]
                params.append(op(User.login_count, login_count["int"]))
            # end for
        # end if

        query = self.session.query(User).filter(*params)

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

    def close_session (self):
        """
        Method to close the session
        """
        self.session.close()
        return
