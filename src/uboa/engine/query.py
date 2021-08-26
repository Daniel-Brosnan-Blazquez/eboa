"""
Engine definition

Written by DEIMOS Space S.L. (jubv)

module uboa
"""

# Import SQLalchemy entities
from sqlalchemy.orm import scoped_session

# Import datamodel
from uboa.datamodel.base import Session, engine, Base
from uboa.datamodel.users import RoleUser, Role, User, ConfigurationUser, Configuration

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

    def get_users(self, user_uuids = None, emails = None, usernames = None, groups = None, last_logins_at = None, current_logins_at = None, last_login_ips = None, current_login_ips = None, login_counts = None, active = None, fs_uniquifiers = None, confirmed_at_filters = None, role_uuids = None, role_names = None, configuration_uuids = None, configuration_names = None, order_by = None, limit = None, offset = None):
        """
        Method to obtain the users filtered by the received parameters
        """
        params = []
        join_tables = False
        tables = {}

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

        # Last logins at
        if last_logins_at != None:
            functions.is_valid_date_filters(last_logins_at)
            for last_login_at in last_logins_at:
                op = arithmetic_operators[last_login_at["op"]]
                params.append(op(User.last_login_at, last_login_at["date"]))
            # end for
        # end if

        # Current logins at
        if current_logins_at != None:
            functions.is_valid_date_filters(current_logins_at)
            for current_login_at in current_logins_at:
                op = arithmetic_operators[current_login_at["op"]]
                params.append(op(User.current_login_at, current_login_at["date"]))
            # end for
        # end if

        # Last logins ip
        if last_login_ips != None:
            functions.is_valid_text_filter(last_login_ips)
            if last_login_ips["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[last_login_ips["op"]]
                params.append(op(User.last_login_ip, last_login_ips["filter"]))
            else:
                filter = eval('User.last_login_ip.' + text_operators[last_login_ips["op"]])
                params.append(filter(last_login_ips["filter"]))
            # end if
        # end if

        # Current logins ip
        if current_login_ips != None:
            functions.is_valid_text_filter(current_login_ips)
            if current_login_ips["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[current_login_ips["op"]]
                params.append(op(User.current_login_ip, current_login_ips["filter"]))
            else:
                filter = eval('User.current_login_ip.' + text_operators[current_login_ips["op"]])
                params.append(filter(current_login_ips["filter"]))
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

        # Active
        if active != None:
            functions.is_valid_bool_filter(active)
            params.append(User.active == active)
        # end if

        # fs_uniquifiers
        if fs_uniquifiers != None:
            functions.is_valid_text_filter(fs_uniquifiers)
            if fs_uniquifiers["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[fs_uniquifiers["op"]]
                params.append(op(User.fs_uniquifier, fs_uniquifiers["filter"]))
            else:
                filter = eval('User.fs_uniquifier.' + text_operators[fs_uniquifiers["op"]])
                params.append(filter(fs_uniquifiers["filter"]))
            # end if
        # end if

        # confirmed_at_filters
        if confirmed_at_filters != None:
            functions.is_valid_date_filters(confirmed_at_filters)
            for confirmed_at_filter in confirmed_at_filters:
                op = arithmetic_operators[confirmed_at_filter["op"]]
                params.append(op(User.confirmed_at, confirmed_at_filter["date"]))
            # end for
        # end if

        # Role UUIDs
        if role_uuids != None:
            functions.is_valid_text_filter(role_uuids)
            if role_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[role_uuids["op"]]
                params.append(op(RoleUser.role_uuid, role_uuids["filter"]))
            else:
                filter = eval('RoleUser.role_uuid.' + text_operators[role_uuids["op"]])
                params.append(filter(role_uuids["filter"]))
            # end if
            join_tables = True
        # end if

        # Role names
        if role_names != None:
            functions.is_valid_text_filter(role_names)
            if role_names["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[role_names["op"]]
                params.append(op(Role.name, role_names["filter"]))
            else:
                filter = eval('Role.name.' + text_operators[role_names["op"]])
                params.append(filter(role_names["filter"]))
            # end if
            join_tables = True
            tables[Role] = Role.role_uuid==RoleUser.role_uuid
        # end if

        query = self.session.query(User)
        if len(tables.keys()) > 0 or join_tables:
            query = query.join(RoleUser, RoleUser.user_uuid==User.user_uuid)
            for table in tables:
                query = query.join(table, tables[table])
            # end for
        # end if

        join_tables = False
        tables = {}

        # Configuration UUIDs
        if configuration_uuids != None:
            functions.is_valid_text_filter(configuration_uuids)
            if configuration_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[configuration_uuids["op"]]
                params.append(op(ConfigurationUser.configuration_uuid, configuration_uuids["filter"]))
            else:
                filter = eval('ConfigurationUser.configuration_uuid.' + text_operators[configuration_uuids["op"]])
                params.append(filter(configuration_uuids["filter"]))
            # end if
            join_tables = True
        # end if

        # Configuration names
        if configuration_names != None:
            functions.is_valid_text_filter(configuration_names)
            if configuration_names["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[configuration_names["op"]]
                params.append(op(Configuration.name, configuration_names["filter"]))
            else:
                filter = eval('Configuration.name.' + text_operators[configuration_names["op"]])
                params.append(filter(configuration_names["filter"]))
            # end if
            join_tables = True
            tables[Configuration] = Configuration.configuration_uuid==ConfigurationUser.configuration_uuid
        # end if

        query = self.session.query(User)
        if len(tables.keys()) > 0 or join_tables:
            query = query.join(ConfigurationUser, ConfigurationUser.user_uuid==User.user_uuid)
            for table in tables:
                query = query.join(table, tables[table])
            # end for
        # end if

        query = query.filter(*params)

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
        users = query.all()

        return users

    def close_session (self):
        """
        Method to close the session
        """
        self.session.close()
        return
