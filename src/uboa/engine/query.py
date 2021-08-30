"""
Engine definition

Written by DEIMOS Space S.L. (jubv)

module uboa
"""
# Import SQLalchemy entities
from sqlalchemy.orm import scoped_session

# Import datamodel
from uboa.datamodel.base import Session, engine, Base
from uboa.datamodel.users import RoleUser, Role, User#, ConfigurationUser, Configuration

# Import auxiliary functions
import eboa.engine.functions as functions

# Import operators
from eboa.engine.operators import arithmetic_operators, text_operators

# Import logging
from uboa.logging import Log

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

    def get_users(self, user_uuids = None, emails = None, usernames = None, groups = None, active = None, role_uuids = None, roles = None, configuration_uuids = None, configuration_names = None, order_by = None, limit = None, offset = None):
        """
        Method to obtain the users filtered by the received parameters

        :param user_uuids: list of user identifiers
        :type user_uuids: text_filter
        :param emails: list of emails
        :type emails: text_filter
        :param usernames: list of usernames
        :type usernames: text_filter
        :param groups: list of the group to which the user belongs
        :type groups: text_filter
        :param active: flag to indicate if the user is active
        :type active: boolean_filter
        :param role_uuids: list of role identifiers
        :type role_uuids: text_filter
        :param roles: list of user's roles
        :type roles: text_filter
        :param configuration_uuids: list of configuration identifiers
        :type configuration_uuids: text_filter
        :param configuration_names: list of the name of the configuration
        :type configuration_names: text_filter
        :param order_by: field to order by
        :type order_by: order_by statement
        :param limit: positive integer to limit the number of results of the query
        :type limit: positive integer
        :param offset: positive integer to offset the pointer to the list of results
        :type offset: positive integer

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

        # Active
        if active != None:
            functions.is_valid_bool_filter(active)
            params.append(User.active == active)
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
        if roles != None:
            functions.is_valid_text_filter(roles)
            if roles["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[roles["op"]]
                params.append(op(Role.name, roles["filter"]))
            else:
                filter = eval('Role.name.' + text_operators[roles["op"]])
                params.append(filter(roles["filter"]))
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

    def get_roles(self, role_uuids = None, roles = None, user_uuids = None, emails = None, usernames = None, groups = None, active = None, order_by = None, limit = None, offset = None):
        """
        Method to obtain the roles filtered by the received parameters

        :param role_uuids: list of role identifiers
        :type role_uuids: text_filter
        :param roles: list of user's roles
        :type roles: text_filter
        :param user_uuids: list of user identifiers
        :type user_uuids: text_filter
        :param emails: list of emails
        :type emails: text_filter
        :param usernames: list of usernames
        :type usernames: text_filter
        :param groups: list of the group to which the user belongs
        :type groups: text_filter
        :param active: flag to indicate if the user is active
        :type active: boolean_filter
        :param order_by: field to order by
        :type order_by: order_by statement
        :param limit: positive integer to limit the number of results of the query
        :type limit: positive integer
        :param offset: positive integer to offset the pointer to the list of results
        :type offset: positive integer

        """
        params = []
        join_tables = False
        tables = {}

        # Role UUIDs
        if role_uuids != None:
            functions.is_valid_text_filter(role_uuids)
            if role_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[role_uuids["op"]]
                params.append(op(Role.role_uuid, role_uuids["filter"]))
            else:
                filter = eval('Role.role_uuid.' + text_operators[role_uuids["op"]])
                params.append(filter(role_uuids["filter"]))
            # end if
        # end if

        # Role names
        if roles != None:
            functions.is_valid_text_filter(roles)
            if roles["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[roles["op"]]
                params.append(op(Role.name, roles["filter"]))
            else:
                filter = eval('Role.name.' + text_operators[roles["op"]])
                params.append(filter(roles["filter"]))
            # end if
        # end if
        
        # User UUIDs
        if user_uuids != None:
            functions.is_valid_text_filter(user_uuids)
            if user_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[user_uuids["op"]]
                params.append(op(RoleUser.user_uuid, user_uuids["filter"]))
            else:
                filter = eval('RoleUser.user_uuid.' + text_operators[user_uuids["op"]])
                params.append(filter(user_uuids["filter"]))
            # end if
            join_tables = True
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
            join_tables = True
            tables[User] = User.user_uuid==RoleUser.user_uuid
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
            join_tables = True
            tables[User] = User.user_uuid==RoleUser.user_uuid
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
            join_tables = True
            tables[User] = User.user_uuid==RoleUser.user_uuid
        # end if

        # Active
        if active != None:
            functions.is_valid_bool_filter(active)
            params.append(User.active == active)

            join_tables = True
            tables[User] = User.user_uuid==RoleUser.user_uuid
        # end if

        query = self.session.query(Role)
        if len(tables.keys()) > 0 or join_tables:
            query = query.join(RoleUser, RoleUser.role_uuid==Role.role_uuid)
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
        roles = query.all()

        return roles

    def close_session (self):
        """
        Method to close the session
        """
        self.session.close()
        return
