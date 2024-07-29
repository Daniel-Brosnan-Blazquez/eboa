"""
Engine definition

Written by DEIMOS Space S.L. (dibb)

module uboa
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
import traceback
import sys

# Import SQLalchemy entities
from sqlalchemy.exc import IntegrityError, InternalError
from sqlalchemy.sql import func
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm.exc import StaleDataError
from sqlalchemy import or_, and_

# Import exceptions
from uboa.engine.errors import RolesDuplicated, RoleAlreadyInserted, UsersDuplicated, UserAlreadyInserted, EmailNotCorrect, UsernameNotCorrect, ErrorParsingDictionary, UserAlreadyInsertedNotMatchingUsernameOrEmail

# Import query module
from uboa.engine.query import Query

# Import parsing module
import uboa.engine.parsing as parsing

# Import debugging
from eboa.debugging import debug, race_condition, race_condition2, race_condition3, race_condition4, race_condition5

# Import datamodel
from uboa.datamodel.base import Session
from uboa.datamodel.users import User, Role, RoleUser

# Import EBOA engine functions
from eboa.engine.functions import get_resources_path

# Import logging
from uboa.logging import Log

logging = Log(name = __name__)
logger = logging.logger

exit_codes = {
    "OK": {
        "status": 0,
        "message": "The processing of the metadata of the user/s has ingested correctly {} role/s and {} user/s"
    },
    "DATA_NOT_VALID": {
        "status": 1,
        "message": "The metadata of the users information does not pass the schema verification"
    },
    "ROLES_DUPLICATED": {
        "status": 2,
        "message": "The metadata of the users information contains the definition of duplicated roles"
    },
    "ROLE_ALREADY_INSERTED": {
        "status": 3,
        "message": "The metadata of the users information contains the definition of roles already inserted in the DDBB"
    },
    "USERS_DUPLICATED": {
        "status": 4,
        "message": "The metadata of the users information contains the definition of duplicated users"
    },
    "USER_ALREADY_INSERTED": {
        "status": 5,
        "message": "The metadata of the users information contains the definition of users already inserted in the DDBB"
    },
    "EMAIL_NOT_CORRECT": {
        "status": 6,
        "message": "The specified email {} has a wrong format"
    },
    "USERNAME_NOT_CORRECT": {
        "status": 7,
        "message": "The specified username {} has a wrong format"
    },
    "FILE_NOT_LOADED": {
        "status": 8,
        "message": "The source file with name {} could not be loaded"
    },
    "FILE_NOT_VALID": {
        "status": 9,
        "message": "The source file with name {} contains data with a wrong structure"
    },
    "FILE_VALID": {
        "status": 10,
        "message": "The source file with name {} was loaded and validated"
    },
    "FILE_NONEXISTENT": {
        "status": 11,
        "message": "The source file with name {} does not exist"
    },
    "USER_ALREADY_INSERTED_NOT_MATCHING_USERNAME_OR_EMAIL": {
        "status": 12,
        "message": "The metadata of the users information contains the definition of the user with email {} or username {} already inserted with same username but different email or viceversa"
    }
}

class Engine():
    """Class for communicating with the engine of the uboa module

    Provides access to the logic for inserting, deleting and updating
    the information stored into the DDBB related to users
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
        self.query = Query()
        self.force_insert = False
    
        return

    ###################
    # PARSING METHODS #
    ###################
    @debug
    def parse_data_from_json(self, json_path, check_schema = True):
        """
        Method to parse a json file for later treatment of its content
        
        :param json_path: path to the json file to be parsed
        :type json_path: str
        :param check_schema: indicates whether to pass a schema over the json file or not
        :type check_schema: bool
        """
        json_name = os.path.basename(json_path)
        # Parse data from the json file
        try:
            with open(json_path) as input_file:
                data = json.load(input_file)
        except ValueError:
            message = exit_codes["FILE_NOT_LOADED"]["message"].format(json_name)
            # Log the error
            logger.error(message)
            logger.error(traceback.format_exc())
            traceback.print_exc(file=sys.stdout)
            returned_information = {
                "status": exit_codes["FILE_NOT_LOADED"]["status"],
                "message": message
            }
            return returned_information
        except FileNotFoundError:
            message = exit_codes["FILE_NONEXISTENT"]["message"].format(json_name)
            # Log the error
            logger.error(message)
            logger.error(traceback.format_exc())
            traceback.print_exc(file=sys.stdout)
            returned_information = {
                "status": exit_codes["FILE_NONEXISTENT"]["status"],
                "message": message
            }
            return returned_information
        # end try

        if check_schema:
            is_valid = self._validate_data(data)
            if not is_valid:
                # Log the error
                message = exit_codes["FILE_NOT_LOADED"]["message"].format(json_name)
                logger.error(message)
                returned_information = {
                    "status": exit_codes["FILE_NOT_LOADED"]["status"],
                    "message": message
                }
                return returned_information
            # end if
        # end if

        self.data=data

        message = exit_codes["FILE_VALID"]["message"].format(json_name)
        returned_information = {
            "status": exit_codes["FILE_VALID"]["status"],
            "message": message
        }
        return returned_information
    
    #########################
    # DATA HANDLING METHODS #
    #########################
    def _validate_data(self, data):
        """
        Method to validate the data structure

        :param data: structure of data to validate
        :type data: dict 
       """

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary as e:
            logger.error(str(e))
            return False
        # end try

        return True

    def treat_data(self, data = None, validate = True):
        """
        Method to treat the data stored in self.data or received by parameter

        :param data: structure of data to treat
        :type data: dict 
        :param validate: flag to indicate if the schema check has to be performed
        :type validate: bool

        :return: exit_codes for every operation with the associated information (generator and report)
        :rtype: list of dictionaries
        """
        if data != None:
            self.data = data
        # end if

        if type (self.data) != dict:
            # Log the error
            logger.error(exit_codes["DATA_NOT_VALID"]["message"])
            return [exit_codes["DATA_NOT_VALID"]]
        # end if
        
        if validate:
            is_valid = self._validate_data(self.data)
            if not is_valid:
                # Log the error
                logger.error(exit_codes["DATA_NOT_VALID"]["message"])
                return [exit_codes["DATA_NOT_VALID"]]
            # end if
        # end if

        returned_values = []
        for self.operation in self.data.get("operations") or []:

            if self.operation.get("mode") == "erase_and_insert":
                self.query.clear_db()
            # end if            

            if self.operation.get("mode") == "force_insert":
                self.force_insert = True
            # end if            

            if self.operation.get("mode") in ["insert", "erase_and_insert", "force_insert"]:
                returned_information = self._insert_data()
                returned_values.append(returned_information)
            # end if
        # end for
        return returned_values

    #####################
    # INSERTION METHODS #
    #####################
    def _initialize_context_insert_data(self):
        # Initialize context
        self.roles = {}

        return

    @debug
    def _insert_data(self):
        """
        Method to insert the metadata of the users into the DDBB for an operation of mode insert
        """
        # Initialize context
        self._initialize_context_insert_data()

        # If something is not correct with the input rollback transactions
        self.session.begin_nested()
        
        # Insert metadata
        # Insert roles
        try:
            self._insert_roles()
        except RolesDuplicated as e:
            self.session.rollback()
            # Log that the specified roles are duplicated
            logger.error(e)
            return exit_codes["ROLES_DUPLICATED"]
        except RoleAlreadyInserted as e:
            self.session.rollback()
            # Log that the specified roles were already inserted into the DDBB
            logger.error(e)
            return exit_codes["ROLE_ALREADY_INSERTED"]
        # end try

        # Race condition to test deletion between insert_roles and insert_users
        race_condition5()
        
        # Insert users
        try:
            self._insert_users()
        except UsersDuplicated as e:
            self.session.rollback()
            # Log that the specified roles are duplicated
            logger.error(e)
            return exit_codes["USERS_DUPLICATED"]
        except UserAlreadyInserted as e:
            self.session.rollback()
            # Log that the specified users were already inserted into the DDBB
            logger.error(e)
            return exit_codes["USER_ALREADY_INSERTED"]
        # end try
        except UserAlreadyInsertedNotMatchingUsernameOrEmail as e:
            self.session.rollback()
            # Log that the specified username is not correct
            logger.error(e)
            returned_information = {
                "status": exit_codes["USER_ALREADY_INSERTED_NOT_MATCHING_USERNAME_OR_EMAIL"]["status"],
                "message": e
            }
            return returned_information
        # end try

        # Commit data
        # At this point all the information has been inserted, commit data twice as there was a begin nested initiated
        self.session.commit()
        self.session.commit()

        self.session.close()
        self.session = self.Scoped_session()

        n_roles = 0
        if "roles" in self.operation:
            users_roles = [role for user in self.operation.get("users") or [] for role in user.get("roles") or []]

            declared_roles = [role.get("name") for role in self.operation.get("roles") or []]

            n_roles = len(set(users_roles + declared_roles))
        # end if

        n_users = 0

        message = exit_codes["OK"]["message"].format(n_roles, n_users)
        logger.info(message)
        returned_information = {
            "status": exit_codes["OK"]["status"],
            "message": message
        }
        return returned_information

    @debug
    def _update_role_description(self, role, description):
        """
        Method to update the role description

        :param role: name of the role
        :type role: str
        :param description: description of the role
        :type description: str
        """

        self.roles[role] = self.session.query(Role).filter(Role.name == role).first()
        self.session.begin_nested()
        try:
            race_condition2()
            self.roles[role].description = description
            self.session.commit()
        except StaleDataError:
            # The role has been deleted between the query and the update. Roll back transaction for
            # re-using the session
            self.session.rollback()
            self._insert_role(role, description)
        # end try

        return
    
    @debug
    def _insert_role(self, role, description):
        """
        Method to insert the roles

        :param role: name of the role
        :type role: str
        :param description: description of the role
        :type description: str

        :return: True if role has been inserted, False otherwise
        :rtype: bool
        """

        role_inserted = False
        
        self.session.begin_nested()
        id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        self.roles[role] = Role(id, role, description)
        self.session.add(self.roles[role])
        try:
            race_condition()
            self.session.commit()
            role_inserted = True
        except IntegrityError:
            # The role has been inserted between the query and the insertion. Roll back transaction for
            # re-using the session
            self.session.rollback()
            self.roles[role] = self.session.query(Role).filter(Role.name == role).first()
        # end try

        return role_inserted
        
    @debug
    def _insert_roles(self):
        """
        Method to insert the roles
        """
        users_roles = [role for user in self.operation.get("users") or [] for role in user.get("roles") or []]

        declared_roles = [role.get("name") for role in self.operation.get("roles") or []]

        if len(declared_roles) > len(set(declared_roles)):
            raise RolesDuplicated(exit_codes["ROLES_DUPLICATED"]["message"])
        # end if

        roles = sorted(set(users_roles + declared_roles))

        for role in roles:
            self.roles[role] = self.session.query(Role).filter(Role.name == role).first()

            description = None
            # Get associated group if exists from the declared explicit references
            declared_role = next(iter([i for i in self.operation.get("roles") or [] if i.get("name") == role]), None)
            if declared_role:
                description = declared_role.get("description")
            # end if
            
            if not self.roles[role]:

                role_inserted = self._insert_role(role, description)
                if not role_inserted and not self.force_insert:
                    raise RoleAlreadyInserted(exit_codes["ROLE_ALREADY_INSERTED"]["message"])
                elif not role_inserted and self.force_insert:
                    self._update_role_description(role, description)
                # end if
                
            elif role in declared_roles:
                if self.force_insert:
                    self._update_role_description(role, description)
                else:
                    raise RoleAlreadyInserted(exit_codes["ROLE_ALREADY_INSERTED"]["message"])
                # end if
            # end if

        # end for

    @debug
    def _update_user_information(self, user):
        """
        Method to update the user information

        :param user: structure with the information of the user
        :type user: dict
        """

        user_ddbb = self.session.query(User).filter(User.email == user.get("email"), User.username == user.get("username")).first()

        if not user_ddbb:
            raise UserAlreadyInsertedNotMatchingUsernameOrEmail(exit_codes["USER_ALREADY_INSERTED_NOT_MATCHING_USERNAME_OR_EMAIL"]["message"].format(user.get("email"), user.get("username")))
        # end if
        
        self.session.begin_nested()
        try:
            race_condition4()
            group = user.get("group")
            if group:
                user_ddbb.group = group
            # end if

            if user.get("active"):
                active = user.get("active").lower()
                if active == "true":
                    user_ddbb.active = True
                else:
                    user_ddbb.active = False
                # end if
            # end if

            user_ddbb.roles = []
            self.session.commit()

            for role in user.get("roles") or []:
                role_user_ddbb = self.session.query(RoleUser).filter(RoleUser.user_uuid == user_ddbb.user_uuid,
                                                                     RoleUser.role_uuid == self.roles[role].role_uuid).first()
                if not role_user_ddbb:
                    role_user_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                    role_user = RoleUser(role_user_uuid, user_ddbb.user_uuid, self.roles[role].role_uuid)
                    self.session.begin_nested()
                    self.session.add(role_user)
                    try:
                        self.session.commit()
                    except IntegrityError:
                        # The role has been deleted between the management of the roles data and management of the users data. Roll back transaction for
                        # re-using the session
                        self.session.rollback()
                        pass
                # end if
            # end for
        except StaleDataError:
            # The user has been deleted between the query and the update. Roll back transaction for
            # re-using the session
            self.session.rollback()
            pass
        # end try

        return
    
    @debug
    def _insert_user(self, user, list_roles_users):
        """
        Method to insert the users

        :param user: structure with the information of the user
        :type user: dict

        :return: True if user has been inserted, False otherwise
        :rtype: bool
        """

        user_inserted = False
        
        self.session.begin_nested()
        user_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        fs_uniquifier = str(uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14)))

        # Get email
        email = user.get("email")

        # Get username
        username = user.get("username")

        active = True
        if user.get("active") and user.get("active").lower() == "false":
            active = False
        # end if

        group = user.get("group")
        if not group or group == "":
            group = "boa_users"
        # end if

        user_to_ddbb = User(user_uuid, email, username, group, user.get("password"), fs_uniquifier, active)
        self.session.add(user_to_ddbb)
        try:
            race_condition3()
            self.session.commit()

            for role in user.get("roles") or []:
                role_user_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                list_roles_users.append(dict(role_user_uuid = role_user_uuid,
                                             user_uuid = user_uuid,
                                             role_uuid = self.roles[role].role_uuid))
            # end for

            user_inserted = True
        except IntegrityError:
            # The user has been inserted between the query and the insertion. Roll back transaction for
            # re-using the session
            self.session.rollback()
        # end try

        return user_inserted
        
    @debug
    def _insert_users(self):
        """
        Method to insert the users
        """
        list_roles_users = []
        users = self.operation.get("users") or []
        user_emails = [user["email"] for user in users]
        usernames = [user["username"] for user in users]

        if len(user_emails) > len(set(user_emails)):
            raise UsersDuplicated(exit_codes["USERS_DUPLICATED"]["message"])
        # end if

        if len(usernames) > len(set(usernames)):
            raise UsersDuplicated(exit_codes["USERS_DUPLICATED"]["message"])
        # end if

        for user in users:
            user_ddbb = self.session.query(User).filter(or_(User.email == user.get("email"), User.username == user.get("username"))).first()

            if not user_ddbb:

                user_inserted = self._insert_user(user, list_roles_users)
                if not user_inserted and not self.force_insert:
                    raise UserAlreadyInserted(exit_codes["USER_ALREADY_INSERTED"]["message"])
                elif not user_inserted and self.force_insert:
                    self._update_user_information(user)
                # end if
            elif self.force_insert:
                self._update_user_information(user)
            else:
                raise UserAlreadyInserted(exit_codes["USER_ALREADY_INSERTED"]["message"])
            # end if

        # end for

        # Insert role-user relationships
        self.session.bulk_insert_mappings(RoleUser, list_roles_users)

    def insert_configuration(self, users_configuration = None):

        if not users_configuration:
            users_configuration = get_resources_path() + "/users.json"
        # end if

        exit_status = self.parse_data_from_json(users_configuration)

        if exit_status["status"] != exit_codes["FILE_VALID"]["status"]:
            return [exit_status]
        # end if

        exit_status = self.treat_data()

        return exit_status
        
    def close_session (self):
        """
        Method to close the session
        """
        self.session.close()
        self.session_progress.close()
        return
