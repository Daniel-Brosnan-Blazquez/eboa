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
import pdb

# Import SQLalchemy entities
from sqlalchemy.exc import IntegrityError, InternalError
from sqlalchemy.sql import func
from sqlalchemy.orm import scoped_session
from sqlalchemy import or_, and_

# Import exceptions
from uboa.engine.errors import RolesDuplicated, RoleAlreadyInserted, UsersDuplicated, UserAlreadyInserted, EmailNotCorrect, UsernameNotCorrect

# Import debugging
from eboa.debugging import debug, race_condition

# Import datamodel
from uboa.datamodel.base import Session
from uboa.datamodel.users import User, Role, RoleUser

# Import parsing module
#import uboa.engine.parsing as parsing

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
    "METADATA_INGESTION_ENDED_UNEXPECTEDLY": {
        "status": 10,
        "message": "The metadata of the report {} was going to be ingested after its generation by the generator {} but the ingestion ended unexpectedly with the error {}"
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
    
        return

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
        
        if validate:
            is_valid = self._validate_data(self.data)
            if not is_valid:
                # Log the error
                logger.error(exit_codes["DATA_NOT_VALID"]["message"])
                returned_information = {
                    "status": exit_codes["DATA_NOT_VALID"]["status"],
                    "message": exit_codes["DATA_NOT_VALID"]["message"]
                }
                return [returned_information]
            # end if
        # end if

        returned_values = []
        for self.operation in self.data.get("operations") or []:

            if self.operation.get("mode") == "insert":
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
        except EmailNotCorrect as e:
            self.session.rollback()
            # Log that the specified email is not correct
            logger.error(e)
            return exit_codes["EMAIL_NOT_CORRECT"]
        # end try
        except UsernameNotCorrect as e:
            self.session.rollback()
            # Log that the specified username is not correct
            logger.error(e)
            return exit_codes["USERNAME_NOT_CORRECT"]
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

        returned_value = exit_codes["OK"]
        returned_value["message"] = exit_codes["OK"]["message"].format(n_roles, n_users)
        
        return exit_codes["OK"]

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
            
            if not self.roles[role]:
                description = None
                # Get associated group if exists from the declared explicit references
                declared_role = next(iter([i for i in self.operation.get("roles") or [] if i.get("name") == role]), None)
                if declared_role:
                    description = declared_role.get("description")
                # end if

                self.session.begin_nested()
                id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                self.roles[role] = Role(id, role, description)
                self.session.add(self.roles[role])
                try:
                    race_condition()
                    self.session.commit()
                except IntegrityError:
                    # The role has been inserted between the query and the insertion. Roll back transaction for
                    # re-using the session
                    self.session.rollback()
                    raise RoleAlreadyInserted(exit_codes["ROLE_ALREADY_INSERTED"]["message"])
                # end try
            else:
                raise RoleAlreadyInserted(exit_codes["ROLE_ALREADY_INSERTED"]["message"])
            # end if

        # end for

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

                self.session.begin_nested()
                user_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                fs_uniquifier = str(uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14)))

                # Check email
                email = user.get("email")
                if not email:
                    raise EmailNotCorrect(exit_codes["EMAIL_NOT_CORRECT"]["message"].format(user.get("email")))
                # end if

                # Check username
                username = user.get("username")
                if not username:
                    raise UsernameNotCorrect(exit_codes["USERNAME_NOT_CORRECT"]["message"].format(user.get("username")))
                # end if

                active = True
                if user.get("active") and user.get("active") == "false":
                    active = False
                # end if

                group = user.get("group")
                if not group or group == "":
                    group = "boa_users"
                # end if

                user_to_ddbb = User(user_uuid, email, username, group, user.get("password"), fs_uniquifier, active)
                self.session.add(user_to_ddbb)
                self.session.commit()
                try:
                    race_condition()
                    self.session.commit()
                except IntegrityError:
                    # The user has been inserted between the query and the insertion. Roll back transaction for
                    # re-using the session
                    self.session.rollback()
                    raise UserAlreadyInserted(exit_codes["USER_ALREADY_INSERTED"]["message"])
                # end try

                for role in user.get("roles") or []:
                    role_user_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                    list_roles_users.append(dict(role_user_uuid = role_user_uuid,
                                                 user_uuid = user_uuid,
                                                 role_uuid = self.roles[role].role_uuid))
                # end for
            else:
                raise UserAlreadyInserted(exit_codes["USER_ALREADY_INSERTED"]["message"])
            # end if

        # end for

        # Insert role-user relationships
        self.session.bulk_insert_mappings(RoleUser, list_roles_users)

    def close_session (self):
        """
        Method to close the session
        """
        self.session.close()
        self.session_progress.close()
        return
