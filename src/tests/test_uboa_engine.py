"""
Automated tests for the engine submodule

Written by DEIMOS Space S.L. (dibb)

module uboa
"""
# Import python utilities
import os
import sys
import unittest
import datetime
import uuid
import random
import before_after
import pdb

# Import flask security utils
from flask_security import hash_password

# Import engine of the DDBB
import uboa.engine.engine as uboa_engine
from uboa.engine.engine import Engine

# Import query interface
from uboa.engine.query import Query

# Import datamodel
from uboa.datamodel.base import Session

# Import datamodel
from uboa.datamodel.users import User, Role, RoleUser

# Create an app context to hash passwords (this is because the hash_password depends on configurations related to the app)
import vboa
app = vboa.create_app()
with app.app_context():
    password = hash_password("password")

class TestEngine(unittest.TestCase):
    def setUp(self):
        # Create the engine to manage the data
        self.engine_uboa = Engine()
        self.query_uboa = Query()

        # Create session to connect to the database
        self.session = Session()

        # Clear all tables before executing the test
        self.query_uboa.clear_db()

    def tearDown(self):

        # Clear all tables before executing the test
        self.query_uboa.clear_db()

        # Insert the default configuration for users
        exit_status = self.engine_uboa.insert_configuration()
        assert len([item for item in exit_status if item["status"] != uboa_engine.exit_codes["OK"]["status"]]) == 0

        # Close connections to the DDBB
        self.engine_uboa.close_session()
        self.query_uboa.close_session()
        self.session.close()

    ######
    # ROLES
    ######
    def test_insert_only_roles(self):

        data = {"operations": [{
            "mode": "insert",
            "roles": [
                {
                    "name": "administrator",
                    "description": "Administrator of the system"
                },{
                    "name": "operator",
                    "description": "Operator of the system"
                },
            ]
        }]}

        exit_status = self.engine_uboa.treat_data(data)

        assert exit_status[0]["status"] == uboa_engine.exit_codes["OK"]["status"]

        roles = self.session.query(Role).all()

        assert len(roles) == 2

        roles = self.session.query(Role).filter(Role.name == "administrator").all()

        assert len(roles) == 1
        assert roles[0].description == "Administrator of the system"

        roles = self.session.query(Role).filter(Role.name == "operator").all()

        assert len(roles) == 1
        assert roles[0].description == "Operator of the system"

    def test_insert_only_roles_twice(self):

        data = {"operations": [{
            "mode": "insert",
            "roles": [
                {
                    "name": "administrator",
                    "description": "Administrator of the system"
                },{
                    "name": "operator",
                    "description": "Operator of the system"
                },
            ]
        }]}

        exit_status = self.engine_uboa.treat_data(data)

        assert exit_status[0]["status"] == uboa_engine.exit_codes["OK"]["status"]

        exit_status = self.engine_uboa.treat_data(data)

        assert exit_status[0]["status"] == uboa_engine.exit_codes["ROLE_ALREADY_INSERTED"]["status"]

        roles = self.session.query(Role).all()

        assert len(roles) == 2

        roles = self.session.query(Role).filter(Role.name == "administrator").all()

        assert len(roles) == 1
        assert roles[0].description == "Administrator of the system"

        roles = self.session.query(Role).filter(Role.name == "operator").all()

        assert len(roles) == 1
        assert roles[0].description == "Operator of the system"

    def test_insert_duplicated_roles(self):

        data = {"operations": [{
            "mode": "insert",
            "roles": [
                {
                    "name": "administrator",
                    "description": "Administrator of the system"
                },                {
                    "name": "administrator",
                    "description": "Another description"
                },{
                    "name": "operator",
                    "description": "Operator of the system"
                },
            ]
        }]}

        exit_status = self.engine_uboa.treat_data(data)

        assert exit_status[0]["status"] == uboa_engine.exit_codes["ROLES_DUPLICATED"]["status"]

        roles = self.session.query(Role).all()

        assert len(roles) == 0

    ######
    # USERS
    ######
    def test_insert_only_users(self):

        data = {"operations": [{
            "mode": "insert",
            "users": [
                {
                    "email": "administrator@test.com",
                    "username": "administrator",
                    "password": password
                },{
                    "email": "operator@test.com",
                    "username": "operator",
                    "password": password
                },
            ]
        }]}

        exit_status = self.engine_uboa.treat_data(data)

        assert exit_status[0]["status"] == uboa_engine.exit_codes["OK"]["status"]

        users = self.session.query(User).all()

        assert len(users) == 2

        users = self.session.query(User).filter(User.email == "administrator@test.com",
                                                User.username == "administrator").all()

        assert len(users) == 1
        assert users[0].group == "boa_users"
        assert users[0].password == password
        assert users[0].active == True

        users = self.session.query(User).filter(User.email == "operator@test.com",
                                                User.username == "operator").all()

        assert len(users) == 1
        assert users[0].group == "boa_users"
        assert users[0].password == password
        assert users[0].active == True

    def test_insert_only_users_twice(self):

        data = {"operations": [{
            "mode": "insert",
            "users": [
                {
                    "email": "administrator@test.com",
                    "username": "administrator",
                    "password": password
                },{
                    "email": "operator@test.com",
                    "username": "operator",
                    "password": password
                },
            ]
        }]}

        exit_status = self.engine_uboa.treat_data(data)

        assert exit_status[0]["status"] == uboa_engine.exit_codes["OK"]["status"]

        exit_status = self.engine_uboa.treat_data(data)

        assert exit_status[0]["status"] == uboa_engine.exit_codes["USER_ALREADY_INSERTED"]["status"]

        users = self.session.query(User).all()

        assert len(users) == 2

        users = self.session.query(User).filter(User.email == "administrator@test.com",
                                                User.username == "administrator").all()

        assert len(users) == 1
        assert users[0].group == "boa_users"
        assert users[0].password == password
        assert users[0].active == True

        users = self.session.query(User).filter(User.email == "operator@test.com",
                                                User.username == "operator").all()

        assert len(users) == 1
        assert users[0].group == "boa_users"
        assert users[0].password == password
        assert users[0].active == True

    def test_insert_users_and_roles_without_description(self):

        data = {"operations": [{
            "mode": "insert",
            "users": [
                {
                    "email": "administrator@test.com",
                    "username": "administrator",
                    "password": password,
                    "roles": ["administrator"]
                },{
                    "email": "operator@test.com",
                    "username": "operator",
                    "password": password,
                    "roles": ["operator"]
                },
            ]
        }]}

        exit_status = self.engine_uboa.treat_data(data)

        assert exit_status[0]["status"] == uboa_engine.exit_codes["OK"]["status"]

        users = self.session.query(User).all()

        assert len(users) == 2

        users = self.session.query(User).filter(User.email == "administrator@test.com",
                                                User.username == "administrator").all()

        assert len(users) == 1
        assert users[0].group == "boa_users"
        assert users[0].password == password
        assert users[0].active == True

        users = self.session.query(User).filter(User.email == "operator@test.com",
                                                User.username == "operator").all()

        assert len(users) == 1
        assert users[0].group == "boa_users"
        assert users[0].password == password
        assert users[0].active == True

        roles = self.session.query(Role).all()

        assert len(roles) == 2

        roles = self.session.query(Role).filter(Role.name == "administrator").all()

        assert len(roles) == 1
        assert not roles[0].description

        roles = self.session.query(Role).filter(Role.name == "operator").all()

        assert len(roles) == 1
        assert not roles[0].description

    def test_insert_users_and_roles_with_description(self):

        data = {"operations": [{
            "mode": "insert",
            "users": [
                {
                    "email": "administrator@test.com",
                    "username": "administrator",
                    "password": password,
                    "roles": ["administrator"]
                },{
                    "email": "operator@test.com",
                    "username": "operator",
                    "password": password,
                    "roles": ["operator"]
                },
            ],
            "roles": [
                {
                    "name": "administrator",
                    "description": "Administrator of the system"
                },{
                    "name": "operator",
                    "description": "Operator of the system"
                },
            ]
        }]}

        exit_status = self.engine_uboa.treat_data(data)

        assert exit_status[0]["status"] == uboa_engine.exit_codes["OK"]["status"]

        users = self.session.query(User).all()

        assert len(users) == 2

        users = self.session.query(User).filter(User.email == "administrator@test.com",
                                                User.username == "administrator").all()

        assert len(users) == 1
        assert users[0].group == "boa_users"
        assert users[0].password == password
        assert users[0].active == True

        users = self.session.query(User).filter(User.email == "operator@test.com",
                                                User.username == "operator").all()

        assert len(users) == 1
        assert users[0].group == "boa_users"
        assert users[0].password == password
        assert users[0].active == True

        roles = self.session.query(Role).all()

        assert len(roles) == 2

        roles = self.session.query(Role).filter(Role.name == "administrator").all()

        assert len(roles) == 1
        assert roles[0].description == "Administrator of the system"

        roles = self.session.query(Role).filter(Role.name == "operator").all()

        assert len(roles) == 1
        assert roles[0].description == "Operator of the system"

    def test_insert_default_users_and_roles_with_description(self):

        data = {"operations": [{
            "mode": "insert",
            "users": [
                {
                    "email": "administrator@test.com",
                    "username": "administrator",
                    "password": "$2b$12$U9sVkIyZuER4eG0Kzfr2cOzHK7mNox5Qc8AtUrb6I4gHuIYiU4n56",
                    "roles": ["administrator"]
                },{
                    "email": "service_administrator@test.com",
                    "username": "service_administrator",
                    "password": "$2b$12$U9sVkIyZuER4eG0Kzfr2cOzHK7mNox5Qc8AtUrb6I4gHuIYiU4n56",
                    "roles": ["service_administrator"]
                },{
                    "email": "operator@test.com",
                    "username": "operator",
                    "password": password,
                    "roles": ["operator"]
                },{
                    "email": "analyst@test.com",
                    "username": "analyst",
                    "password": password,
                    "roles": ["analyst"]
                },{
                    "email": "operator_observer@test.com",
                    "username": "operator_observer",
                    "password": password,
                    "roles": ["operator_observer"]
                },{
                    "email": "observer@test.com",
                    "username": "observer",
                    "password": password,
                    "roles": ["observer"]
                }
            ],
            "roles": [
                {
                    "name": "administrator",
                    "description": "Administrator of the system. All operations allowed."
                },{
                    "name": "service_administrator",
                    "description": "Administrator of the service. All operations allowed, except management of users."
                },{
                    "name": "operator",
                    "description": "Operator of the service. All operations allowed, except management of users and management of services."
                },{
                    "name": "analyst",
                    "description": "Analyst of the service. Read access to data (including EBOA navigation) and write access to include justifications."
                },{
                    "name": "operator_observer",
                    "description": "Operator of the service. Read access to data (including EBOA navigation)."
                },{
                    "name": "observer",
                    "description": "Read access to specfic views."
                }
            ]
        }]}

        exit_status = self.engine_uboa.treat_data(data)

        assert exit_status[0]["status"] == uboa_engine.exit_codes["OK"]["status"]

        users = self.session.query(User).all()

        assert len(users) == 6

        roles = self.session.query(Role).all()

        assert len(roles) == 6

    def test_insert_from_json(self):

        filename = "users.json"
        exit_status = self.engine_uboa.parse_data_from_json(os.path.dirname(os.path.abspath(__file__)) + "/json_inputs/" + filename)

        assert exit_status["status"] == uboa_engine.exit_codes["FILE_VALID"]["status"]

        exit_status = self.engine_uboa.treat_data()

        assert len([item for item in exit_status if item["status"] != uboa_engine.exit_codes["OK"]["status"]]) == 0

        users = self.session.query(User).all()

        assert len(users) == 6

        roles = self.session.query(Role).all()

        assert len(roles) == 6
