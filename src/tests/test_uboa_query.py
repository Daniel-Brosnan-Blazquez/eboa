"""
Automated tests for the query submodule

Written by DEIMOS Space S.L. (jubv)

module uboa
"""
# Import python utilities
import unittest

# Import flask security utils
from flask_security import hash_password

# Import engine of the DDBB
import uboa.engine.engine as uboa_engine
from uboa.engine.engine import Engine
from uboa.engine.query import Query

# Import datamodel
from uboa.datamodel.users import RoleUser, Role, User

# Import exceptions
from eboa.engine.errors import InputError

# Create an app context to hash passwords (this is because the hash_password depends on configurations related to the app)
import vboa

app = vboa.create_app()
with app.app_context():
    password = hash_password("password")

class TestQuery(unittest.TestCase):
    def setUp(self):
        # Instantiate the query component
        self.query = Query()

        # Create the engine to manage the data
        self.engine_uboa = Engine()

        # Clear all tables before executing the test
        self.query.clear_db()

    def tearDown(self):

        # Clear all tables before executing the test
        self.query.clear_db()

        # Insert the default configuration for users
        exit_status = self.engine_uboa.insert_configuration()
        assert len([item for item in exit_status if item["status"] != uboa_engine.exit_codes["OK"]["status"]]) == 0

        # Close connections to the DDBB
        self.engine_uboa.close_session()
        self.query.close_session()

    def test_query_users(self):

        data = {"operations": [{
            "mode": "insert",
            "users": [
                {
                    "email": "administrator@test.com",
                    "username": "administrator",
                    "password": password,
                    "roles": ["administrator"],
                    "group": "Deimos"
                },{
                    "email": "operator@test.com",
                    "username": "operator",
                    "password": password,
                    "roles": ["operator"],
                    "group": "Deimos"
                },
            ]
        }]}

        exit_status = self.engine_uboa.treat_data(data, False)

        assert exit_status[0]["status"] == uboa_engine.exit_codes["OK"]["status"]

        users = self.query.get_users()

        assert len(users) == 2

        # User UUID
        users = self.query.get_users(user_uuids = {"filter": [users[0].user_uuid], "op": "in"})
        assert len(users) == 1

        # Emails
        users = self.query.get_users(emails = {"filter": "administrator@test.com", "op": "=="})
        assert len(users) == 1
        
        # Usernames
        users = self.query.get_users(usernames = {"filter": "administrator", "op": "=="})
        assert len(users) == 1

        # Groups
        users = self.query.get_users(groups = {"filter": "Deimos", "op": "=="})
        assert len(users) == 2

        # Active
        users = self.query.get_users(active = True)
        assert len(users) == 2

        # Roles
        users = self.query.get_users(roles = {"filter": "administrator", "op": "=="})
        assert len(users) == 1

        self.query.get_users(delete = True)

        users = self.query.get_users()

        assert len(users) == 0

    def test_query_roles(self):

        data = {"operations": [{
            "mode": "insert",
            "users": [
                {
                    "email": "administrator@test.com",
                    "username": "administrator",
                    "password": password,
                    "roles": ["administrator"],
                    "group": "Deimos"
                },{
                    "email": "operator@test.com",
                    "username": "operator",
                    "password": password,
                    "roles": ["operator"],
                    "group": "Deimos"
                },
            ]
        }]}

        exit_status = self.engine_uboa.treat_data(data, False)

        assert exit_status[0]["status"] == uboa_engine.exit_codes["OK"]["status"]

        roles = self.query.get_roles()

        assert len(roles) == 2

        users = self.query.get_users(emails = {"filter": "administrator@test.com", "op": "=="})

        # User UUID
        roles = self.query.get_roles(user_uuids = {"filter": [users[0].user_uuid], "op": "in"})
        assert len(roles) == 1

        # Emails
        roles = self.query.get_roles(emails = {"filter": "administrator@test.com", "op": "=="})
        assert len(roles) == 1
        
        # Usernames
        roles = self.query.get_roles(usernames = {"filter": "administrator", "op": "=="})
        assert len(roles) == 1

        # Groups
        roles = self.query.get_roles(groups = {"filter": "Deimos", "op": "=="})
        assert len(roles) == 2

        # Active
        roles = self.query.get_roles(active = True)
        assert len(roles) == 2
    
        # Roles
        roles = self.query.get_roles(roles = {"filter": "administrator", "op": "=="})
        assert len(roles) == 1

        # Role UUID
        roles = self.query.get_roles(role_uuids = {"filter": [roles[0].role_uuid], "op": "in"})
        assert len(roles) == 1
