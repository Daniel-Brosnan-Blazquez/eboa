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

        # fs uniquifier
        users = self.query.get_users(fs_uniquifiers = {"filter": [users[0].fs_uniquifier], "op": "in"})
        assert len(users) == 1

        # Roles
        users = self.query.get_users(roles = {"filter": "administrator", "op": "=="})
        assert len(users) == 1