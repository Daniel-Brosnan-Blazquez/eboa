"""
Users data model definition

Written by DEIMOS Space S.L. (dibb)

module uboa
"""
from uboa.datamodel.base import Base
from flask_security import UserMixin, RoleMixin
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Table, DateTime, ForeignKey, Text, Integer, Boolean, JSON
from sqlalchemy.dialects import postgresql

from uboa.datamodel.errors import WrongParameter

class RoleUser(Base):
    __tablename__ = 'roles_users'
    role_user_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    user_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('users.user_uuid'))
    role_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('roles.role_uuid'))

    def __init__(self, role_user_uuid, user_uuid, role_uuid):
        self.role_user_uuid = role_user_uuid
        self.user_uuid = user_uuid
        self.role_uuid = role_uuid
        
class Role(Base, RoleMixin):
    __tablename__ = 'roles'
    role_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    name = Column(Text)
    description = Column(Text)

    def __init__(self, role_uuid, name, description = None):
        self.role_uuid = role_uuid
        self.name = name
        self.description = description

class User(Base, UserMixin):
    __tablename__ = 'users'
    user_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    email = Column(Text)
    username = Column(Text)
    group = Column(Text)
    password = Column(Text)
    last_login_at = Column(DateTime)
    current_login_at = Column(DateTime)
    last_login_ip = Column(Text)
    current_login_ip = Column(Text)
    login_count = Column(Integer)
    active = Column(Boolean())
    fs_uniquifier = Column(Text)
    confirmed_at = Column(DateTime)
    roles = relationship("Role", secondary="roles_users", backref="users")

    def __init__(self, user_uuid, email, username, group, password, fs_uniquifier, active = True):
        """
        User is active by default
        """
        self.user_uuid = user_uuid
        self.email = email
        self.username = username
        self.group = group
        self.password = password
        self.fs_uniquifier = fs_uniquifier
        self.active = active

    def has_one_of_these_roles(self, roles):
        """
        Method to check if the user has associated one of the received roles

        :param roles: list of roles to check
        :type roles: list
        """
        if type(roles) != list:
            raise WrongParameter("roles parameter must be a list. Received type: {}".format(type(roles)))
        # end if

        # Look for a match
        user_roles = [role.name for role in self.roles]
        match = False
        for role in roles:
            if type(role) != str:
                raise WrongParameter("role {} inside roles parameter must be a str. Received type: {}".format(role, type(role)))
            # end if
            if role in user_roles:
                match = True
                break
            # end if
        # end for

        return match

    def has_none_of_these_roles(self, roles):
        """
        Method to check if the user does not have associated any of the received roles

        :param roles: list of roles to check
        :type roles: list
        """
        if type(roles) != list:
            raise WrongParameter("roles parameter must be a list. Received type: {}".format(type(roles)))
        # end if

        # Look for a match
        user_roles = [role.name for role in self.roles]
        no_match = True
        for role in roles:
            if type(role) != str:
                raise WrongParameter("role {} inside roles parameter must be a str. Received type: {}".format(role, type(role)))
            # end if
            if role in user_roles:
                no_match = False
                break
            # end if
        # end for

        return no_match

class Configuration(Base):
    __tablename__ = 'configurations'
    configuration_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    name = Column(Text)
    configuration = Column(JSON)
    permission = Column(Integer)
    diff_previous_version = Column(Text)
    active = Column(Boolean())
    user_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('users.user_uuid'))
    creator = relationship("User", backref="created_configurations")

class ConfigurationUser(Base):
    """
    This table is used to associate configurations to a user.
    Do not confuse this relation with the 'created_configurations' relation which specifies which user created the configuration
    """
    __tablename__ = 'configurations_users'
    configuration_user_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    user_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('users.user_uuid'))
    configuration_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('configurations.configuration_uuid'))


class ConfigurationChance(Base):
    """
    This table is used to track the configuration changes a user made on a specific configuration.
    """
    __tablename__ = 'configuration_changes'
    configuration_change_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    timestamp = Column(DateTime)
    type = Column(Integer)
    user_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('users.user_uuid'))
    configuration_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('configurations.configuration_uuid'))
    editor = relationship("User", backref="edit_configurations")
    configuration = relationship("Configuration", backref="changes")

