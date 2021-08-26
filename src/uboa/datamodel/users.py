"""
Users data model definition

Written by DEIMOS Space S.L. (dibb)

module uboa
"""
from uboa.datamodel.base import Base
from flask_security import UserMixin, RoleMixin
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Table, DateTime, ForeignKey, Text, Integer, Boolean
from sqlalchemy.dialects import postgresql

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
