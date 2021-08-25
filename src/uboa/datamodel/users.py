"""
Users data model definition

Written by DEIMOS Space S.L. (dibb)

module uboa
"""
from eboa.datamodel.base import Base
from flask_security import UserMixin, RoleMixin
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Table, DateTime, ForeignKey, Text, Integer, Boolean
from sqlalchemy.dialects import postgresql

class RoleUser(Base):
    __tablename__ = 'roles_users'
    id = Column(Integer(), primary_key=True)
    user_uuid = Column(Integer(), ForeignKey('users.id'))
    role_uuid = Column(Integer(), ForeignKey('roles.id'))

class Role(Base, RoleMixin):
    __tablename__ = 'roles'
    id = Column(Integer(), primary_key=True)
    name = Column(Text)
    description = Column(Text)

class User(Base, UserMixin):
    __tablename__ = 'users'
    id = Column(Integer(), primary_key=True)
    email = Column(Text)
    username = Column(Text)
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
