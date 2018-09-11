"""
data model definition for testing uuids

Written by DEIMOS Space S.L. (dibb)

module eboa
"""

from sqlalchemy import Column, Integer, Table, ForeignKey, Text
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship

# Adding path to the datamodel package
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from datamodel.base import Base

class UuidTest(Base):
    __tablename__ = 'uuid'

    uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)

    def __init__(self, uuid):
        self.uuid = uuid

