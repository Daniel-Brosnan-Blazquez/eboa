"""
Alert data model definition

Written by DEIMOS Space S.L. (dibb)

module eboa
"""

from sqlalchemy import Column, Integer, Table, ForeignKey, Text, DateTime, Float, Interval, JSON, Numeric
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship

from eboa.datamodel.base import Base

class Alert(Base):
    __tablename__ = 'alerts'

    alert_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    name = Column(Text)
    severity = Column(Integer)
    description = Column(Text)

    def __init__(self, alert_uuid, name, severity, description = None):
        self.alert_uuid = alert_uuid
        self.name = name
        self.severity = severity
        self.description = description

