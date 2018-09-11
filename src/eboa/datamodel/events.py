"""
Events data model definition

Written by DEIMOS Space S.L. (dibb)

module eboa
"""

from sqlalchemy import Column, Integer, Table, DateTime, ForeignKey, Text, Float, Boolean
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry

from eboa.datamodel.base import Base

class Event(Base):
    __tablename__ = 'event_tb'

    event_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    start = Column(DateTime)
    stop = Column(DateTime)
    ingestion_time = Column(DateTime)
    visible = Column(Boolean)
    gauge_id = Column(postgresql.UUID(as_uuid=True), ForeignKey('gauge_cnf_tb.gauge_id'))
    explicit_ref_id = Column(postgresql.UUID(as_uuid=True), ForeignKey('explicit_ref_tb.explicit_ref_id'))
    processing_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('dim_processing_tb.processing_uuid'))
    gauge = relationship("Gauge", backref="events")
    explicitRef = relationship("ExplicitRef", backref="events")
    source = relationship("DimProcessing", backref="events")

    def __init__(self, event_uuid, start, stop, ingestion_time, gauge, dim_processing, explicit_ref = None, visible = True):
        self.event_uuid = event_uuid
        self.start = start
        self.stop = stop
        self.ingestion_time = ingestion_time
        self.visible = visible
        self.gauge = gauge
        self.explicitRef = explicit_ref
        self.source = dim_processing

class EventLink(Base):
    __tablename__ = 'event_link_tb'
    event_uuid_link = Column(postgresql.UUID(as_uuid=True))
    name = Column(Text)
    event_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('event_tb.event_uuid'))
    event = relationship("Event", backref="eventLinks")
    __mapper_args__ = {
        'primary_key':[event_uuid_link, name, event_uuid]
    }

    def __init__(self, link, name, event):
        self.event_uuid_link = link
        self.name = name
        self.event = event

class EventKey(Base):
    __tablename__ = 'event_key_tb'

    event_key = Column(Text)
    visible = Column(Boolean)
    event_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('event_tb.event_uuid'))
    event = relationship("Event", backref="eventKeys")
    dim_signature_id = Column(postgresql.UUID(as_uuid=True), ForeignKey('dim_signature_tb.dim_signature_id'))
    dim_signature = relationship("DimSignature", backref="eventKeys")
    __mapper_args__ = {
        'primary_key':[event_uuid]
    }

    def __init__(self, key, event, dim_signature, visible = False):
        self.event_key = key
        self.visible = visible
        self.event = event
        self.dim_signature = dim_signature

class EventBoolean(Base):
    __tablename__ = 'event_boolean_tb'

    name = Column(Text)
    value = Column(Boolean)
    level_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    event_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('event_tb.event_uuid'))
    event = relationship("Event", backref="eventBooleans")
    __mapper_args__ = {
        'primary_key':[name, level_position, parent_level, parent_position, event_uuid]
    }

    def __init__(self, name, value, level_position, parent_level, parent_position, event):
        self.name = name
        self.value = value
        self.level_position = level_position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.event = event

class EventText(Base):
    __tablename__ = 'event_text_tb'

    name = Column(Text)
    value = Column(Text)
    level_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    event_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('event_tb.event_uuid'))
    event = relationship("Event", backref="eventTexts")
    __mapper_args__ = {
        'primary_key':[name, level_position, parent_level, parent_position, event_uuid]
    }

    def __init__(self, name, value, level_position, parent_level, parent_position, event):
        self.name = name
        self.value = value
        self.level_position = level_position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.event = event

class EventDouble(Base):
    __tablename__ = 'event_double_tb'

    name = Column(Text)
    value = Column(Float)
    level_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    event_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('event_tb.event_uuid'))
    event = relationship("Event", backref="eventDoubles")
    __mapper_args__ = {
        'primary_key':[name, level_position, parent_level, parent_position, event_uuid]
    }

    def __init__(self, name, value, level_position, parent_level, parent_position, event):
        self.name = name
        self.value = value
        self.level_position = level_position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.event = event

class EventTimestamp(Base):
    __tablename__ = 'event_timestamp_tb'

    name = Column(Text)
    value = Column(DateTime)
    level_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    event_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('event_tb.event_uuid'))
    event = relationship("Event", backref="eventTimestamps")
    __mapper_args__ = {
        'primary_key':[name, level_position, parent_level, parent_position, event_uuid]
    }

    def __init__(self, name, value, level_position, parent_level, parent_position, event):
        self.name = name
        self.value = value
        self.level_position = level_position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.event = event

class EventObject(Base):
    __tablename__ = 'event_object_tb'

    name = Column(Text)
    level_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    event_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('event_tb.event_uuid'))
    event = relationship("Event", backref="eventObjects")
    __mapper_args__ = {
        'primary_key':[name, level_position, parent_level, parent_position, event_uuid]
    }

    def __init__(self, name, level_position, parent_level, parent_position, event):
        self.name = name
        self.level_position = level_position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.event = event

class EventGeometry(Base):
    __tablename__ = 'event_geometry_tb'

    name = Column(Text)
    value = Column(Geometry('POLYGON'))
    level_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    event_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('event_tb.event_uuid'))
    event = relationship("Event", backref="eventGeometrys")
    __mapper_args__ = {
        'primary_key':[name, level_position, parent_level, parent_position, event_uuid]
    }

    def __init__(self, name, value, level_position, parent_level, parent_position, event):
        self.name = name
        self.value = value
        self.level_position = level_position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.event = event
