"""
Events data model definition

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""

from sqlalchemy import Column, Integer, Table, DateTime, ForeignKey, Text, Float, Boolean
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry

from .base import Base

class Event(Base):
    __tablename__ = 'event_tb'

    event_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    start = Column(DateTime)
    stop = Column(DateTime)
    generation_time = Column(DateTime)
    ingestion_time = Column(DateTime)
    visible = Column(Boolean)
    gauge_id = Column(Integer, ForeignKey('gauge_cnf_tb.gauge_id'))
    explicit_ref_id = Column(Integer, ForeignKey('explicit_ref_tb.explicit_ref_id'))
    processing_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('dim_processing_tb.processing_uuid'))
    gauge = relationship("Gauge", backref="events")
    explicitRef = relationship("ExplicitRef", backref="events")
    source = relationship("DimProcessing", backref="events")

    def __init__(self, event_uuid, start, stop, generation_time, ingestion_time, gauge, explicit_ref = None, dim_processing = None, visible = True):
        self.event_uuid = event_uuid
        self.start = start
        self.stop = stop
        self.generation_time = generation_time
        self.ingestion_time = ingestion_time
        self.visible = visible
        self.gauge = gauge
        self.explicitRef = explicit_ref
        self.source = dim_processing

class EventLink(Base):
    __tablename__ = 'event_links_tb'

    event_uuid_link = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    name = Column(Text)
    event_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('event_tb.event_uuid'))
    event = relationship("Event", backref="eventLinks")

    def __init__(self, link, name, event):
        self.event_uuid_link = link
        self.name = name
        self.event = event

class EventKey(Base):
    __tablename__ = 'event_keys_tb'

    event_key = Column(Text, primary_key=True)
    generation_time = Column(DateTime)
    visible = Column(Boolean)
    event_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('event_tb.event_uuid'))
    event = relationship("Event", backref="eventKeys")
    dim_signature_id = Column(Integer, ForeignKey('dim_signature_tb.dim_signature_id'))
    dim_signature = relationship("DimSignature", backref="eventKeys")

    def __init__(self, key, generation_time, event, dim_signature, visible = False):
        self.event_uuid_link = link
        self.generation_time = generation_time
        self.visible = visible
        self.event = event
        self.dim_signature = dim_signature

class EventBoolean(Base):
    __tablename__ = 'event_boolean_tb'

    name = Column(Text, primary_key=True)
    value = Column(Boolean)
    level_position = Column(Integer)
    child_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    event_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('event_tb.event_uuid'))
    event = relationship("Event", backref="eventBooleans")

    def __init__(self, name, value, level_position, child_position, parent_level, parent_position, event):
        self.name = name
        self.value = value
        self.level_position = level_position
        self.child_position = child_position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.event = event

class EventText(Base):
    __tablename__ = 'event_text_tb'

    name = Column(Text, primary_key=True)
    value = Column(Text)
    level_position = Column(Integer)
    child_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    event_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('event_tb.event_uuid'))
    event = relationship("Event", backref="eventTexts")

    def __init__(self, name, value, level_position, child_position, parent_level, parent_position, event):
        self.name = name
        self.value = value
        self.level_position = level_position
        self.child_position = child_position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.event = event

class EventDouble(Base):
    __tablename__ = 'event_double_tb'

    name = Column(Text, primary_key=True)
    value = Column(Float)
    level_position = Column(Integer)
    child_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    event_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('event_tb.event_uuid'))
    event = relationship("Event", backref="eventDoubles")

    def __init__(self, name, value, level_position, child_position, parent_level, parent_position, event):
        self.name = name
        self.value = value
        self.level_position = level_position
        self.child_position = child_position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.event = event

class EventTimestamp(Base):
    __tablename__ = 'event_timestamp_tb'

    name = Column(Text, primary_key=True)
    value = Column(DateTime)
    level_position = Column(Integer)
    child_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    event_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('event_tb.event_uuid'))
    event = relationship("Event", backref="eventTimestamps")

    def __init__(self, name, value, level_position, child_position, parent_level, parent_position, event):
        self.name = name
        self.value = value
        self.level_position = level_position
        self.child_position = child_position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.event = event

class EventObject(Base):
    __tablename__ = 'event_object_tb'

    name = Column(Text, primary_key=True)
    level_position = Column(Integer)
    child_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    event_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('event_tb.event_uuid'))
    event = relationship("Event", backref="eventObjects")

    def __init__(self, name, level_position, child_position, parent_level, parent_position, event):
        self.name = name
        self.level_position = level_position
        self.child_position = child_position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.event = event

class EventGeometry(Base):
    __tablename__ = 'event_geometry_tb'

    name = Column(Text, primary_key=True)
    value = Column(Geometry('POLYGON'))
    level_position = Column(Integer)
    child_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    event_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('event_tb.event_uuid'))
    event = relationship("Event", backref="eventGeometrys")

    def __init__(self, name, value, level_position, child_position, parent_level, parent_position, event):
        self.name = name
        self.value = value
        self.level_position = level_position
        self.child_position = child_position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.event = event
