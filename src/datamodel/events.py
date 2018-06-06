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
    gauge_id = Column(Integer, ForeignKey('gauge_cnf_tb.gauge_id'))
    explicit_ref_id = Column(Integer, ForeignKey('explicit_ref_tb.explicit_ref_id'))
    processing_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('dim_processing_tb.processing_uuid'))
    gauge = relationship("Gauge", backref="events")
    explicitRef = relationship("ExplicitRef", backref="events")
    source = relationship("DimProcessing", backref="events")

    def __init__(self, eventUuid, start, stop, generationTime, ingestionTime, gauge, explicitRef = None, dimProcessing = None):
        self.event_uuid = eventUuid
        self.start = start
        self.stop = stop
        self.generation_time = generationTime
        self.ingestion_time = ingestionTime
        self.gauge = gauge
        self.explicitRef = explicitRef
        self.source = dimProcessing

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
    event_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('event_tb.event_uuid'))
    event = relationship("Event", backref="eventKeys")

    def __init__(self, key, generation_time, event):
        self.event_uuid_link = link
        self.generation_time = generation_time
        self.event = event

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

    def __init__(self, name, value, levelPosition, childPosition, parentLevel, parentPosition, event):
        self.name = name
        self.value = value
        self.level_position = levelPosition
        self.child_position = childPosition
        self.parent_level = parentLevel
        self.parent_position = parentPosition
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

    def __init__(self, name, value, levelPosition, childPosition, parentLevel, parentPosition, event):
        self.name = name
        self.value = value
        self.level_position = levelPosition
        self.child_position = childPosition
        self.parent_level = parentLevel
        self.parent_position = parentPosition
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

    def __init__(self, name, value, levelPosition, childPosition, parentLevel, parentPosition, event):
        self.name = name
        self.value = value
        self.level_position = levelPosition
        self.child_position = childPosition
        self.parent_level = parentLevel
        self.parent_position = parentPosition
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

    def __init__(self, name, levelPosition, childPosition, parentLevel, parentPosition, event):
        self.name = name
        self.level_position = levelPosition
        self.child_position = childPosition
        self.parent_level = parentLevel
        self.parent_position = parentPosition
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

    def __init__(self, name, value, levelPosition, childPosition, parentLevel, parentPosition, event):
        self.name = name
        self.value = value
        self.level_position = levelPosition
        self.child_position = childPosition
        self.parent_level = parentLevel
        self.parent_position = parentPosition
        self.event = event
