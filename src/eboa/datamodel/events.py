"""
Events data model definition

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
from geoalchemy2.shape import to_shape
from sqlalchemy import Column, Integer, Table, DateTime, ForeignKey, Text, Float, Boolean
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry

from eboa.datamodel.base import Base
import eboa.engine.export as export

class Event(Base):
    __tablename__ = 'events'

    event_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    start = Column(DateTime)
    stop = Column(DateTime)
    ingestion_time = Column(DateTime)
    visible = Column(Boolean)
    gauge_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('gauges.gauge_uuid'))
    explicit_ref_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('explicit_refs.explicit_ref_uuid'))
    source_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('sources.source_uuid'))
    gauge = relationship("Gauge", backref="events")
    explicitRef = relationship("ExplicitRef", backref="events")
    source = relationship("Source", backref="events")

    def __init__(self, event_uuid, start, stop, ingestion_time, gauge, source, explicit_ref = None, visible = True):
        self.event_uuid = event_uuid
        self.start = start
        self.stop = stop
        self.ingestion_time = ingestion_time
        self.visible = visible
        self.gauge = gauge
        self.explicitRef = explicit_ref
        self.source = source

    def get_structured_values(self):
        """
        Method to obtain the structure of values in a python dictionary format
        """

        values = []
        for values_relation in ["eventTexts", "eventDoubles", "eventObjects", "eventGeometries", "eventBooleans", "eventTimestamps"]:
            values += eval("self." + values_relation)
        # end for

        json_values = []
        if len(values) > 0:
            export.build_values_structure(values, json_values)
        # end if

        return json_values

    def get_values(self):
        """
        Method to obtain the associated values
        """

        values = []
        for values_relation in ["eventTexts", "eventDoubles", "eventObjects", "eventGeometries", "eventBooleans", "eventTimestamps"]:
            values += eval("self." + values_relation)
        # end for

        return values

class EventLink(Base):
    __tablename__ = 'event_links'
    event_uuid_link = Column(postgresql.UUID(as_uuid=True))
    name = Column(Text)
    event_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('events.event_uuid'))
    event = relationship("Event", backref="eventLinks")
    __mapper_args__ = {
        'primary_key':[event_uuid_link, name, event_uuid]
    }

    def __init__(self, link, name, event):
        self.event_uuid_link = link
        self.name = name
        self.event = event

class EventKey(Base):
    __tablename__ = 'event_keys'

    event_key = Column(Text)
    visible = Column(Boolean)
    event_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('events.event_uuid'))
    event = relationship("Event", backref="eventKeys")
    dim_signature_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('dim_signatures.dim_signature_uuid'))
    dim_signature = relationship("DimSignature", backref="eventKeys")
    __mapper_args__ = {
        'primary_key':[event_uuid]
    }

    def __init__(self, key, event, dim_signature, visible = False):
        self.event_key = key
        self.visible = visible
        self.event = event
        self.dim_signature = dim_signature

    def jsonify(self):
        return {
            "event_key": self.event_key,
            "event_uuid": self.event_uuid,
            "dim_signature_uuid": self.dim_signature_uuid
        }

class EventBoolean(Base):
    __tablename__ = 'event_booleans'

    name = Column(Text)
    value = Column(Boolean)
    level_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    event_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('events.event_uuid'))
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

    def jsonify(self):
        return {
            "type": "boolean",
            "name": self.name,
            "value": self.value,
            "level_position": self.level_position,
            "parent_level": self.parent_level,
            "parent_position": self.parent_position,
        }

class EventText(Base):
    __tablename__ = 'event_texts'

    name = Column(Text)
    value = Column(Text)
    level_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    event_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('events.event_uuid'))
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

    def jsonify(self):
        return {
            "type": "text",
            "name": self.name,
            "value": self.value,
            "level_position": self.level_position,
            "parent_level": self.parent_level,
            "parent_position": self.parent_position,
        }

class EventDouble(Base):
    __tablename__ = 'event_doubles'

    name = Column(Text)
    value = Column(Float)
    level_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    event_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('events.event_uuid'))
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

    def jsonify(self):
        return {
            "type": "double",
            "name": self.name,
            "value": self.value,
            "level_position": self.level_position,
            "parent_level": self.parent_level,
            "parent_position": self.parent_position,
        }

class EventTimestamp(Base):
    __tablename__ = 'event_timestamps'

    name = Column(Text)
    value = Column(DateTime)
    level_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    event_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('events.event_uuid'))
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

    def jsonify(self):
        return {
            "type": "timestamp",
            "name": self.name,
            "value": self.value.isoformat(),
            "level_position": self.level_position,
            "parent_level": self.parent_level,
            "parent_position": self.parent_position,
        }

class EventObject(Base):
    __tablename__ = 'event_objects'

    name = Column(Text)
    level_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    event_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('events.event_uuid'))
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

    def jsonify(self):
        return {
            "type": "object",
            "name": self.name,
            "value": "",
            "level_position": self.level_position,
            "parent_level": self.parent_level,
            "parent_position": self.parent_position,
        }

class EventGeometry(Base):
    __tablename__ = 'event_geometries'

    name = Column(Text)
    value = Column(Geometry('POLYGON'))
    level_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    event_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('events.event_uuid'))
    event = relationship("Event", backref="eventGeometries")
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

    def jsonify(self):
        return {
            "type": "geometry",
            "name": self.name,
            "value": to_shape(self.value).to_wkt(),
            "level_position": self.level_position,
            "parent_level": self.parent_level,
            "parent_position": self.parent_position,
        }

class EventAlert(Base):
    __tablename__ = 'event_alerts'

    message = Column(Text)
    validated = Column(Boolean)
    ingestion_time = Column(DateTime)
    generator = Column(Text)
    notified = Column(Boolean)
    event_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('events.event_uuid'))
    event = relationship("Event", backref="alerts")
    alert_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('alerts.alert_uuid'))
    alert = relationship("Alert", backref="event_alerts")
    __mapper_args__ = {
        'primary_key':[alert_uuid, event_uuid]
    }

    def __init__(self, time_stamp, status, event, log = None):
        self.time_stamp = time_stamp
        self.status = status
        self.log = log
        self.event = event
