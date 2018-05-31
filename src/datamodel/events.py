"""
Events data model definition

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""

from sqlalchemy import Column, Integer, Table, DateTime, ForeignKey, Text
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship

from .base import Base

class Event(Base):
    __tablename__ = 'event_tb'

    event_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    start = Column(DateTime)
    stop = Column(DateTime)
    time_stamp = Column(DateTime)
    ingestion_time = Column(DateTime)
    gauge_id = Column(Integer, ForeignKey('gauge_cnf_tb.gauge_id'))
    explicit_ref_id = Column(Integer, ForeignKey('explicit_ref_tb.explicit_ref_id'))
    processing_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('dim_processing_tb.processing_uuid'))
    gauge = relationship("Gauge", backref="events")
    explicitRef = relationship("ExplicitRef", backref="events")
    source = relationship("DimProcessing", backref="events")

    def __init__(self, eventUuid, start, stop, timeStamp, ingestionTime, gauge, explicitRef = None, dimProcessing = None):
        self.event_uuid = eventUuid
        self.start = start
        self.stop = stop
        self.time_stamp = timeStamp
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

