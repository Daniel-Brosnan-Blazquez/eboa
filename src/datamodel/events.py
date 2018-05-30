"""
Events data model definition

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""

from sqlalchemy import Column, Integer, Table, DateTime, ForeignKey
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
    #explicit_ref_id = Column(Integer, ForeignKey('explicit_ref_tb.explicit_ref_id'))
    #processing_uuid = Column(postgresql.UUID, ForeignKey('dim_processing_tb.processing_uuid'))
    gauge = relationship("Gauge", backref="events")

    def __init__(self, eventUuid, start, stop, timeStamp, ingestionTime, gauge, explicitRefId = None, processingUuid = None):
        self.event_uuid = eventUuid
        self.start = start
        self.stop = stop
        self.time_stamp = timeStamp
        self.ingestion_time = ingestionTime
        self.gauge = gauge
        self.explicit_ref_id = explicitRefId
        self.processing_uuid = processingUuid
