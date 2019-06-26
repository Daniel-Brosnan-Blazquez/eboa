"""
Source data model definition

Written by DEIMOS Space S.L. (dibb)

module eboa
"""

from sqlalchemy import Column, Integer, Table, ForeignKey, Text, DateTime, Float, Interval, JSON, Numeric, Boolean
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship

from eboa.datamodel.base import Base

class Source(Base):
    __tablename__ = 'sources'

    source_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    name = Column(Text)
    validity_start = Column(DateTime)
    validity_stop = Column(DateTime)
    generation_time = Column(DateTime)
    ingested = Column(Boolean)
    ingestion_error = Column(Boolean)
    ingestion_time = Column(DateTime)
    ingestion_duration = Column(Interval)
    processing_duration = Column(Interval)
    processor = Column(Text)
    processor_version = Column(Text)
    content_json = Column(JSON)
    content_text = Column(Text)
    parse_error = Column(Text)
    processor_progress = Column(Numeric(5,2))
    ingestion_progress = Column(Numeric(5,2))
    ingestion_completeness = Column(Integer)
    dim_signature_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('dim_signatures.dim_signature_uuid'))
    dimSignature = relationship("DimSignature", backref="sources")

    def __init__(self, source_uuid, name, generation_time = None, processor_version = None, dim_signature = None, validity_start = None, validity_stop = None, ingestion_time = None, processor = None):
        self.source_uuid = source_uuid
        self.name = name
        self.validity_start = validity_start
        self.validity_stop = validity_stop
        self.generation_time = generation_time
        self.ingestion_time = ingestion_time
        self.processor_version = processor_version
        self.processor = processor
        self.dimSignature = dim_signature

    def jsonify(self):
        return {
            "source_uuid": self.source_uuid,
            "name": self.name,
            "validity_start": self.validity_start,
            "validity_stop": self.validity_stop,
            "generation_time": self.generation_time,
            "ingested": self.ingested,
            "ingestion_error": self.ingestion_error,
            "ingestion_time": self.ingestion_time,
            "ingestion_duration": self.ingestion_duration,
            "processing_duration": self.processing_duration,
            "processor": self.processor,
            "processor_version": self.processor_version,
            "dim_signature_uuid": self.dim_signature_uuid
        }

class SourceStatus(Base):
    __tablename__ = 'source_statuses'

    time_stamp = Column(DateTime)
    status = Column(Integer)
    log = Column(Text)
    source_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('sources.source_uuid'))
    source = relationship("Source", backref="statuses")
    __mapper_args__ = {
        'primary_key':[source_uuid]
    }

    def __init__(self, time_stamp, status, source, log = None):
        self.time_stamp = time_stamp
        self.status = status
        self.log = log
        self.source = source
