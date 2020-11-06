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
    reported_validity_start = Column(DateTime)
    reported_validity_stop = Column(DateTime)
    reception_time = Column(DateTime)
    generation_time = Column(DateTime)
    reported_generation_time = Column(DateTime)
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
    ingestion_completeness = Column(Boolean)
    ingestion_completeness_message = Column(Text)
    priority = Column(Integer)
    dim_signature_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('dim_signatures.dim_signature_uuid'))
    dimSignature = relationship("DimSignature", backref="sources")

    def __init__(self, source_uuid, name, reception_time, generation_time = None, processor_version = None, dim_signature = None, validity_start = None, validity_stop = None, ingestion_time = None, processor = None, processing_duration = None, processor_progress = None):
        self.source_uuid = source_uuid
        self.name = name
        self.validity_start = validity_start
        self.validity_stop = validity_stop
        self.reception_time = reception_time
        self.generation_time = generation_time
        self.ingestion_time = ingestion_time
        self.processor_version = processor_version
        self.processor = processor
        self.dimSignature = dim_signature
        self.processing_duration = processing_duration
        self.processor_progress = processor_progress

    def jsonify(self):
        return {
            "source_uuid": self.source_uuid,
            "name": self.name,
            "validity_start": str(self.validity_start).replace(" ", "T"),
            "validity_stop": str(self.validity_stop).replace(" ", "T"),
            "reception_time": str(self.generation_time).replace(" ", "T"),
            "generation_time": str(self.generation_time).replace(" ", "T"),
            "ingested": self.ingested,
            "ingestion_error": self.ingestion_error,
            "ingestion_time": str(self.ingestion_time).replace(" ", "T"),
            "ingestion_duration": str(self.ingestion_duration),
            "processing_duration": str(self.processing_duration),
            "processor": self.processor,
            "processor_version": self.processor_version,
            "dim_signature_uuid": self.dim_signature_uuid
        }

    def get_ingestion_progress(self):

        ingestion_progress = 0
        if self.ingestion_progress:
            ingestion_progress = self.ingestion_progress
        # end if
        
        return ingestion_progress

    def get_processor_progress(self):

        processor_progress = 0
        if self.processor_progress:
            processor_progress = self.processor_progress
        # end if
        
        return processor_progress

    def get_general_progress(self):

        ingestion_progress = 0
        if self.ingestion_progress:
            ingestion_progress = self.ingestion_progress
        # end if
        processor_progress = 0
        if self.processor_progress:
            processor_progress = self.processor_progress
        # end if
        
        return (ingestion_progress + processor_progress) / 2

    def get_triggering_duration(self):

        triggering_duration = None
        if self.reception_time and self.ingestion_time:
            triggering_duration = self.ingestion_time - self.reception_time
        # end if
        
        return triggering_duration

class SourceStatus(Base):
    __tablename__ = 'source_statuses'

    source_status_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    time_stamp = Column(DateTime)
    status = Column(Integer)
    log = Column(Text)
    source_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('sources.source_uuid'))
    source = relationship("Source", backref="statuses")

    def __init__(self, source_status_uuid, time_stamp, status, source, log = None):
        self.source_status_uuid = source_status_uuid
        self.time_stamp = time_stamp
        self.status = status
        self.log = log
        self.source = source

    def jsonify(self):
        return {
            "source_uuid": self.source.source_uuid,
            "time_stamp": str(self.time_stamp).replace(" ", "T"),
            "log": self.log,
            "status": self.status,
        }
