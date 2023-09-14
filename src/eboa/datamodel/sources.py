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

    def __init__(self, source_uuid, name, reception_time, generation_time = None, processor_version = None, dim_signature = None, validity_start = None, validity_stop = None, ingestion_time = None, processor = None, processing_duration = None, processor_progress = None, reported_generation_time = None, reported_validity_start = None, reported_validity_stop = None, priority = None, ingestion_completeness = None, ingestion_completeness_message = None):
        self.source_uuid = source_uuid
        self.name = name
        self.validity_start = validity_start
        self.validity_stop = validity_stop
        self.reported_validity_start = reported_validity_start
        self.reported_validity_stop = reported_validity_stop
        self.reception_time = reception_time
        self.generation_time = generation_time
        self.reported_generation_time = reported_generation_time
        self.ingestion_time = ingestion_time
        self.processor_version = processor_version
        self.processor = processor
        self.dimSignature = dim_signature
        self.processing_duration = processing_duration
        self.processor_progress = processor_progress
        self.priority = priority
        self.ingestion_completeness = ingestion_completeness
        self.ingestion_completeness_message = ingestion_completeness_message

    def jsonify(self, include_source_statuses = False):
        """
        Method to obtain the structure of the source in a python dictionary format

        :param include_source_statuses: flag to indicate to include the statuses associated to the source
        :type include_source_statuses: boolean

        :return: structure of the source
        :rtype: dict
        """

        structure = {
            "source_uuid": str(self.source_uuid),
            "name": self.name,
            "validity_start": self.validity_start.isoformat(),
            "validity_stop": self.validity_stop.isoformat(),
            "reported_validity_start": self.reported_validity_start.isoformat(),
            "reported_validity_stop": self.reported_validity_stop.isoformat(),
            "reception_time": self.generation_time.isoformat(),
            "generation_time": self.generation_time.isoformat(),
            "reported_generation_time": self.reported_generation_time.isoformat(),
            "ingested": self.ingested,
            "ingestion_error": self.ingestion_error,
            "ingestion_time": self.ingestion_time.isoformat(),
            "ingestion_duration": str(self.ingestion_duration),
            "processing_duration": str(self.processing_duration),
            "processor": self.processor,
            "processor_version": self.processor_version,
            "ingestion_completeness": self.ingestion_completeness,
            "ingestion_completeness_message": self.ingestion_completeness_message,
            "priority": self.priority,
            "dim_signature": str(self.dimSignature.dim_signature),
            "number_of_events": len(self.events),
            "number_of_annotations": len(self.annotations)
        }
        
        if include_source_statuses:
            structure["statuses"] = []
            for status in sorted(self.statuses, key=lambda status: status.time_stamp):
                structure["statuses"].append(status.jsonify())
            # end for
        # end if

        return structure

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
            "source_uuid": str(self.source.source_uuid),
            "time_stamp": self.time_stamp.isoformat(),
            "log": self.log,
            "status": self.status,
        }
