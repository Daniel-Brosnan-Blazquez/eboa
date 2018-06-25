"""
DIM processing data model definition

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""

from sqlalchemy import Column, Integer, Table, ForeignKey, Text, DateTime, Float, Interval
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship

from .base import Base

class DimProcessing(Base):
    __tablename__ = 'dim_processing_tb'

    processing_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    name = Column(Text)
    validity_start = Column(DateTime)
    validity_stop = Column(DateTime)
    generation_time = Column(DateTime)
    ingestion_time = Column(DateTime)
    ingestion_duration = Column(Interval)
    dim_exec_version = Column(Text)
    dim_signature_id = Column(Integer, ForeignKey('dim_signature_tb.dim_signature_id'))
    dimSignature = relationship("DimSignature", backref="dimProcessings")

    def __init__(self, processing_uuid, name, generation_time, exec_version, dim_signature, validity_start = None, validity_stop = None, ingestion_time = None):
        self.processing_uuid = processing_uuid
        self.name = name
        self.validity_start = validity_start
        self.validity_stop = validity_stop
        self.generation_time = generation_time
        self.ingestion_time = ingestion_time
        self.dim_exec_version = exec_version
        self.dimSignature = dim_signature

class DimProcessingStatus(Base):
    __tablename__ = 'dim_processing_status_tb'

    time_stamp = Column(DateTime)
    proc_status = Column(Integer, primary_key=True)
    processing_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('dim_processing_tb.processing_uuid'))
    source = relationship("DimProcessing", backref="statuses")

    def __init__(self, time_stamp, status, dim_processing):
        self.time_stamp = time_stamp
        self.proc_status = status
        self.source = dim_processing
