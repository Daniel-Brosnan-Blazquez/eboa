"""
DIM processing data model definition

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""

from sqlalchemy import Column, Integer, Table, ForeignKey, Text, DateTime, Float
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship

from .base import Base

class DimProcessing(Base):
    __tablename__ = 'dim_processing_tb'

    processing_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    filename = Column(Text)
    validity_start = Column(DateTime)
    validity_stop = Column(DateTime)
    generation_time = Column(DateTime)
    ingestion_time = Column(DateTime)
    ingestion_duration = Column(Float)
    dim_exec_version = Column(Text)
    dim_signature_id = Column(Integer, ForeignKey('dim_signature_tb.dim_signature_id'))
    dimSignature = relationship("DimSignature", backref="dimProcessings")

    def __init__(self, processingUuid, filename, validityStart, validityStop, generationTime, ingestionTime, execVersion, dimSignature):
        self.processing_uuid = processingUuid
        self.filename = filename
        self.validity_start = validityStart
        self.validity_stop = validityStop
        self.generation_time = generationTime
        self.ingestion_time = ingestionTime
        self.dim_exec_version = execVersion
        self.dimSignature = dimSignature

class DimProcessingStatus(Base):
    __tablename__ = 'dim_processing_status_tb'

    time_stamp = Column(DateTime)
    proc_status = Column(Integer, primary_key=True)
    processing_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('dim_processing_tb.processing_uuid'))
    processing = relationship("DimProcessing", backref="statuses")

    def __init__(self, timeStamp, status, dimProcessing):
        self.time_stamp = timeStamp
        self.status = status
        self.processing = dimProcessing
