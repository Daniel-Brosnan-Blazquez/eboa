"""
Annotations data model definition

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""

from sqlalchemy import Column, Integer, Table, ForeignKey, Text, DateTime
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship

from .base import Base


class Annotation(Base):
    __tablename__ = 'annot_tb'

    annotation_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    time_stamp = Column(DateTime)
    ingestion_time = Column(DateTime)
    annotation_cnf_id = Column(Integer, ForeignKey('annot_cnf_tb.annotation_cnf_id'))
    explicit_ref_id = Column(Integer, ForeignKey('explicit_ref_tb.explicit_ref_id'))
    processing_uuid = Column(postgresql.UUID, ForeignKey('dim_processing_tb.processing_uuid'))
    annotationCnf = relationship("AnnotationCnf", backref="annotations")
    explicitRef = relationship("ExplicitRef", backref="annotations")
    source = relationship("DimProcessing", backref="annotations")

    def __init__(self, annotationUuid, timeStamp, ingestionTime, annotationCnf, explicitRef, dimProcessing = None):
        self.annotation_uuid = annotationUuid
        self.time_stamp = timeStamp
        self.ingestion_time = ingestionTime
        self.annotationCnf = annotationCnf
        self.explicitRef = explicitRef
        self.source = dimProcessing

class AnnotationCnf(Base):
    __tablename__ = 'annot_cnf_tb'

    annotation_cnf_id = Column(Integer, primary_key=True)
    name = Column(Text)
    dim_signature_id = Column(Integer, ForeignKey('dim_signature_tb.dim_signature_id'))
    dim_signature = relationship("DimSignature", backref="annotationCnfs")
    
    def __init__(self, name, dimSignature):
        self.name = name
        self.dim_signature = dimSignature

