"""
Annotations data model definition

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""

from sqlalchemy import Column, Integer, Table, DateTime, ForeignKey, Text, Float, Boolean
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry

from .base import Base

class Annotation(Base):
    __tablename__ = 'annot_tb'

    annotation_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    generation_time = Column(DateTime)
    ingestion_time = Column(DateTime)
    annotation_cnf_id = Column(Integer, ForeignKey('annot_cnf_tb.annotation_cnf_id'))
    explicit_ref_id = Column(Integer, ForeignKey('explicit_ref_tb.explicit_ref_id'))
    processing_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('dim_processing_tb.processing_uuid'))
    annotationCnf = relationship("AnnotationCnf", backref="annotations")
    explicitRef = relationship("ExplicitRef", backref="annotations")
    source = relationship("DimProcessing", backref="annotations")

    def __init__(self, annotation_uuid, generation_time, ingestion_time, annotation_cnf, explicit_ref, dim_processing = None):
        self.annotation_uuid = annotation_uuid
        self.generation_time = generation_time
        self.ingestion_time = ingestion_time
        self.annotationCnf = annotation_cnf
        self.explicitRef = explicit_ref
        self.source = dim_processing

class AnnotationCnf(Base):
    __tablename__ = 'annot_cnf_tb'

    annotation_cnf_id = Column(Integer, primary_key=True)
    name = Column(Text)
    system = Column(Text)
    dim_signature_id = Column(Integer, ForeignKey('dim_signature_tb.dim_signature_id'))
    dim_signature = relationship("DimSignature", backref="annotationCnfs")
    
    def __init__(self, name, dim_signature, system = None):
        self.name = name
        self.system = system
        self.dim_signature = dim_signature

class AnnotationBoolean(Base):
    __tablename__ = 'annot_boolean_tb'

    name = Column(Text, primary_key=True)
    value = Column(Boolean)
    level_position = Column(Integer)
    child_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    annotation_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('annot_tb.annotation_uuid'))
    annotation = relationship("Annotation", backref="annotBooleans")

    def __init__(self, name, value, level_position, child_position, parent_level, parent_position, annotation):
        self.name = name
        self.value = value
        self.level_position = level_position
        self.child_position = child_position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.annotation = annotation

class AnnotationText(Base):
    __tablename__ = 'annot_text_tb'

    name = Column(Text, primary_key=True)
    value = Column(Text)
    level_position = Column(Integer)
    child_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    annotation_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('annot_tb.annotation_uuid'))
    annotation = relationship("Annotation", backref="annotTexts")

    def __init__(self, name, value, level_position, child_position, parent_level, parent_position, annotation):
        self.name = name
        self.value = value
        self.level_position = level_position
        self.child_position = child_position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.annotation = annotation

class AnnotationDouble(Base):
    __tablename__ = 'annot_double_tb'

    name = Column(Text, primary_key=True)
    value = Column(Float)
    level_position = Column(Integer)
    child_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    annotation_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('annot_tb.annotation_uuid'))
    annotation = relationship("Annotation", backref="annotDoubles")

    def __init__(self, name, value, level_position, child_position, parent_level, parent_position, annotation):
        self.name = name
        self.value = value
        self.level_position = level_position
        self.child_position = child_position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.annotation = annotation

class AnnotationTimestamp(Base):
    __tablename__ = 'annot_timestamp_tb'

    name = Column(Text, primary_key=True)
    value = Column(DateTime)
    level_position = Column(Integer)
    child_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    annotation_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('annot_tb.annotation_uuid'))
    annotation = relationship("Annotation", backref="annotTimestamps")

    def __init__(self, name, value, level_position, child_position, parent_level, parent_position, annotation):
        self.name = name
        self.value = value
        self.level_position = level_position
        self.child_position = child_position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.annotation = annotation

class AnnotationObject(Base):
    __tablename__ = 'annot_object_tb'

    name = Column(Text, primary_key=True)
    level_position = Column(Integer)
    child_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    annotation_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('annot_tb.annotation_uuid'))
    annotation = relationship("Annotation", backref="annotObjects")

    def __init__(self, name, level_position, child_position, parent_level, parent_position, annotation):
        self.name = name
        self.level_position = level_position
        self.child_position = child_position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.annotation = annotation

class AnnotationGeometry(Base):
    __tablename__ = 'annot_geometry_tb'

    name = Column(Text, primary_key=True)
    value = Column(Geometry('POLYGON'))
    level_position = Column(Integer)
    child_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    annotation_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('annot_tb.annotation_uuid'))
    annotation = relationship("Annotation", backref="annotGeometrys")

    def __init__(self, name, value, level_position, child_position, parent_level, parent_position, annotation):
        self.name = name
        self.value = value
        self.level_position = level_position
        self.child_position = child_position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.annotation = annotation
