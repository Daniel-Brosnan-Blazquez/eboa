"""
Annotations data model definition

Written by DEIMOS Space S.L. (dibb)

module eboa
"""

from sqlalchemy import Column, Integer, Table, DateTime, ForeignKey, Text, Float, Boolean
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry

from eboa.datamodel.base import Base

class Annotation(Base):
    __tablename__ = 'annotations'

    annotation_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    ingestion_time = Column(DateTime)
    visible = Column(Boolean)
    annotation_cnf_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('annotation_cnfs.annotation_cnf_uuid'))
    explicit_ref_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('explicit_refs.explicit_ref_uuid'))
    source_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('sources.source_uuid'))
    annotationCnf = relationship("AnnotationCnf", backref="annotations")
    explicitRef = relationship("ExplicitRef", backref="annotations")
    source = relationship("Source", backref="annotations")

    def __init__(self, annotation_uuid, ingestion_time, annotation_cnf, explicit_ref, source, visible = False):
        self.annotation_uuid = annotation_uuid
        self.ingestion_time = ingestion_time
        self.visible = visible
        self.annotationCnf = annotation_cnf
        self.explicitRef = explicit_ref
        self.source = source

class AnnotationCnf(Base):
    __tablename__ = 'annotation_cnfs'

    annotation_cnf_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    name = Column(Text)
    system = Column(Text)
    dim_signature_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('dim_signatures.dim_signature_uuid'))
    dim_signature = relationship("DimSignature", backref="annotationCnfs")
    
    def __init__(self, annotation_cnf_uuid, name, dim_signature, system = None):
        self.annotation_cnf_uuid = annotation_cnf_uuid
        self.name = name
        self.system = system
        self.dim_signature = dim_signature

    def jsonify(self):
        return {
            "annotation_cnf_uuid": self.annotation_cnf_uuid,
            "system": self.system,
            "name": self.name,
            "dim_signature_uuid": self.dim_signature_uuid
        }

class AnnotationBoolean(Base):
    __tablename__ = 'annotation_booleans'

    name = Column(Text)
    value = Column(Boolean)
    level_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    annotation_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('annotations.annotation_uuid'))
    annotation = relationship("Annotation", backref="annotBooleans")
    __mapper_args__ = {
        'primary_key':[name, level_position, parent_level, parent_position, annotation_uuid]
    }

    def __init__(self, name, value, level_position, parent_level, parent_position, annotation):
        self.name = name
        self.value = value
        self.level_position = level_position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.annotation = annotation

class AnnotationText(Base):
    __tablename__ = 'annotation_texts'

    name = Column(Text)
    value = Column(Text)
    level_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    annotation_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('annotations.annotation_uuid'))
    annotation = relationship("Annotation", backref="annotTexts")
    __mapper_args__ = {
        'primary_key':[name, level_position, parent_level, parent_position, annotation_uuid]
    }

    def __init__(self, name, value, level_position, parent_level, parent_position, annotation):
        self.name = name
        self.value = value
        self.level_position = level_position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.annotation = annotation

class AnnotationDouble(Base):
    __tablename__ = 'annotation_doubles'

    name = Column(Text)
    value = Column(Float)
    level_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    annotation_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('annotations.annotation_uuid'))
    annotation = relationship("Annotation", backref="annotDoubles")
    __mapper_args__ = {
        'primary_key':[name, level_position, parent_level, parent_position, annotation_uuid]
    }

    def __init__(self, name, value, level_position, parent_level, parent_position, annotation):
        self.name = name
        self.value = value
        self.level_position = level_position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.annotation = annotation

class AnnotationTimestamp(Base):
    __tablename__ = 'annotation_timestamps'

    name = Column(Text)
    value = Column(DateTime)
    level_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    annotation_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('annotations.annotation_uuid'))
    annotation = relationship("Annotation", backref="annotTimestamps")
    __mapper_args__ = {
        'primary_key':[name, level_position, parent_level, parent_position, annotation_uuid]
    }

    def __init__(self, name, value, level_position, parent_level, parent_position, annotation):
        self.name = name
        self.value = value
        self.level_position = level_position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.annotation = annotation

class AnnotationObject(Base):
    __tablename__ = 'annotation_objects'

    name = Column(Text)
    level_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    annotation_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('annotations.annotation_uuid'))
    annotation = relationship("Annotation", backref="annotObjects")
    __mapper_args__ = {
        'primary_key':[name, level_position, parent_level, parent_position, annotation_uuid]
    }

    def __init__(self, name, level_position, parent_level, parent_position, annotation):
        self.name = name
        self.level_position = level_position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.annotation = annotation

class AnnotationGeometry(Base):
    __tablename__ = 'annotation_geometrys'

    name = Column(Text)
    value = Column(Geometry('POLYGON'))
    level_position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    annotation_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('annotations.annotation_uuid'))
    annotation = relationship("Annotation", backref="annotGeometrys")
    __mapper_args__ = {
        'primary_key':[name, level_position, parent_level, parent_position, annotation_uuid]
    }

    def __init__(self, name, value, level_position, parent_level, parent_position, annotation):
        self.name = name
        self.value = value
        self.level_position = level_position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.annotation = annotation
