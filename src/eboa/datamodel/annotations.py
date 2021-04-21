"""
Annotations data model definition

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
from geoalchemy2.shape import to_shape
from sqlalchemy import Column, Integer, Table, DateTime, ForeignKey, Text, Float, Boolean
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry

from eboa.datamodel.base import Base
import eboa.engine.export as export

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

    def jsonify(self):
        """
        Method to obtain the structure of the annotation in a python dictionary format

        :return: structure of the annotation
        :rtype: dict
        """

        structure = {
            "annotation_uuid": str(self.annotation_uuid),
            "explicit_reference": {
                "uuid": str(self.explicitRef.explicit_ref_uuid),
                "name": self.explicitRef.explicit_ref
            },
            "ingestion_time": self.ingestion_time.isoformat(),
            "configuration": {
                "uuid": str(self.annotationCnf.annotation_cnf_uuid),
                "dim_signature": self.annotationCnf.dim_signature.dim_signature,
                "name": self.annotationCnf.name,
                "system": self.annotationCnf.system,
                "description": self.annotationCnf.description,
            },
            "source": {
                "source_uuid": str(self.source.source_uuid),
                "name": self.source.name,
            },
            "values": self.get_structured_values(),
            "alerts": [alert.jsonify() for alert in self.alerts]
        }

        return structure

    def get_structured_values(self, position = 0, parent_level = -1, parent_position = 0):
        """
        Method to obtain the structure of values in a python dictionary format
        """

        values = []
        for values_relation in ["annotationTexts", "annotationDoubles", "annotationObjects", "annotationGeometries", "annotationBooleans", "annotationTimestamps"]:
            values += eval("self." + values_relation)
        # end for

        json_values = []
        if len(values) > 0:
            export.build_values_structure(values, json_values,
                                          position = position,
                                          parent_level = parent_level,
                                          parent_position = parent_position)
        # end if

        return json_values

class AnnotationCnf(Base):
    __tablename__ = 'annotation_cnfs'

    annotation_cnf_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    name = Column(Text)
    system = Column(Text)
    description = Column(Text)
    dim_signature_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('dim_signatures.dim_signature_uuid'))
    dim_signature = relationship("DimSignature", backref="annotationCnfs")
    
    def __init__(self, annotation_cnf_uuid, name, dim_signature, system = None, description = None):
        self.annotation_cnf_uuid = annotation_cnf_uuid
        self.name = name
        self.system = system
        self.description = description
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
    position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    annotation_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('annotations.annotation_uuid'))
    annotation = relationship("Annotation", backref="annotationBooleans")
    __mapper_args__ = {
        'primary_key':[name, position, parent_level, parent_position, annotation_uuid]
    }

    def __init__(self, name, value, position, parent_level, parent_position, annotation):
        self.name = name
        self.value = value
        self.position = position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.annotation = annotation

    def jsonify(self):
        return {
            "type": "boolean",
            "name": self.name,
            "value": self.value,
            "position": self.position,
            "parent_level": self.parent_level,
            "parent_position": self.parent_position,
        }

class AnnotationText(Base):
    __tablename__ = 'annotation_texts'

    name = Column(Text)
    value = Column(Text)
    position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    annotation_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('annotations.annotation_uuid'))
    annotation = relationship("Annotation", backref="annotationTexts")
    __mapper_args__ = {
        'primary_key':[name, position, parent_level, parent_position, annotation_uuid]
    }

    def __init__(self, name, value, position, parent_level, parent_position, annotation):
        self.name = name
        self.value = value
        self.position = position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.annotation = annotation

    def jsonify(self):
        return {
            "type": "text",
            "name": self.name,
            "value": self.value,
            "position": self.position,
            "parent_level": self.parent_level,
            "parent_position": self.parent_position,
        }

class AnnotationDouble(Base):
    __tablename__ = 'annotation_doubles'

    name = Column(Text)
    value = Column(Float)
    position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    annotation_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('annotations.annotation_uuid'))
    annotation = relationship("Annotation", backref="annotationDoubles")
    __mapper_args__ = {
        'primary_key':[name, position, parent_level, parent_position, annotation_uuid]
    }

    def __init__(self, name, value, position, parent_level, parent_position, annotation):
        self.name = name
        self.value = value
        self.position = position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.annotation = annotation

    def jsonify(self):
        return {
            "type": "double",
            "name": self.name,
            "value": self.value,
            "position": self.position,
            "parent_level": self.parent_level,
            "parent_position": self.parent_position,
        }

class AnnotationTimestamp(Base):
    __tablename__ = 'annotation_timestamps'

    name = Column(Text)
    value = Column(DateTime)
    position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    annotation_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('annotations.annotation_uuid'))
    annotation = relationship("Annotation", backref="annotationTimestamps")
    __mapper_args__ = {
        'primary_key':[name, position, parent_level, parent_position, annotation_uuid]
    }

    def __init__(self, name, value, position, parent_level, parent_position, annotation):
        self.name = name
        self.value = value
        self.position = position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.annotation = annotation

    def jsonify(self):
        return {
            "type": "timestamp",
            "name": self.name,
            "value": self.value.isoformat(),
            "position": self.position,
            "parent_level": self.parent_level,
            "parent_position": self.parent_position,
        }

class AnnotationObject(Base):
    __tablename__ = 'annotation_objects'

    name = Column(Text)
    position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    annotation_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('annotations.annotation_uuid'))
    annotation = relationship("Annotation", backref="annotationObjects")
    __mapper_args__ = {
        'primary_key':[name, position, parent_level, parent_position, annotation_uuid]
    }

    def __init__(self, name, position, parent_level, parent_position, annotation):
        self.name = name
        self.position = position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.annotation = annotation

    def jsonify(self):
        return {
            "type": "object",
            "name": self.name,
            "value": "",
            "position": self.position,
            "parent_level": self.parent_level,
            "parent_position": self.parent_position,
        }

class AnnotationGeometry(Base):
    __tablename__ = 'annotation_geometries'

    name = Column(Text)
    value = Column(Geometry('POLYGON'))
    position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    annotation_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('annotations.annotation_uuid'))
    annotation = relationship("Annotation", backref="annotationGeometries")
    __mapper_args__ = {
        'primary_key':[name, position, parent_level, parent_position, annotation_uuid]
    }

    def __init__(self, name, value, position, parent_level, parent_position, annotation):
        self.name = name
        self.value = value
        self.position = position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.annotation = annotation

    def jsonify(self):
        return {
            "type": "geometry",
            "name": self.name,
            "value": to_shape(self.value).wkt,
            "position": self.position,
            "parent_level": self.parent_level,
            "parent_position": self.parent_position,
        }

    def to_wkt(self):
        return {
            "value": to_shape(self.value).wkt,
            "name": self.name
        }
