"""
Report data model definition

Written by DEIMOS Space S.L. (dibb)

module rboa
"""

from sqlalchemy import Column, Integer, Table, ForeignKey, Text, DateTime, Float, Interval, JSON, Numeric, Boolean
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry

from eboa.datamodel.base import Base

class Report(Base):
    __tablename__ = 'reports'

    report_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    name = Column(Text)
    generation_mode = Column(Text)
    relative_path = Column(Text)
    validity_start = Column(DateTime)
    validity_stop = Column(DateTime)
    triggering_time = Column(DateTime)
    generation_start = Column(DateTime)
    generation_stop = Column(DateTime)
    metadata_ingestion_duration = Column(Interval)
    generated = Column(Boolean)
    compressed = Column(Boolean)
    generation_progress = Column(Numeric(5,2))
    metadata_ingestion_progress = Column(Numeric(5,2))
    generator = Column(Text)
    generator_version = Column(Text)
    generation_error = Column(Boolean)
    content_json = Column(JSON)
    report_group_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('report_groups.report_group_uuid'))
    reportGroup = relationship("ReportGroup", backref="reports")

    def __init__(self, report_uuid, name, triggering_time, report_group, generation_mode = None, validity_start = None, validity_stop = None, generation_start = None, generation_stop = None, generator = None, generator_version = None, generation_progress = None):
        self.report_uuid = report_uuid
        self.name = name
        self.triggering_time = triggering_time
        self.generation_mode = generation_mode
        self.validity_start = validity_start
        self.validity_stop = validity_stop
        self.generation_start = generation_start
        self.generation_stop = generation_stop
        self.generator = generator
        self.generator_version = generator_version
        self.generation_progress = generation_progress
        self.reportGroup = report_group

    def get_structured_values(self, position = 0, parent_level = -1, parent_position = 0):
        """
        Method to obtain the structure of values in a python dictionary format
        """

        values = []
        for values_relation in ["reportTexts", "reportDoubles", "reportObjects", "reportGeometries", "reportBooleans", "reportTimestamps"]:
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

    def get_values(self):
        """
        Method to obtain the associated values
        """

        values = []
        for values_relation in ["reportTexts", "reportDoubles", "reportObjects", "reportGeometries", "reportBooleans", "reportTimestamps"]:
            values += eval("self." + values_relation)
        # end for

        return values

    def jsonify(self):
        return {
            "report_uuid": self.report_uuid,
            "name": self.name,
            "generation_mode": self.generation_mode,
            "relative_path": self.relative_path,
            "validity_start": str(self.validity_start).replace(" ", "T"),
            "validity_stop": str(self.validity_stop).replace(" ", "T"),
            "triggering_time": str(self.triggering_time).replace(" ", "T"),
            "generation_start": str(self.generation_start).replace(" ", "T"),
            "generation_stop": str(self.generation_stop).replace(" ", "T"),
            "metadata_ingestion_duration": str(self.metadata_ingestion_duration),
            "generated": self.generated,
            "compressed": self.compressed,
            "generator": self.generator,
            "generator_version": self.generator_version,
            "generation_error": self.generation_error,
            "report_group_uuid": self.report_group_uuid
        }

    def get_metadata_ingestion_progress(self):

        metadata_ingestion_progress = 0
        if self.metadata_ingestion_progress:
            metadata_ingestion_progress = self.metadata_ingestion_progress
        # end if
        
        return metadata_ingestion_progress

    def get_generation_progress(self):

        generation_progress = 0
        if self.generation_progress:
            generation_progress = self.generation_progress
        # end if
        
        return generation_progress

    def get_general_progress(self):

        metadata_ingestion_progress = 0
        if self.metadata_ingestion_progress:
            metadata_ingestion_progress = self.metadata_ingestion_progress
        # end if
        generation_progress = 0
        if self.generation_progress:
            generation_progress = self.generation_progress
        # end if
        
        return (metadata_ingestion_progress + generation_progress) / 2

    def get_generation_duration(self):

        generation_duration = None
        if self.generation_start and self.generation_stop:
            generation_duration = self.generation_stop - self.generation_start
        # end if
        
        return generation_duration

class ReportStatus(Base):
    __tablename__ = 'report_statuses'

    report_status_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    time_stamp = Column(DateTime)
    status = Column(Integer)
    log = Column(Text)
    report_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('reports.report_uuid'))
    report = relationship("Report", backref="statuses")

    def __init__(self, report_status_uuid, time_stamp, status, report, log = None):
        self.report_status_uuid = report_status_uuid
        self.time_stamp = time_stamp
        self.status = status
        self.log = log
        self.report = report

    def jsonify(self):
        return {
            "report_uuid": self.report.report_uuid,
            "time_stamp": str(self.time_stamp).replace(" ", "T"),
            "log": self.log,
            "status": self.status,
        }

    
class ReportGroup(Base):
    __tablename__ = 'report_groups'

    report_group_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    name = Column(Text)
    description = Column(Text)

    def __init__(self, report_group_uuid, name, description = None):
        self.report_group_uuid = report_group_uuid
        self.name = name
        self.description = description

    def jsonify(self):
        return {
            "report_group_uuid": self.report_group_uuid,
            "name": self.name,
            "description": self.description
        }
    
class ReportBoolean(Base):
    __tablename__ = 'report_booleans'

    name = Column(Text)
    value = Column(Boolean)
    position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    report_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('reports.report_uuid'))
    report = relationship("Report", backref="reportBooleans")
    __mapper_args__ = {
        'primary_key':[name, position, parent_level, parent_position, report_uuid]
    }

    def __init__(self, name, value, position, parent_level, parent_position, report):
        self.name = name
        self.value = value
        self.position = position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.report = report

    def jsonify(self):
        return {
            "type": "boolean",
            "name": self.name,
            "value": self.value,
            "position": self.position,
            "parent_level": self.parent_level,
            "parent_position": self.parent_position,
        }

class ReportText(Base):
    __tablename__ = 'report_texts'

    name = Column(Text)
    value = Column(Text)
    position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    report_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('reports.report_uuid'))
    report = relationship("Report", backref="reportTexts")
    __mapper_args__ = {
        'primary_key':[name, position, parent_level, parent_position, report_uuid]
    }

    def __init__(self, name, value, position, parent_level, parent_position, report):
        self.name = name
        self.value = value
        self.position = position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.report = report

    def jsonify(self):
        return {
            "type": "text",
            "name": self.name,
            "value": self.value,
            "position": self.position,
            "parent_level": self.parent_level,
            "parent_position": self.parent_position,
        }

class ReportDouble(Base):
    __tablename__ = 'report_doubles'

    name = Column(Text)
    value = Column(Float)
    position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    report_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('reports.report_uuid'))
    report = relationship("Report", backref="reportDoubles")
    __mapper_args__ = {
        'primary_key':[name, position, parent_level, parent_position, report_uuid]
    }

    def __init__(self, name, value, position, parent_level, parent_position, report):
        self.name = name
        self.value = value
        self.position = position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.report = report

    def jsonify(self):
        return {
            "type": "double",
            "name": self.name,
            "value": self.value,
            "position": self.position,
            "parent_level": self.parent_level,
            "parent_position": self.parent_position,
        }

class ReportTimestamp(Base):
    __tablename__ = 'report_timestamps'

    name = Column(Text)
    value = Column(DateTime)
    position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    report_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('reports.report_uuid'))
    report = relationship("Report", backref="reportTimestamps")
    __mapper_args__ = {
        'primary_key':[name, position, parent_level, parent_position, report_uuid]
    }

    def __init__(self, name, value, position, parent_level, parent_position, report):
        self.name = name
        self.value = value
        self.position = position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.report = report

    def jsonify(self):
        return {
            "type": "timestamp",
            "name": self.name,
            "value": self.value.isoformat(),
            "position": self.position,
            "parent_level": self.parent_level,
            "parent_position": self.parent_position,
        }

class ReportObject(Base):
    __tablename__ = 'report_objects'

    name = Column(Text)
    position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    report_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('reports.report_uuid'))
    report = relationship("Report", backref="reportObjects")
    __mapper_args__ = {
        'primary_key':[name, position, parent_level, parent_position, report_uuid]
    }

    def __init__(self, name, position, parent_level, parent_position, report):
        self.name = name
        self.position = position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.report = report

    def jsonify(self):
        return {
            "type": "object",
            "name": self.name,
            "value": "",
            "position": self.position,
            "parent_level": self.parent_level,
            "parent_position": self.parent_position,
        }

class ReportGeometry(Base):
    __tablename__ = 'report_geometries'

    name = Column(Text)
    value = Column(Geometry('POLYGON'))
    position = Column(Integer)
    parent_level = Column(Integer)
    parent_position = Column(Integer)
    report_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('reports.report_uuid'))
    report = relationship("Report", backref="reportGeometries")
    __mapper_args__ = {
        'primary_key':[name, position, parent_level, parent_position, report_uuid]
    }

    def __init__(self, name, value, position, parent_level, parent_position, report):
        self.name = name
        self.value = value
        self.position = position
        self.parent_level = parent_level
        self.parent_position = parent_position
        self.report = report

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
