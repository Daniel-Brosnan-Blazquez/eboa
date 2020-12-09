"""
Alert data model definition

Written by DEIMOS Space S.L. (dibb)

module eboa
"""

from sqlalchemy import Column, Integer, Table, ForeignKey, Text, DateTime, Float, Interval, JSON, Numeric, Boolean
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship

from eboa.datamodel.base import Base

class Alert(Base):
    __tablename__ = 'alerts'

    alert_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    name = Column(Text)
    severity = Column(Integer)
    description = Column(Text)
    alert_group_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('alert_groups.alert_group_uuid'))
    group = relationship("AlertGroup", backref="alerts")
    
    def __init__(self, alert_uuid, name, severity, group, description = None):
        self.alert_uuid = alert_uuid
        self.name = name
        self.severity = severity
        self.description = description
        self.group = group

class AlertGroup(Base):
    __tablename__ = 'alert_groups'

    alert_group_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    name = Column(Text)

    def __init__(self, alert_group_uuid, name):
        self.alert_group_uuid = alert_group_uuid
        self.name = name

class EventAlert(Base):
    __tablename__ = 'event_alerts'

    event_alert_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    message = Column(Text)
    validated = Column(Boolean)
    ingestion_time = Column(DateTime)
    generator = Column(Text)
    notified = Column(Boolean)
    solved = Column(Boolean)
    solved_time = Column(DateTime)
    notification_time = Column(DateTime)
    justification = Column(Text)
    alert_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('alerts.alert_uuid'))
    event_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('events.event_uuid'))
    event = relationship("Event", backref="alerts")
    alertDefinition = relationship("Alert", backref="eventAlerts")
    
    def __init__(self, event_alert_uuid, message, ingestion_time, generator, notification_time, event_uuid, alert_uuid):
        self.event_alert_uuid = event_alert_uuid
        self.message = message
        self.ingestion_time = ingestion_time
        self.generator = generator
        self.notification_time = notification_time
        self.event_uuid = event_uuid
        self.alert_uuid = alert_uuid

class AnnotationAlert(Base):
    __tablename__ = 'annotation_alerts'

    annotation_alert_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    message = Column(Text)
    validated = Column(Boolean)
    ingestion_time = Column(DateTime)
    generator = Column(Text)
    notified = Column(Boolean)
    solved = Column(Boolean)
    solved_time = Column(DateTime)
    notification_time = Column(DateTime)
    justification = Column(Text)
    alert_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('alerts.alert_uuid'))
    annotation_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('annotations.annotation_uuid'))
    annotation = relationship("Annotation", backref="alerts")
    alertDefinition = relationship("Alert", backref="annotationAlerts")
    
    def __init__(self, annotation_alert_uuid, message, ingestion_time, generator, notification_time, annotation_uuid, alert_uuid):
        self.annotation_alert_uuid = annotation_alert_uuid
        self.message = message
        self.ingestion_time = ingestion_time
        self.generator = generator
        self.notification_time = notification_time
        self.annotation_uuid = annotation_uuid
        self.alert_uuid = alert_uuid

class SourceAlert(Base):
    __tablename__ = 'source_alerts'

    source_alert_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    message = Column(Text)
    validated = Column(Boolean)
    ingestion_time = Column(DateTime)
    generator = Column(Text)
    notified = Column(Boolean)
    solved = Column(Boolean)
    solved_time = Column(DateTime)
    notification_time = Column(DateTime)
    justification = Column(Text)
    alert_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('alerts.alert_uuid'))
    source_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('sources.source_uuid'))
    source = relationship("Source", backref="alerts")
    alertDefinition = relationship("Alert", backref="sourceAlerts")
    
    def __init__(self, source_alert_uuid, message, ingestion_time, generator, notification_time, source_uuid, alert_uuid):
        self.source_alert_uuid = source_alert_uuid
        self.message = message
        self.ingestion_time = ingestion_time
        self.generator = generator
        self.notification_time = notification_time
        self.source_uuid = source_uuid
        self.alert_uuid = alert_uuid

class ExplicitRefAlert(Base):
    __tablename__ = 'explicit_ref_alerts'

    explicit_ref_alert_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    message = Column(Text)
    validated = Column(Boolean)
    ingestion_time = Column(DateTime)
    generator = Column(Text)
    notified = Column(Boolean)
    solved = Column(Boolean)
    solved_time = Column(DateTime)
    notification_time = Column(DateTime)
    justification = Column(Text)
    alert_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('alerts.alert_uuid'))
    explicit_ref_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('explicit_refs.explicit_ref_uuid'))
    explicitRef = relationship("ExplicitRef", backref="alerts")
    alertDefinition = relationship("Alert", backref="explicitRefAlerts")
    
    def __init__(self, explicit_ref_alert_uuid, message, ingestion_time, generator, notification_time, explicit_ref_uuid, alert_uuid):
        self.explicit_ref_alert_uuid = explicit_ref_alert_uuid
        self.message = message
        self.ingestion_time = ingestion_time
        self.generator = generator
        self.notification_time = notification_time
        self.explicit_ref_uuid = explicit_ref_uuid
        self.alert_uuid = alert_uuid
