"""
Alert data model definition

Written by DEIMOS Space S.L. (dibb)

module eboa
"""

from sqlalchemy import Column, Integer, Table, ForeignKey, Text, DateTime, Float, Interval, JSON, Numeric, Boolean
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship

from eboa.datamodel.base import Base

class ReportAlert(Base):
    __tablename__ = 'report_alerts'

    report_alert_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    message = Column(Text)
    validated = Column(Boolean)
    ingestion_time = Column(DateTime)
    generator = Column(Text)
    notified = Column(Boolean)
    solved = Column(Boolean)
    solved_time = Column(DateTime)
    notification_time = Column(DateTime)
    alert_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('alerts.alert_uuid'))
    report_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('reports.report_uuid'))
    report = relationship("Report", backref="alerts")
    alertDefinition = relationship("Alert", backref="reportAlerts")
    
    def __init__(self, report_alert_uuid, message, ingestion_time, generator, notification_time, report_uuid, alert_uuid):
        self.report_alert_uuid = report_alert_uuid
        self.message = message
        self.ingestion_time = ingestion_time
        self.generator = generator
        self.notification_time = notification_time
        self.report_uuid = report_uuid
        self.alert_uuid = alert_uuid
