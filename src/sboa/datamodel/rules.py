"""
Scheduler data model definition

Written by DEIMOS Space S.L. (dibb)

module sboa
"""

from sqlalchemy import Column, Table, ForeignKey, Text, DateTime, Float, Boolean
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship

from sboa.datamodel.base import Base

class Rule(Base):
    __tablename__ = 'rules'

    rule_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    name = Column(Text)
    periodicity = Column(Float)
    window_delay = Column(Float)
    window_size = Column(Float)    

    def __init__(self, rule_uuid, name, periodicity, window_delay, window_size):
        self.rule_uuid = report_uuid
        self.name = name
        self.periodicity = periodicity
        self.window_delay = window_delay
        self.window_size = window_size

    def jsonify(self):
        return {
            "rule_uuid": self.rule_uuid,
            "name": self.name,
            "periodicity": self.periodicity,
            "window_delay": self.window_delay,
            "window_size": self.window_size
        }

class Task(Base):
    __tablename__ = 'tasks'

    task_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    name = Column(Text)
    command = Column(Text)
    triggering_time = Column(DateTime)
    add_window_arguments = Column(Boolean)
    rule_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('rules.rule_uuid'))
    rule = relationship("Rule", backref="tasks")

    def __init__(self, task_uuid, name, command, triggering_time, add_window_arguments, rule_uuid):
        self.task_uuid = task_uuid
        self.name = name
        self.command = command
        self.triggering_time = triggering_time
        self.add_window_arguments = add_window_arguments
        self.rule_uuid = rule_uuid

    def jsonify(self):
        return {
            "task_uuid": self.task_uuid,
            "name": self.name,
            "command": self.command,
            "triggering_time": str(self.triggering_time).replace(" ", "T"),
            "triggering_time": self.add_window_arguments,
            "rule_uuid": self.rule.rule_uuid
        }

class Triggering(Base):
    __tablename__ = 'triggerings'

    triggering_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    date = Column(DateTime)
    triggered = Column(Boolean)
    task_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('tasks.task_uuid'))
    task = relationship("Task", backref="triggerings")

    def __init__(self, triggering_uuid, date, triggered, task_uuid):
        self.triggering_uuid = triggering_uuid
        self.date = date
        self.triggered = triggered
        self.task_uuid = task_uuid

    def jsonify(self):
        return {
            "triggering_uuid": self.triggering_uuid,
            "date": str(self.date).replace(" ", "T"),
            "triggered": self.triggered,
            "task_uuid": self.task.task_uuid
        }
