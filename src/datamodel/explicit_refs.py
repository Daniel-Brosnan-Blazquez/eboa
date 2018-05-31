"""
Explicit references data model definition

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""

from sqlalchemy import Column, Integer, Table, ForeignKey, Text, DateTime
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship

from .base import Base

class ExplicitRef(Base):
    __tablename__ = 'explicit_ref_tb'

    explicit_ref_id = Column(Integer, primary_key=True)
    ingestion_time = Column(DateTime)
    explicit_ref = Column(Text)
    expl_ref_cnf_id = Column(Integer, ForeignKey('explicit_ref_cnf_tb.expl_ref_cnf_id'))
    group = relationship("ExplicitRefGrp", backref="explicitRefs")

    def __init__(self, ingestionTime, explicitRef, group = None):
        self.ingestion_time = ingestionTime
        self.explicit_ref = explicitRef
        self.group = group

class ExplicitRefGrp(Base):
    __tablename__ = 'explicit_ref_cnf_tb'

    expl_ref_cnf_id = Column(Integer, primary_key=True)
    name = Column(Text)

    def __init__(self, name):
        self.name = name

class ExplicitRefLink(Base):
    __tablename__ = 'explicit_ref_links_tb'

    explicit_ref_id_link = Column(Integer, primary_key=True)
    name = Column(Text)
    explicit_ref_id = Column(Integer, ForeignKey('explicit_ref_tb.explicit_ref_id'))
    explicitRef = relationship("ExplicitRef", backref="explicitRefLinks")

    def __init__(self, link, name, explicitRef):
        self.explicit_ref_id_link = link
        self.name = name
        self.explicitRef = explicitRef
