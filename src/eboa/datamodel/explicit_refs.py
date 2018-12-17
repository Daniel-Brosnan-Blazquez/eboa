"""
Explicit references data model definition

Written by DEIMOS Space S.L. (dibb)

module eboa
"""

from sqlalchemy import Column, Table, ForeignKey, Text, DateTime
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship

from eboa.datamodel.base import Base

class ExplicitRef(Base):
    __tablename__ = 'explicit_ref_tb'

    explicit_ref_id = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    ingestion_time = Column(DateTime)
    explicit_ref = Column(Text)
    expl_ref_cnf_id = Column(postgresql.UUID(as_uuid=True), ForeignKey('explicit_ref_cnf_tb.expl_ref_cnf_id'))
    group = relationship("ExplicitRefGrp", backref="explicitRefs")

    def __init__(self, explicit_ref_id, ingestionTime, explicitRef, group = None):
        self.explicit_ref_id = explicit_ref_id
        self.ingestion_time = ingestionTime
        self.explicit_ref = explicitRef
        self.group = group

    def jsonify(self):
        return {
            "explicit_ref_id": self.explicit_ref_id,
            "ingestion_time": self.ingestion_time,
            "explicit_ref": self.explicit_ref,
            "expl_ref_cnf_id": self.expl_ref_cnf_id
        }

class ExplicitRefGrp(Base):
    __tablename__ = 'explicit_ref_cnf_tb'

    expl_ref_cnf_id = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    name = Column(Text)

    def __init__(self, expl_ref_cnf_id, name):
        self.expl_ref_cnf_id = expl_ref_cnf_id
        self.name = name

class ExplicitRefLink(Base):
    __tablename__ = 'explicit_ref_link_tb'

    explicit_ref_id_link = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    name = Column(Text)
    explicit_ref_id = Column(postgresql.UUID(as_uuid=True), ForeignKey('explicit_ref_tb.explicit_ref_id'))
    explicitRef = relationship("ExplicitRef", backref="explicitRefLinks")
    __mapper_args__ = {
        'primary_key':[explicit_ref_id_link, name, explicit_ref_id]
    }

    def __init__(self, link, name, explicitRef):
        self.explicit_ref_id_link = link
        self.name = name
        self.explicitRef = explicitRef
