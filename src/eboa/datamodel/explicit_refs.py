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
    __tablename__ = 'explicit_refs'

    explicit_ref_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    ingestion_time = Column(DateTime)
    explicit_ref = Column(Text)
    expl_ref_cnf_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('explicit_ref_cnfs.expl_ref_cnf_uuid'))
    group = relationship("ExplicitRefGrp", backref="explicitRefs")

    def __init__(self, explicit_ref_uuid, ingestionTime, explicitRef, group = None):
        self.explicit_ref_uuid = explicit_ref_uuid
        self.ingestion_time = ingestionTime
        self.explicit_ref = explicitRef
        self.group = group

    def jsonify(self):
        return {
            "explicit_ref_uuid": self.explicit_ref_uuid,
            "ingestion_time": self.ingestion_time,
            "explicit_ref": self.explicit_ref,
            "expl_ref_cnf_uuid": self.expl_ref_cnf_uuid
        }

class ExplicitRefGrp(Base):
    __tablename__ = 'explicit_ref_cnfs'

    expl_ref_cnf_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    name = Column(Text)

    def __init__(self, expl_ref_cnf_uuid, name):
        self.expl_ref_cnf_uuid = expl_ref_cnf_uuid
        self.name = name

    def jsonify(self):
        return {
            "expl_ref_cnf_uuid": self.expl_ref_cnf_uuid,
            "name": self.name
        }

class ExplicitRefLink(Base):
    __tablename__ = 'explicit_ref_links'

    explicit_ref_uuid_link = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    name = Column(Text)
    explicit_ref_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('explicit_refs.explicit_ref_uuid'))
    explicitRef = relationship("ExplicitRef", backref="explicitRefLinks")
    __mapper_args__ = {
        'primary_key':[explicit_ref_uuid_link, name, explicit_ref_uuid]
    }

    def __init__(self, link, name, explicitRef):
        self.explicit_ref_uuid_link = link
        self.name = name
        self.explicitRef = explicitRef
