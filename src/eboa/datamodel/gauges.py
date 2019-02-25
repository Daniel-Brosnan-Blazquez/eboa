"""
Gauges data model definition

Written by DEIMOS Space S.L. (dibb)

module eboa
"""

from sqlalchemy import Column, Table, ForeignKey, Text
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship

from eboa.datamodel.base import Base

class Gauge(Base):
    __tablename__ = 'gauges'

    gauge_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    system = Column(Text)
    name = Column(Text)
    description = Column(Text)
    dim_signature_uuid = Column(postgresql.UUID(as_uuid=True), ForeignKey('dim_signatures.dim_signature_uuid'))
    dim_signature = relationship("DimSignature", backref="gauges")
    
    def __init__(self, gauge_uuid, name, dim_signature, system = None, description = None):
        self.gauge_uuid = gauge_uuid
        self.system = system
        self.name = name
        self.description = description
        self.dim_signature = dim_signature

    def jsonify(self):
        return {
            "gauge_uuid": self.gauge_uuid,
            "system": self.system,
            "name": self.name,
            "dim_signature_uuid": self.dim_signature_uuid
        }
