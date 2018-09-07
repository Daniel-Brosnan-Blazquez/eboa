"""
Gauges data model definition

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""

from sqlalchemy import Column, Table, ForeignKey, Text
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship

from gsdm.datamodel.base import Base

class Gauge(Base):
    __tablename__ = 'gauge_cnf_tb'

    gauge_id = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    system = Column(Text)
    name = Column(Text)
    dim_signature_id = Column(postgresql.UUID(as_uuid=True), ForeignKey('dim_signature_tb.dim_signature_id'))
    dim_signature = relationship("DimSignature", backref="gauges")
    
    def __init__(self, gauge_id, name, dim_signature, system = None):
        self.gauge_id = gauge_id
        self.system = system
        self.name = name
        self.dim_signature = dim_signature

