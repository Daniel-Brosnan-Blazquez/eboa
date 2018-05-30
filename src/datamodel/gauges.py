"""
Gauges data model definition

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""

from sqlalchemy import Column, Integer, Table, ForeignKey, Text
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship

from .base import Base

class Gauge(Base):
    __tablename__ = 'gauge_cnf_tb'

    gauge_id = Column(Integer, primary_key=True)
    system = Column(Text)
    name = Column(Text)
    description = Column(Text,nullable = True)
    dim_signature_id = Column(Integer, ForeignKey('dim_signature_tb.dim_signature_id'))
    dim_signature = relationship("DimSignature", backref="gauges")
    
    def __init__(self, name, dimSignature, system = None, description = None):
        self.system = system
        self.name = name
        self.description = description
        self.dim_signature = dimSignature

