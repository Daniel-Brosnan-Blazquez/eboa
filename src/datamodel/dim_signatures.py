"""
DIM Signatures data model definition

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""

from sqlalchemy import Column, Integer, Table, ForeignKey, Text
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship

from .base import Base

class DimSignature(Base):
    __tablename__ = 'dim_signature_tb'

    dim_signature_id = Column(Integer, primary_key=True)
    dim_signature = Column(Text)
    dim_exec_name = Column(Text)

    def __init__(self, dimSignature, dimExecName):
        self.dim_signature = dimSignature
        self.dim_exec_name = dimExecName

