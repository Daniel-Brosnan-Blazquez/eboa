"""
DIM Signatures data model definition

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""

from sqlalchemy import Column, Table, ForeignKey, Text
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship

from gsdm.datamodel.base import Base

class DimSignature(Base):
    __tablename__ = 'dim_signature_tb'

    dim_signature_id = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    dim_signature = Column(Text)
    dim_exec_name = Column(Text)

    def __init__(self, dim_signature_id, dim_signature, dim_exec_name):
        self.dim_signature_id = dim_signature_id
        self.dim_signature = dim_signature
        self.dim_exec_name = dim_exec_name

