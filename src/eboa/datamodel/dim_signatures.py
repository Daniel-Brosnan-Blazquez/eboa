"""
DIM Signatures data model definition

Written by DEIMOS Space S.L. (dibb)

module eboa
"""

from sqlalchemy import Column, Table, ForeignKey, Text
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship

from eboa.datamodel.base import Base

class DimSignature(Base):
    __tablename__ = 'dim_signatures'

    dim_signature_uuid = Column(postgresql.UUID(as_uuid=True), primary_key=True)
    dim_signature = Column(Text)

    def __init__(self, dim_signature_uuid, dim_signature):
        self.dim_signature_uuid = dim_signature_uuid
        self.dim_signature = dim_signature

    def jsonify(self):
        return {
            "dim_signature_uuid": self.dim_signature_uuid,
            "dim_signature": self.dim_signature
        }
