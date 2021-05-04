"""
Explicit references data model definition

Written by DEIMOS Space S.L. (dibb)

module eboa
"""

from sqlalchemy import Column, Table, ForeignKey, Text, DateTime, Boolean
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

    def jsonify(self, include_annotations = True):
        """
        Method to obtain the structure of explicit references in a python dictionary format

        :param include_annotations: flag to indicate if the detail of the annotations has to be included
        :type include_annotations: boolean

        :return: structure of the explicit reference
        :rtype: dict
        """

        structure = {
            "explicit_ref_uuid": str(self.explicit_ref_uuid),
            "ingestion_time": self.ingestion_time.isoformat(),
            "explicit_ref": self.explicit_ref,
            "alerts": [alert.jsonify() for alert in self.alerts]
        }

        # Insert explicit reference group
        if self.group:
            structure["group"] = self.group.name
        # end if

        if include_annotations:
            
            structure["annotations"] = {}
            for annotation in self.annotations:
                name = annotation.annotationCnf.name
                system = annotation.annotationCnf.system
                if name not in structure["annotations"]:
                    structure["annotations"][name] = []
                # end if
                structure["annotations"][name].append({
                    "annotation_uuid": str(annotation.annotation_uuid),
                    "name": name,
                    "system": system
                })
            # end for
        # end if
            
        return structure

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
