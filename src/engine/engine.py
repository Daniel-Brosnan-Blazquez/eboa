"""
Engine definition

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
import os
import sys

# Adding path to the datamodel package
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import SQLalchemy exceptions
from sqlalchemy.exc import IntegrityError

from datamodel.base import Session, engine, Base
from datamodel.dim_signatures import DimSignature
from datamodel.events import Event, EventLink, EventText, EventDouble, EventObject, EventGeometry
from datamodel.gauges import Gauge
from datamodel.dim_processings import DimProcessing, DimProcessingStatus
from datamodel.explicit_refs import ExplicitRef, ExplicitRefGrp, ExplicitRefLink
from datamodel.annotations import Annotation, AnnotationCnf, AnnotationText, AnnotationDouble, AnnotationObject, AnnotationGeometry
import datetime
import uuid
import pprint
from math import pi
import random
from lxml import etree
import datetime

class Engine():

    session = Session()
    data = None
    dim_signature = None
    source = None
    gauges = {}
    annotation_cnfs = {}
    expl_groups = {}
    explicit_refs = {}
    
    def insert_data(self, xml):
        """
        
        """
        # Parse data from the xml file
        self.data = etree.XPathEvaluator(etree.parse(xml))
        
        # Insert the DIM signature
        self._insert_dim_signature()

        # Insert the source
        try:
            self._insert_source()
        except:
            # Log that the source file has been already been processed
            print("The DIM Processing was already ingested")
            self.session.rollback()
            self.session.close()
            return
        # end try

        # Insert gauges
        self._insert_gauges()

        # Insert annotation configuration
        self._insert_annotation_cnfs()

        # Insert explicit reference groups
        self._insert_expl_groups()

        # Insert links between explicit references
        self._insert_explicit_refs()

        # Insert annotations
        self._insert_links_explicit_refs()

        # Insert events
        self._insert_events()

        # Insert annotations
        self._insert_annotations()

        self.session.commit()
        self.session.close()
        return

    def _insert_dim_signature(self):
        """
        """
        self.session.begin_nested()
        dim_signature = self.data("/gsd/dim_signature").pop()
        dim_name = dim_signature.get("name")
        exec_name = dim_signature.get("exec")
        self.session.add (DimSignature(dim_name, exec_name))
        try:
            self.session.commit()
        except IntegrityError:
            # The DIM signature was already available. Roll back
            # transaction for re-using the session
            self.session.rollback()
            pass
        # end try

        # Get the stored DIM signature
        self.dim_signature = self.session.query(DimSignature).filter(DimSignature.dim_signature == dim_name, DimSignature.dim_exec_name == exec_name).first()
        return
        
    def _insert_source(self):
        """
        """
        self.session.begin_nested()
        exec_version = self.data("/gsd/dim_signature/@version").pop()
        source = self.data("/gsd/source").pop()
        name = source.get("name")
        generation_time = source.get("generation_time")
        validity_start = source.get("validity_start")
        validity_stop = source.get("validity_stop")
        id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        self.source = DimProcessing(id, name, validity_start,
                                      validity_stop, generation_time, datetime.datetime.now(),
                                      exec_version, self.dim_signature)
        self.session.add (self.source)
        try:
            self.session.commit()
        except IntegrityError:
            # The DIM processing was already ingested
            self.session.rollback()
            raise Exception("The data associated to the source with name {} associated to the DIM signature {} and DIM processing {} with version {} has been already ingested".format (name, self.dim_signature.dim_signature, self.dim_signature.dim_exec_name, exec_version))
        # end try

        return
        
    def _insert_gauges(self):
        """
        """
        gauges = self.data("/gsd/data/event/gauge")
        for gauge in gauges:
            self.session.begin_nested()
            name = gauge.get("name")
            system = gauge.get("system")
            gauge_ddbb = Gauge(name, self.dim_signature, system)
            self.session.add (gauge_ddbb)
            try:
                self.session.commit()
            except IntegrityError:
                # The gauge already exists into DDBB
                self.session.rollback()
                gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == name, Gauge.system == system, Gauge.dim_signature_id == self.dim_signature.dim_signature_id).first()
                pass
            # end try
            self.gauges[(name,system)] = gauge_ddbb
        # end for

        return
        
    def _insert_annotation_cnfs(self):
        """
        """
        annotation_cnfs = self.data("/gsd/data/annotation/annotation_cnf")
        for annotation_cnf in annotation_cnfs:
            self.session.begin_nested()
            name = annotation_cnf.get("name")
            system = annotation_cnf.get("system")
            annotation_cnf_ddbb = AnnotationCnf(name, self.dim_signature, system)
            self.session.add (annotation_cnf_ddbb)
            try:
                self.session.commit()
            except IntegrityError:
                # The annotation already exists into DDBB
                self.session.rollback()
                annotation_cnf_ddbb = self.session.query(AnnotationCnf).filter(AnnotationCnf.name == name, AnnotationCnf.system == system, AnnotationCnf.dim_signature_id == self.dim_signature.dim_signature_id).first()
                pass
            # end try
            self.annotation_cnfs[(name,system)] = annotation_cnf_ddbb
        # end for

        return
        
    def _insert_expl_groups(self):
        """
        """
        expl_groups = self.data("/gsd/data/explicit_reference/@group")
        for expl_group in expl_groups:
            self.session.begin_nested()
            
            expl_group_ddbb = ExplicitRefGrp(expl_group)
            self.session.add (expl_group_ddbb)
            try:
                self.session.commit()
            except IntegrityError:
                # The explicit reference group has been inserted already by other process while checking
                self.session.rollback()
                expl_group_ddbb = self.session.query(ExplicitRefGrp).filter(ExplicitRefGrp.name == expl_group).first()
                pass
            # end try
            self.expl_groups[expl_group] = expl_group_ddbb
        # end for

        return
        
    def _insert_explicit_refs(self):
        """
        """
        explicit_refs = self.data("/gsd/data/event/@explicit_reference")
        data_node = self.data("/gsd/data").pop()
        for explicit_ref in explicit_refs:
            self.session.begin_nested()
            group = None
            explicit_ref_grp = None
            group_list = data_node.xpath("explicit_reference[@name = $name]/@group", name = explicit_ref)
            if len(group_list) == 1:
                group = group_list.pop()
                explicit_ref_grp = self.expl_groups[group]
            explicit_ref_ddbb = ExplicitRef(datetime.datetime.now(), explicit_ref, explicit_ref_grp)
            self.session.add (explicit_ref_ddbb)
            try:
                self.session.commit()
            except IntegrityError:
                # The gauge has been inserted already by other process while checking
                self.session.rollback()
                explicit_ref_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == explicit_ref).first()
                pass
            # end try
            self.explicit_refs[explicit_ref] = explicit_ref_ddbb
        # end for

        return
        
    def _insert_links_explicit_refs(self):
        """
        """
        list_explicit_reference_links = []
        explicit_refs_with_links = self.data("/gsd/data").pop().xpath("explicit_reference[boolean(links)]")
        for explicit_ref in explicit_refs_with_links:
            links = explicit_ref.xpath("links/link")
            for link in links:
                explicit_ref_id1 = self.explicit_refs[explicit_ref.get("name")].explicit_ref_id
                explicit_ref_id2 = self.explicit_refs[link.text].explicit_ref_id
                list_explicit_reference_links.append(dict(explicit_ref_id_link = explicit_ref_id1,
                                                          name = link.get("name"), 
                                                          explicit_ref_id = explicit_ref_id2))
                # Insert the back ref if specified
                if bool(link.get("back_ref")):
                    list_explicit_reference_links.append(dict(explicit_ref_id_link = explicit_ref_id2,
                                                              name = link.get("name"), 
                                                              explicit_ref_id = explicit_ref_id1))
                # end if
            # end for
        # end for

        # Bulk insert links between explicit_references
        self.session.bulk_insert_mappings(ExplicitRefLink, list_explicit_reference_links)
        self.session.commit()

        return

    def _insert_events(self):
        """
        """
        self.session.begin_nested()
        list_events = []
        events = self.data("/gsd/data/event")
        for event in events:
            id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
            start = event.get("start")
            stop = event.get("stop")
            generation_time = event.get("generation_time")
            gauge_info = event.xpath("gauge").pop()
            gauge = self.gauges[(gauge_info.get("name"), gauge_info.get("system"))]
            explicit_ref = self.explicit_refs[event.get("explicit_reference")]

            # Insert the event into the list for bulk ingestion
            list_events.append(dict(event_uuid = id, start = start,
                                    stop = stop, generation_time = generation_time,
                                    ingestion_time = datetime.datetime.now(),
                                    gauge_id = gauge.gauge_id,
                                    explicit_ref_id = explicit_ref.explicit_ref_id,
                                    processing_uuid = self.source.processing_uuid))
            # end for
            
        # Bulk insert events
        self.session.bulk_insert_mappings(Event, list_events)
        self.session.commit()

        return
        
    def _insert_annotations(self):
        """
        """
        self.session.begin_nested()
        list_annotations = []
        annotations = self.data("/gsd/data/annotation")
        for annotation in annotations:
            id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
            generation_time = annotation.get("generation_time")
            annotation_cnf_info = annotation.xpath("annotation_cnf").pop()
            annotation_cnf = self.annotation_cnfs[(annotation_cnf_info.get("name"), annotation_cnf_info.get("system"))]
            explicit_ref = self.explicit_refs[annotation.get("explicit_reference")]

            # Insert the annotation into the list for bulk ingestion
            list_annotations.append (dict (annotation_uuid = id, generation_time = generation_time,
                                     ingestion_time = datetime.datetime.now(),
                                     annotation_cnf_id = annotation_cnf.annotation_cnf_id,
                                     explicit_ref_id = explicit_ref.explicit_ref_id,
                                     processing_uuid = self.source.processing_uuid))
        # end for
            
        # Bulk insert annotations
        self.session.bulk_insert_mappings(Annotation, list_annotations)
        self.session.commit()

        return
