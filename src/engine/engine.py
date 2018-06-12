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

    data = {}
    operation = None

    def parse_data_from_xml (self, xml):
        """
        """
        # Parse data from the xml file
        parsed_xml = etree.XPathEvaluator(etree.parse(xml))

        # Pass schema
        
        self.data["operations"] = []
        for operation in parsed_xml("/gsd/operation"):
            if operation.get("mode") == "insert":
                self._parse_insert_operation_from_xml(operation)
            # end if
        # end for

        return 

    def _parse_insert_operation_from_xml(self, operation):
        """
        """
        data = {}
        data["mode"] = "insert"
        # Extract dim_signature
        data["dim_signature"] = {"name": operation.xpath("dim_signature")[0].get("name"),
                                 "version": operation.xpath("dim_signature")[0].get("version"),
                                 "exec": operation.xpath("dim_signature")[0].get("exec")}
        # end if

        # Extract source
        if len (operation.xpath("source")) == 1:
            data["source"] = {"name": operation.xpath("source")[0].get("name"),
                              "generation_time": operation.xpath("source")[0].get("generation_time"),
                              "validity_start": operation.xpath("source")[0].get("validity_start"),
                              "validity_stop": operation.xpath("source")[0].get("validity_stop")} 
        # end if

        # Extract explicit_references
        if len (operation.xpath("data/explicit_reference")) > 0:
            data["explicit_references"] = []
            for explicit_ref in operation.xpath("data/explicit_reference"):
                explicit_reference = {}
                explicit_reference["name"] = explicit_ref.get("name")
                explicit_reference["group"] = explicit_ref.get("group")
                # Add links
                if len (explicit_ref.xpath("links/link")) > 0:
                    links = []
                    for link in explicit_ref.xpath("links/link"):
                        link_info = {}
                        link_info["name"] = link.get("name")
                        link_info["link"] = link.text
                        if link.get("back_ref"):
                            link_info["back_ref"] = link.get("back_ref")
                        # end if
                        links.append(link_info)
                    # end for
                    explicit_reference["links"] = links
                # end if
                data["explicit_references"].append(explicit_reference)
            # end for
        # end if

        # Extract events
        if len (operation.xpath("data/event")) > 0:
            data["events"] = []
            for event in operation.xpath("data/event"):
                event_info = {}
                event_info["start"] = event.get("start")
                event_info["stop"] = event.get("stop")
                event_info["generation_time"] = event.get("generation_time")
                if event.get("key"):
                    event_info["key"] = event.get("key")
                # end if
                if event.get("explicit_reference"):
                    event_info["explicit_reference"] = event.get("explicit_reference")
                # end if
                event_info["gauge"] = {"name": event.xpath("gauge")[0].get("name"),
                                       "system": event.xpath("gauge")[0].get("system"),
                                       "insertion_type": event.xpath("gauge")[0].get("insertion_type")}
                # end if
                # Add links
                if len (event.xpath("links/link")) > 0:
                    links = []
                    for link in explicit_ref.xpath("links/link"):
                        links.append({"name": link.get("name"),
                                      "link": link.text})
                    # end for
                    event_info["links"] = links
                # end if

                # Add values
                ### PENDING (recursive method)

                data["events"].append(event_info)
            # end for
        # end if


        # Extract annotations
        if len (operation.xpath("data/annotation")) > 0:
            data["annotations"] = []
            for annotation in operation.xpath("data/annotation"):
                annotation_info = {}
                annotation_info["generation_time"] = annotation.get("generation_time")
                annotation_info["explicit_reference"] = annotation.get("explicit_reference")
                annotation_info["annotation_cnf"] = {"name": annotation.xpath("annotation_cnf")[0].get("name"),
                                                     "system": annotation.xpath("annotation_cnf")[0].get("system")}
                # end if
                # Add values
                ### PENDING (recursive method)

                data["annotations"].append(annotation_info)
            # end for
        # end if

        self.data["operations"].append(data)
        return

    def treat_data(self):
        """
        
        """
        # Pass schema
    
        for self.operation in self.data.get("operations"):
            # Open session
            self.session = Session()
            if self.operation.get("mode") == "insert":
                self._insert_data()
            # end if

            # Commit data and close connection
            self.session.commit()
            self.session.close()
        # end for
        return

    def _insert_data(self):
        """
        
        """
        # Initialize context
        self.dim_signature = None
        self.source = None
        self.gauges = {}
        self.annotation_cnfs = {}
        self.expl_groups = {}
        self.explicit_refs = {}

        # Insert the DIM signature
        self._insert_dim_signature()

        # Insert the source if exists
        if "source" in self.operation:
            try:
                self._insert_source()
            except:
                # Log that the source file has been already been processed
                print("The DIM Processing was already ingested")
                self.session.rollback()
                self.session.close()
                return
            # end try
        # end if

        # Insert gauges
        self._insert_gauges()

        # Insert annotation configuration
        self._insert_annotation_cnfs()
        
        # Insert explicit reference groups
        self._insert_expl_groups()

        # Insert links between explicit references
        self._insert_explicit_refs()

        # Insert explicit references
        self._insert_links_explicit_refs()

        # Insert events
        self._insert_events()

        # Insert annotations
        self._insert_annotations()

        return

    def _insert_dim_signature(self):
        """
        """
        self.session.begin_nested()
        dim_signature = self.operation.get("dim_signature")
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
        exec_version = self.operation.get("dim_signature").get("version")
        source = self.operation.get("source")
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
        for event in self.operation.get("events"):
            gauge = event.get("gauge")
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
        for annotation in self.operation.get("annotations"):
            self.session.begin_nested()
            annotation_cnf = annotation.get("annotation_cnf")
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
        for explicit_ref in self.operation.get("explicit_references"):
            if "group" in explicit_ref:
                self.session.begin_nested()
                expl_group_ddbb = ExplicitRefGrp(explicit_ref.get("group"))
                self.session.add (expl_group_ddbb)
                try:
                    self.session.commit()
                except IntegrityError:
                    # The explicit reference group has been inserted already by other process while checking
                    self.session.rollback()
                    expl_group_ddbb = self.session.query(ExplicitRefGrp).filter(ExplicitRefGrp.name == expl_group).first()
                    pass
                # end try
                self.expl_groups[explicit_ref.get("group")] = expl_group_ddbb
            # end if
        # end for

        return
        
    def _insert_explicit_refs(self):
        """
        """
        events_explicit_refs = [event.get("explicit_reference") for event in self.operation.get("events")]
        annotations_explicit_refs = [annotation.get("explicit_reference") for annotation in self.operation.get("annotations")]
        explicit_references = set(events_explicit_refs + annotations_explicit_refs)
        for explicit_ref in explicit_references:
            self.session.begin_nested()
            explicit_ref_grp = None
            # Get associated group if exists from the declared explicit references
            declared_explicit_reference = next(iter(list(filter(lambda i: i.get("name") == explicit_ref, self.operation.get("explicit_references")))), None)
            if declared_explicit_reference:
                explicit_ref_grp = self.expl_groups.get(declared_explicit_reference.get("group"))
            # end if
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
        for explicit_ref in list(filter(lambda i: i.get("links"), self.operation.get("explicit_references"))):
            for link in explicit_ref.get("links"):
                explicit_ref_id1 = self.explicit_refs[explicit_ref.get("name")].explicit_ref_id
                explicit_ref_id2 = self.explicit_refs[link.get("link")].explicit_ref_id
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
        for event in self.operation.get("events"):
            id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
            start = event.get("start")
            stop = event.get("stop")
            generation_time = event.get("generation_time")
            gauge_info = event.get("gauge")
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
        for annotation in self.operation.get("annotations"):
            id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
            generation_time = annotation.get("generation_time")
            annotation_cnf_info = annotation.get("annotation_cnf")
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
