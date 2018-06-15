"""
Engine definition

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
# Import python utilities
import datetime
import uuid
import random
import os
import sys
from dateutil import parser

# Adding path to the datamodel package
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import SQLalchemy exceptions
from sqlalchemy.exc import IntegrityError

# Import exceptions
from .errors import WrongEventLink, WrongPeriod, SourceAlreadyIngested

# Import datamodel
from datamodel.base import Session, engine, Base
from datamodel.dim_signatures import DimSignature
from datamodel.events import Event, EventLink, EventText, EventDouble, EventObject, EventGeometry, EventKey
from datamodel.gauges import Gauge
from datamodel.dim_processings import DimProcessing, DimProcessingStatus
from datamodel.explicit_refs import ExplicitRef, ExplicitRefGrp, ExplicitRefLink
from datamodel.annotations import Annotation, AnnotationCnf, AnnotationText, AnnotationDouble, AnnotationObject, AnnotationGeometry

# Import xml parser
from lxml import etree

class Engine():

    operation = None
    exit_codes = {
        "OK": {
            "status": 0,
            "message": "The source file {} associated to the DIM signature {} and DIM processing {} with version {} has been ingested correctly"
        },
        "INGESTION_STARTED": {
            "status": 1,
            "message": "The source file {} associated to the DIM signature {} and DIM processing {} with version {} is going to be ingested"
        },
        "SOURCE_ALREADY_INGESTED": {
            "status": 2,
            "message": "The source file {} associated to the DIM signature {} and DIM processing {} with version {} has been already ingested"
        },
        "WRONG_PERIOD_SOURCE": {
            "status": 3,
            "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} has a validity period which its stop ({}) is lower than its start ({})"
        },
        "WRONG_PERIOD_EVENT_SOURCE": {
            "status": 4,
            "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} contains an event with a stop value {} lower than its start value {}"
        },
        "WRONG_PERIOD_EVENT": {
            "message": "The event has a stop value {} lower than its start value {}"
        },
        "INCOMPLTE_LINKS_SOURCE": {
            "status": 5,
            "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} contains an event which defines the link id {} to other events that are not specified"
        },
        "INCOMPLTE_LINKS": {
            "message": "There is an event which defines the link id {} to other events that are not specified"
        }
    }

    def __init__(self, data = {}):
        """
        """
        self.data = data
    
        return

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
                    for link in event.xpath("links/link"):
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
    
        for self.operation in self.data.get("operations") or []:
            if self.operation.get("mode") == "insert":
                self._insert_data()
            # end if
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

        # Open session
        self.session = Session()

        # Apply insertion types effects
        self._apply_insertion_types()

        # Insert the DIM signature
        self._insert_dim_signature()

        # Insert the source if exists
        if "source" in self.operation:
            try:
                self._insert_source()
                self.ingestion_start = datetime.datetime.now()
                self._insert_proc_status(self.exit_codes["INGESTION_STARTED"]["status"])
                ### Log that the ingestion of the source file has been started
                print(self.exit_codes["INGESTION_STARTED"]["message"].format(
                    self.operation["source"]["name"],
                    self.dim_signature.dim_signature,
                    self.dim_signature.dim_exec_name, 
                    self.operation["dim_signature"]["version"]))
            except SourceAlreadyIngested as e:
                self.session.rollback()
                self._insert_proc_status(self.exit_codes["SOURCE_ALREADY_INGESTED"]["status"])
                ### Log that the source file has been already been processed
                print(e)
                self.session.commit()
                self.session.close()
                return
            # end try
            except WrongPeriod as e:
                self.session.rollback()
                self._insert_proc_status(self.exit_codes["WRONG_PERIOD_SOURCE"]["status"])
                ### Log that the source file has a wrong specified period as the stop is lower than the start
                print(e)
                self.session.commit()
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
        try:
            self._insert_events()
        except WrongEventLink as e:
            self.session.rollback()
            # If the data comes associated to a source then register the error on DDBB
            if "source" in self.operation:
                self._insert_proc_status(self.exit_codes["INCOMPLTE_LINKS_SOURCE"]["status"])
            # end if
            self.session.commit()
            self.session.close()
            ### Log the error
            print(e)
            return
        except WrongPeriod as e:
            self.session.rollback()
            # If the data comes associated to a source then register the error on DDBB
            if "source" in self.operation:
                self._insert_proc_status(self.exit_codes["WRONG_PERIOD_EVENT_SOURCE"]["status"])
            # end if
            self.session.commit()
            self.session.close()
            ### Log the error
            print(e)
            return
        # end try

        # Insert annotations
        self._insert_annotations()

        if "source" in self.operation:
            self._insert_proc_status(self.exit_codes["OK"]["status"],True)
            ### Log that the file has been ingested correctly
            print(self.exit_codes["OK"]["message"].format(
                    self.source.filename,
                    self.dim_signature.dim_signature,
                    self.dim_signature.dim_exec_name, 
                    self.source.dim_exec_version))
        # end if
        
        # Commit data and close connection
        self.session.commit()
        self.session.close()
        return

    def _apply_insertion_types(self):
        """
        """
        #self.session.begin_nested()
        
        
        
        return

    def _insert_dim_signature(self):
        """
        """
        dim_signature = self.operation.get("dim_signature")
        dim_name = dim_signature.get("name")
        exec_name = dim_signature.get("exec")
        self.dim_signature = self.session.query(DimSignature).filter(DimSignature.dim_signature == dim_name, DimSignature.dim_exec_name == exec_name).first()
        if not self.dim_signature:
            self.session.add(DimSignature(dim_name, exec_name))
            try:
                self.session.commit()
            except IntegrityError:
                # The DIM signature has been inserted between the
                # query and the insertion. Roll back transaction for
                # re-using the session
                self.session.rollback()
                pass
            # end try
            # Get the stored DIM signature
            self.dim_signature = self.session.query(DimSignature).filter(DimSignature.dim_signature == dim_name, DimSignature.dim_exec_name == exec_name).first()
        # end if
        return
        
    def _insert_source(self):
        """
        """
        version = self.operation.get("dim_signature").get("version")
        source = self.operation.get("source")
        name = source.get("name")
        generation_time = source.get("generation_time")
        validity_start = source.get("validity_start")
        validity_stop = source.get("validity_stop")
        self.source = self.session.query(DimProcessing).filter(DimProcessing.filename == name,
                                                               DimProcessing.dim_signature_id == self.dim_signature.dim_signature_id,
                                                               DimProcessing.dim_exec_version == version).first()
        if self.source and self.source.ingestion_duration:
            # The source has been already ingested
            raise SourceAlreadyIngested(self.exit_codes["SOURCE_ALREADY_INGESTED"]["message"].format(name,
                                                                                                     self.dim_signature.dim_signature,
                                                                                                     self.dim_signature.dim_exec_name, 
                                                                                                     version))
        # end if
        id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        if parser.parse(validity_stop) < parser.parse(validity_start):
            # The validity period is not correct (stop > start)
            # Create DimProcessing for registering the error in the DDBB
            self.source = DimProcessing(id, name, generation_time,
                                        version, self.dim_signature)
            self.session.add(self.source)
            try:
                self.session.commit()
            except IntegrityError:
                # The DIM processing was already ingested
                self.session.rollback()
                self.source = self.session.query(DimProcessing).filter(DimProcessing.filename == name,
                                                                       DimProcessing.dim_signature_id == self.dim_signature.dim_signature_id,
                                                                       DimProcessing.dim_exec_version == version).first()
            # end try
            raise WrongPeriod(self.exit_codes["WRONG_PERIOD_SOURCE"]["message"].format(name, self.dim_signature.dim_signature, self.dim_signature.dim_exec_name, version, validity_stop, validity_start))
        # end if
        self.source = DimProcessing(id, name,
                                    generation_time, version, self.dim_signature,
                                    validity_start, validity_stop)
        self.session.add(self.source)
        try:
            self.session.commit()
        except IntegrityError:
            # The DIM processing has been ingested between the query and the insertion
            self.session.rollback()
            self.source = self.session.query(DimProcessing).filter(DimProcessing.filename == name,
                                                                   DimProcessing.dim_signature_id == self.dim_signature.dim_signature_id,
                                                                   DimProcessing.dim_exec_version == version).first()
            raise SourceAlreadyIngested(self.exit_codes["SOURCE_ALREADY_INGESTED"]["message"].format(name,
                                                              self.dim_signature.dim_signature,
                                                              self.dim_signature.dim_exec_name, 
                                                              version))
        # end try

        return
        
    def _insert_gauges(self):
        """
        """
        for event in self.operation.get("events") or []:
            gauge = event.get("gauge")
            name = gauge.get("name")
            system = gauge.get("system")
            self.gauges[(name,system)] = self.session.query(Gauge).filter(Gauge.name == name, Gauge.system == system, Gauge.dim_signature_id == self.dim_signature.dim_signature_id).first()
            if not self.gauges[(name,system)]:
                self.session.begin_nested()
                self.gauges[(name,system)] = Gauge(name, self.dim_signature, system)
                self.session.add(self.gauges[(name,system)])
                try:
                    self.session.commit()
                except IntegrityError:
                    # The gauge has been inserted between the query and the insertion. Roll back transaction for
                    # re-using the session
                    self.session.rollback()
                    # Get the stored gauge
                    self.gauges[(name,system)] = self.session.query(Gauge).filter(Gauge.name == name, Gauge.system == system, Gauge.dim_signature_id == self.dim_signature.dim_signature_id).first()
                    pass
                # end try
            # end if
        # end for

        return
        
    def _insert_annotation_cnfs(self):
        """
        """
        for annotation in self.operation.get("annotations") or []:
            annotation_cnf = annotation.get("annotation_cnf")
            name = annotation_cnf.get("name")
            system = annotation_cnf.get("system")
            self.annotation_cnfs[(name,system)] = self.session.query(AnnotationCnf).filter(AnnotationCnf.name == name, AnnotationCnf.system == system, AnnotationCnf.dim_signature_id == self.dim_signature.dim_signature_id).first()
            if not self.annotation_cnfs[(name,system)]:
                self.session.begin_nested()
                self.annotation_cnfs[(name,system)] = AnnotationCnf(name, self.dim_signature, system)
                self.session.add(self.annotation_cnfs[(name,system)])
                try:
                    self.session.commit()
                except IntegrityError:
                    # The annotation has been inserted between the query and the insertion. Roll back transaction for
                    # re-using the session
                    self.session.rollback()
                    # Get the stored annotation configuration
                    self.annotation_cnfs[(name,system)] = self.session.query(AnnotationCnf).filter(AnnotationCnf.name == name, AnnotationCnf.system == system, AnnotationCnf.dim_signature_id == self.dim_signature.dim_signature_id).first()
                    pass
                # end try
            # end if
        # end for

        return
        
    def _insert_expl_groups(self):
        """
        """
        for explicit_ref in self.operation.get("explicit_references") or []:
            if "group" in explicit_ref:
                self.session.begin_nested()
                expl_group_ddbb = ExplicitRefGrp(explicit_ref.get("group"))
                self.session.add(expl_group_ddbb)
                try:
                    self.session.commit()
                except IntegrityError:
                    # The explicit reference group exists already into DDBB
                    self.session.rollback()
                    expl_group_ddbb = self.session.query(ExplicitRefGrp).filter(ExplicitRefGrp.name == explicit_ref.get("group")).first()
                    pass
                # end try
                self.expl_groups[explicit_ref.get("group")] = expl_group_ddbb
            # end if
        # end for

        return
        
    def _insert_explicit_refs(self):
        """
        """
        events_explicit_refs = [event.get("explicit_reference") for event in self.operation.get("events") or []]
        annotations_explicit_refs = [annotation.get("explicit_reference") for annotation in self.operation.get("annotations") or []]
        explicit_references = set(events_explicit_refs + annotations_explicit_refs)
        for explicit_ref in explicit_references:
            self.explicit_refs[explicit_ref] = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == explicit_ref).first()
            if not self.explicit_refs[explicit_ref]:
                self.session.begin_nested()
                explicit_ref_grp = None
                # Get associated group if exists from the declared explicit references
                declared_explicit_reference = next(iter([i for i in self.operation.get("explicit_references") or [] if i.get("name") == explicit_ref]), None)
                if declared_explicit_reference:
                    explicit_ref_grp = self.expl_groups.get(declared_explicit_reference.get("group"))
                # end if
                self.explicit_refs[explicit_ref] = ExplicitRef(datetime.datetime.now(), explicit_ref, explicit_ref_grp)
                self.session.add(self.explicit_refs[explicit_ref])
                try:
                    self.session.commit()
                except IntegrityError:
                    # The explicit reference has been inserted between the query and the insertion. Roll back transaction for
                    # re-using the session
                    self.session.rollback()
                    self.explicit_refs[explicit_ref] = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == explicit_ref).first()
                    pass
                # end try
            # end if
        # end for

        return
        
    def _insert_links_explicit_refs(self):
        """
        """
        list_explicit_reference_links = []
        for explicit_ref in [i for i in self.operation.get("explicit_references") or [] if i.get("links")]:
            for link in explicit_ref.get("links") or []:
                explicit_ref1 = self.explicit_refs[explicit_ref.get("name")]
                explicit_ref2 = self.explicit_refs[link.get("link")]
                link_ddbb = self.session.query(ExplicitRefLink).filter(ExplicitRefLink.explicit_ref_id_link == explicit_ref1.explicit_ref_id, ExplicitRefLink.name == link.get("name"), ExplicitRefLink.explicit_ref_id == explicit_ref2.explicit_ref_id).first()
                if not link_ddbb:
                    self.session.begin_nested()
                    self.session.add(ExplicitRefLink(explicit_ref1.explicit_ref_id, link.get("name"), explicit_ref2))
                    try:
                        self.session.commit()
                    except IntegrityError:
                        # The link exists already into DDBB
                        self.session.rollback()
                        pass
                    # end try
                # end if
                # Insert the back ref if specified
                if bool(link.get("back_ref")):
                    link_ddbb = self.session.query(ExplicitRefLink).filter(ExplicitRefLink.explicit_ref_id_link == explicit_ref2.explicit_ref_id, ExplicitRefLink.name == link.get("name"), ExplicitRefLink.explicit_ref_id == explicit_ref1.explicit_ref_id).first()
                    if not link_ddbb:
                        self.session.begin_nested()
                        self.session.add(ExplicitRefLink(explicit_ref2.explicit_ref_id, link.get("name"), explicit_ref1))
                        try:
                            self.session.commit()
                        except IntegrityError:
                            # The link exists already into DDBB
                            self.session.rollback()
                            pass
                        # end try
                    # end if
                # end if
            # end for
        # end for

        return

    def _insert_events(self):
        """
        """
        self.session.begin_nested()
        list_events = []
        list_keys = []
        list_event_links = {}
        for event in self.operation.get("events") or []:
            id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
            start = event.get("start")
            stop = event.get("stop")
            if parser.parse(stop) < parser.parse(start):
                # The period of the event is not correct (stop > start)
                self.session.rollback()
                if "source" in self.operation:
                    raise WrongPeriod(self.exit_codes["WRONG_PERIOD_EVENT_SOURCE"]["message"].format(self.source.filename, self.dim_signature.dim_signature, self.dim_signature.dim_exec_name, self.source.dim_exec_version, stop, start))
                else:
                    raise WrongPeriod(self.exit_codes["WRONG_PERIOD_EVENT"]["message"].format(stop, start))
                # end if
            # end if

            generation_time = event.get("generation_time")
            gauge_info = event.get("gauge")
            gauge = self.gauges[(gauge_info.get("name"), gauge_info.get("system"))]
            explicit_ref = self.explicit_refs[event.get("explicit_reference")]
            key = event.get("key")
            visible = False
            if gauge_info["insertion_type"] == "SIMPLE_UPDATE":
                visible = True
            # end if
            # Insert the event into the list for bulk ingestion
            list_events.append(dict(event_uuid = id, start = start, stop = stop,
                                    generation_time = generation_time,
                                    ingestion_time = datetime.datetime.now(),
                                    gauge_id = gauge.gauge_id,
                                    explicit_ref_id = explicit_ref.explicit_ref_id,
                                    processing_uuid = self.source.processing_uuid,
                                    visible = visible))
            # Insert the key into the list for bulk ingestion
            list_keys.append(dict(event_key = key, event_uuid = id,
                                  generation_time = generation_time, 
                                  visible = False,
                                  dim_signature_id = self.dim_signature.dim_signature_id))

            # Build links for later ingestion
            if "links" in event:
                for link in event["links"]:
                    if not link["link"] in list_event_links:
                        list_event_links[link["link"]] = []
                    # end if
                    list_event_links[link["link"]].append({"name": link["name"],
                                                           "event_uuid": id})
                # end for
            # end if
        # end for
        # Bulk insert events
        self.session.bulk_insert_mappings(Event, list_events)
        # Bulk insert keys
        self.session.bulk_insert_mappings(EventKey, list_keys)

        # Insert links
        list_event_links_ddbb = []
        for link in list_event_links:
            event_uuids = set([i.get("event_uuid") for i in list_event_links[link]])
            if len(event_uuids) == 1:
                self.session.rollback()
                if "source" in self.operation:
                    raise WrongEventLink(self.exit_codes["INCOMPLTE_LINKS_SOURCE"]["message"].format(self.filename, self.dim_signature.dim_signature, self.dim_signature.dim_exec_name, self.dim_exec_version, link))
                else:
                    raise WrongEventLink(self.exit_codes["INCOMPLTE_LINKS"]["message"].format(link))
                # end if
            # end if
            list_event_links_ddbb = [dict(event_uuid_link = x["event_uuid"],
                                          name = x["name"],
                                          event_uuid = y["event_uuid"])
                                     for x in list_event_links[link] for y in list_event_links[link] if x != y]
        # end for
        
        # Bulk insert links
        self.session.bulk_insert_mappings(EventLink, list_event_links_ddbb)

        # Commit data
        self.session.commit()

        return
        
    def _insert_annotations(self):
        """
        """
        list_annotations = []
        for annotation in self.operation.get("annotations") or []:
            id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
            generation_time = annotation.get("generation_time")
            annotation_cnf_info = annotation.get("annotation_cnf")
            annotation_cnf = self.annotation_cnfs[(annotation_cnf_info.get("name"), annotation_cnf_info.get("system"))]
            explicit_ref = self.explicit_refs[annotation.get("explicit_reference")]

            # Insert the annotation into the list for bulk ingestion
            list_annotations.append(dict(annotation_uuid = id, generation_time = generation_time,
                                         ingestion_time = datetime.datetime.now(),
                                         annotation_cnf_id = annotation_cnf.annotation_cnf_id,
                                         explicit_ref_id = explicit_ref.explicit_ref_id,
                                         processing_uuid = self.source.processing_uuid,
                                         visible = False))
        # end for
            
        # Bulk insert annotations
        self.session.bulk_insert_mappings(Annotation, list_annotations)

        return

    def _insert_proc_status(self, status, final = False):
        """
        """
        self.session.begin_nested()
        # Insert processing status
        self.session.add(DimProcessingStatus(datetime.datetime.now(),status,self.source))

        if final:
            # Insert processing duration
            self.source.ingestion_duration = datetime.datetime.now() - self.ingestion_start
        # end if

        self.session.commit()

        return
