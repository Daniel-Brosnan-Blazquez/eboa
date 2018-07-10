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
from dateutil import parser
from itertools import chain
from oslo_concurrency import lockutils

# Import SQLalchemy entities
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import func

# Import GEOalchemy entities
from geoalchemy2 import functions
from geoalchemy2.shape import to_shape

# Import exceptions
from .errors import WrongEventLink, WrongPeriod, SourceAlreadyIngested, WrongValue, OddNumberOfCoordinates

# Import datamodel
from gsdm.datamodel.base import Session, engine, Base
from gsdm.datamodel.dim_signatures import DimSignature
from gsdm.datamodel.events import Event, EventLink, EventKey, EventText, EventDouble, EventObject, EventGeometry, EventBoolean, EventTimestamp
from gsdm.datamodel.gauges import Gauge
from gsdm.datamodel.dim_processings import DimProcessing, DimProcessingStatus
from gsdm.datamodel.explicit_refs import ExplicitRef, ExplicitRefGrp, ExplicitRefLink
from gsdm.datamodel.annotations import Annotation, AnnotationCnf, AnnotationText, AnnotationDouble, AnnotationObject, AnnotationGeometry, AnnotationBoolean, AnnotationTimestamp

# Import xml parser
from lxml import etree

class Engine():
    # Set the synchronized module
    synchronized = lockutils.synchronized_with_prefix('gsdm-')

    def __init__(self, data = None):
        """
        """
        if data == None:
            data = {}
        # end if
        self.data = data
        self.session = Session()
        self.operation = None
        self.exit_codes = {
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
            "WRONG_SOURCE_PERIOD": {
                "status": 3,
                "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} has a validity period which its stop ({}) is lower than its start ({})"
            },
            "WRONG_EVENT_PERIOD": {
                "status": 4,
                "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} contains an event with a stop value {} lower than its start value {}"
            },
            "EVENT_PERIOD_NOT_IN_SOURCE_PERIOD": {
                "status": 5,
                "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} contains an event with a period ({}_{}) outside the period of the source ({}_{})"
            },
            "INCOMPLETE_EVENT_LINKS": {
                "status": 6,
                "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} contains an event which defines the link id {} to other events that are not specified"
            },
            "WRONG_VALUE": {
                "status": 7,
                "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} contains an event which defines the value {} that cannot be converted to the specified type {}"
            },
            "ODD_NUMBER_OF_COORDINATES": {
                "status": 8,
                "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} contains an event which defines the geometry value {} with an odd number of coordinates"
            }
        }
    
        return

    #################
    # QUERY METHODS #
    #################

    def get_source(self, name):
        """
        """
        # Get start of the query
        start_query = datetime.datetime.now()

        # Generate metadata of the query
        data = {}
        data["mode"] = "query"
        data["request"] = {"name": "get_source",
                           "parameters": {"name": name}
                       }
                
        # Get the dim_processing from the DDBB
        dim_processing = self.get_sources([name])

        if len(dim_processing) == 0:
            ### Log that the name provided does not exist into DDBB
            print("There is no dim_processing into the DDBB with name {}".format(name))
            return -1
        # end if

        data["source"] = {"processing_uuid": dim_processing[0].processing_uuid,
                          "name": dim_processing[0].name,
                          "generation_time": dim_processing[0].generation_time,
                          "validity_start": dim_processing[0].validity_start,
                          "validity_stop": dim_processing[0].validity_stop,
                          "ingestion_time": dim_processing[0].ingestion_time,
                          "ingestion_duration": dim_processing[0].ingestion_duration.total_seconds()
                      }

        exec_version = dim_processing[0].dim_exec_version

        # Get the status of the source from the DDBB
        dim_processing_status = self.get_sources_statuses([dim_processing[0].processing_uuid])

        # Get the dim_signature from the DDBB
        dim_signature = self.get_dim_signatures([dim_processing[0].dim_signature_id])

        data["dim_signature"] = {"name": dim_signature[0].dim_signature,
                                 "version": exec_version,
                                 "exec": dim_signature[0].dim_exec_name
                      }

        # Get the events from the DDBB
        events = self.get_events([dim_processing[0].processing_uuid])

        if len(events) > 0:
            data["events"] = []
            for event in events:
                event_info = {"event_uuid": event.event_uuid,
                              "start": event.start,
                              "stop": event.stop,
                              "ingestion_time": event.ingestion_time
                          }
                values = self.get_event_values([event.event_uuid])
                if len (values) > 0:
                    event_info["values"] = []
                    self._build_values_structure(values, event_info["values"])
                # end if
                data["events"].append(event_info)
            # end for
        # en if

        # Get the annotations from the DDBB
        annotations = self.get_annotations([dim_processing[0].processing_uuid])
        if len(annotations) > 0:
            data["annotations"] = []
            for annotation in annotations:
                annotation_info = {"annotation_uuid": annotation.annotation_uuid,
                              "ingestion_time": annotation.ingestion_time,
                              "visible": annotation.visible
                          }
                values = self.get_annotation_values([annotation.annotation_uuid])
                if len (values) > 0:
                    annotation_info["values"] = []
                    self._build_values_structure(values, annotation_info["values"])
                # end if
                data["annotations"].append(annotation_info)
            # end for
        # en if

        # Annotate the stop of the query
        stop_query = datetime.datetime.now()

        data["request"]["duration"] = (stop_query - start_query).total_seconds()

        return data

    def get_source_xml(self, name, output):
        """
        """
        # Get the data from DDBB
        data = self.get_source(name)

        # Get start of the query
        start_parsing = datetime.datetime.now()

        gsd = etree.Element("gsd")

        operation = etree.SubElement(gsd, "operation", mode=data["mode"])

        # Include the request
        request = etree.SubElement(operation, "request", name="get_source_xml", duration=str(data["request"]["duration"]))
        if "parameters" in data["request"]:
            parameters = etree.SubElement(request, "parameters")
            for parameter in data["request"]["parameters"]:
                tag = etree.SubElement(parameters, parameter)
                tag.text = data["request"]["parameters"][parameter]
            # end for
        # end if

        # Include the DIM signature
        etree.SubElement(operation, "dim_signature", name=data["dim_signature"]["name"], 
                         version=data["dim_signature"]["version"], 
                         exec=data["dim_signature"]["exec"])
        # Include the DIM processing
        etree.SubElement(operation, "source", name=data["source"]["name"], 
                         generation_time=str(data["source"]["generation_time"]), 
                         validity_start=str(data["source"]["validity_start"]), 
                         validity_stop=str(data["source"]["validity_stop"]), 
                         ingestion_time=str(data["source"]["ingestion_time"]), 
                         ingestion_duration=str(data["source"]["ingestion_duration"]))

        data_xml = etree.SubElement(operation, "dim_signature")

        # Include events
        for event in data["events"]:
            event_xml = etree.SubElement(data_xml, "event", id=str(event["event_uuid"]), start=str(event["start"]),
                             stop=str(event["stop"]), ingestion_time=str(event["ingestion_time"]))
            if "values" in event:
                self._transform_value_to_xml(event["values"][0], event_xml)
        # end for

        # Include annotations
        for annotation in data["annotations"]:
            annotation_xml = etree.SubElement(data_xml, "annotation",
                                              id=str(annotation["annotation_uuid"]),
                                              ingestion_time=str(annotation["ingestion_time"]))
            if "values" in annotation:
                self._transform_value_to_xml(annotation["values"][0], annotation_xml)
        # end for

        # Annotate the duration of the parsing
        parsing_duration = (datetime.datetime.now() - start_parsing).total_seconds()
        request.set("parsing_duration", str(parsing_duration))

        etree.ElementTree(gsd).write(output, pretty_print=True)
        return

    def _transform_value_to_xml(self, values, node):
        """
        """
        values_xml = etree.SubElement(node, "values", name=values["name"])

        for item in values["values"]:
            if item["type"] == "object":
                self._transform_value_to_xml(item, values_xml)
            else:
                value = etree.SubElement(values_xml, "value", name=item["name"],
                                 type=item["type"])
                if item["type"] == "geometry":
                    value.text = to_shape(item["value"]).to_wkt().replace("POLYGON ((", "").replace("))", "").replace(",", "")
                else:
                    value.text = str(item["value"])
                # end if
            # end if
        # end for

        return

    def _build_values_structure(self, values, structure, level_position = 0, parent_level = -1, parent_level_position = 0):
        """
        """
        object_entity = [value for value in values if value.parent_level == parent_level and value.parent_position == parent_level_position and value.level_position == level_position][0]

        object_entity_structure = {"name": object_entity.name,
                                   "type": "object",
                                   "values": []
        }

        child_values = sorted([value for value in values if value.parent_level == parent_level + 1 and value.parent_position == level_position], key=lambda x: x.level_position)
        structure.append(object_entity_structure)
        for value in child_values:
            if type(value) in [EventBoolean, AnnotationBoolean]:
                value_type = "boolean"
            elif type(value) in [EventDouble, AnnotationDouble]:
                value_type = "double"
            elif type(value) in [EventTimestamp, AnnotationTimestamp]:
                value_type = "timestamp"
            elif type(value) in [EventGeometry, AnnotationGeometry]:
                value_type = "geometry"
            elif type(value) in [EventObject, AnnotationObject]:
                value_type = "object"
            else:
                value_type = "text"
            # end if

            if value_type != "object":
                object_entity_structure["values"].append({"name": value.name,
                                                          "type": value_type,
                                                          "value": value.value
                                                      })
            else:
                self._build_values_structure(values, object_entity_structure["values"], value.level_position, parent_level + 1, level_position)
            # end if
        # end for
        return

    def get_dim_signatures(self, dim_signature_ids = None):
        """
        """
        if dim_signature_ids:
            if type(dim_signature_ids) != list:
                raise
            # end if
            dim_signatures = self.session.query(DimSignature).filter(DimSignature.dim_signature_id.in_(dim_signature_ids)).all()
        else:
            dim_signatures = self.session.query(DimSignature).all()
        # end if
        return dim_signatures

    def get_sources(self, names = None):
        """
        """
        if names:
            if type(names) != list:
                raise
            # end if
            sources = self.session.query(DimProcessing).filter(DimProcessing.name.in_(names)).all()
        else:
            sources = self.session.query(DimProcessing).all()
        # end if
        return sources

    def get_sources_statuses(self, processing_uuids = None):
        """
        """
        if processing_uuids:
            if type(processing_uuids) != list:
                raise
            # end if
            source_statuses = self.session.query(DimProcessingStatus).filter(DimProcessingStatus.processing_uuid.in_(processing_uuids)).all()
        else:
            source_statuses = self.session.query(DimProcessingStatus).all()
        # end if
        return source_statuses

    def get_events(self, processing_uuids = None):
        """
        """
        if processing_uuids:
            if type(processing_uuids) != list:
                raise
            # end if
            events = self.session.query(Event).filter(Event.processing_uuid.in_(processing_uuids)).all()
        else:
            events = self.session.query(Event).all()
        # end if
        return events

    def get_gauges(self):
        """
        """
        
        return self.session.query(Gauge).all()

    def get_event_keys(self, event_uuids = None):
        """
        """
        if event_uuids:
            if type(event_uuids) != list:
                raise
            # end if
            keys = self.session.query(EventKey).filter(EventKey.event_uuid.in_(event_uuids)).all()
        else:
            keys = self.session.query(EventKey).all()
        # end if

        return keys

    def get_event_links(self, event_uuids = None):
        """
        """
        if event_uuids:
            if type(event_uuids) != list:
                raise
            # end if
            links = self.session.query(EventLink).filter(EventLink.event_uuid_link.in_(event_uuids)).all()
        else:
            links = self.session.query(EventLink).all()
        # end if

        return links

    def get_annotations(self, processing_uuids = None):
        """
        """
        if processing_uuids:
            if type(processing_uuids) != list:
                raise
            # end if
            events = self.session.query(Annotation).filter(Annotation.processing_uuid.in_(processing_uuids)).all()
        else:
            events = self.session.query(Annotation).all()
        # end if
        return events

    def get_annotations_configurations(self):
        """
        """
        
        return self.session.query(AnnotationCnf).all()

    def get_explicit_references(self):
        """
        """
        
        return self.session.query(ExplicitRef).all()

    def get_explicit_references_links(self):
        """
        """
        
        return self.session.query(ExplicitRefLink).all()

    def get_explicit_references_groups(self):
        """
        """
        
        return self.session.query(ExplicitRefGrp).all()

    def get_event_booleans(self):
        """
        """
        
        return self.session.query(EventBoolean).all()

    def get_event_texts(self):
        """
        """
        
        return self.session.query(EventText).all()

    def get_event_doubles(self):
        """
        """
        
        return self.session.query(EventDouble).all()

    def get_event_timestamps(self):
        """
        """
        
        return self.session.query(EventTimestamp).all()

    def get_event_objects(self):
        """
        """
        
        return self.session.query(EventObject).all()

    def get_event_geometries(self):
        """
        """
        
        return self.session.query(EventGeometry).all()

    def get_annotation_booleans(self):
        """
        """
        
        return self.session.query(AnnotationBoolean).all()

    def get_annotation_texts(self):
        """
        """
        
        return self.session.query(AnnotationText).all()

    def get_annotation_doubles(self):
        """
        """
        
        return self.session.query(AnnotationDouble).all()

    def get_annotation_timestamps(self):
        """
        """
        
        return self.session.query(AnnotationTimestamp).all()

    def get_annotation_objects(self):
        """
        """
        
        return self.session.query(AnnotationObject).all()

    def get_annotation_geometries(self):
        """
        """
        
        return self.session.query(AnnotationGeometry).all()

    def get_event_values(self, event_uuids = None):
        """
        """
        values = []
        for value_class in [EventText, EventDouble, EventObject, EventGeometry, EventBoolean, EventTimestamp]:
            if event_uuids:
                if type(event_uuids) != list:
                    raise
                # end if
                values.append(self.get_event_values_type(value_class, event_uuids))
            else:
                values.append(self.get_event_values_type(value_class))
            # end if
        # end for
        return [value for values_per_class in values for value in values_per_class]
        
    def get_event_values_type(self, value_class, event_uuids = None):
        """
        """
        if event_uuids:
            if type(event_uuids) != list:
                raise
            # end if
            values = self.session.query(value_class).filter(value_class.event_uuid.in_(event_uuids)).all()
        else:
            values = self.session.query(value_class).all()
        # end if
        return values

    def get_annotation_values(self, annotation_uuids = None):
        """
        """
        values = []
        for value_class in [AnnotationText, AnnotationDouble, AnnotationObject, AnnotationGeometry, AnnotationBoolean, AnnotationTimestamp]:
            if annotation_uuids:
                if type(annotation_uuids) != list:
                    raise
                # end if
                values.append(self.get_annotation_values_type(value_class, annotation_uuids))
            else:
                values.append(self.get_annotation_values_type(value_class))
            # end if
        # end for
        return [value for values_per_class in values for value in values_per_class]
        
    def get_annotation_values_type(self, value_class, annotation_uuids = None):
        """
        """
        if annotation_uuids:
            if type(annotation_uuids) != list:
                raise
            # end if
            values = self.session.query(value_class).filter(value_class.annotation_uuid.in_(annotation_uuids)).all()
        else:
            values = self.session.query(value_class).all()
        # end if
        return values

    def parse_data_from_xml(self, xml):
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

    ###################
    # PARSING METHODS #
    ###################

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
                if len(event.xpath("values")) > 0:
                    event_info["values"] = []
                    self._parse_values_from_xml(event.xpath("values")[0], event_info["values"])
                # end if

                data["events"].append(event_info)
            # end for
        # end if


        # Extract annotations
        if len (operation.xpath("data/annotation")) > 0:
            data["annotations"] = []
            for annotation in operation.xpath("data/annotation"):
                annotation_info = {}
                annotation_info["explicit_reference"] = annotation.get("explicit_reference")
                annotation_info["annotation_cnf"] = {"name": annotation.xpath("annotation_cnf")[0].get("name"),
                                                     "system": annotation.xpath("annotation_cnf")[0].get("system")}
                # end if
                # Add values
                if len(annotation.xpath("values")) > 0:
                    annotation_info["values"] = []
                    self._parse_values_from_xml(annotation.xpath("values")[0], annotation_info["values"])
                # end if

                data["annotations"].append(annotation_info)
            # end for
        # end if

        self.data["operations"].append(data)
        return

    def _parse_values_from_xml(self, node, parent):
        """
        """
        parent_object = {"name": node.get("name"),
                         "type": "object",
                         "values": []
                     }
        parent.append(parent_object)
        for child_node in node.xpath("child::*"):
            if child_node.tag == "value":
                parent_object["values"].append({"name": child_node.get("name"),
                                         "type": child_node.get("type"),
                                         "value": child_node.text
                                     })
            else:
                self._parse_values_from_xml(child_node, parent_object["values"])
            # end if
        # end for
        return

    def treat_data(self):
        """
        
        """
        # Pass schema
        
        for self.operation in self.data.get("operations") or []:
            if self.operation.get("mode") == "insert":
                returned_value = self._insert_data()
            # end if
            if returned_value != self.exit_codes["OK"]["status"]:
                return returned_value
            # end if
        # end for
        return self.exit_codes["OK"]["status"]

    #####################
    # INSERTION METHODS #
    #####################

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
        self.erase_and_replace_gauges = []
        self.annotation_cnfs_explicit_refs = []
        self.keys_events = []

        # Insert the DIM signature
        self._insert_dim_signature()

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
            return self.exit_codes["SOURCE_ALREADY_INGESTED"]["status"]
        except WrongPeriod as e:
            self.session.rollback()
            self._insert_proc_status(self.exit_codes["WRONG_SOURCE_PERIOD"]["status"])
            ### Log that the source file has a wrong specified period as the stop is lower than the start
            print(e)
            self.session.commit()
            self.session.close()
            return self.exit_codes["WRONG_SOURCE_PERIOD"]["status"]
        # end try

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
            self._insert_proc_status(self.exit_codes["INCOMPLETE_EVENT_LINKS"]["status"])
            self.session.commit()
            self.session.close()
            ### Log the error
            print(e)
            return self.exit_codes["INCOMPLETE_EVENT_LINKS"]["status"]
        except WrongPeriod as e:
            self.session.rollback()
            self._insert_proc_status(self.exit_codes["WRONG_EVENT_PERIOD"]["status"])
            self.session.commit()
            self.session.close()
            ### Log the error
            print(e)
            return self.exit_codes["WRONG_EVENT_PERIOD"]["status"]
        except WrongValue as e:
            self.session.rollback()
            self._insert_proc_status(self.exit_codes["WRONG_VALUE"]["status"])
            self.session.commit()
            self.session.close()
            ### Log the error
            print(e)
            return self.exit_codes["WRONG_VALUE"]["status"]
        except OddNumberOfCoordinates as e:
            self.session.rollback()
            self._insert_proc_status(self.exit_codes["ODD_NUMBER_OF_COORDINATES"]["status"])
            self.session.commit()
            self.session.close()
            ### Log the error
            print(e)
            return self.exit_codes["ODD_NUMBER_OF_COORDINATES"]["status"]
        # end try

        # Insert annotations
        try:
            self._insert_annotations()
        except WrongValue as e:
            self.session.rollback()
            self._insert_proc_status(self.exit_codes["WRONG_VALUE"]["status"])
            self.session.commit()
            self.session.close()
            ### Log the error
            print(e)
            return self.exit_codes["WRONG_VALUE"]["status"]
        except OddNumberOfCoordinates as e:
            self.session.rollback()
            self._insert_proc_status(self.exit_codes["ODD_NUMBER_OF_COORDINATES"]["status"])
            self.session.commit()
            self.session.close()
            ### Log the error
            print(e)
            return self.exit_codes["ODD_NUMBER_OF_COORDINATES"]["status"]
        # end try

        # Review the inserted events (with modes EVENT_KEYS and
        # ERASE_and_REPLACE) and annotations for removing the
        # information that is deprecated
        self._remove_deprecated_data()

        ### Log that the file has been ingested correctly
        self._insert_proc_status(self.exit_codes["OK"]["status"],True)
        print(self.exit_codes["OK"]["message"].format(
            self.source.name,
            self.dim_signature.dim_signature,
            self.dim_signature.dim_exec_name, 
            self.source.dim_exec_version))
        self.source.ingestion_time = datetime.datetime.now()
        
        # Commit data and close connection
        self.session.commit()
        self.session.close()
        return self.exit_codes["OK"]["status"]

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
        self.source = self.session.query(DimProcessing).filter(DimProcessing.name == name,
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
                self.source = self.session.query(DimProcessing).filter(DimProcessing.name == name,
                                                                       DimProcessing.dim_signature_id == self.dim_signature.dim_signature_id,
                                                                       DimProcessing.dim_exec_version == version).first()
            # end try
            raise WrongPeriod(self.exit_codes["WRONG_SOURCE_PERIOD"]["message"].format(name, self.dim_signature.dim_signature, self.dim_signature.dim_exec_name, version, validity_stop, validity_start))
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
            self.source = self.session.query(DimProcessing).filter(DimProcessing.name == name,
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
        # Join all sources of explicit references
        events_explicit_refs = [event.get("explicit_reference") for event in self.operation.get("events") or []]
        annotations_explicit_refs = [annotation.get("explicit_reference") for annotation in self.operation.get("annotations") or []]
        declared_explicit_refs = [i.get("name") for i in self.operation.get("explicit_references") or []]
        linked_explicit_refs = [link.get("link") for explicit_ref in self.operation.get("explicit_references") or [] if explicit_ref.get("links") for link in explicit_ref.get("links")]

        explicit_references = set(events_explicit_refs + annotations_explicit_refs + declared_explicit_refs + linked_explicit_refs)
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

    def _insert_event(self, list_events, id, start, stop, gauge, explicit_ref_id, visible, source = None, source_id = None):
        """
        """
        if not type(start) == datetime.datetime:
            start = parser.parse(start)
        # end if
        if not type(stop) == datetime.datetime:
            stop = parser.parse(stop)
        # end if
        if stop < start:
            # The period of the event is not correct (stop > start)
            self.session.rollback()
            raise WrongPeriod(self.exit_codes["WRONG_EVENT_PERIOD"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.dim_signature.dim_exec_name, self.source.dim_exec_version, stop, start))
        # end if
        
        if source == None:
            source = self.session.query(DimProcessing).filter(DimProcessing.processing_uuid == source_id).first()
        # end if

        source_start = source.validity_start
        source_stop = source.validity_stop
        if stop < source_start or start > source_stop:
            # The period of the event is not inside the validity period of the input
            self.session.rollback()
            raise WrongPeriod(self.exit_codes["EVENT_PERIOD_NOT_IN_SOURCE_PERIOD"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.dim_signature.dim_exec_name, self.source.dim_exec_version, start, stop, source_start, source_stop))
        # end if
            
        list_events.append(dict(event_uuid = id, start = start, stop = stop,
                                ingestion_time = datetime.datetime.now(),
                                gauge_id = gauge.gauge_id,
                                explicit_ref_id = explicit_ref_id,
                                processing_uuid = source.processing_uuid,
                                visible = visible))
        return

    def _insert_events(self):
        """
        """
        self.session.begin_nested()
        list_events = []
        list_keys = []
        list_event_links = {}
        list_values = {}
        for event in self.operation.get("events") or []:
            id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
            start = event.get("start")
            stop = event.get("stop")
            gauge_info = event.get("gauge")
            gauge = self.gauges[(gauge_info.get("name"), gauge_info.get("system"))]
            explicit_ref = self.explicit_refs[event.get("explicit_reference")]
            key = event.get("key")
            visible = False
            if gauge_info["insertion_type"] == "SIMPLE_UPDATE":
                visible = True
            elif gauge_info["insertion_type"] == "ERASE_and_REPLACE":
                self.erase_and_replace_gauges.append(gauge)
            elif gauge_info["insertion_type"] == "EVENT_KEYS":
                self.keys_events.append(key)
            # end if
            # Insert the event into the list for bulk ingestion
            self._insert_event(list_events, id, start, stop, gauge, explicit_ref.explicit_ref_id,
                                    visible, source = self.source)

            # Make the key visible only in case it is not going to be inserted as EVENT_KEYS
            visible = True
            if gauge_info["insertion_type"] == "EVENT_KEYS":
                visible = False
            # end if
            # Insert the key into the list for bulk ingestion
            list_keys.append(dict(event_key = key, event_uuid = id,
                                  visible = visible,
                                  dim_signature_id = self.dim_signature.dim_signature_id))

            # Insert values
            if "values" in event:
                entity_uuid = {"name": "event_uuid",
                               "id": id
                }
                self._insert_values(event.get("values")[0], entity_uuid, list_values)
            # end if
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

        # Bulk insert values
        if "objects" in list_values:
            self.session.bulk_insert_mappings(EventObject, list_values["objects"])
        # end if
        if "booleans" in list_values:
            self.session.bulk_insert_mappings(EventBoolean, list_values["booleans"])
        # end if
        if "texts" in list_values:
            self.session.bulk_insert_mappings(EventText, list_values["texts"])
        # end if
        if "doubles" in list_values:
            self.session.bulk_insert_mappings(EventDouble, list_values["doubles"])
        # end if
        if "timestamps" in list_values:
            self.session.bulk_insert_mappings(EventTimestamp, list_values["timestamps"])
        # end if
        if "geometries" in list_values:
            self.session.bulk_insert_mappings(EventGeometry, list_values["geometries"])
        # end if

        # Insert links
        list_event_links_ddbb = []
        for link in list_event_links:
            event_uuids = set([i.get("event_uuid") for i in list_event_links[link]])
            if len(event_uuids) == 1:
                self.session.rollback()
                raise WrongEventLink(self.exit_codes["INCOMPLETE_EVENT_LINKS"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.dim_signature.dim_exec_name, self.source.dim_exec_version, link))
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

    def _insert_values(self, values, entity_uuid, list_values, level_position = 0, parent_level = -1, parent_level_position = 0, level_positions = None):
        """
        """
        if level_positions == None:
            level_positions = {}
        # end if
        if not "objects" in list_values:
            list_values["objects"] = []
        # end if
        list_values["objects"].append(dict([("name", values.get("name")),
                                           ("level_position",  level_position),
                                           ("parent_level",  parent_level),
                                           ("parent_position",  parent_level_position),
                                           (entity_uuid["name"], entity_uuid["id"])]
        ))
        parent_level += 1
        parent_level_position = level_position
        if not parent_level in level_positions:
            # List for keeping track of the positions occupied in the level (parent_level = level - 1)
            level_positions[parent_level] = 0
        # end if
        for item in values["values"]:
            if item["type"] == "object":
                self._insert_values(item, entity_uuid, list_values, level_positions[parent_level], parent_level, parent_level_position, level_positions)
            else:
                value = bool(str(item.get("value")))
                if item["type"] == "boolean":
                    if not "booleans" in list_values:
                        list_values["booleans"] = []
                    # end if
                    if item.get("value").lower() == "true":
                        value = True
                    elif item.get("value").lower() == "false":
                        value = False
                    else:
                        self.session.rollback()
                        raise WrongValue(self.exit_codes["WRONG_VALUE"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.dim_signature.dim_exec_name, self.source.dim_exec_version, item.get("value"), item["type"]))
                    list_to_use = list_values["booleans"]
                elif item["type"] == "text":
                    value = str(item.get("value"))
                    if not "texts" in list_values:
                        list_values["texts"] = []
                    # end if
                    list_to_use = list_values["texts"]
                elif item["type"] == "double":
                    try:
                        value = float(item.get("value"))
                    except ValueError:
                        self.session.rollback()
                        raise WrongValue(self.exit_codes["WRONG_VALUE"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.dim_signature.dim_exec_name, self.source.dim_exec_version, item.get("value"), item["type"]))
                    # end try
                    if not "doubles" in list_values:
                        list_values["doubles"] = []
                    # end if
                    list_to_use = list_values["doubles"]
                elif item["type"] == "timestamp":
                    try:
                        value = parser.parse(item.get("value"))
                    except ValueError:
                        self.session.rollback()
                        raise WrongValue(self.exit_codes["WRONG_VALUE"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.dim_signature.dim_exec_name, self.source.dim_exec_version, item.get("value"), item["type"]))
                    # end try
                    if not "timestamps" in list_values:
                        list_values["timestamps"] = []
                    # end if
                    list_to_use = list_values["timestamps"]
                elif item["type"] == "geometry":
                    list_coordinates = item.get("value").split(" ")
                    if len (list_coordinates) % 2 != 0:
                        self.session.rollback()
                        raise OddNumberOfCoordinates(self.exit_codes["ODD_NUMBER_OF_COORDINATES"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.dim_signature.dim_exec_name, self.source.dim_exec_version, item.get("value")))
                    # end if
                    coordinates = 0
                    value = "POLYGON(("
                    for coordinate in list_coordinates:
                        if coordinates == 2:
                            value = value + ","
                            coordinates = 0
                        # end if
                        try:
                            float(coordinate)
                        except ValueError:
                            self.session.rollback()
                            raise WrongValue(self.exit_codes["WRONG_VALUE"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.dim_signature.dim_exec_name, self.source.dim_exec_version, coordinate, "float"))
                        # end try
                        value = value + coordinate
                        coordinates += 1
                        if coordinates == 1:
                            value = value + " "
                        # end if
                    # end for
                    value = value + "))"
                    if not "geometries" in list_values:
                        list_values["geometries"] = []
                    # end if
                    list_to_use = list_values["geometries"]
                # end if
                list_to_use.append(dict([("name", item.get("name")),
                                         ("value", value),
                                         ("level_position",  level_positions[parent_level]),
                                         ("parent_level",  parent_level),
                                         ("parent_position",  parent_level_position),
                                         (entity_uuid["name"], entity_uuid["id"])]
                                    ))
            # end if
            level_positions[parent_level] += 1
        # end for

        return
        
    def _insert_annotations(self):
        """
        """
        list_annotations = []
        list_values = {}
        for annotation in self.operation.get("annotations") or []:
            id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
            annotation_cnf_info = annotation.get("annotation_cnf")
            annotation_cnf = self.annotation_cnfs[(annotation_cnf_info.get("name"), annotation_cnf_info.get("system"))]
            explicit_ref = self.explicit_refs[annotation.get("explicit_reference")]

            self.annotation_cnfs_explicit_refs.append({"explicit_ref": explicit_ref,
                                                       "annotation_cnf": annotation_cnf
                                                   })

            # Insert the annotation into the list for bulk ingestion
            list_annotations.append(dict(annotation_uuid = id, ingestion_time = datetime.datetime.now(),
                                         annotation_cnf_id = annotation_cnf.annotation_cnf_id,
                                         explicit_ref_id = explicit_ref.explicit_ref_id,
                                         processing_uuid = self.source.processing_uuid,
                                         visible = False))
            # Insert values
            if "values" in annotation:
                entity_uuid = {"name": "annotation_uuid",
                               "id": id
                }
                self._insert_values(annotation.get("values")[0], entity_uuid, list_values)
            # end if

        # end for
            
        # Bulk insert annotations
        self.session.bulk_insert_mappings(Annotation, list_annotations)

        # Bulk insert values
        if "objects" in list_values:
            self.session.bulk_insert_mappings(AnnotationObject, list_values["objects"])
        # end if
        if "booleans" in list_values:
            self.session.bulk_insert_mappings(AnnotationBoolean, list_values["booleans"])
        # end if
        if "texts" in list_values:
            self.session.bulk_insert_mappings(AnnotationText, list_values["texts"])
        # end if
        if "doubles" in list_values:
            self.session.bulk_insert_mappings(AnnotationDouble, list_values["doubles"])
        # end if
        if "timestamps" in list_values:
            self.session.bulk_insert_mappings(AnnotationTimestamp, list_values["timestamps"])
        # end if
        if "geometries" in list_values:
            self.session.bulk_insert_mappings(AnnotationGeometry, list_values["geometries"])
        # end if


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

    def _remove_deprecated_data(self):
        """
        """
        # Remove events due to ERASE_and_REPLACE insertion mode
        self._remove_deprecated_events_by_erase_and_replace()

        # Remove events due to EVENT_KEYS insertion mode
        self._remove_deprecated_events_event_keys()

        # Remove annotations due to the generation time
        self._remove_deprecated_annotations()

        return

    def _remove_deprecated_events_by_erase_and_replace(self):
        """
        """
        list_events_to_be_created = {"events": [],
                                     "values": {},
                                     "keys": [],
                                     "links": []}
        for gauge in self.erase_and_replace_gauges:
            # Make this method process and thread safe (lock_path -> where the lockfile will be stored)
            # /dev/shm is shared memory (RAM)
            lock = 'erase_and_replace' + str(gauge.gauge_id)
            @self.synchronized(lock, external=True, lock_path="/dev/shm")
            def _remove_deprecated_events_by_erase_and_replace_per_gauge(self, gauge, list_events_to_be_created, list_events_to_be_created_not_ending_on_period, list_split_events):
                # Get the sources of events intersecting the validity period
                sources = self.session.query(DimProcessing).join(Event).filter(Event.gauge_id == gauge.gauge_id,
                                                                               Event.start < self.source.validity_stop,
                                                                               Event.stop > self.source.validity_start)
                
                # Get the timeline of validity periods intersecting
                timeline_points = set(list(chain.from_iterable([[source.validity_start,source.validity_stop] for source in sources])))

                filtered_timeline_points = [timestamp for timestamp in timeline_points if timestamp >= self.source.validity_start and timestamp <= self.source.validity_stop]

                # Sort list
                filtered_timeline_points.sort()

                # Iterate through the periods
                next_timestamp = 1
                for timestamp in filtered_timeline_points:
                    
                    # Check if for the last period there are pending splits
                    if next_timestamp == len(filtered_timeline_points):
                        for event_uuid in list_split_events:
                            event = list_split_events[event_uuid]
                            id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                            if timestamp > event.stop:
                                raise
                            # end if
                            self._insert_event(list_events_to_be_created["events"], id, timestamp, event.stop,
                                               gauge, event.explicit_ref_id, True, source_id = event.processing_uuid)
                            self._replicate_event_values(event_uuid, id, list_events_to_be_created["values"])
                            self._replicate_event_links(event_uuid, id, list_events_to_be_created["links"])
                            self._replicate_event_keys(event_uuid, id, list_events_to_be_created["keys"])
                            # Remove event
                            self.session.query(Event).filter(Event.event_uuid == event_uuid).delete(synchronize_session=False)
                        # end for
                        break
                    # end if

                    validity_start = timestamp
                    validity_stop = filtered_timeline_points[next_timestamp]
                    next_timestamp += 1
                    # Get the maximum generation time at this moment
                    max_generation_time = self.session.query(func.max(DimProcessing.generation_time)).join(Event).filter(Event.gauge_id == gauge.gauge_id,
                                                                                                                         Event.start < validity_stop,
                                                                                                                         Event.stop > validity_start)
                
                    # The period does not contain events
                    if max_generation_time.first()[0] == None:
                        break
                    # end if
                    # Get the related source
                    source_max_generation_time = self.session.query(DimProcessing).join(Event).filter(DimProcessing.generation_time == max_generation_time,
                                                                                                     Event.gauge_id == gauge.gauge_id,
                                                                                                     Event.start < validity_stop,
                                                                                                     Event.stop > validity_start).first()

                    # Events related to the DIM processing with the maximum generation time
                    events_max_generation_time = self.session.query(Event).filter(Event.processing_uuid == source_max_generation_time.processing_uuid,
                                                                                  Event.gauge_id == gauge.gauge_id,
                                                                                  Event.start < validity_stop,
                                                                                  Event.stop > validity_start).all()
                    
                    # Review events with higher generation time in the period
                    for event in events_max_generation_time:
                        if event.event_uuid in list_split_events:
                            if event.stop <= validity_stop:
                                if validity_start > event.stop:
                                    raise
                                # end if
                                id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                                self._insert_event(list_events_to_be_created["events"], id, validity_start, event.stop,
                                                   gauge, event.explicit_ref_id, True, source_id = event.processing_uuid)
                                self._replicate_event_values(event.event_uuid, id, list_events_to_be_created["values"])
                                self._replicate_event_links(event.event_uuid, id, list_events_to_be_created["links"])
                                self._replicate_event_keys(event.event_uuid, id, list_events_to_be_created["keys"])
                                self.session.query(Event).filter(Event.event_uuid == event.event_uuid).delete(synchronize_session=False)
                            else:
                                list_events_to_be_created_not_ending_on_period[event.event_uuid] = validity_start
                            # end if
                            del list_split_events[event.event_uuid]
                        # end if
                        event.visible = True
                    # end for

                    # Delete deprecated events fully contained into the validity period
                    self.session.query(Event).filter(Event.processing_uuid != source_max_generation_time.processing_uuid,
                                                     Event.gauge_id == gauge.gauge_id,
                                                     Event.start >= validity_start,
                                                     Event.stop <= validity_stop).delete(synchronize_session="fetch")

                    # Get the events ending on the current period to be removed
                    events_not_staying_ending_on_period = self.session.query(Event).join(DimProcessing).filter(DimProcessing.generation_time <= max_generation_time,
                                                                                                               Event.gauge_id == gauge.gauge_id,
                                                                                                               Event.start <= validity_start,
                                                                                                               Event.stop > validity_start,
                                                                                                               Event.stop <= validity_stop,
                                                                                                               Event.processing_uuid != source_max_generation_time.processing_uuid).all()

                    # Get the events ending on the current period to be removed
                    events_not_staying_not_ending_on_period = self.session.query(Event).join(DimProcessing).filter(DimProcessing.generation_time <= max_generation_time,
                                                                                                                   Event.gauge_id == gauge.gauge_id,
                                                                                                                   Event.start < validity_stop,
                                                                                                                   Event.stop > validity_stop,
                                                                                                                   Event.processing_uuid != source_max_generation_time.processing_uuid).all()

                    events_not_staying = events_not_staying_ending_on_period + events_not_staying_not_ending_on_period
                    for event in events_not_staying:
                        if not event.event_uuid in list_split_events:
                            if event.start < validity_start:
                                if event.event_uuid in list_events_to_be_created_not_ending_on_period:
                                    start = list_events_to_be_created_not_ending_on_period[event.event_uuid]
                                    del list_events_to_be_created_not_ending_on_period[event.event_uuid]
                                else:
                                    start = event.start
                                # end if
                                if start > validity_start:
                                    raise
                                # end if
                                id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                                self._insert_event(list_events_to_be_created["events"], id, start, validity_start,
                                                   gauge, event.explicit_ref_id, True, source_id = event.processing_uuid)
                                self._replicate_event_values(event.event_uuid, id, list_events_to_be_created["values"])
                                self._replicate_event_links(event.event_uuid, id, list_events_to_be_created["links"])
                                self._replicate_event_keys(event.event_uuid, id, list_events_to_be_created["keys"])
                            # end if
                            if event.stop > validity_stop:
                                list_split_events[event.event_uuid] = event
                            else:
                                self.session.query(Event).filter(Event.event_uuid == event.event_uuid).delete(synchronize_session=False)
                            # end if
                        elif event.event_uuid in list_split_events and event.stop <= validity_stop:
                            self.session.query(Event).filter(Event.event_uuid == event.event_uuid).delete(synchronize_session=False)
                            del list_split_events[event.event_uuid]
                        # end if
                    # end for
                # end for
            # end def
            list_events_to_be_created_not_ending_on_period = {}
            list_split_events = {}
            _remove_deprecated_events_by_erase_and_replace_per_gauge(self, gauge, list_events_to_be_created, list_events_to_be_created_not_ending_on_period, list_split_events)
        # end for

        # Bulk insert events
        self.session.bulk_insert_mappings(Event, list_events_to_be_created["events"])
        # Bulk insert keys
        self.session.bulk_insert_mappings(EventKey, list_events_to_be_created["keys"])
        # Bulk insert links
        self.session.bulk_insert_mappings(EventLink, list_events_to_be_created["links"])

        # Bulk insert values
        if EventObject in list_events_to_be_created["values"]:
            self.session.bulk_insert_mappings(EventObject, list_events_to_be_created["values"][EventObject])
        # end if
        if EventBoolean in list_events_to_be_created["values"]:
            self.session.bulk_insert_mappings(EventBoolean, list_events_to_be_created["values"][EventBoolean])
        # end if
        if EventText in list_events_to_be_created["values"]:
            self.session.bulk_insert_mappings(EventText, list_events_to_be_created["values"][EventText])
        # end if
        if EventDouble in list_events_to_be_created["values"]:
            self.session.bulk_insert_mappings(EventDouble, list_events_to_be_created["values"][EventDouble])
        # end if
        if EventTimestamp in list_events_to_be_created["values"]:
            self.session.bulk_insert_mappings(EventTimestamp, list_events_to_be_created["values"][EventTimestamp])
        # end if
        if EventGeometry in list_events_to_be_created["values"]:
            self.session.bulk_insert_mappings(EventGeometry, list_events_to_be_created["values"][EventGeometry])
        # end if

        return

    def _replicate_event_values(self, from_event_uuid, to_event_uuid, list_values_to_be_created):
        """
        """
        values = self.get_event_values([from_event_uuid])
        for value in values:
            if not type(value) in list_values_to_be_created:
                list_values_to_be_created[type(value)] = []
            # end if
            value_to_insert = {"event_uuid": to_event_uuid,
                               "name": value.name,
                               "level_position": value.level_position,
                               "parent_level": value.parent_level,
                               "parent_position": value.parent_position
            }
            if not type(value) in (EventObject, EventGeometry):
                value_to_insert["value"] = value.value
            elif type(value) == EventGeometry:
                value_to_insert["value"] = to_shape(value.value).to_wkt()
            # end if
            list_values_to_be_created[type(value)].append(value_to_insert)
        # end for
        
        return

    def _replicate_event_links(self, from_event_uuid, to_event_uuid, list_links_to_be_created):
        """
        """
        links = self.get_event_links([from_event_uuid])
        for link in links:
            list_links_to_be_created.append(dict(name = link.name,
                                                 event_uuid_link = to_event_uuid,
                                                 event_uuid = link.event_uuid))
        # end for
        
        return

    def _replicate_event_keys(self, from_event_uuid, to_event_uuid, list_keys_to_be_created):
        """
        """
        keys = self.get_event_keys([from_event_uuid])
        for key in keys:
            list_keys_to_be_created.append(dict(event_key = key.event_key,
                                                event_uuid = to_event_uuid,
                                                visible = True,
                                                dim_signature_id = key.dim_signature_id))
        # end for
        
        return

    def _remove_deprecated_events_event_keys(self):
        """
        """
        for key in self.keys_events:
            max_generation_time = self.session.query(func.max(DimProcessing.generation_time)).join(Event).join(EventKey).filter(EventKey.event_key == key)

            event_max_generation_time = self.session.query(Event).join(DimProcessing).join(EventKey).filter(DimProcessing.generation_time == max_generation_time,
                                                                                                            EventKey.event_key == key).first()

            # Delete deprecated events
            events_uuids_to_delete = self.session.query(Event.event_uuid).join(DimProcessing).join(EventKey).filter(Event.processing_uuid != event_max_generation_time.processing_uuid,
                                                                                                                    DimProcessing.generation_time <= max_generation_time,
                                                                                                                    EventKey.event_key == key)
            self.session.query(Event).filter(Event.event_uuid == events_uuids_to_delete).delete(synchronize_session=False)

            # Make events visible
            events_uuids_to_update = self.session.query(Event.event_uuid).join(EventKey).filter(Event.processing_uuid == event_max_generation_time.processing_uuid,
                                                                                                EventKey.event_key == key)
            self.session.query(EventKey).filter(EventKey.event_uuid == events_uuids_to_update).update({"visible": True}, synchronize_session=False)
            self.session.query(Event).filter(Event.event_uuid == events_uuids_to_update).update({"visible": True}, synchronize_session=False)
        # end for

        return

    def _remove_deprecated_annotations(self):
        """
        """
        for annotation_cnf_explicit_ref in self.annotation_cnfs_explicit_refs:
            annotation_cnf = annotation_cnf_explicit_ref["annotation_cnf"]
            explicit_ref = annotation_cnf_explicit_ref["explicit_ref"]
            max_generation_time = self.session.query(func.max(DimProcessing.generation_time)) \
                                              .join(Annotation) \
                                              .join(AnnotationCnf) \
                                              .join(ExplicitRef).filter(AnnotationCnf.annotation_cnf_id == annotation_cnf.annotation_cnf_id,
                                                                        ExplicitRef.explicit_ref_id == explicit_ref.explicit_ref_id)

            annotation_max_generation_time = self.session.query(Annotation) \
                                                         .join(DimProcessing) \
                                                         .join(AnnotationCnf) \
                                                         .join(ExplicitRef).filter(DimProcessing.generation_time == max_generation_time,
                                                                                   AnnotationCnf.annotation_cnf_id == annotation_cnf.annotation_cnf_id,
                                                                                   ExplicitRef.explicit_ref_id == explicit_ref.explicit_ref_id).first()

            # Delete deprecated annotations
            annotations_uuids_to_delete = self.session.query(Annotation.annotation_uuid) \
                                                      .join(DimProcessing) \
                                                      .join(AnnotationCnf). \
                                                      join(ExplicitRef).filter(Annotation.processing_uuid != annotation_max_generation_time.processing_uuid,
                                                                               DimProcessing.generation_time <= max_generation_time,
                                                                               AnnotationCnf.annotation_cnf_id == annotation_cnf.annotation_cnf_id,
                                                                               ExplicitRef.explicit_ref_id == explicit_ref.explicit_ref_id)
            self.session.query(Annotation).filter(Annotation.annotation_uuid == annotations_uuids_to_delete).delete(synchronize_session=False)

            # Make annotations visible
            annotations_uuids_to_update = self.session.query(Annotation.annotation_uuid) \
                                                      .join(DimProcessing) \
                                                      .join(AnnotationCnf) \
                                                      .join(ExplicitRef).filter(Annotation.processing_uuid == annotation_max_generation_time.processing_uuid,
                                                                                DimProcessing.generation_time <= max_generation_time,
                                                                                AnnotationCnf.annotation_cnf_id == annotation_cnf.annotation_cnf_id,
                                                                                ExplicitRef.explicit_ref_id == explicit_ref.explicit_ref_id)
            self.session.query(Annotation).filter(Annotation.annotation_uuid == annotations_uuids_to_update).update({"visible": True}, synchronize_session=False)
        # end for

        return

