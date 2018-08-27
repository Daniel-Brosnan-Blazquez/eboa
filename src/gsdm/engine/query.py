"""
Engine definition

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
# Import python utilities
import datetime
from lxml import etree
import operator

# Import GEOalchemy entities
from geoalchemy2 import functions
from geoalchemy2.shape import to_shape

# Import datamodel
from gsdm.datamodel.base import Session, engine, Base
from gsdm.datamodel.dim_signatures import DimSignature
from gsdm.datamodel.events import Event, EventLink, EventKey, EventText, EventDouble, EventObject, EventGeometry, EventBoolean, EventTimestamp
from gsdm.datamodel.gauges import Gauge
from gsdm.datamodel.dim_processings import DimProcessing, DimProcessingStatus
from gsdm.datamodel.explicit_refs import ExplicitRef, ExplicitRefGrp, ExplicitRefLink
from gsdm.datamodel.annotations import Annotation, AnnotationCnf, AnnotationText, AnnotationDouble, AnnotationObject, AnnotationGeometry, AnnotationBoolean, AnnotationTimestamp
from sqlalchemy.dialects import postgresql

# Import logging
from gsdm.engine.logging import Log

logging = Log()
logger = logging.logger

# Import auxiliary functions
from gsdm.engine.functions import is_datetime

class Query():

    operators = {
        ">": operator.gt,
        ">=": operator.ge,
        "<": operator.lt,
        "<=": operator.le,
    }

    def __init__(self, session = None):
        """
        """
        if session == None:
            self.session = Session()
        else:
            self.session = session
        # end if
    
        return


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
            # Log that the name provided does not exist into DDBB
            logger.error("There is no dim_processing into the DDBB with name {}".format(name))
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
        events = self.get_events(processing_uuids = [dim_processing[0].processing_uuid])

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

        query = etree.SubElement(gsd, "query", mode=data["mode"])

        # Include the request
        request = etree.SubElement(query, "request", name="get_source_xml", duration=str(data["request"]["duration"]))
        if "parameters" in data["request"]:
            parameters = etree.SubElement(request, "parameters")
            for parameter in data["request"]["parameters"]:
                tag = etree.SubElement(parameters, parameter)
                tag.text = data["request"]["parameters"][parameter]
            # end for
        # end if

        # Include the DIM signature
        etree.SubElement(query, "dim_signature", name=data["dim_signature"]["name"], 
                         version=data["dim_signature"]["version"], 
                         exec=data["dim_signature"]["exec"])
        # Include the DIM processing
        etree.SubElement(query, "source", name=data["source"]["name"], 
                         generation_time=str(data["source"]["generation_time"]), 
                         validity_start=str(data["source"]["validity_start"]), 
                         validity_stop=str(data["source"]["validity_stop"]), 
                         ingestion_time=str(data["source"]["ingestion_time"]), 
                         ingestion_duration=str(data["source"]["ingestion_duration"]))

        data_xml = etree.SubElement(query, "data")

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

    def get_events(self, processing_uuids = None, explicit_ref_ids = None, gauge_ids = None, start = None, start_operator = None, stop = None, stop_operator = None):
        """
        """
        params = []
        # Allow only obtain visible events
        params.append(Event.visible == True)

        # processing_uuids
        if processing_uuids:
            if type(processing_uuids) != list:
                raise
            # end if
            params.append(Event.processing_uuid.in_(processing_uuids))
        # end if

        # explicit_ref_ids
        if explicit_ref_ids:
            if type(explicit_ref_ids) != list:
                raise
            # end if
            params.append(Event.explicit_ref_id.in_(explicit_ref_ids))
        # end if

        # start
        if (start != None and start_operator == None):
            logger.error("The start_operator has to be specified when the start value is specified")
            raise
        # end if
        if not start_operator in self.operators:
            logger.error("The start_operator is not a valid operator")
            raise
        # end if
        if start != None:
            if not is_datetime(start):
                raise
            # end if
            op = self.operators[start_operator]
            params.append(op(Event.start, start))
        # end if

        # stop
        if (stop != None and stop_operator == None):
            logger.error("The stop_operator has to be specified when the stop value is specified")
            raise
        # end if
        if not stop_operator in self.operators:
            logger.error("The stop_operator is not a valid operator")
            raise
        # end if
        if stop != None:
            if not is_datetime(stop):
                raise
            # end if
            op = self.operators[stop_operator]
            params.append(op(Event.stop, stop))
        # end if

        events = self.session.query(Event).filter(*params).all()
        return events

    def get_events_join(self, filename = None, explicit_ref = None, gauge_name = None, gauge_system = None, start_cmp = None, start_operator = None, stop_cmp = None, stop_operator = None, event_uuids = None):
        """
        """
        params = []
        # Allow only obtain visible events
        params.append(Event.visible == True)

        tables = [Event]

        # Add mandatory joined tables
        tables.append(DimProcessing)
        tables.append(Gauge)

        # Add filters
        # Event uuids
        if event_uuids != None:
            params.append(Event.event_uuid.in_(event_uuids))
        # end if

        # Source
        if filename != None:
            params.append(DimProcessing.name == filename)
        # end if

        # Explicit reference
        if explicit_ref != None:
            tables.append(ExplicitRef)
            params.append(ExplicitRef.name.like(explicit_ref))
        # end if

        # Gauge name
        if gauge_name != None:
            params.append(Gauge.name.like(gauge_name))
        # end if

        # Gauge system
        if gauge_system != None:
            params.append(Gauge.system.like(gauge_system))
        # end if

        # start
        if (start_cmp != None and start_operator == None):
            logger.error("The start_operator has to be specified when the start_cmp value is specified")
            raise
        # end if
        if start_cmp != None and not start_operator in self.operators:
            logger.error("The start_operator is not a valid operator")
            raise
        # end if
        if start_cmp != None:
            if not is_datetime(start_cmp):
                raise
            # end if
            op = self.operators[start_operator]
            params.append(op(Event.start, start_cmp))
        # end if

        # stop
        if (stop_cmp != None and stop_operator == None):
            logger.error("The stop_operator has to be specified when the stop_cmp value is specified")
            raise
        # end if
        if stop_cmp != None and not stop_operator in self.operators:
            logger.error("The stop_operator is not a valid operator")
            raise
        # end if
        if stop_cmp != None:
            if not is_datetime(stop_cmp):
                raise
            # end if
            op = self.operators[stop_operator]
            params.append(op(Event.stop, stop_cmp))
        # end if

        query = self.session.query(Event)
        for table in tables:
            if table != Event:
                query = query.join(table)
            # end if
        # end for

        events = query.filter(*params).all()
        
        # logger.debug("The following query is going to be executed: {}".format(str(query.filter(*params).statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))))

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

    def get_event_links_pointing_to_events(self, event_uuids, event_uuid_links):
        """
        """
        if type(event_uuids) != list or type(event_uuid_links) != list:
            raise
        # end if
        links = self.session.query(EventLink).filter(EventLink.event_uuid.in_(event_uuids),
                                                     EventLink.event_uuid_link.in_(event_uuid_links)).all()

        return links

    def get_event_links(self, event_uuid_links = None, event_uuids = None, link_names = None):
        """
        """
        params = []
        if event_uuid_links:
            params.append(EventLink.event_uuid_link.in_(event_uuid_links))
        # end if

        if event_uuids:
            params.append(EventLink.event_uuid.in_(event_uuids))
        # end if

        if link_names:
            params.append(EventLink.name.in_(link_names))
        # end if

        query = self.session.query(EventLink)
        links = query.filter(*params).all()

        # logger.debug("The following query is going to be executed: {}".format(str(query.filter(*params).statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))))

        return links

    def get_linked_events_join(self, filename = None, explicit_ref = None, gauge_name = None, gauge_system = None, start_cmp = None, start_operator = None, stop_cmp = None, stop_operator = None, link_names = None):
        
        # Obtain prime events 
        prime_events = self.get_events_join(filename = filename, explicit_ref = explicit_ref, gauge_name = gauge_name, gauge_system = gauge_system, start_cmp = start_cmp, start_operator = start_operator, stop_cmp = stop_cmp, stop_operator = stop_operator)

        prime_event_uuids = [str(event.__dict__["event_uuid"]) for event in prime_events]

        # Obtain the links from the prime events to other events
        links = []
        if len(prime_event_uuids) > 0:
            links = self.get_event_links(event_uuid_links = prime_event_uuids, link_names = link_names)
        # end if

        # Obtain the events linked by the prime events
        linked_event_uuids = [str(link.event_uuid) for link in links]
        linked_events = []
        if len(linked_event_uuids) > 0:
            linked_events = self.get_events_join(event_uuids = linked_event_uuids)
        # end if

        return prime_events + linked_events

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
