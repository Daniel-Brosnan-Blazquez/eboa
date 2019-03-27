"""
Engine definition

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import datetime
from datetime import timedelta
from lxml import etree
import operator
import uuid

# Import GEOalchemy entities
from geoalchemy2 import functions
from geoalchemy2.shape import to_shape

# Import datamodel
from eboa.datamodel.base import Session, engine, Base
from eboa.datamodel.dim_signatures import DimSignature
from eboa.datamodel.events import Event, EventLink, EventKey, EventText, EventDouble, EventObject, EventGeometry, EventBoolean, EventTimestamp
from eboa.datamodel.gauges import Gauge
from eboa.datamodel.sources import Source, SourceStatus
from eboa.datamodel.explicit_refs import ExplicitRef, ExplicitRefGrp, ExplicitRefLink
from eboa.datamodel.annotations import Annotation, AnnotationCnf, AnnotationText, AnnotationDouble, AnnotationObject, AnnotationGeometry, AnnotationBoolean, AnnotationTimestamp
from sqlalchemy.dialects import postgresql

# Import exceptions
from eboa.engine.errors import InputError

# Import auxiliary functions
import eboa.engine.functions as functions

# Import logging
from eboa.logging import Log

# Import query printing facilities
from eboa.engine.printing import literal_query

logging = Log()
logger = logging.logger


arithmetic_operators = {
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
    "==": operator.eq,
    "!=": operator.ne
}

text_operators = {
    "like": "like",
    "notlike": "notlike",
    "in": "in_",
    "notin": "notin_",
}

event_value_entities = {
    "text": EventText,
    "boolean": EventBoolean,
    "timestamp": EventTimestamp,
    "double": EventDouble,
    "geometry": EventGeometry,
    "object": EventObject
}

annotation_value_entities = {
    "text": AnnotationText,
    "boolean": AnnotationBoolean,
    "timestamp": AnnotationTimestamp,
    "double": AnnotationDouble,
    "geometry": AnnotationGeometry,
    "object": AnnotationObject
}

def log_query(query):

    logger.debug("The following query is going to be executed: {}".format(literal_query(query.statement)))

    return

class Query():

    def __init__(self, session = None):
        """
        Class for querying the information stored into DDBB

        :param session: opened session
        :type session: sqlalchemy.orm.sessionmaker
        """
        if session == None:
            self.session = Session()
        else:
            self.session = session
        # end if
    
        return

    def clear_db(self):
        for table in reversed(Base.metadata.sorted_tables):
            engine.execute(table.delete())
        # end for

    def get_dim_signatures(self, dim_signature_uuids = None, dim_signatures = None):
        """
        Method to obtain the DIM signature entities filtered by the received parameters

        :param dim_signature_uuids: list of DIM signature identifiers
        :type dim_signature_uuids: text_filter
        :param dim_signatures: DIM signature filters
        :type dim_signatures: text_filter

        :return: found DIM signatures
        :rtype: list
        """
        params = []
        
        # DIM signature UUIDs
        if dim_signature_uuids != None:
            functions.is_valid_text_filter(dim_signature_uuids)
            filter = eval('DimSignature.dim_signature_uuid.' + text_operators[dim_signature_uuids["op"]])
            params.append(filter(dim_signature_uuids["filter"]))
        # end if

        # DIM signatures
        if dim_signatures != None:
            functions.is_valid_text_filter(dim_signatures)
            filter = eval('DimSignature.dim_signature.' + text_operators[dim_signatures["op"]])
            params.append(filter(dim_signatures["filter"]))
        # end if

        query = self.session.query(DimSignature).filter(*params)
        log_query(query)
        dim_signatures = query.all()

        return dim_signatures

    def get_sources(self, names = None, validity_start_filters = None, validity_stop_filters = None, generation_time_filters = None, ingestion_time_filters = None, ingestion_duration_filters = None, processors = None, processor_version_filters = None, dim_signature_uuids = None, source_uuids = None, dim_signatures = None, statuses = None):
        """
        Method to obtain the sources entities filtered by the received parameters

        :param source_uuids: list of source identifiers
        :type source_uuids: text_filter
        :param names: source name filters
        :type names: text_filter
        :param validity_start_filters: list of start filters
        :type validity_start_filters: date_filters
        :param validity_stop_filters: list of stop filters
        :type validity_stop_filters: date_filters
        :param generation_time_filters: list of generation time filters
        :type generation_time_filters: date_filters
        :param ingestion_time_filters: list of ingestion time filters
        :type ingestion_time_filters: date_filters
        :param ingestion_duration_filters: list of ingestion duration filters
        :type ingestion_duration_filters: float_filters
        :param processor_version_filters: list of version filters
        :type processor_version_filters: string_filter
        :param procesors: processor filters
        :type procesors: text_filter
        :param dim_signature_uuids: list of DIM signature identifiers
        :type dim_signature_uuids: text_filter
        :param source_uuids: list of source identifiers
        :type source_uuids: text_filter
        :param dim_signatures: DIM signature filters
        :type dim_signatures: text_filter
        :param statuses: status filters
        :type statuses: float_filter

        :return: found sources
        :rtype: list
        """
        params = []
        tables = []
        # DIM signature UUIDs
        if dim_signature_uuids != None:
            functions.is_valid_text_filter(dim_signature_uuids)
            filter = eval('Source.dim_signature_uuid.' + text_operators[dim_signature_uuids["op"]])
            params.append(filter(dim_signature_uuids["filter"]))
        # end if

        # Source UUIDs
        if source_uuids != None:
            functions.is_valid_text_filter(source_uuids)
            filter = eval('Source.source_uuid.' + text_operators[source_uuids["op"]])
            params.append(filter(source_uuids["filter"]))
        # end if

        # Source names
        if names != None:
            functions.is_valid_text_filter(names)
            filter = eval('Source.name.' + text_operators[names["op"]])
            params.append(filter(names["filter"]))
        # end if

        # validity_start filters
        if validity_start_filters != None:
            functions.is_valid_date_filters(validity_start_filters, arithmetic_operators)
            for validity_start_filter in validity_start_filters:
                op = arithmetic_operators[validity_start_filter["op"]]
                params.append(op(Source.validity_start, validity_start_filter["date"]))
            # end for
        # end if

        # validity_stop filters
        if validity_stop_filters != None:
            functions.is_valid_date_filters(validity_stop_filters, arithmetic_operators)
            for validity_stop_filter in validity_stop_filters:
                op = arithmetic_operators[validity_stop_filter["op"]]
                params.append(op(Source.validity_stop, validity_stop_filter["date"]))
            # end for
        # end if

        # generation_time filters
        if generation_time_filters != None:
            functions.is_valid_date_filters(generation_time_filters, arithmetic_operators)
            for generation_time_filter in generation_time_filters:
                op = arithmetic_operators[generation_time_filter["op"]]
                params.append(op(Source.generation_time, generation_time_filter["date"]))
            # end for
        # end if

        # ingestion_time filters
        if ingestion_time_filters != None:
            functions.is_valid_date_filters(ingestion_time_filters, arithmetic_operators)
            for ingestion_time_filter in ingestion_time_filters:
                op = arithmetic_operators[ingestion_time_filter["op"]]
                params.append(op(Source.ingestion_time, ingestion_time_filter["date"]))
            # end for
        # end if

        # ingestion_duration filters
        if ingestion_duration_filters != None:
            functions.is_valid_float_filters(ingestion_duration_filters, arithmetic_operators)
            for ingestion_duration_filter in ingestion_duration_filters:
                op = arithmetic_operators[ingestion_duration_filter["op"]]
                params.append(op(Source.ingestion_duration, timedelta(seconds = ingestion_duration_filter["float"])))
            # end for
        # end if

        # Processors
        if processors != None:
            functions.is_valid_text_filter(processors)
            filter = eval('Source.processor.' + text_operators[processors["op"]])
            params.append(filter(processors["filter"]))
        # end if

        # processor_version filters
        if processor_version_filters != None:
            functions.is_valid_string_filters(processor_version_filters, arithmetic_operators)
            for processor_version_filter in processor_version_filters:
                op = arithmetic_operators[processor_version_filter["op"]]
                params.append(op(Source.processor_version, processor_version_filter["str"]))
            # end for
        # end if

        # status filters
        if statuses != None:
            functions.is_valid_float_filters(statuses, arithmetic_operators)
            for status in statuses:
                op = arithmetic_operators[status["op"]]
                params.append(op(SourceStatus.status, status["float"]))
            # end for
        # end if

        # DIM signatures
        if dim_signatures != None:
            functions.is_valid_text_filter(dim_signatures)
            filter = eval('DimSignature.dim_signature.' + text_operators[dim_signatures["op"]])
            params.append(filter(dim_signatures["filter"]))
            tables.append(DimSignature)
        # end if

        query = self.session.query(Source).filter(*params)
        log_query(query)
        sources = query.all()

        return sources

    def get_gauges(self, gauge_uuids = None, names = None, systems = None, dim_signature_uuids = None, dim_signatures = None):
        """
        Method to obtain the gauges entities filtered by the received parameters

        :param gauge_uuids: list of gauge identifiers
        :type gauge_uuids: text_filter
        :param names: gauge name filters
        :type names: text_filter
        :param systems: gauge system filters
        :type systems: text_filter
        :param dim_signature_uuids: list of DIM signature identifiers
        :type dim_signature_uuids: text_filter
        :param dim_signatures: DIM signature filters
        :type dim_signatures: text_filter

        :return: found gauges
        :rtype: list
        """
        params = []
        tables = []

        # Gauge UUIDs
        if gauge_uuids != None:
            functions.is_valid_text_filter(gauge_uuids)
            filter = eval('Gauge.gauge_uuid.' + text_operators[gauge_uuids["op"]])
            params.append(filter(gauge_uuids["filter"]))
        # end if

        # Gauge names
        if names != None:
            functions.is_valid_text_filter(names)
            filter = eval('Gauge.name.' + text_operators[names["op"]])
            params.append(filter(names["filter"]))
        # end if

        # Gauge systems
        if systems != None:
            functions.is_valid_text_filter(systems)
            filter = eval('Gauge.system.' + text_operators[systems["op"]])
            params.append(filter(systems["filter"]))
        # end if

        # DIM signature UUIDs
        if dim_signature_uuids != None:
            functions.is_valid_text_filter(dim_signature_uuids)
            filter = eval('Gauge.dim_signature_uuid.' + text_operators[dim_signature_uuids["op"]])
            params.append(filter(dim_signature_uuids["filter"]))
        # end if

        # DIM signatures
        if dim_signatures != None:
            functions.is_valid_text_filter(dim_signatures)
            filter = eval('DimSignature.dim_signature.' + text_operators[dim_signatures["op"]])
            params.append(filter(dim_signatures["filter"]))
            tables.append(DimSignature)
        # end if
        
        query = self.session.query(Gauge).filter(*params)
        log_query(query)
        gauges = query.all()

        return gauges

    def prepare_query_values(self, value_filters, value_entities, params, tables = None):
        # value filters
        if value_filters != None:
            functions.is_valid_value_filters(value_filters, arithmetic_operators, text_operators)
            for value_filter in value_filters:
                # Type
                value_type = value_entities[value_filter["type"]]
                if tables != None:
                    tables.append(value_type)
                # end if

                # Name
                value_name = value_filter["name"]["str"]
                op_name = eval("value_type.name." + text_operators[value_filter["name"]["op"]])
                params.append(op_name(value_name))
                
                # Value
                if "value" in value_filter:
                    value = value_filter["value"]
                    if value["op"] in arithmetic_operators.keys():
                        op = arithmetic_operators[value["op"]]
                        params.append(op(value_type.value, value["value"]))
                    else:
                        op = eval("value_type.value." + text_operators[value["op"]])
                        params.append(op(value["value"]))
                    # end if
                # end if
            # end for
        # end if
    # end def

    def get_events(self, event_uuids = None, start_filters = None, stop_filters = None, ingestion_time_filters = None, value_filters = None, gauge_uuids = None, source_uuids = None, explicit_ref_uuids = None, sources = None, explicit_refs = None, gauge_names = None, gauge_systems = None, keys = None, limit = None):
        """
        """
        params = []
        # Allow only obtain visible events
        params.append(Event.visible == True)

        tables = []

        # event_uuids
        if event_uuids != None:
            functions.is_valid_text_filter(event_uuids)
            filter = eval('Event.event_uuid.' + text_operators[event_uuids["op"]])
            params.append(filter(event_uuids["filter"]))
        # end if

        # source_uuids
        if source_uuids != None:
            functions.is_valid_text_filter(source_uuids)
            filter = eval('Event.source_uuid.' + text_operators[source_uuids["op"]])
            params.append(filter(source_uuids["filter"]))
        # end if

        # explicit_ref_uuids
        if explicit_ref_uuids != None:
            functions.is_valid_text_filter(explicit_ref_uuids)
            filter = eval('Event.explicit_ref_uuid.' + text_operators[explicit_ref_uuids["op"]])
            params.append(filter(explicit_ref_uuids["filter"]))
        # end if

        # gauge_uuids
        if gauge_uuids != None:
            functions.is_valid_text_filter(gauge_uuids)
            filter = eval('Event.gauge_uuid.' + text_operators[gauge_uuids["op"]])
            params.append(filter(gauge_uuids["filter"]))
        # end if

        # Sources
        if sources != None:
            functions.is_valid_text_filter(sources)
            filter = eval('Source.name.' + text_operators[sources["op"]])
            params.append(filter(sources["filter"]))
            tables.append(Source)
        # end if

        # Explicit references
        if explicit_refs != None:
            functions.is_valid_text_filter(explicit_refs)
            filter = eval('ExplicitRef.explicit_ref.' + text_operators[explicit_refs["op"]])
            params.append(filter(explicit_refs["filter"]))
            tables.append(ExplicitRef)
        # end if

        # Gauge names
        if gauge_names != None:
            functions.is_valid_text_filter(gauge_names)
            filter = eval('Gauge.name.' + text_operators[gauge_names["op"]])
            params.append(filter(gauge_names["filter"]))
            tables.append(Gauge)
        # end if

        # Gauge systems
        if gauge_systems != None:
            functions.is_valid_text_filter(gauge_systems)
            filter = eval('Gauge.system.' + text_operators[gauge_systems["op"]])
            params.append(filter(gauge_systems["filter"]))
            tables.append(Gauge)
        # end if

        # keys
        if keys != None:
            functions.is_valid_text_filter(keys)
            filter = eval('EventKey.event_key.' + text_operators[keys["op"]])
            params.append(filter(keys["filter"]))
        # end if

        # start filters
        if start_filters != None:
            functions.is_valid_date_filters(start_filters, arithmetic_operators)
            for start_filter in start_filters:
                op = arithmetic_operators[start_filter["op"]]
                params.append(op(Event.start, start_filter["date"]))
            # end for
        # end if

        # stop filters
        if stop_filters != None:
            functions.is_valid_date_filters(stop_filters, arithmetic_operators)
            for stop_filter in stop_filters:
                op = arithmetic_operators[stop_filter["op"]]
                params.append(op(Event.stop, stop_filter["date"]))
            # end for
        # end if

        # ingestion_time filters
        if ingestion_time_filters != None:
            functions.is_valid_date_filters(ingestion_time_filters, arithmetic_operators)
            for ingestion_time_filter in ingestion_time_filters:
                op = arithmetic_operators[ingestion_time_filter["op"]]
                params.append(op(Event.ingestion_time, ingestion_time_filter["date"]))
            # end for
        # end if

        # value filters
        self.prepare_query_values(value_filters, event_value_entities, params, tables)

        query = self.session.query(Event)
        for table in set(tables):
            query = query.join(table)
        # end for

        logger.debug(tables)

        query = query.filter(*params)

        if limit != None:
            query = query.limit(limit)
        # end if

        log_query(query)
        events = query.all()

        return events

    def get_event_keys(self, event_uuids = None, dim_signature_uuids = None, keys = None):
        """
        """
        params = []
        
        # DIM signature UUIDs
        if dim_signature_uuids != None:
            functions.is_valid_text_filter(dim_signature_uuids)
            filter = eval('EventKey.dim_signature_uuid.' + text_operators[dim_signature_uuids["op"]])
            params.append(filter(dim_signature_uuids["filter"]))
        # end if

        # event_uuids
        if event_uuids != None:
            functions.is_valid_text_filter(event_uuids)
            filter = eval('EventKey.event_uuid.' + text_operators[event_uuids["op"]])
            params.append(filter(event_uuids["filter"]))
        # end if

        # keys
        if keys != None:
            functions.is_valid_text_filter(keys)
            filter = eval('EventKey.event_key.' + text_operators[keys["op"]])
            params.append(filter(keys["filter"]))
        # end if

        query = self.session.query(EventKey).filter(*params)
        log_query(query)
        event_keys = query.all()

        return event_keys

    def get_event_links(self, event_uuid_links = None, event_uuids = None, link_names = None):
        """
        """
        params = []
        if event_uuid_links:
            functions.is_valid_text_filter(event_uuid_links)
            filter = eval('EventLink.event_uuid_link.' + text_operators[event_uuid_links["op"]])
            params.append(filter(event_uuid_links["filter"]))
        # end if

        if event_uuids:
            functions.is_valid_text_filter(event_uuids)
            filter = eval('EventLink.event_uuid.' + text_operators[event_uuids["op"]])
            params.append(filter(event_uuids["filter"]))
        # end if

        if link_names:
            functions.is_valid_text_filter(link_names)
            filter = eval('EventLink.name.' + text_operators[link_names["op"]])
            params.append(filter(link_names["filter"]))
        # end if

        query = self.session.query(EventLink).filter(*params)
        log_query(query)
        links = query.all()

        return links

    def get_linked_events(self, event_uuids = None, source_uuids = None, explicit_ref_uuids = None, gauge_uuids = None, start_filters = None, stop_filters = None, link_names = None, sources = None, explicit_refs = None, gauge_names = None, gauge_systems = None, value_filters = None, return_prime_events = True, keys = None, back_ref = False):
        
        # Obtain prime events 
        prime_events = self.get_events(event_uuids = event_uuids, source_uuids = source_uuids, explicit_ref_uuids = explicit_ref_uuids, gauge_uuids = gauge_uuids, sources = sources, explicit_refs = explicit_refs, gauge_names = gauge_names, gauge_systems = gauge_systems, keys = keys, start_filters = start_filters, stop_filters = stop_filters, value_filters = value_filters)

        prime_event_uuids = [str(event.__dict__["event_uuid"]) for event in prime_events]

        # Obtain the links from the prime events to other events
        links = []
        if len(prime_event_uuids) > 0:
            links = self.get_event_links(event_uuid_links = {"filter": prime_event_uuids, "op": "in"}, link_names = link_names)
        # end if

        # Obtain the events linked by the prime events
        linked_event_uuids = [str(link.event_uuid) for link in links]
        linked_events = []
        if len(linked_event_uuids) > 0:
            linked_events = self.get_events(event_uuids = {"filter": linked_event_uuids, "op": "in"})
        # end if
        
        events = {}
        if return_prime_events:
            events["prime_events"] = prime_events
        # end if
        events["linked_events"] = linked_events

        if back_ref:
            # Obtain the events linking the prime events
            links = self.get_event_links(event_uuids = {"filter": prime_event_uuids, "op": "in"})
            event_linking_uuids = [str(link.event_uuid_link) for link in links]
            events_linking = []
            if len(event_linking_uuids) > 0:
                events_linking = self.get_events(event_uuids = {"filter": event_linking_uuids, "op": "in"})
            # end if

            events["events_linking"] = events_linking
        # end if

        return events

    def get_linked_events_details(self, event_uuid, return_prime_events = True, back_ref = False):

        if type(event_uuid) != uuid.UUID:
            raise InputError("The parameter event_uuid has to be specified and must be a UUID (received event_uuid: {}).".format(event_uuid))
        # end if

        events = {}
        if return_prime_events:
            events["prime_events"] = self.get_events(event_uuids = {"filter": [event_uuid], "op": "in"})
        # end if

        # Obtain the links from the prime events to other events
        links = self.get_event_links(event_uuid_links = {"filter": [event_uuid], "op": "in"})

        # Obtain the events linked by the prime events
        linked_event_uuids = [str(link.event_uuid) for link in links]
        linked_events = []
        if len(linked_event_uuids) > 0:
            linked_events = self.get_events(event_uuids = {"filter": linked_event_uuids, "op": "in"})
        # end if

        events["linked_events"] = []
        for event in linked_events:
            link_name = [str(link.name) for link in links if link.event_uuid == event.event_uuid][0]
            events["linked_events"].append({"link_name": link_name,
                                            "event": event})
        # end for
        
        if back_ref:
            # Obtain the events linking the prime events
            links = self.get_event_links(event_uuids = {"filter": [event_uuid], "op": "in"})
            event_linking_uuids = [str(link.event_uuid_link) for link in links]
            events_linking = []
            if len(event_linking_uuids) > 0:
                events_linking = self.get_events(event_uuids = {"filter": event_linking_uuids, "op": "in"})
            # end if

            events["events_linking"] = []
            for event in events_linking:
                link_name = [str(link.name) for link in links if link.event_uuid_link == event.event_uuid][0]
                events["events_linking"].append({"link_name": link_name,
                                                "event": event})
            # end for
        # end if

        return events

    def get_annotation_cnfs(self, dim_signature_uuids = None, annotation_cnf_uuids = None, names = None, systems = None, dim_signatures = None):
        """
        """
        params = []
        tables = []
        # DIM signature UUIDs
        if dim_signature_uuids != None:
            functions.is_valid_text_filter(dim_signature_uuids)
            filter = eval('AnnotationCnf.dim_signature_uuid.' + text_operators[dim_signature_uuids["op"]])
            params.append(filter(dim_signature_uuids["filter"]))
        # end if
        # DIM signatures
        if dim_signatures != None:
            functions.is_valid_text_filter(dim_signatures)
            filter = eval('DimSignature.dim_signature.' + text_operators[dim_signatures["op"]])
            params.append(filter(dim_signatures["filter"]))
            tables.append(DimSignature)
        # end if

        # AnnotationCnf UUIDs
        if annotation_cnf_uuids != None:
            functions.is_valid_text_filter(annotation_cnf_uuids)
            filter = eval('AnnotationCnf.annotation_cnf_uuid.' + text_operators[annotation_cnf_uuids["op"]])
            params.append(filter(annotation_cnf_uuids["filter"]))
        # end if

        # AnnotationCnf names
        if names != None:
            functions.is_valid_text_filter(names)
            filter = eval('AnnotationCnf.name.' + text_operators[names["op"]])
            params.append(filter(names["filter"]))
        # end if

        # AnnotationCnf systems
        if systems != None:
            functions.is_valid_text_filter(systems)
            filter = eval('AnnotationCnf.system.' + text_operators[systems["op"]])
            params.append(filter(systems["filter"]))
        # end if
        
        query = self.session.query(AnnotationCnf).filter(*params)
        log_query(query)
        annotation_cnfs = query.all()

        return annotation_cnfs

    def get_annotations(self, source_uuids = None, explicit_ref_uuids = None, annotation_cnf_uuids = None, ingestion_time_filters = None, annotation_uuids = None, sources = None, explicit_refs = None, annotation_cnf_names = None, annotation_cnf_systems = None, value_filters = None):
        """
        """
        params = []
        tables = []
        # Allow only obtain visible annotations
        params.append(Annotation.visible == True)

        # source_uuids
        if source_uuids != None:
            functions.is_valid_text_filter(source_uuids)
            filter = eval('Annotation.source_uuid.' + text_operators[source_uuids["op"]])
            params.append(filter(source_uuids["filter"]))
        # end if

        # explicit_ref_uuids
        if explicit_ref_uuids != None:
            functions.is_valid_text_filter(explicit_ref_uuids)
            filter = eval('Annotation.explicit_ref_uuid.' + text_operators[explicit_ref_uuids["op"]])
            params.append(filter(explicit_ref_uuids["filter"]))
        # end if

        # annotation_cnf_uuids
        if annotation_cnf_uuids != None:
            functions.is_valid_text_filter(annotation_cnf_uuids)
            filter = eval('Annotation.annotation_cnf_uuid.' + text_operators[annotation_cnf_uuids["op"]])
            params.append(filter(annotation_cnf_uuids["filter"]))
        # end if

        # annotation_uuids
        if annotation_uuids != None:
            functions.is_valid_text_filter(annotation_uuids)
            filter = eval('Annotation.annotation_uuid.' + text_operators[annotation_uuids["op"]])
            params.append(filter(annotation_uuids["filter"]))
        # end if

        # ingestion_time filters
        if ingestion_time_filters != None:
            functions.is_valid_date_filters(ingestion_time_filters, arithmetic_operators)
            for ingestion_time_filter in ingestion_time_filters:
                op = arithmetic_operators[ingestion_time_filter["op"]]
                params.append(op(Annotation.ingestion_time, ingestion_time_filter["date"]))
            # end for
        # end if

        # value filters
        self.prepare_query_values(value_filters, annotation_value_entities, params, tables)

        query = self.session.query(Annotation).filter(*params)
        log_query(query)
        annotations = query.all()

        return annotations

    def get_explicit_refs(self, group_ids = None, explicit_ref_uuids = None, explicit_ref_ingestion_time_filters = None, explicit_refs = None, groups = None, sources = None, source_uuids = None, event_uuids = None, gauge_names = None, gauge_systems = None, gauge_uuids = None, start_filters = None, stop_filters = None, event_ingestion_time_filters = None, event_value_filters = None, keys = None, annotation_ingestion_time_filters = None, annotation_uuids = None, annotation_cnf_names = None, annotation_cnf_systems = None, annotation_cnf_uuids = None, annotation_value_filters = None):
        """
        """
        params = []

        tables = []

        # group_ids
        if group_ids != None:
            functions.is_valid_text_filter(group_ids)
            filter = eval('ExplicitRef.expl_ref_cnf_uuid.' + text_operators[group_ids["op"]])
            params.append(filter(group_ids["filter"]))
        # end if

        # explicit_ref_uuids
        if explicit_ref_uuids != None:
            functions.is_valid_text_filter(explicit_ref_uuids)
            filter = eval('ExplicitRef.explicit_ref_uuid.' + text_operators[explicit_ref_uuids["op"]])
            params.append(filter(explicit_ref_uuids["filter"]))
        # end if

        # Explicit references
        if explicit_refs != None:
            functions.is_valid_text_filter(explicit_refs)
            filter = eval('ExplicitRef.explicit_ref.' + text_operators[explicit_refs["op"]])
            params.append(filter(explicit_refs["filter"]))
        # end if

        # Groups
        if groups != None:
            functions.is_valid_text_filter(groups)
            filter = eval('ExplicitRefGrp.name.' + text_operators[groups["op"]])
            params.append(filter(groups["filter"]))
            tables.append(ExplicitRefGrp)
        # end if

        # explicit references ingestion_time filters
        if explicit_ref_ingestion_time_filters != None:
            functions.is_valid_date_filters(explicit_ref_ingestion_time_filters, arithmetic_operators)
            for ingestion_time_filter in explicit_ref_ingestion_time_filters:
                op = arithmetic_operators[ingestion_time_filter["op"]]
                params.append(op(ExplicitRef.ingestion_time, ingestion_time_filter["date"]))
            # end for
        # end if

        # Events
        explicit_ref_uuids_events = []
        if event_uuids or start_filters or stop_filters or event_ingestion_time_filters or event_value_filters or gauge_uuids or source_uuids or explicit_ref_uuids or sources or explicit_refs or gauge_names or gauge_systems or keys:
            events = self.get_events(event_uuids = event_uuids, start_filters = start_filters, stop_filters = stop_filters, ingestion_time_filters = event_ingestion_time_filters, value_filters = event_value_filters, gauge_uuids = gauge_uuids, source_uuids = source_uuids, explicit_ref_uuids = explicit_ref_uuids, sources = sources, explicit_refs = explicit_refs, gauge_names = gauge_names, gauge_systems = gauge_systems, keys = keys)

            explicit_ref_uuids_events = [str(event.explicit_ref_uuid) for event in events]
        # end if

        # Annotations
        explicit_ref_uuids_annotations = []
        if source_uuids or explicit_ref_uuids or annotation_cnf_uuids or annotation_ingestion_time_filters or annotation_uuids or sources or explicit_refs or annotation_cnf_names or annotation_cnf_systems or annotation_value_filters:
            annotations = self.get_annotations(source_uuids = source_uuids, explicit_ref_uuids = explicit_ref_uuids, annotation_cnf_uuids = annotation_cnf_uuids, ingestion_time_filters = annotation_ingestion_time_filters, annotation_uuids = annotation_uuids, sources = sources, explicit_refs = explicit_refs, annotation_cnf_names = annotation_cnf_names, annotation_cnf_systems = annotation_cnf_systems, value_filters = annotation_value_filters)

            explicit_ref_uuids_annotations = [str(annotation.explicit_ref_uuid) for annotation in annotations]
        # end if

        # explicit references uuids
        explicit_ref_uuids = set (explicit_ref_uuids_events + explicit_ref_uuids_annotations)
        if len(explicit_ref_uuids) > 0:
            params.append(ExplicitRef.explicit_ref_uuid.in_(explicit_ref_uuids))
        # end if

        query = self.session.query(ExplicitRef)
        for table in set(tables):
            query = query.join(table)
        # end for

        query = query.filter(*params)
        log_query(query)
        explicit_refs = query.all()

        return explicit_refs

    def get_explicit_ref_links(self, explicit_ref_uuid_links = None, explicit_ref_uuids = None, link_names = None):
        """
        """
        params = []
        if explicit_ref_uuid_links:
            functions.is_valid_text_filter(explicit_ref_uuid_links)
            filter = eval('ExplicitRefLink.explicit_ref_uuid_link.' + text_operators[explicit_ref_uuid_links["op"]])
            params.append(filter(explicit_ref_uuid_links["filter"]))
        # end if

        if explicit_ref_uuids:
            functions.is_valid_text_filter(explicit_ref_uuids)
            filter = eval('ExplicitRefLink.explicit_ref_uuid.' + text_operators[explicit_ref_uuids["op"]])
            params.append(filter(explicit_ref_uuids["filter"]))
        # end if

        if link_names:
            functions.is_valid_text_filter(link_names)
            filter = eval('ExplicitRefLink.name.' + text_operators[link_names["op"]])
            params.append(filter(link_names["filter"]))
        # end if

        query = self.session.query(ExplicitRefLink).filter(*params)
        log_query(query)
        links = query.all()

        return links

    def get_linked_explicit_refs(self, group_ids = None, explicit_ref_uuids = None, explicit_refs = None, ingestion_time_filters = None, link_names = None, groups = None, return_prime_explicit_refs = True, back_ref = False):
        
        # Obtain prime explicit_refs 
        prime_explicit_refs = self.get_explicit_refs(group_ids = group_ids, groups = groups, explicit_ref_uuids = explicit_ref_uuids, explicit_refs = explicit_refs, explicit_ref_ingestion_time_filters = ingestion_time_filters)

        prime_explicit_ref_uuids = [str(explicit_ref.explicit_ref_uuid) for explicit_ref in prime_explicit_refs]

        # Obtain the links from the prime explicit_refs to other explicit_refs
        links = []
        if len(prime_explicit_ref_uuids) > 0:
            links = self.get_explicit_ref_links(explicit_ref_uuid_links = {"filter": prime_explicit_ref_uuids, "op": "in"}, link_names = link_names)
        # end if

        # Obtain the explicit_refs linked by the prime explicit_refs
        linked_explicit_ref_uuids = [str(link.explicit_ref_uuid) for link in links]
        linked_explicit_refs = []
        if len(linked_explicit_ref_uuids) > 0:
            linked_explicit_refs = self.get_explicit_refs(explicit_ref_uuids = {"filter": linked_explicit_ref_uuids, "op": "in"})
        # end if
        
        explicit_refs = {}
        if return_prime_explicit_refs:
            explicit_refs["prime_explicit_refs"] = prime_explicit_refs
        # end if
        explicit_refs["linked_explicit_refs"] = linked_explicit_refs

        if back_ref:
            # Obtain the explicit_refs linking the prime explicit_refs
            links = self.get_explicit_ref_links(explicit_ref_uuids = {"filter": prime_explicit_ref_uuids, "op": "in"})
            explicit_ref_linking_uuids = [str(link.explicit_ref_uuid_link) for link in links]
            explicit_refs_linking = []
            if len(explicit_ref_linking_uuids) > 0:
                explicit_refs_linking = self.get_explicit_refs(explicit_ref_uuids = {"filter": explicit_ref_linking_uuids, "op": "in"})
            # end if

            explicit_refs["explicit_refs_linking"] = explicit_refs_linking
        # end if

        return explicit_refs

    def get_linked_explicit_refs_details(self, explicit_ref_uuid, return_prime_explicit_refs = True, back_ref = False):

        if type(explicit_ref_uuid) != uuid.UUID:
            raise InputError("The parameter explicit_ref_uuid has to be specified and must be a UUID (received explicit_ref_uuid: {}).".format(explicit_ref_uuid))
        # end if

        explicit_refs = {}
        if return_prime_explicit_refs:
            explicit_refs["prime_explicit_refs"] = self.get_explicit_refs(explicit_ref_uuids = {"filter": [explicit_ref_uuid], "op": "in"})
        # end if

        # Obtain the links from the prime explicit_refs to other explicit_refs
        links = self.get_explicit_ref_links(explicit_ref_uuid_links = {"filter": [explicit_ref_uuid], "op": "in"})

        # Obtain the explicit_refs linked by the prime explicit_refs
        linked_explicit_ref_uuids = [str(link.explicit_ref_uuid) for link in links]
        linked_explicit_refs = []
        if len(linked_explicit_ref_uuids) > 0:
            linked_explicit_refs = self.get_explicit_refs(explicit_ref_uuids = {"filter": linked_explicit_ref_uuids, "op": "in"})
        # end if

        explicit_refs["linked_explicit_refs"] = []
        for explicit_ref in linked_explicit_refs:
            link_name = [str(link.name) for link in links if link.explicit_ref_uuid == explicit_ref.explicit_ref_uuid][0]
            explicit_refs["linked_explicit_refs"].append({"link_name": link_name,
                                            "explicit_ref": explicit_ref})
        # end for
        
        if back_ref:
            # Obtain the explicit_refs linking the prime explicit_refs
            links = self.get_explicit_ref_links(explicit_ref_uuids = {"filter": [explicit_ref_uuid], "op": "in"})
            explicit_ref_linking_uuids = [str(link.explicit_ref_uuid_link) for link in links]
            explicit_refs_linking = []
            if len(explicit_ref_linking_uuids) > 0:
                explicit_refs_linking = self.get_explicit_refs(explicit_ref_uuids = {"filter": explicit_ref_linking_uuids, "op": "in"})
            # end if

            explicit_refs["explicit_refs_linking"] = []
            for explicit_ref in explicit_refs_linking:
                link_name = [str(link.name) for link in links if link.explicit_ref_uuid_link == explicit_ref.explicit_ref_uuid][0]
                explicit_refs["explicit_refs_linking"].append({"link_name": link_name,
                                                "explicit_ref": explicit_ref})
            # end for
        # end if

        return explicit_refs

    def get_explicit_refs_groups(self, group_ids = None, names = None):
        """
        """
        params = []
        # Group UUIDs
        if group_ids != None:
            functions.is_valid_text_filter(group_ids)
            filter = eval('ExplicitRefGrp.expl_ref_cnf_uuid.' + text_operators[group_ids["op"]])
            params.append(filter(group_ids["filter"]))
        # end if

        # Gauge names
        if names != None:
            functions.is_valid_text_filter(names)
            filter = eval('ExplicitRefGrp.name.' + text_operators[names["op"]])
            params.append(filter(names["filter"]))
        # end if
        
        query = self.session.query(ExplicitRefGrp).filter(*params)
        log_query(query)
        expl_groups = query.all()

        return expl_groups

    def get_event_values(self, event_uuids = None):
        """
        """
        values = []
        for value_class in [EventText, EventDouble, EventObject, EventGeometry, EventBoolean, EventTimestamp]:
            if event_uuids:
                if type(event_uuids) != list:
                    raise InputError("The parameter event_uuids must be a list of UUIDs.")
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
                raise InputError("The parameter event_uuids must be a list of UUIDs.")
            # end if
            values = self.session.query(value_class).filter(value_class.event_uuid.in_(event_uuids)).all()
        else:
            values = self.session.query(value_class).all()
        # end if
        return values

    def get_event_values_interface(self, value_type, value_filters = None, event_uuids = None):
        """
        """
        params = []
        # event_uuids
        if event_uuids != None:
            functions.is_valid_text_filter(event_uuids)
            filter = eval('event_value_entities[value_type].event_uuid.' + text_operators[event_uuids["op"]])
            params.append(filter(event_uuids["filter"]))
        # end if

        # value filters
        self.prepare_query_values(value_filters, event_value_entities, params)

        query = self.session.query(event_value_entities[value_type])

        query = query.filter(*params)
        log_query(query)
        values = query.all()

        return values

    def get_annotation_values(self, annotation_uuids = None):
        """
        """
        values = []
        for value_class in [AnnotationText, AnnotationDouble, AnnotationObject, AnnotationGeometry, AnnotationBoolean, AnnotationTimestamp]:
            if annotation_uuids:
                if type(annotation_uuids) != list:
                    raise InputError("The parameter annotation_uuids must be a list of UUIDs.")
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
                raise InputError("The parameter annotation_uuids must be a list of UUIDs.")
            # end if
            values = self.session.query(value_class).filter(value_class.annotation_uuid.in_(annotation_uuids)).all()
        else:
            values = self.session.query(value_class).all()
        # end if
        return values

    def get_annotation_values_interface(self, value_type, value_filters = None, values_names_type = None, values_name_type_like = None, annotation_uuids = None):
        """
        """
        params = []
        # annotation_uuids
        if annotation_uuids != None:
            functions.is_valid_text_filter(annotation_uuids)
            filter = eval('annotation_value_entities[value_type].annotation_uuid.' + text_operators[annotation_uuids["op"]])
            params.append(filter(annotation_uuids["filter"]))
        # end if

        # value filters
        self.prepare_query_values(value_filters, annotation_value_entities, params)

        query = self.session.query(annotation_value_entities[value_type])

        query = query.filter(*params)
        log_query(query)
        values = query.all()

        return values

    def close_session (self):
        """
        Method to close the session
        """
        self.session.close()
        return
