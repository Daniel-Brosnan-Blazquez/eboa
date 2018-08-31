"""
Engine definition

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
# Import python utilities
import datetime
from datetime import timedelta
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

# Import exceptions
from .errors import InputError

# Import auxiliary functions
from gsdm.engine.functions import is_datetime, is_valid_date_filters, is_valid_float_filters, is_valid_string_filters, is_valid_value_filters, is_valid_values_names_type, is_valid_values_name_type_like, is_valid_operator_list, is_valid_operator_like

# Import logging
from gsdm.logging import Log

# Import query printing facilities
from gsdm.engine.printing import literal_query

logging = Log()
logger = logging.logger

def log_query(query):

    logger.debug("The following query is going to be executed: {}".format(literal_query(query.statement)))

    return

class Query():

    operators = {
        ">": operator.gt,
        ">=": operator.ge,
        "<": operator.lt,
        "<=": operator.le,
        "==": operator.eq,
        "!=": operator.ne
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

    def __init__(self, session = None):
        """
        """
        if session == None:
            self.session = Session()
        else:
            self.session = session
        # end if
    
        return

    def get_dim_signatures(self, dim_signature_ids = None, dim_signatures = None, dim_signature_like = None, dim_exec_names = None, dim_exec_name_like = None):
        """
        """
        params = []
        
        # DIM signature UUIDs
        if dim_signature_ids != None:
            is_valid_operator_list(dim_signature_ids)
            filter = eval('DimSignature.dim_signature_id.' + dim_signature_ids["op"] + '_')
            params.append(filter(dim_signature_ids["list"]))
        # end if

        # DIM signatures
        if dim_signatures != None:
            is_valid_operator_list(dim_signatures)
            filter = eval('DimSignature.dim_signature.' + dim_signatures["op"] + '_')
            params.append(filter(dim_signatures["list"]))
        # end if
        if dim_signature_like != None:
            is_valid_operator_like(dim_signature_like)
            filter = eval('DimSignature.dim_signature.' + dim_signature_like["op"])
            params.append(filter(dim_signature_like["str"]))
        # end if

        # DIM exec names
        if dim_exec_names != None:
            is_valid_operator_list(dim_exec_names)
            filter = eval('DimSignature.dim_exec_name.' + dim_exec_names["op"] + '_')
            params.append(filter(dim_exec_names["list"]))
        # end if
        if dim_exec_name_like != None:
            is_valid_operator_like(dim_exec_name_like)
            filter = eval('DimSignature.dim_exec_name.' + dim_exec_name_like["op"])
            params.append(filter(dim_exec_name_like["str"]))
        # end if

        query = self.session.query(DimSignature).filter(*params)
        log_query(query)
        dim_signatures = query.all()

        return dim_signatures

    def get_sources(self, names = None, name_like = None, validity_start_filters = None, validity_stop_filters = None, generation_time_filters = None, ingestion_time_filters = None, ingestion_duration_filters = None, dim_exec_version_filters = None, dim_signature_ids = None, processing_uuids = None):
        """
        """
        params = []
        # DIM signature UUIDs
        if dim_signature_ids != None:
            is_valid_operator_list(dim_signature_ids)
            filter = eval('DimProcessing.dim_signature_id.' + dim_signature_ids["op"] + '_')
            params.append(filter(dim_signature_ids["list"]))
        # end if

        # Processing UUIDs
        if processing_uuids != None:
            is_valid_operator_list(processing_uuids)
            filter = eval('DimProcessing.processing_uuid.' + processing_uuids["op"] + '_')
            params.append(filter(processing_uuids["list"]))
        # end if

        # File names
        if names != None:
            is_valid_operator_list(names)
            filter = eval('DimProcessing.name.' + names["op"] + '_')
            params.append(filter(names["list"]))
        # end if
        if name_like != None:
            is_valid_operator_like(name_like)
            filter = eval('DimProcessing.name.' + name_like["op"])
            params.append(filter(name_like["str"]))
        # end if

        # validity_start filters
        if validity_start_filters != None:
            is_valid_date_filters(validity_start_filters, self.operators)
            for validity_start_filter in validity_start_filters:
                op = self.operators[validity_start_filter["op"]]
                params.append(op(DimProcessing.validity_start, validity_start_filter["date"]))
            # end for
        # end if

        # validity_stop filters
        if validity_stop_filters != None:
            is_valid_date_filters(validity_stop_filters, self.operators)
            for validity_stop_filter in validity_stop_filters:
                op = self.operators[validity_stop_filter["op"]]
                params.append(op(DimProcessing.validity_stop, validity_stop_filter["date"]))
            # end for
        # end if

        # generation_time filters
        if generation_time_filters != None:
            is_valid_date_filters(generation_time_filters, self.operators)
            for generation_time_filter in generation_time_filters:
                op = self.operators[generation_time_filter["op"]]
                params.append(op(DimProcessing.generation_time, generation_time_filter["date"]))
            # end for
        # end if

        # ingestion_time filters
        if ingestion_time_filters != None:
            is_valid_date_filters(ingestion_time_filters, self.operators)
            for ingestion_time_filter in ingestion_time_filters:
                op = self.operators[ingestion_time_filter["op"]]
                params.append(op(DimProcessing.ingestion_time, ingestion_time_filter["date"]))
            # end for
        # end if

        # ingestion_duration filters
        if ingestion_duration_filters != None:
            is_valid_float_filters(ingestion_duration_filters, self.operators)
            for ingestion_duration_filter in ingestion_duration_filters:
                op = self.operators[ingestion_duration_filter["op"]]
                params.append(op(DimProcessing.ingestion_duration, timedelta(seconds = ingestion_duration_filter["float"])))
            # end for
        # end if

        # dim_exec_version filters
        if dim_exec_version_filters != None:
            is_valid_string_filters(dim_exec_version_filters, self.operators)
            for dim_exec_version_filter in dim_exec_version_filters:
                op = self.operators[dim_exec_version_filter["op"]]
                params.append(op(DimProcessing.dim_exec_version, dim_exec_version_filter["str"]))
            # end for
        # end if

        query = self.session.query(DimProcessing).filter(*params)
        log_query(query)
        sources = query.all()

        return sources

    def get_sources_join(self, names = None, name_like = None, validity_start_filters = None, validity_stop_filters = None, generation_time_filters = None, ingestion_time_filters = None, ingestion_duration_filters = None, dim_exec_version_filters = None, dim_signatures = None, dim_signature_like = None, dim_exec_names = None, dim_exec_name_like = None, status_filters = None):
        """
        """
        params = []

        tables = []

        # DIM signature names
        if dim_signatures != None:
            tables.append(DimSignature)
            is_valid_operator_list(dim_signatures)
            filter = eval('DimSignature.dim_signature.' + dim_signatures["op"] + '_')
            params.append(filter(dim_signatures["list"]))
        # end if
        if dim_signature_like != None:
            tables.append(DimSignature)
            is_valid_operator_like(dim_signature_like)
            filter = eval('DimSignature.dim_signature.' + dim_signature_like["op"])
            params.append(filter(dim_signature_like["str"]))
        # end if

        # DIM exec names
        if dim_exec_names != None:
            tables.append(DimSignature)
            is_valid_operator_list(dim_exec_names)
            filter = eval('DimSignature.dim_exec_name.' + dim_exec_names["op"] + '_')
            params.append(filter(dim_exec_names["list"]))
        # end if
        if dim_exec_name_like != None:
            tables.append(DimSignature)
            is_valid_operator_like(dim_exec_name_like)
            filter = eval('DimSignature.dim_exec_name.' + dim_exec_name_like["op"])
            params.append(filter(dim_exec_name_like["str"]))
        # end if

        # status filters
        if status_filters != None:
            is_valid_float_filters(status_filters, self.operators)
            for status_filter in status_filters:
                op = self.operators[status_filter["op"]]
                params.append(op(DimProcessingStatus.proc_status, status_filter["float"]))
            # end for
        # end if

        # File names
        if names != None:
            is_valid_operator_list(names)
            filter = eval('DimProcessing.name.' + names["op"] + '_')
            params.append(filter(names["list"]))
        # end if
        if name_like != None:
            is_valid_operator_like(name_like)
            filter = eval('DimProcessing.name.' + name_like["op"])
            params.append(filter(name_like["str"]))
        # end if

        # validity_start filters
        if validity_start_filters != None:
            is_valid_date_filters(validity_start_filters, self.operators)
            for validity_start_filter in validity_start_filters:
                op = self.operators[validity_start_filter["op"]]
                params.append(op(DimProcessing.validity_start, validity_start_filter["date"]))
            # end for
        # end if

        # validity_stop filters
        if validity_stop_filters != None:
            is_valid_date_filters(validity_stop_filters, self.operators)
            for validity_stop_filter in validity_stop_filters:
                op = self.operators[validity_stop_filter["op"]]
                params.append(op(DimProcessing.validity_stop, validity_stop_filter["date"]))
            # end for
        # end if

        # generation_time filters
        if generation_time_filters != None:
            is_valid_date_filters(generation_time_filters, self.operators)
            for generation_time_filter in generation_time_filters:
                op = self.operators[generation_time_filter["op"]]
                params.append(op(DimProcessing.generation_time, generation_time_filter["date"]))
            # end for
        # end if

        # ingestion_time filters
        if ingestion_time_filters != None:
            is_valid_date_filters(ingestion_time_filters, self.operators)
            for ingestion_time_filter in ingestion_time_filters:
                op = self.operators[ingestion_time_filter["op"]]
                params.append(op(DimProcessing.ingestion_time, ingestion_time_filter["date"]))
            # end for
        # end if

        # ingestion_duration filters
        if ingestion_duration_filters != None:
            is_valid_float_filters(ingestion_duration_filters, self.operators)
            for ingestion_duration_filter in ingestion_duration_filters:
                op = self.operators[ingestion_duration_filter["op"]]
                params.append(op(DimProcessing.ingestion_duration, timedelta(seconds = ingestion_duration_filter["float"])))
            # end for
        # end if

        # dim_exec_version filters
        if dim_exec_version_filters != None:
            is_valid_string_filters(dim_exec_version_filters, self.operators)
            for dim_exec_version_filter in dim_exec_version_filters:
                op = self.operators[dim_exec_version_filter["op"]]
                params.append(op(DimProcessing.dim_exec_version, dim_exec_version_filter["str"]))
            # end for
        # end if

        query = self.session.query(DimProcessing)
        for table in set(tables):
            query = query.join(table)
        # end for

        query = query.filter(*params)
        log_query(query)
        sources = query.all()

        return sources

    def get_gauges(self, dim_signature_ids = None, gauge_ids = None, names = None, name_like = None, systems = None, system_like = None):
        """
        """
        params = []
        # DIM signature UUIDs
        if dim_signature_ids != None:
            is_valid_operator_list(dim_signature_ids)
            filter = eval('Gauge.dim_signature_id.' + dim_signature_ids["op"] + '_')
            params.append(filter(dim_signature_ids["list"]))
        # end if

        # Gauge UUIDs
        if gauge_ids != None:
            is_valid_operator_list(gauge_ids)
            filter = eval('Gauge.gauge_id.' + gauge_ids["op"] + '_')
            params.append(filter(gauge_ids["list"]))
        # end if

        # Gauge names
        if names != None:
            is_valid_operator_list(names)
            filter = eval('Gauge.name.' + names["op"] + '_')
            params.append(filter(names["list"]))
        # end if
        if name_like != None:
            is_valid_operator_like(name_like)
            filter = eval('Gauge.name.' + name_like["op"])
            params.append(filter(name_like["str"]))
        # end if

        # Gauge systems
        if systems != None:
            is_valid_operator_list(systems)
            filter = eval('Gauge.system.' + systems["op"] + '_')
            params.append(filter(systems["list"]))
        # end if
        if system_like != None:
            is_valid_operator_like(system_like)
            filter = eval('Gauge.system.' + system_like["op"])
            params.append(filter(system_like["str"]))
        # end if
        
        query = self.session.query(Gauge).filter(*params)
        log_query(query)
        gauges = query.all()

        return gauges

    def get_gauges_join(self, dim_signatures = None, dim_signature_like = None, dim_exec_names = None, dim_exec_name_like = None, names = None, name_like = None, systems = None, system_like = None):
        """
        """
        params = []

        tables = []

        # DIM signature names
        if dim_signatures != None:
            is_valid_operator_list(dim_signatures)
            filter = eval('DimSignature.dim_signature.' + dim_signatures["op"] + '_')
            params.append(filter(dim_signatures["list"]))
            tables.append(DimSignature)
        # end if
        if dim_signature_like != None:
            is_valid_operator_like(dim_signature_like)
            filter = eval('DimSignature.dim_signature.' + dim_signature_like["op"])
            params.append(filter(dim_signature_like["str"]))
            tables.append(DimSignature)
        # end if

        # DIM exec names
        if dim_exec_names != None:
            is_valid_operator_list(dim_exec_names)
            filter = eval('DimSignature.dim_exec_name.' + dim_exec_names["op"] + '_')
            params.append(filter(dim_exec_names["list"]))
            tables.append(DimSignature)
        # end if
        if dim_exec_name_like != None:
            is_valid_operator_like(dim_exec_name_like)
            filter = eval('DimSignature.dim_exec_name.' + dim_exec_name_like["op"])
            params.append(filter(dim_exec_name_like["str"]))
            tables.append(DimSignature)
        # end if

        # Gauge names
        if names != None:
            is_valid_operator_list(names)
            filter = eval('Gauge.name.' + names["op"] + '_')
            params.append(filter(names["list"]))
        # end if
        if name_like != None:
            is_valid_operator_like(name_like)
            filter = eval('Gauge.name.' + name_like["op"])
            params.append(filter(name_like["str"]))
        # end if

        # Gauge systems
        if systems != None:
            is_valid_operator_list(systems)
            filter = eval('Gauge.system.' + systems["op"] + '_')
            params.append(filter(systems["list"]))
        # end if
        if system_like != None:
            is_valid_operator_like(system_like)
            filter = eval('Gauge.system.' + system_like["op"])
            params.append(filter(system_like["str"]))
        # end if
        
        query = self.session.query(Gauge)
        for table in set(tables):
            query = query.join(table)
        # end for

        query = query.filter(*params)
        log_query(query)
        gauges = query.all()

        return gauges

    def get_events(self, processing_uuids = None, explicit_ref_ids = None, gauge_ids = None, start_filters = None, stop_filters = None, ingestion_time_filters = None, event_uuids = None):
        """
        """
        params = []
        # Allow only obtain visible events
        params.append(Event.visible == True)

        # processing_uuids
        if processing_uuids != None:
            is_valid_operator_list(processing_uuids)
            filter = eval('Event.processing_uuid.' + processing_uuids["op"] + '_')
            params.append(filter(processing_uuids["list"]))
        # end if

        # explicit_ref_ids
        if explicit_ref_ids != None:
            is_valid_operator_list(explicit_ref_ids)
            filter = eval('Event.explicit_ref_id.' + explicit_ref_ids["op"] + '_')
            params.append(filter(explicit_ref_ids["list"]))
        # end if

        # gauge_ids
        if gauge_ids != None:
            is_valid_operator_list(gauge_ids)
            filter = eval('Event.gauge_id.' + gauge_ids["op"] + '_')
            params.append(filter(gauge_ids["list"]))
        # end if

        # event_uuids
        if event_uuids != None:
            is_valid_operator_list(event_uuids)
            filter = eval('Event.event_uuid.' + event_uuids["op"] + '_')
            params.append(filter(event_uuids["list"]))
        # end if

        # start filters
        if start_filters != None:
            is_valid_date_filters(start_filters, self.operators)
            for start_filter in start_filters:
                op = self.operators[start_filter["op"]]
                params.append(op(Event.start, start_filter["date"]))
            # end for
        # end if

        # stop filters
        if stop_filters != None:
            is_valid_date_filters(stop_filters, self.operators)
            for stop_filter in stop_filters:
                op = self.operators[stop_filter["op"]]
                params.append(op(Event.stop, stop_filter["date"]))
            # end for
        # end if

        # ingestion_time filters
        if ingestion_time_filters != None:
            is_valid_date_filters(ingestion_time_filters, self.operators)
            for ingestion_time_filter in ingestion_time_filters:
                op = self.operators[ingestion_time_filter["op"]]
                params.append(op(Event.ingestion_time, ingestion_time_filter["date"]))
            # end for
        # end if

        query = self.session.query(Event).filter(*params)
        log_query(query)
        events = query.all()

        return events

    def get_events_join(self, sources = None, source_like = None, explicit_refs = None, explicit_ref_like = None, gauge_names = None, gauge_name_like = None, gauge_systems = None, gauge_system_like = None, start_filters = None, stop_filters = None, ingestion_time_filters = None, value_filters = None, values_names_type = None, values_name_type_like = None):
        """
        """
        params = []
        # Allow only obtain visible events
        params.append(Event.visible == True)

        tables = []

        # Sources
        if sources != None:
            is_valid_operator_list(sources)
            filter = eval('DimProcessing.name.' + sources["op"] + '_')
            params.append(filter(sources["list"]))
            tables.append(DimProcessing)
        # end if
        if source_like != None:
            is_valid_operator_like(source_like)
            filter = eval('DimProcessing.name.' + source_like["op"])
            params.append(filter(source_like["str"]))
            tables.append(DimProcessing)
        # end if

        # Explicit references
        if explicit_refs != None:
            is_valid_operator_list(explicit_refs)
            filter = eval('ExplicitRef.explicit_ref.' + explicit_refs["op"] + '_')
            params.append(filter(explicit_refs["list"]))
            tables.append(ExplicitRef)
        # end if
        if explicit_ref_like != None:
            is_valid_operator_like(explicit_ref_like)
            filter = eval('ExplicitRef.explicit_ref.' + explicit_ref_like["op"])
            params.append(filter(explicit_ref_like["str"]))
            tables.append(ExplicitRef)
        # end if

        # Gauge names
        if gauge_names != None:
            is_valid_operator_list(gauge_names)
            filter = eval('Gauge.name.' + gauge_names["op"] + '_')
            params.append(filter(gauge_names["list"]))
            tables.append(Gauge)
        # end if
        if gauge_name_like != None:
            is_valid_operator_like(gauge_name_like)
            filter = eval('Gauge.name.' + gauge_name_like["op"])
            params.append(filter(gauge_name_like["str"]))
            tables.append(Gauge)
        # end if

        # Gauge systems
        if gauge_systems != None:
            is_valid_operator_list(gauge_systems)
            filter = eval('Gauge.system.' + gauge_systems["op"] + '_')
            params.append(filter(gauge_systems["list"]))
            tables.append(Gauge)
        # end if
        if gauge_system_like != None:
            is_valid_operator_like(gauge_system_like)
            filter = eval('Gauge.system.' + gauge_system_like["op"])
            params.append(filter(gauge_system_like["str"]))
            tables.append(Gauge)
        # end if

        # start filters
        if start_filters != None:
            is_valid_date_filters(start_filters, self.operators)
            for start_filter in start_filters:
                op = self.operators[start_filter["op"]]
                params.append(op(Event.start, start_filter["date"]))
            # end for
        # end if

        # stop filters
        if stop_filters != None:
            is_valid_date_filters(stop_filters, self.operators)
            for stop_filter in stop_filters:
                op = self.operators[stop_filter["op"]]
                params.append(op(Event.stop, stop_filter["date"]))
            # end for
        # end if

        # ingestion_time filters
        if ingestion_time_filters != None:
            is_valid_date_filters(ingestion_time_filters, self.operators)
            for ingestion_time_filter in ingestion_time_filters:
                op = self.operators[ingestion_time_filter["op"]]
                params.append(op(Event.ingestion_time, ingestion_time_filter["date"]))
            # end for
        # end if

        # value filters
        if value_filters != None:
            is_valid_value_filters(value_filters, self.operators)
            for value_filter in value_filters:
                op = self.operators[value_filter["op"]]
                tables.append(self.event_value_entities[value_filter["type"]])
                params.append(op(self.event_value_entities[value_filter["type"]].value, value_filter["value"]))
            # end for
        # end if

        # Value names
        if values_names_type != None:
            is_valid_values_names_type(values_names_type)
            for value_names_type in values_names_type:
                tables.append(self.event_value_entities[value_names_type["type"]])
                params.append(self.event_value_entities[value_names_type["type"]].name.in_(value_names_type["names"]))
            # end for
        # end if
        if values_name_type_like != None:
            is_valid_values_name_type_like(values_name_type_like)
            for value_name_type_like in values_name_type_like:
                tables.append(self.event_value_entities[value_name_type_like["type"]])
                params.append(self.event_value_entities[value_name_type_like["type"]].name.like(value_name_type_like["name_like"]))
            # end for
        # end if

        query = self.session.query(Event)
        for table in set(tables):
            query = query.join(table)
        # end for

        query = query.filter(*params)
        log_query(query)
        events = query.all()

        return events

    def get_event_keys(self, event_uuids = None, dim_signature_ids = None, keys = None, key_like = None):
        """
        """
        params = []
        
        # DIM signature UUIDs
        if dim_signature_ids != None:
            is_valid_operator_list(dim_signature_ids)
            filter = eval('EventKey.dim_signature_id.' + dim_signature_ids["op"] + '_')
            params.append(filter(dim_signature_ids["list"]))
        # end if

        # event_uuids
        if event_uuids != None:
            is_valid_operator_list(event_uuids)
            filter = eval('EventKey.event_uuid.' + event_uuids["op"] + '_')
            params.append(filter(event_uuids["list"]))
        # end if

        # keys
        if keys != None:
            is_valid_operator_list(keys)
            filter = eval('EventKey.event_key.' + keys["op"] + '_')
            params.append(filter(keys["list"]))
        # end if
        if key_like != None:
            is_valid_operator_like(key_like)
            filter = eval('EventKey.event_key.' + key_like["op"])
            params.append(filter(key_like["str"]))
        # end if

        query = self.session.query(EventKey).filter(*params)
        log_query(query)
        event_keys = query.all()

        return event_keys

    def get_event_links(self, event_uuid_links = None, event_uuids = None, link_names = None, link_name_like = None):
        """
        """
        params = []
        if event_uuid_links:
            is_valid_operator_list(event_uuid_links)
            filter = eval('EventLink.event_uuid_link.' + event_uuid_links["op"] + '_')
            params.append(filter(event_uuid_links["list"]))
        # end if

        if event_uuids:
            is_valid_operator_list(event_uuids)
            filter = eval('EventLink.event_uuid.' + event_uuids["op"] + '_')
            params.append(filter(event_uuids["list"]))
        # end if

        if link_names:
            is_valid_operator_list(link_names)
            filter = eval('EventLink.name.' + link_names["op"] + '_')
            params.append(filter(link_names["list"]))
        # end if

        if link_name_like:
            is_valid_operator_like(link_name_like)
            filter = eval('EventLink.name.' + link_name_like["op"])
            params.append(filter(link_name_like["str"]))
        # end if

        query = self.session.query(EventLink).filter(*params)
        log_query(query)
        links = query.all()

        return links

    def get_linked_events(self, processing_uuids = None, explicit_ref_ids = None, gauge_ids = None, start_filters = None, stop_filters = None, link_names = None, link_name_like = None, event_uuids = None):
        
        # Obtain prime events 
        prime_events = self.get_events(processing_uuids = processing_uuids, explicit_ref_ids = explicit_ref_ids, gauge_ids = gauge_ids, start_filters = start_filters, stop_filters = stop_filters, event_uuids = event_uuids)

        prime_event_uuids = [str(event.__dict__["event_uuid"]) for event in prime_events]

        # Obtain the links from the prime events to other events
        links = []
        if len(prime_event_uuids) > 0:
            links = self.get_event_links(event_uuid_links = {"list": prime_event_uuids, "op": "in"}, link_names = link_names, link_name_like = link_name_like)
        # end if

        # Obtain the events linked by the prime events
        linked_event_uuids = [str(link.event_uuid) for link in links]
        linked_events = []
        if len(linked_event_uuids) > 0:
            linked_events = self.get_events(event_uuids = {"list": linked_event_uuids, "op": "in"})
        # end if

        return prime_events + linked_events

    def get_linked_events_join(self, sources = None, source_like = None, explicit_refs = None, explicit_ref_like = None, gauge_names = None, gauge_name_like = None, gauge_systems = None, gauge_system_like = None, start_filters = None, stop_filters = None, link_names = None, link_name_like = None):
        
        # Obtain prime events 
        prime_events = self.get_events_join(sources = sources, source_like = source_like, explicit_refs = explicit_refs, explicit_ref_like = explicit_ref_like, gauge_names = gauge_names, gauge_name_like = gauge_name_like, gauge_systems = gauge_systems, gauge_system_like = gauge_system_like, start_filters = start_filters, stop_filters = stop_filters)

        prime_event_uuids = [str(event.__dict__["event_uuid"]) for event in prime_events]

        # Obtain the links from the prime events to other events
        links = []
        if len(prime_event_uuids) > 0:
            links = self.get_event_links(event_uuid_links = {"list": prime_event_uuids, "op": "in"}, link_names = link_names, link_name_like = link_name_like)
        # end if

        # Obtain the events linked by the prime events
        linked_event_uuids = [str(link.event_uuid) for link in links]
        linked_events = []
        if len(linked_event_uuids) > 0:
            linked_events = self.get_events(event_uuids = {"list": linked_event_uuids, "op": "in"})
        # end if

        return prime_events + linked_events

    def get_annotation_cnfs(self, dim_signature_ids = None, annotation_cnf_ids = None, names = None, name_like = None, systems = None, system_like = None):
        """
        """
        params = []
        # DIM signature UUIDs
        if dim_signature_ids != None:
            is_valid_operator_list(dim_signature_ids)
            filter = eval('AnnotationCnf.dim_signature_id.' + dim_signature_ids["op"] + '_')
            params.append(filter(dim_signature_ids["list"]))
        # end if

        # AnnotationCnf UUIDs
        if annotation_cnf_ids != None:
            is_valid_operator_list(annotation_cnf_ids)
            filter = eval('AnnotationCnf.annotation_cnf_id.' + annotation_cnf_ids["op"] + '_')
            params.append(filter(annotation_cnf_ids["list"]))
        # end if

        # AnnotationCnf names
        if names != None:
            is_valid_operator_list(names)
            filter = eval('AnnotationCnf.name.' + names["op"] + '_')
            params.append(filter(names["list"]))
        # end if
        if name_like != None:
            is_valid_operator_like(name_like)
            filter = eval('AnnotationCnf.name.' + name_like["op"])
            params.append(filter(name_like["str"]))
        # end if

        # AnnotationCnf systems
        if systems != None:
            is_valid_operator_list(systems)
            filter = eval('AnnotationCnf.system.' + systems["op"] + '_')
            params.append(filter(systems["list"]))
        # end if
        if system_like != None:
            is_valid_operator_like(system_like)
            filter = eval('AnnotationCnf.system.' + system_like["op"])
            params.append(filter(system_like["str"]))
        # end if
        
        query = self.session.query(AnnotationCnf).filter(*params)
        log_query(query)
        annotation_cnfs = query.all()

        return annotation_cnfs

    def get_annotation_cnfs_join(self, dim_signatures = None, dim_signature_like = None, dim_exec_names = None, dim_exec_name_like = None, names = None, name_like = None, systems = None, system_like = None):
        """
        """
        params = []

        tables = []

        # DIM signature names
        if dim_signatures != None:
            is_valid_operator_list(dim_signatures)
            filter = eval('DimSignature.dim_signature.' + dim_signatures["op"] + '_')
            params.append(filter(dim_signatures["list"]))
            tables.append(DimSignature)
        # end if
        if dim_signature_like != None:
            is_valid_operator_like(dim_signature_like)
            filter = eval('DimSignature.dim_signature.' + dim_signature_like["op"])
            params.append(filter(dim_signature_like["str"]))
            tables.append(DimSignature)
        # end if

        # DIM exec names
        if dim_exec_names != None:
            is_valid_operator_list(dim_exec_names)
            filter = eval('DimSignature.dim_exec_name.' + dim_exec_names["op"] + '_')
            params.append(filter(dim_exec_names["list"]))
            tables.append(DimSignature)
        # end if
        if dim_exec_name_like != None:
            is_valid_operator_like(dim_exec_name_like)
            filter = eval('DimSignature.dim_exec_name.' + dim_exec_name_like["op"])
            params.append(filter(dim_exec_name_like["str"]))
            tables.append(DimSignature)
        # end if

        # AnnotationCnf names
        if names != None:
            is_valid_operator_list(names)
            filter = eval('AnnotationCnf.name.' + names["op"] + '_')
            params.append(filter(names["list"]))
        # end if
        if name_like != None:
            is_valid_operator_like(name_like)
            filter = eval('AnnotationCnf.name.' + name_like["op"])
            params.append(filter(name_like["str"]))
        # end if

        # AnnotationCnf systems
        if systems != None:
            is_valid_operator_list(systems)
            filter = eval('AnnotationCnf.system.' + systems["op"] + '_')
            params.append(filter(systems["list"]))
        # end if
        if system_like != None:
            is_valid_operator_like(system_like)
            filter = eval('AnnotationCnf.system.' + system_like["op"])
            params.append(filter(system_like["str"]))
        # end if
        
        query = self.session.query(AnnotationCnf)
        for table in set(tables):
            query = query.join(table)
        # end for

        query = query.filter(*params)
        log_query(query)
        annotation_cnfs = query.all()

        return annotation_cnfs

    def get_annotations(self, processing_uuids = None, explicit_ref_ids = None, annotation_cnf_ids = None, ingestion_time_filters = None, annotation_uuids = None):
        """
        """
        params = []
        # Allow only obtain visible annotations
        params.append(Annotation.visible == True)

        # processing_uuids
        if processing_uuids != None:
            is_valid_operator_list(processing_uuids)
            filter = eval('Annotation.processing_uuid.' + processing_uuids["op"] + '_')
            params.append(filter(processing_uuids["list"]))
        # end if

        # explicit_ref_ids
        if explicit_ref_ids != None:
            is_valid_operator_list(explicit_ref_ids)
            filter = eval('Annotation.explicit_ref_id.' + explicit_ref_ids["op"] + '_')
            params.append(filter(explicit_ref_ids["list"]))
        # end if

        # annotation_cnf_ids
        if annotation_cnf_ids != None:
            is_valid_operator_list(annotation_cnf_ids)
            filter = eval('Annotation.annotation_cnf_id.' + annotation_cnf_ids["op"] + '_')
            params.append(filter(annotation_cnf_ids["list"]))
        # end if

        # annotation_uuids
        if annotation_uuids != None:
            is_valid_operator_list(annotation_uuids)
            filter = eval('Annotation.annotation_uuid.' + annotation_uuids["op"] + '_')
            params.append(filter(annotation_uuids["list"]))
        # end if

        # ingestion_time filters
        if ingestion_time_filters != None:
            is_valid_date_filters(ingestion_time_filters, self.operators)
            for ingestion_time_filter in ingestion_time_filters:
                op = self.operators[ingestion_time_filter["op"]]
                params.append(op(Annotation.ingestion_time, ingestion_time_filter["date"]))
            # end for
        # end if

        query = self.session.query(Annotation).filter(*params)
        log_query(query)
        annotations = query.all()

        return annotations

    def get_annotations_join(self, sources = None, source_like = None, explicit_refs = None, explicit_ref_like = None, annotation_cnf_names = None, annotation_cnf_name_like = None, annotation_cnf_systems = None, annotation_cnf_system_like = None, ingestion_time_filters = None, value_filters = None, values_names_type = None, values_name_type_like = None):
        """
        """
        params = []
        # Allow only obtain visible annotations
        params.append(Annotation.visible == True)

        tables = []

        # Sources
        if sources != None:
            is_valid_operator_list(sources)
            filter = eval('DimProcessing.name.' + sources["op"] + '_')
            params.append(filter(sources["list"]))
            tables.append(DimProcessing)
        # end if
        if source_like != None:
            is_valid_operator_like(source_like)
            filter = eval('DimProcessing.name.' + source_like["op"])
            params.append(filter(source_like["str"]))
            tables.append(DimProcessing)
        # end if

        # Explicit references
        if explicit_refs != None:
            is_valid_operator_list(explicit_refs)
            filter = eval('ExplicitRef.explicit_ref.' + explicit_refs["op"] + '_')
            params.append(filter(explicit_refs["list"]))
            tables.append(ExplicitRef)
        # end if
        if explicit_ref_like != None:
            is_valid_operator_like(explicit_ref_like)
            filter = eval('ExplicitRef.explicit_ref.' + explicit_ref_like["op"])
            params.append(filter(explicit_ref_like["str"]))
            tables.append(ExplicitRef)
        # end if

        # Annotation configuration names
        if annotation_cnf_names != None:
            is_valid_operator_list(annotation_cnf_names)
            filter = eval('AnnotationCnf.name.' + annotation_cnf_names["op"] + '_')
            params.append(filter(annotation_cnf_names["list"]))
            tables.append(AnnotationCnf)
        # end if
        if annotation_cnf_name_like != None:
            is_valid_operator_like(annotation_cnf_name_like)
            filter = eval('AnnotationCnf.name.' + annotation_cnf_name_like["op"])
            params.append(filter(annotation_cnf_name_like["str"]))
            tables.append(AnnotationCnf)
        # end if

        # Annotation configuration systems
        if annotation_cnf_systems != None:
            is_valid_operator_list(annotation_cnf_systems)
            filter = eval('AnnotationCnf.system.' + annotation_cnf_systems["op"] + '_')
            params.append(filter(annotation_cnf_systems["list"]))
            tables.append(AnnotationCnf)
        # end if
        if annotation_cnf_system_like != None:
            is_valid_operator_like(annotation_cnf_system_like)
            filter = eval('AnnotationCnf.system.' + annotation_cnf_system_like["op"])
            params.append(filter(annotation_cnf_system_like["str"]))
            tables.append(AnnotationCnf)
        # end if

        # ingestion_time filters
        if ingestion_time_filters != None:
            is_valid_date_filters(ingestion_time_filters, self.operators)
            for ingestion_time_filter in ingestion_time_filters:
                op = self.operators[ingestion_time_filter["op"]]
                params.append(op(Annotation.ingestion_time, ingestion_time_filter["date"]))
            # end for
        # end if

        # value filters
        if value_filters != None:
            is_valid_value_filters(value_filters, self.operators)
            for value_filter in value_filters:
                op = self.operators[value_filter["op"]]
                tables.append(self.annotation_value_entities[value_filter["type"]])
                params.append(op(self.annotation_value_entities[value_filter["type"]].value, value_filter["value"]))
            # end for
        # end if

        # Value names
        if values_names_type != None:
            is_valid_values_names_type(values_names_type)
            for value_names_type in values_names_type:
                tables.append(self.annotation_value_entities[value_names_type["type"]])
                params.append(self.annotation_value_entities[value_names_type["type"]].name.in_(value_names_type["names"]))
            # end for
        # end if
        if values_name_type_like != None:
            is_valid_values_name_type_like(values_name_type_like)
            for value_name_type_like in values_name_type_like:
                tables.append(self.annotation_value_entities[value_name_type_like["type"]])
                params.append(self.annotation_value_entities[value_name_type_like["type"]].name.like(value_name_type_like["name_like"]))
            # end for
        # end if

        query = self.session.query(Annotation)
        for table in set(tables):
            query = query.join(table)
        # end for

        query = query.filter(*params)
        log_query(query)
        annotations = query.all()

        return annotations

    def get_explicit_references(self, group_ids = None, explicit_ref_ids = None, explicit_refs = None, explicit_ref_like = None, ingestion_time_filters = None):
        """
        """
        params = []

        # group_ids
        if group_ids != None:
            is_valid_operator_list(group_ids)
            filter = eval('ExplicitRef.group_id.' + group_ids["op"] + '_')
            params.append(filter(group_ids["list"]))
        # end if

        # explicit_ref_ids
        if explicit_ref_ids != None:
            is_valid_operator_list(explicit_ref_ids)
            filter = eval('ExplicitRef.explicit_ref_id.' + explicit_ref_ids["op"] + '_')
            params.append(filter(explicit_ref_ids["list"]))
        # end if

        # Explicit references
        if explicit_refs != None:
            is_valid_operator_list(explicit_refs)
            filter = eval('ExplicitRef.name.' + explicit_refs["op"] + '_')
            params.append(filter(explicit_refs["list"]))
        # end if
        if explicit_ref_like != None:
            is_valid_operator_like(explicit_ref_like)
            filter = eval('ExplicitRef.name.' + explicit_ref_like["op"])
            params.append(filter(explicit_ref_like["str"]))
        # end if

        # ingestion_time filters
        if ingestion_time_filters != None:
            is_valid_date_filters(ingestion_time_filters, self.operators)
            for ingestion_time_filter in ingestion_time_filters:
                op = self.operators[ingestion_time_filter["op"]]
                params.append(op(Event.ingestion_time, ingestion_time_filter["date"]))
            # end for
        # end if

        query = self.session.query(ExplicitRef).filter(*params)
        log_query(query)
        explicit_references = query.all()

        return explicit_references

    def get_explicit_references_join(self, explicit_refs = None, explicit_ref_like = None, gauge_names = None, gauge_name_like = None, gauge_systems = None, gauge_system_like = None, start_filters = None, stop_filters = None, explicit_ref_ingestion_time_filters = None, event_value_filters = None, event_values_names_type = None, event_values_name_type_like = None, annotation_cnf_names = None, annotation_cnf_name_like = None, annotation_cnf_systems = None, annotation_cnf_system_like = None, ingestion_time_filters = None, annotation_value_filters = None, annotation_values_names_type = None, annotation_values_name_type_like = None, expl_groups = None, expl_group_like = None):
        """
        """
        params = []
        # Allow only obtain visible events
        params.append(Event.visible == True)

        tables = []

        # Explicit references
        if explicit_refs != None:
            is_valid_operator_list(explicit_refs)
            filter = eval('ExplicitRef.explicit_ref.' + explicit_refs["op"] + '_')
            params.append(filter(explicit_refs["list"]))
        # end if
        if explicit_ref_like != None:
            is_valid_operator_like(explicit_ref_like)
            filter = eval('ExplicitRef.explicit_ref.' + explicit_ref_like["op"])
            params.append(filter(explicit_ref_like["str"]))
        # end if

        # Groups
        if expl_groups != None:
            is_valid_operator_list(expl_groups)
            filter = eval('ExplicitRefGrp.name.' + expl_groups["op"] + '_')
            params.append(filter(expl_groups["list"]))
            tables.append(ExplicitRefGrp)
        # end if
        if expl_group_like != None:
            is_valid_operator_like(expl_group_like)
            filter = eval('ExplicitRefGrp.name.' + expl_group_like["op"])
            params.append(filter(expl_group_like["str"]))
            tables.append(ExplicitRefGrp)
        # end if

        # Gauge names
        if gauge_names != None:
            is_valid_operator_list(gauge_names)
            filter = eval('Gauge.name.' + gauge_names["op"] + '_')
            params.append(filter(gauge_names["list"]))
            tables.append(Gauge)
        # end if
        if gauge_name_like != None:
            is_valid_operator_like(gauge_name_like)
            filter = eval('Gauge.name.' + gauge_name_like["op"])
            params.append(filter(gauge_name_like["str"]))
            tables.append(Gauge)
        # end if

        # Gauge systems
        if gauge_systems != None:
            is_valid_operator_list(gauge_systems)
            filter = eval('Gauge.system.' + gauge_systems["op"] + '_')
            params.append(filter(gauge_systems["list"]))
            tables.append(Gauge)
        # end if
        if gauge_system_like != None:
            is_valid_operator_like(gauge_system_like)
            filter = eval('Gauge.system.' + gauge_system_like["op"])
            params.append(filter(gauge_system_like["str"]))
            tables.append(Gauge)
        # end if

        # Annotation configuration names
        if annotation_cnf_names != None:
            is_valid_operator_list(annotation_cnf_names)
            filter = eval('AnnotationCnf.name.' + annotation_cnf_names["op"] + '_')
            params.append(filter(annotation_cnf_names["list"]))
            tables.append(AnnotationCnf)
        # end if
        if annotation_cnf_name_like != None:
            is_valid_operator_like(annotation_cnf_name_like)
            filter = eval('AnnotationCnf.name.' + annotation_cnf_name_like["op"])
            params.append(filter(annotation_cnf_name_like["str"]))
            tables.append(AnnotationCnf)
        # end if

        # Annotation configuration systems
        if annotation_cnf_systems != None:
            is_valid_operator_list(annotation_cnf_systems)
            filter = eval('AnnotationCnf.system.' + annotation_cnf_systems["op"] + '_')
            params.append(filter(annotation_cnf_systems["list"]))
            tables.append(AnnotationCnf)
        # end if
        if annotation_cnf_system_like != None:
            is_valid_operator_like(annotation_cnf_system_like)
            filter = eval('AnnotationCnf.system.' + annotation_cnf_system_like["op"])
            params.append(filter(annotation_cnf_system_like["str"]))
            tables.append(AnnotationCnf)
        # end if

        # start filters
        if start_filters != None:
            is_valid_date_filters(start_filters, self.operators)
            for start_filter in start_filters:
                op = self.operators[start_filter["op"]]
                params.append(op(Event.start, start_filter["date"]))
            # end for
            tables.append(Event)
        # end if

        # stop filters
        if stop_filters != None:
            is_valid_date_filters(stop_filters, self.operators)
            for stop_filter in stop_filters:
                op = self.operators[stop_filter["op"]]
                params.append(op(Event.stop, stop_filter["date"]))
            # end for
            tables.append(Event)
        # end if

        # explicit references ingestion_time filters
        if explicit_ref_ingestion_time_filters != None:
            is_valid_date_filters(explicit_ref_ingestion_time_filters, self.operators)
            for ingestion_time_filter in explicit_ref_ingestion_time_filters:
                op = self.operators[ingestion_time_filter["op"]]
                params.append(op(ExplicitRef.ingestion_time, ingestion_time_filter["date"]))
            # end for
        # end if

        # Event value filters
        if event_value_filters != None:
            is_valid_event_value_filters(event_value_filters, self.operators)
            for value_filter in value_filters:
                op = self.operators[value_filter["op"]]
                tables.append(self.event_value_entities[value_filter["type"]])
                params.append(op(self.event_value_entities[value_filter["type"]].value, value_filter["value"]))
            # end for
        # end if

        # Event value names
        if event_values_names_type != None:
            is_valid_values_names_type(event_values_names_type)
            for value_names_type in event_values_names_type:
                tables.append(self.event_value_entities[value_names_type["type"]])
                params.append(self.event_value_entities[value_names_type["type"]].name.in_(value_names_type["names"]))
            # end for
        # end if
        if event_values_name_type_like != None:
            is_valid_values_name_type_like(event_values_name_type_like)
            for value_name_type_like in event_values_name_type_like:
                tables.append(self.event_value_entities[value_name_type_like["type"]])
                params.append(self.event_value_entities[value_name_type_like["type"]].name.like(value_name_type_like["name_like"]))
            # end for
        # end if

        # Annotation value filters
        if annotation_value_filters != None:
            is_valid_annotation_value_filters(annotation_value_filters, self.operators)
            for value_filter in value_filters:
                op = self.operators[value_filter["op"]]
                tables.append(self.annotation_value_entities[value_filter["type"]])
                params.append(op(self.annotation_value_entities[value_filter["type"]].value, value_filter["value"]))
            # end for
        # end if

        # Annotation value names
        if annotation_values_names_type != None:
            is_valid_values_names_type(annotation_values_names_type)
            for value_names_type in annotation_values_names_type:
                tables.append(self.annotation_value_entities[value_names_type["type"]])
                params.append(self.annotation_value_entities[value_names_type["type"]].name.in_(value_names_type["names"]))
            # end for
        # end if
        if annotation_values_name_type_like != None:
            is_valid_values_name_type_like(annotation_values_name_type_like)
            for value_name_type_like in annotation_values_name_type_like:
                tables.append(self.annotation_value_entities[value_name_type_like["type"]])
                params.append(self.annotation_value_entities[value_name_type_like["type"]].name.like(value_name_type_like["name_like"]))
            # end for
        # end if

        query = self.session.query(ExplicitRef)
        for table in set(tables):
            query = query.join(table)
        # end for

        query = query.filter(*params)
        log_query(query)
        explicit_references = query.all()

        return explicit_references

    def get_explicit_references_links(self, explicit_ref_id_links = None, explicit_ref_ids = None, link_names = None, link_name_like = None):
        """
        """
        params = []
        if explicit_ref_id_links:
            is_valid_operator_list(explicit_ref_id_links)
            filter = eval('ExplicitRefLink.explicit_ref_id_link.' + explicit_ref_id_links["op"] + '_')
            params.append(filter(explicit_ref_id_links["list"]))
        # end if

        if explicit_ref_ids:
            is_valid_operator_list(explicit_ref_ids)
            filter = eval('ExplicitRefLink.explicit_ref_id.' + explicit_ref_ids["op"] + '_')
            params.append(filter(explicit_ref_ids["list"]))
        # end if

        if link_names:
            is_valid_operator_list(link_names)
            filter = eval('ExplicitRefLink.name.' + link_names["op"] + '_')
            params.append(filter(link_names["list"]))
        # end if

        if link_name_like:
            is_valid_operator_like(link_name_like)
            filter = eval('ExplicitRefLink.name.' + link_name_like["op"])
            params.append(filter(link_name_like["str"]))
        # end if

        query = self.session.query(ExplicitRefLink).filter(*params)
        log_query(query)
        links = query.all()

        return links

    def get_explicit_references_groups(self, group_ids = None, names = None, name_like = None):
        """
        """
        # Group UUIDs
        if group_ids != None:
            is_valid_operator_list(group_ids)
            filter = eval('ExplicitRefGrp.group_id.' + group_ids["op"] + '_')
            params.append(filter(group_ids["list"]))
        # end if

        # Gauge names
        if names != None:
            is_valid_operator_list(names)
            filter = eval('ExplicitRefGrp.name.' + names["op"] + '_')
            params.append(filter(names["list"]))
        # end if
        if name_like != None:
            is_valid_operator_like(name_like)
            filter = eval('ExplicitRefGrp.name.' + name_like["op"])
            params.append(filter(name_like["str"]))
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
