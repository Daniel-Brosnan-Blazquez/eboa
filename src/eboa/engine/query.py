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
from eboa.datamodel.dim_processings import DimProcessing, DimProcessingStatus
from eboa.datamodel.explicit_refs import ExplicitRef, ExplicitRefGrp, ExplicitRefLink
from eboa.datamodel.annotations import Annotation, AnnotationCnf, AnnotationText, AnnotationDouble, AnnotationObject, AnnotationGeometry, AnnotationBoolean, AnnotationTimestamp
from sqlalchemy.dialects import postgresql

# Import exceptions
from eboa.engine.errors import InputError

# Import auxiliary functions
from eboa.engine.functions import is_datetime, is_valid_date_filters, is_valid_float_filters, is_valid_string_filters, is_valid_value_filters, is_valid_values_names_type, is_valid_values_name_type_like, is_valid_operator_list, is_valid_operator_like

# Import logging
from eboa.logging import Log

# Import query printing facilities
from eboa.engine.printing import literal_query

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

    def get_dim_signatures(self, dim_signature_ids = None, dim_signatures = None, dim_signature_like = None, dim_exec_names = None, dim_exec_name_like = None):
        """
        Method to obtain the DIM signature entities filtered by the received parameters

        :param dim_signature_ids: list of DIM signature identifiers
        :type dim_signature_ids: operator_list
        :param dim_signatures: list of DIM signature names
        :type dim_signatures: operator_list
        :param dim_signature_like: dictionary with a string and the associated operation to perform
        :type dim_signature_like: operator_like
        :param dim_exec_names: list of DIM execution names
        :type dim_exec_names: operator_list
        :param dim_exec_name_like: dictionary with a string and the associated operation to perform
        :type dim_exec_name_like: operator_like

        :return: found DIM signatures
        :rtype: list
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
        Method to obtain the sources entities filtered by the received parameters

        :param names: list of source names
        :type names: operator_list
        :param name_like: dictionary with a string and the associated operation to perform
        :type name_like: operator_like
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
        :param dim_exec_version_filters: list of version filters
        :type dim_exec_version_filters: string_filter
        :param dim_signature_ids: list of DIM signature identifiers
        :type dim_signature_ids: operator_list
        :param processing_uuids: list of DIM processing identifiers
        :type processing_uuids: operator_list

        :return: found sources
        :rtype: list
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

    def get_linked_events(self, processing_uuids = None, explicit_ref_ids = None, gauge_ids = None, start_filters = None, stop_filters = None, link_names = None, link_name_like = None, event_uuids = None, return_prime_events = True, back_ref = False):

        if not event_uuids or (event_uuids and return_prime_events):
            # Obtain prime events
            parameter_event_uuids = None
            if event_uuids:
                parameter_event_uuids = {"list": event_uuids, "op": "in"}
            # end if
            prime_events = self.get_events(processing_uuids = processing_uuids, explicit_ref_ids = explicit_ref_ids, gauge_ids = gauge_ids, start_filters = start_filters, stop_filters = stop_filters, event_uuids = parameter_event_uuids)
        # end if

        if event_uuids:
            if type(event_uuids) != list:
                raise InputError("The parameter event_uuids must be a list of UUIDs.")
            # end if
            prime_event_uuids = event_uuids
        else:
            prime_event_uuids = [str(event.__dict__["event_uuid"]) for event in prime_events]
        # end if

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

        events = {}
        if return_prime_events:
            events["prime_events"] = prime_events
        # end if
        events["linked_events"] = linked_events
        
        if back_ref:
            # Obtain the events linking the prime events
            links = self.get_event_links(event_uuids = {"list": prime_event_uuids, "op": "in"})
            event_linking_uuids = [str(link.event_uuid_link) for link in links]
            events_linking = []
            if len(event_linking_uuids) > 0:
                events_linking = self.get_events(event_uuids = {"list": event_linking_uuids, "op": "in"})
            # end if

            events["events_linking"] = events_linking
        # end if

        return events

    def get_linked_events_join(self, sources = None, source_like = None, explicit_refs = None, explicit_ref_like = None, gauge_names = None, gauge_name_like = None, gauge_systems = None, gauge_system_like = None, start_filters = None, stop_filters = None, link_names = None, link_name_like = None, value_filters = None, values_names_type = None, values_name_type_like = None, return_prime_events = True, back_ref = False):
        
        # Obtain prime events 
        prime_events = self.get_events_join(sources = sources, source_like = source_like, explicit_refs = explicit_refs, explicit_ref_like = explicit_ref_like, gauge_names = gauge_names, gauge_name_like = gauge_name_like, gauge_systems = gauge_systems, gauge_system_like = gauge_system_like, start_filters = start_filters, stop_filters = stop_filters, value_filters = value_filters, values_names_type = values_names_type, values_name_type_like = values_name_type_like)

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
        
        events = {}
        if return_prime_events:
            events["prime_events"] = prime_events
        # end if
        events["linked_events"] = linked_events

        if back_ref:
            # Obtain the events linking the prime events
            links = self.get_event_links(event_uuids = {"list": prime_event_uuids, "op": "in"})
            event_linking_uuids = [str(link.event_uuid_link) for link in links]
            events_linking = []
            if len(event_linking_uuids) > 0:
                events_linking = self.get_events(event_uuids = {"list": event_linking_uuids, "op": "in"})
            # end if

            events["events_linking"] = events_linking
        # end if

        return events

    def get_linked_events_details(self, event_uuid, return_prime_events = True, back_ref = False):

        if not event_uuid or type(event_uuid) != uuid.UUID:
            raise InputError("The parameter event_uuid must be a UUID.")
        # end if

        events = {}
        if return_prime_events:
            events["prime_events"] = self.get_events(event_uuids = {"list": [event_uuid], "op": "in"})
        # end if

        # Obtain the links from the prime events to other events
        links = self.get_event_links(event_uuid_links = {"list": [event_uuid], "op": "in"})

        # Obtain the events linked by the prime events
        linked_event_uuids = [str(link.event_uuid) for link in links]
        linked_events = []
        if len(linked_event_uuids) > 0:
            linked_events = self.get_events(event_uuids = {"list": linked_event_uuids, "op": "in"})
        # end if

        events["linked_events"] = []
        for event in linked_events:
            link_name = [str(link.name) for link in links if link.event_uuid == event.event_uuid][0]
            events["linked_events"].append({"link_name": link_name,
                                            "event": event})
        # end for
        
        if back_ref:
            # Obtain the events linking the prime events
            links = self.get_event_links(event_uuids = {"list": [event_uuid], "op": "in"})
            event_linking_uuids = [str(link.event_uuid_link) for link in links]
            events_linking = []
            if len(event_linking_uuids) > 0:
                events_linking = self.get_events(event_uuids = {"list": event_linking_uuids, "op": "in"})
            # end if

            events["events_linking"] = []
            for event in events_linking:
                link_name = [str(link.name) for link in links if link.event_uuid_link == event.event_uuid][0]
                events["events_linking"].append({"link_name": link_name,
                                                "event": event})
            # end for
        # end if

        return events

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

    def get_explicit_refs(self, group_ids = None, explicit_ref_ids = None, explicit_refs = None, explicit_ref_like = None, ingestion_time_filters = None):
        """
        """
        params = []

        # group_ids
        if group_ids != None:
            is_valid_operator_list(group_ids)
            filter = eval('ExplicitRef.expl_ref_cnf_id.' + group_ids["op"] + '_')
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
            filter = eval('ExplicitRef.explicit_ref.' + explicit_refs["op"] + '_')
            params.append(filter(explicit_refs["list"]))
        # end if
        if explicit_ref_like != None:
            is_valid_operator_like(explicit_ref_like)
            filter = eval('ExplicitRef.explicit_ref.' + explicit_ref_like["op"])
            params.append(filter(explicit_ref_like["str"]))
        # end if

        # ingestion_time filters
        if ingestion_time_filters != None:
            is_valid_date_filters(ingestion_time_filters, self.operators)
            for ingestion_time_filter in ingestion_time_filters:
                op = self.operators[ingestion_time_filter["op"]]
                params.append(op(ExplicitRef.ingestion_time, ingestion_time_filter["date"]))
            # end for
        # end if

        query = self.session.query(ExplicitRef).filter(*params)
        log_query(query)
        explicit_refs = query.all()

        return explicit_refs

    def get_explicit_refs_join(self, explicit_refs = None, explicit_ref_like = None, sources = None, source_like = None, gauge_names = None, gauge_name_like = None, gauge_systems = None, gauge_system_like = None, start_filters = None, stop_filters = None, explicit_ref_ingestion_time_filters = None, event_value_filters = None, event_values_names_type = None, event_values_name_type_like = None, annotation_cnf_names = None, annotation_cnf_name_like = None, annotation_cnf_systems = None, annotation_cnf_system_like = None, annotation_value_filters = None, annotation_values_names_type = None, annotation_values_name_type_like = None, expl_groups = None, expl_group_like = None):
        """
        """
        params = []

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

        # explicit references ingestion_time filters
        if explicit_ref_ingestion_time_filters != None:
            is_valid_date_filters(explicit_ref_ingestion_time_filters, self.operators)
            for ingestion_time_filter in explicit_ref_ingestion_time_filters:
                op = self.operators[ingestion_time_filter["op"]]
                params.append(op(ExplicitRef.ingestion_time, ingestion_time_filter["date"]))
            # end for
        # end if

        # Events
        events = self.get_events_join(sources = sources, source_like = source_like, explicit_refs = explicit_refs, explicit_ref_like = explicit_ref_like, gauge_names = gauge_names, gauge_name_like = gauge_name_like, gauge_systems = gauge_systems, gauge_system_like = gauge_system_like, start_filters = start_filters, stop_filters = stop_filters, value_filters = event_value_filters, values_names_type = event_values_names_type, values_name_type_like = event_values_name_type_like)

        explicit_ref_uuids_events = [str(event.explicit_ref_id) for event in events]

        # Annotations
        annotations = self.get_annotations_join(sources = sources, source_like = source_like, explicit_refs = explicit_refs, explicit_ref_like = explicit_ref_like, annotation_cnf_names = annotation_cnf_names, annotation_cnf_name_like = annotation_cnf_name_like, annotation_cnf_systems = annotation_cnf_systems, annotation_cnf_system_like = annotation_cnf_system_like, value_filters = annotation_value_filters, values_names_type = annotation_values_names_type, values_name_type_like = annotation_values_name_type_like)

        explicit_ref_uuids_annotations = [str(annotation.explicit_ref_id) for annotation in annotations]

        # explicit references uuids
        explicit_ref_uuids = set (explicit_ref_uuids_events + explicit_ref_uuids_annotations)
        if len(explicit_ref_uuids) > 0:
            params.append(ExplicitRef.explicit_ref_id.in_(explicit_ref_uuids))
        # end if

        query = self.session.query(ExplicitRef)
        for table in set(tables):
            query = query.join(table)
        # end for

        query = query.filter(*params)
        log_query(query)
        explicit_refs = query.all()

        return explicit_refs

    def get_explicit_ref_links(self, explicit_ref_id_links = None, explicit_ref_ids = None, link_names = None, link_name_like = None):
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

    def get_linked_explicit_refs(self, group_ids = None, explicit_ref_ids = None, explicit_refs = None, explicit_ref_like = None, ingestion_time_filters = None, link_names = None, link_name_like = None):
        
        # Obtain prime explicit_refs 
        prime_explicit_refs = self.get_explicit_refs(group_ids = group_ids, explicit_ref_ids = explicit_ref_ids, explicit_refs = explicit_refs, explicit_ref_like = explicit_ref_like, ingestion_time_filters = ingestion_time_filters)

        prime_explicit_ref_uuids = [str(explicit_ref.explicit_ref_id) for explicit_ref in prime_explicit_refs]

        # Obtain the links from the prime explicit_refs to other explicit_refs
        links = []
        if len(prime_explicit_ref_uuids) > 0:
            links = self.get_explicit_ref_links(explicit_ref_id_links = {"list": prime_explicit_ref_uuids, "op": "in"}, link_names = link_names, link_name_like = link_name_like)
        # end if

        # Obtain the explicit_refs linked by the prime explicit_refs
        linked_explicit_ref_uuids = [str(link.explicit_ref_id) for link in links]
        linked_explicit_refs = []
        if len(linked_explicit_ref_uuids) > 0:
            linked_explicit_refs = self.get_explicit_refs(explicit_ref_ids = {"list": linked_explicit_ref_uuids, "op": "in"})
        # end if

        return prime_explicit_refs + linked_explicit_refs

    def get_linked_explicit_refs_join(self, explicit_refs = None, explicit_ref_like = None, sources = None, source_like = None, gauge_names = None, gauge_name_like = None, gauge_systems = None, gauge_system_like = None, start_filters = None, stop_filters = None, explicit_ref_ingestion_time_filters = None, event_value_filters = None, event_values_names_type = None, event_values_name_type_like = None, annotation_cnf_names = None, annotation_cnf_name_like = None, annotation_cnf_systems = None, annotation_cnf_system_like = None, annotation_value_filters = None, annotation_values_names_type = None, annotation_values_name_type_like = None, expl_groups = None, expl_group_like = None, link_names = None, link_name_like = None):
        
        # Obtain prime events 
        prime_explicit_refs = self.get_explicit_refs_join(explicit_refs = explicit_refs, explicit_ref_like = explicit_ref_like, sources = sources, source_like = source_like, gauge_names = gauge_names, gauge_name_like = gauge_name_like, gauge_systems = gauge_systems, gauge_system_like = gauge_system_like, start_filters = start_filters, stop_filters = stop_filters, explicit_ref_ingestion_time_filters = explicit_ref_ingestion_time_filters, event_value_filters = event_value_filters, event_values_names_type = event_values_names_type, event_values_name_type_like = event_values_name_type_like, annotation_cnf_names = annotation_cnf_names, annotation_cnf_name_like = annotation_cnf_name_like, annotation_cnf_systems = annotation_cnf_systems, annotation_cnf_system_like = annotation_cnf_system_like, annotation_value_filters = annotation_value_filters, annotation_values_names_type = annotation_values_names_type, annotation_values_name_type_like = annotation_values_name_type_like, expl_groups = expl_groups, expl_group_like = expl_group_like)

        prime_explicit_ref_uuids = [str(explicit_ref.explicit_ref_id) for explicit_ref in prime_explicit_refs]

        # Obtain the links from the prime explicit_refs to other explicit_refs
        links = []
        if len(prime_explicit_ref_uuids) > 0:
            links = self.get_explicit_ref_links(explicit_ref_id_links = {"list": prime_explicit_ref_uuids, "op": "in"}, link_names = link_names, link_name_like = link_name_like)
        # end if

        # Obtain the explicit_refs linked by the prime explicit_refs
        linked_explicit_ref_uuids = [str(link.explicit_ref_id) for link in links]
        linked_explicit_refs = []
        if len(linked_explicit_ref_uuids) > 0:
            linked_explicit_refs = self.get_explicit_refs(explicit_ref_ids = {"list": linked_explicit_ref_uuids, "op": "in"})
        # end if

        return prime_explicit_refs + linked_explicit_refs

    def get_explicit_refs_groups(self, group_ids = None, names = None, name_like = None):
        """
        """
        params = []
        # Group UUIDs
        if group_ids != None:
            is_valid_operator_list(group_ids)
            filter = eval('ExplicitRefGrp.expl_ref_cnf_id.' + group_ids["op"] + '_')
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
