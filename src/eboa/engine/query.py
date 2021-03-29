"""
Engine definition

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import time
import datetime
from datetime import timedelta
from lxml import etree
import uuid
from oslo_concurrency import lockutils

# Import GEOalchemy entities
from geoalchemy2 import functions
from geoalchemy2.shape import to_shape

# Import SQLalchemy entities
from sqlalchemy import extract
from sqlalchemy.orm import scoped_session

# Import datamodel
from eboa.datamodel.base import Session, engine, Base
from eboa.datamodel.dim_signatures import DimSignature
from eboa.datamodel.alerts import Alert, AlertGroup, EventAlert, AnnotationAlert, SourceAlert, ExplicitRefAlert
from eboa.datamodel.events import Event, EventLink, EventKey, EventText, EventDouble, EventObject, EventGeometry, EventBoolean, EventTimestamp
from eboa.datamodel.gauges import Gauge
from eboa.datamodel.sources import Source, SourceStatus
from eboa.datamodel.explicit_refs import ExplicitRef, ExplicitRefGrp, ExplicitRefLink
from eboa.datamodel.annotations import Annotation, AnnotationCnf, AnnotationText, AnnotationDouble, AnnotationObject, AnnotationGeometry, AnnotationBoolean, AnnotationTimestamp
from sqlalchemy.dialects import postgresql
from rboa.datamodel.reports import Report, ReportGroup, ReportStatus, ReportText, ReportDouble, ReportObject, ReportGeometry, ReportBoolean, ReportTimestamp
from rboa.datamodel.alerts import ReportAlert
from sqlalchemy.sql import text

# Import exceptions
from eboa.engine.errors import InputError, ErrorParsingFilters

# Import auxiliary functions
import eboa.engine.functions as functions

# Import logging
from eboa.logging import Log

# Import query printing facilities
from eboa.engine.printing import literal_query

# Import operators
from eboa.engine.operators import arithmetic_operators, text_operators, regex_operators

# Import SQLalchemy exceptions
from sqlalchemy.orm.exc import StaleDataError

# Import alert severity codes
from eboa.engine.alerts import alert_severity_codes

# Import parsing module
import eboa.engine.parsing as parsing

logging = Log(name = __name__)
logger = logging.logger

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

    try:
        logger.debug("The following query is going to be executed: {}".format(literal_query(query.statement)))
    except NotImplementedError as e:
        logger.debug("The query has some elements which cannot be displayed. The exception occurred, trying to displayed the query, was the following: {}".format(e))
        pass
    # end try

    return

def check_key_in_filters(filters, key):

    result = False
    if filters != None and key in filters and filters[key] != None:
        result = True
    # end if

    return result

class Query():
    """Class for querying data to the eboa module

    Provides access to the logic for querying
    the information stored into the DDBB
    """

    # Set the synchronized module
    synchronized_eboa = lockutils.synchronized_with_prefix('eboa-')
    synchronized_rboa = lockutils.synchronized_with_prefix('rboa-')

    def __init__(self, session = None):
        """
        Class for querying the information stored into DDBB

        :param session: opened session
        :type session: sqlalchemy.orm.sessionmaker
        """
        if session == None:
            Scoped_session = scoped_session(Session)
            self.session = Scoped_session()
        else:
            self.session = session
        # end if

        return

    def clear_db(self):
        for table in reversed(Base.metadata.sorted_tables):
            engine.execute(table.delete())
        # end for

    def delete(self, query):
        logger.info("Deletion request received")
        query_executed = False
        while not query_executed:
            try:
                query.delete(synchronize_session=False)
                self.session.commit()
                query_executed = True
            except StaleDataError:
                pass
            # end try
            if not query_executed:
                logger.info("Deletion could not be performed. Trying again after 1 second...")
                time.sleep(1)
            # end if
        # end while
        logger.info("Deletion request performed")
    # end def
        
    def get_dim_signatures(self, dim_signature_uuids = None, dim_signatures = None, order_by = None, limit = None, offset = None):
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
            if dim_signature_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[dim_signature_uuids["op"]]
                params.append(op(DimSignature.dim_signature_uuid, dim_signature_uuids["filter"]))
            else:
                filter = eval('DimSignature.dim_signature_uuid.' + text_operators[dim_signature_uuids["op"]])
                params.append(filter(dim_signature_uuids["filter"]))
            # end if
        # end if

        # DIM signatures
        if dim_signatures != None:
            functions.is_valid_text_filter(dim_signatures)
            if dim_signatures["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[dim_signatures["op"]]
                params.append(op(DimSignature.dim_signature, dim_signatures["filter"]))
            else:
                filter = eval('DimSignature.dim_signature.' + text_operators[dim_signatures["op"]])
                params.append(filter(dim_signatures["filter"]))
            # end if
        # end if

        query = self.session.query(DimSignature).filter(*params)

        # Order by
        if order_by != None:
            functions.is_valid_order_by(order_by)
            if order_by["descending"]:
                order_by_statement = eval("DimSignature." + order_by["field"] + ".desc()")
            else:
                order_by_statement = eval("DimSignature." + order_by["field"])
            # end if
            query = query.order_by(order_by_statement)
        # end if

        # Limit
        if limit != None:
            functions.is_valid_positive_integer(limit)
            query = query.limit(limit)
        # end if

        # Offset
        if offset != None:
            functions.is_valid_positive_integer(offset)
            query = query.offset(offset)
        # end if
        
        log_query(query)
        dim_signatures = query.all()

        return dim_signatures

    def get_sources(self, names = None, validity_start_filters = None, validity_stop_filters = None, validity_duration_filters = None, reported_validity_start_filters = None, reported_validity_stop_filters = None, reported_validity_duration_filters = None, reception_time_filters = None, generation_time_filters = None, reported_generation_time_filters = None, ingestion_time_filters = None, processing_duration_filters = None, ingestion_duration_filters = None, processors = None, processor_version_filters = None, ingestion_completeness = None, ingested = None, ingestion_error = None, dim_signature_uuids = None, source_uuids = None, dim_signatures = None, statuses = None, delete = False, synchronize_deletion = True, order_by = None, limit = None, offset = None):
        """
        Method to obtain the sources entities filtered by the received parameters

        :param names: source name filters
        :type names: text_filter
        :param validity_start_filters: list of validity start filters
        :type validity_start_filters: date_filters
        :param validity_stop_filters: list of validity stop filters
        :type validity_stop_filters: date_filters
        :param validity_duration_filters: list of validity duration filters
        :type validity_duration_filters: float_filters
        :param reported_validity_start_filters: list of reported validity start filters
        :type reported_validity_start_filters: date_filters
        :param reported_validity_stop_filters: list of reported validity stop filters
        :type reported_validity_stop_filters: date_filters
        :param reported_validity_duration_filters: list of reported validity duration filters
        :type reported_validity_duration_filters: float_filters
        :param reception_time_filters: list of reception time filters
        :type reception_time_filters: date_filters
        :param generation_time_filters: list of generation time filters
        :type generation_time_filters: date_filters
        :param reported_generation_time_filters: list of reported generation time filters
        :type reported_generation_time_filters: date_filters
        :param ingestion_time_filters: list of ingestion time filters
        :type ingestion_time_filters: date_filters
        :param processing_duration_filters: list of processing duration filters
        :type processing_duration_filters: float_filters
        :param ingestion_duration_filters: list of ingestion duration filters
        :type ingestion_duration_filters: float_filters
        :param procesors: processor filters
        :type procesors: text_filter
        :param processor_version_filters: list of version filters
        :type processor_version_filters: text_filter
        :param ingestion_completeness: flag to indicate if the ingestion is complete (all dependencies met)
        :type ingestion_completeness: boolean_filter
        :param ingested: flag to indicate if file was processed and ingested
        :type ingested: boolean_filter
        :param ingestion_error: flag to indicate if ingestion ended up in error
        :type ingestion_error: boolean_filter
        :param dim_signature_uuids: list of DIM signature identifiers
        :type dim_signature_uuids: text_filter
        :param source_uuids: list of source identifiers
        :type source_uuids: text_filter
        :param dim_signatures: DIM signature filters
        :type dim_signatures: text_filter
        :param statuses: status filters
        :type statuses: float_filter
        :param delete: flag to indicate if the sources found by the query have to be deleted
        :type delete: bool
        :param synchronize_deletion: flag to indicate if the deletion process has to be synchronized to avoid race conditions
        :type synchronize_deletion: bool
        :param order_by: field to order by
        :type order_by: order_by statement
        :param limit: Positive integer to limit the number of results of the query
        :type limit: Positive integer
        :param offset: Positive integer to offset the pointer to the list of results
        :type offset: Positive integer

        :return: found sources
        :rtype: list
        """
        params = []
        tables = []
        # DIM signature UUIDs
        if dim_signature_uuids != None:
            functions.is_valid_text_filter(dim_signature_uuids)
            if dim_signature_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[dim_signature_uuids["op"]]
                params.append(op(Source.dim_signature_uuid, dim_signature_uuids["filter"]))
            else:
                filter = eval('Source.dim_signature_uuid.' + text_operators[dim_signature_uuids["op"]])
                params.append(filter(dim_signature_uuids["filter"]))
            # end if
        # end if

        # Source UUIDs
        if source_uuids != None:
            functions.is_valid_text_filter(source_uuids)
            if source_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[source_uuids["op"]]
                params.append(op(Source.source_uuid, source_uuids["filter"]))
            else:
                filter = eval('Source.source_uuid.' + text_operators[source_uuids["op"]])
                params.append(filter(source_uuids["filter"]))
            # end if
        # end if

        # Source names
        if names != None:
            functions.is_valid_text_filter(names)
            if names["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[names["op"]]
                params.append(op(Source.name, names["filter"]))
            else:
                filter = eval('Source.name.' + text_operators[names["op"]])
                params.append(filter(names["filter"]))
            # end if
        # end if

        # validity_start filters
        if validity_start_filters != None:
            functions.is_valid_date_filters(validity_start_filters)
            for validity_start_filter in validity_start_filters:
                op = arithmetic_operators[validity_start_filter["op"]]
                params.append(op(Source.validity_start, validity_start_filter["date"]))
            # end for
        # end if

        # validity_stop filters
        if validity_stop_filters != None:
            functions.is_valid_date_filters(validity_stop_filters)
            for validity_stop_filter in validity_stop_filters:
                op = arithmetic_operators[validity_stop_filter["op"]]
                params.append(op(Source.validity_stop, validity_stop_filter["date"]))
            # end for
        # end if

        # validity duration filters
        if validity_duration_filters != None:
            functions.is_valid_float_filters(validity_duration_filters)
            for validity_duration_filter in validity_duration_filters:
                op = arithmetic_operators[validity_duration_filter["op"]]
                params.append(op((extract("epoch", Source.validity_stop) - extract("epoch", Source.validity_start)), validity_duration_filter["float"]))
            # end for
        # end if

        # reported_validity_start filters
        if reported_validity_start_filters != None:
            functions.is_valid_date_filters(reported_validity_start_filters)
            for reported_validity_start_filter in reported_validity_start_filters:
                op = arithmetic_operators[reported_validity_start_filter["op"]]
                params.append(op(Source.reported_validity_start, reported_validity_start_filter["date"]))
            # end for
        # end if

        # reported_validity_stop filters
        if reported_validity_stop_filters != None:
            functions.is_valid_date_filters(reported_validity_stop_filters)
            for reported_validity_stop_filter in reported_validity_stop_filters:
                op = arithmetic_operators[reported_validity_stop_filter["op"]]
                params.append(op(Source.reported_validity_stop, reported_validity_stop_filter["date"]))
            # end for
        # end if

        # reported_validity duration filters
        if reported_validity_duration_filters != None:
            functions.is_valid_float_filters(reported_validity_duration_filters)
            for reported_validity_duration_filter in reported_validity_duration_filters:
                op = arithmetic_operators[reported_validity_duration_filter["op"]]
                params.append(op((extract("epoch", Source.reported_validity_stop) - extract("epoch", Source.reported_validity_start)), reported_validity_duration_filter["float"]))
            # end for
        # end if

        # reception_time filters
        if reception_time_filters != None:
            functions.is_valid_date_filters(reception_time_filters)
            for reception_time_filter in reception_time_filters:
                op = arithmetic_operators[reception_time_filter["op"]]
                params.append(op(Source.reception_time, reception_time_filter["date"]))
            # end for
        # end if
        
        # generation_time filters
        if generation_time_filters != None:
            functions.is_valid_date_filters(generation_time_filters)
            for generation_time_filter in generation_time_filters:
                op = arithmetic_operators[generation_time_filter["op"]]
                params.append(op(Source.generation_time, generation_time_filter["date"]))
            # end for
        # end if

        # reported_generation_time filters
        if reported_generation_time_filters != None:
            functions.is_valid_date_filters(reported_generation_time_filters)
            for reported_generation_time_filter in reported_generation_time_filters:
                op = arithmetic_operators[reported_generation_time_filter["op"]]
                params.append(op(Source.reported_generation_time, reported_generation_time_filter["date"]))
            # end for
        # end if

        # ingestion_time filters
        if ingestion_time_filters != None:
            functions.is_valid_date_filters(ingestion_time_filters)
            for ingestion_time_filter in ingestion_time_filters:
                op = arithmetic_operators[ingestion_time_filter["op"]]
                params.append(op(Source.ingestion_time, ingestion_time_filter["date"]))
            # end for
        # end if

        # processing_duration filters
        if processing_duration_filters != None:
            functions.is_valid_float_filters(processing_duration_filters)
            for processing_duration_filter in processing_duration_filters:
                op = arithmetic_operators[processing_duration_filter["op"]]
                params.append(op(Source.processing_duration, timedelta(seconds = processing_duration_filter["float"])))
            # end for
        # end if

        # ingestion_duration filters
        if ingestion_duration_filters != None:
            functions.is_valid_float_filters(ingestion_duration_filters)
            for ingestion_duration_filter in ingestion_duration_filters:
                op = arithmetic_operators[ingestion_duration_filter["op"]]
                params.append(op(Source.ingestion_duration, timedelta(seconds = ingestion_duration_filter["float"])))
            # end for
        # end if

        # ingestion_completeness filter
        if ingestion_completeness != None:
            functions.is_valid_bool_filter(ingestion_completeness)
            params.append(Source.ingestion_completeness == ingestion_completeness)
        # end if

        # ingested filter
        if ingested != None:
            functions.is_valid_bool_filter(ingested)
            params.append(Source.ingested == ingested)
        # end if

        # ingestion_error filter
        if ingestion_error != None:
            functions.is_valid_bool_filter_with_op(ingestion_error)
            op = arithmetic_operators[ingestion_error["op"]]
            params.append(op(Source.ingestion_error, ingestion_error["filter"]))
        # end if

        # Processors
        if processors != None:
            functions.is_valid_text_filter(processors)
            if processors["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[processors["op"]]
                params.append(op(Source.processor, processors["filter"]))
            else:
                filter = eval('Source.processor.' + text_operators[processors["op"]])
                params.append(filter(processors["filter"]))
            # end if
        # end if

        # processor_version filters
        if processor_version_filters != None:
            for processor_version_filter in processor_version_filters:
                functions.is_valid_text_filter(processor_version_filter)

                if processor_version_filter["op"] in arithmetic_operators.keys():
                    op = arithmetic_operators[processor_version_filter["op"]]
                    params.append(op(Source.processor_version, processor_version_filter["filter"]))
                else:
                    filter = eval('Source.processor_version.' + text_operators[processor_version_filter["op"]])
                    params.append(filter(processor_version_filter["filter"]))
                # end if
            # end for
        # end if

        # status filters
        if statuses != None:
            functions.is_valid_text_filter(statuses)
            if statuses["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[statuses["op"]]
                params.append(op(SourceStatus.status, statuses["filter"]))
            else:
                filter = eval('SourceStatus.status.' + text_operators[statuses["op"]])
                params.append(filter(statuses["filter"]))
            # end if
            tables.append(SourceStatus)
        # end if

        # DIM signatures
        if dim_signatures != None:
            functions.is_valid_text_filter(dim_signatures)
            if dim_signatures["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[dim_signatures["op"]]
                params.append(op(DimSignature.dim_signature, dim_signatures["filter"]))
            else:
                filter = eval('DimSignature.dim_signature.' + text_operators[dim_signatures["op"]])
                params.append(filter(dim_signatures["filter"]))
            # end if
            tables.append(DimSignature)
        # end if

        query = self.session.query(Source)
        for table in set(tables):
            query = query.join(table)
        # end for

        query = query.filter(*params)

        # Order by
        if order_by != None:
            functions.is_valid_order_by(order_by)
            if order_by["descending"]:
                order_by_statement = eval("Source." + order_by["field"] + ".desc()")
            else:
                order_by_statement = eval("Source." + order_by["field"])
            # end if
            query = query.order_by(order_by_statement)
        # end if

        # Limit
        if limit != None:
            functions.is_valid_positive_integer(limit)
            query = query.limit(limit)
        # end if

        # Offset
        if offset != None:
            functions.is_valid_positive_integer(offset)
            query = query.offset(offset)
        # end if

        log_query(query)

        sources = []
        if delete:
            if synchronize_deletion:
                sources = sorted(query.all(), key=lambda x:(x.name))
                for source in sources:
                    name = source.name
                    logger.info("The source with name {} is going to be removed".format(name))
                    uuid = source.source_uuid
                    dim_signature_uuid = source.dim_signature_uuid
                    lock = "treat_data_" + source.name
                    @self.synchronized_eboa(lock, external=True, lock_path="/dev/shm")
                    def _delete_source(self, source):
                        # Obtain events related to the source
                        event_uuids_related_to_source = self.session.query(Event.event_uuid).filter(Event.source_uuid == source.source_uuid)
                        # Remove the event links which source is related to the events associated to the source to be removed
                        self.session.query(EventLink).filter(EventLink.event_uuid_link.in_(event_uuids_related_to_source)).delete(synchronize_session=False)
                        # Remove source
                        self.delete(self.session.query(Source).with_for_update().filter(Source.source_uuid == source.source_uuid))
                    # end def
                    _delete_source(self, source)
                    logger.info("The source with name {}, UUID {} and DIM signature UUID {} has been removed".format(name, uuid, dim_signature_uuid))
                # end for
            else:
                sources = query.all()
                source_names = [source.name for source in sources]
                logger.info("The sources with names {} are going to be removed".format(source_names))
                # Obtain events related to the source
                event_uuids_related_to_sources = self.session.query(Event.event_uuid).filter(Event.source_uuid.in_([source.source_uuid for source in sources]))
                # Remove the event links which source is related to the events associated to the source to be removed
                self.session.query(EventLink).filter(EventLink.event_uuid_link.in_(event_uuids_related_to_sources)).delete(synchronize_session=False)
                # Remove sources
                self.delete(self.session.query(Source).with_for_update().filter(Source.source_uuid.in_([source.source_uuid for source in sources])))
                logger.info("The sources with names {} have been removed".format(source_names))
            # end if
        else:
            sources = query.all()
        # end if

        return sources

    def get_source_alerts(self, filters = None):
        """
        Method to obtain the alerts associated to source entities filtered by the received filters

        :param filters: dictionary with the filters to apply to the query
        :type filters: dict

        :return: found source_alerts
        :rtype: list
        """
        params = []
        join_tables = False
        tables = {}

        # Source UUIDs
        if check_key_in_filters(filters, "source_uuids"):
            functions.is_valid_text_filter(filters["source_uuids"])
            if filters["source_uuids"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["source_uuids"]["op"]]
                params.append(op(SourceAlert.source_uuid, filters["source_uuids"]["filter"]))
            else:
                filter = eval('SourceAlert.source_uuid.' + text_operators[filters["source_uuids"]["op"]])
                params.append(filter(filters["source_uuids"]["filter"]))
            # end if
        # end if

        # DIM signature UUIDs
        if check_key_in_filters(filters, "dim_signature_uuids"):
            functions.is_valid_text_filter(filters["dim_signature_uuids"])
            if filters["dim_signature_uuids"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["dim_signature_uuids"]["op"]]
                params.append(op(Source.dim_signature_uuid, filters["dim_signature_uuids"]["filter"]))
            else:
                filter = eval('Source.dim_signature_uuid.' + text_operators[filters["dim_signature_uuids"]["op"]])
                params.append(filter(filters["dim_signature_uuids"]["filter"]))
            # end if
            join_tables = True
        # end if

        # Source names
        if check_key_in_filters(filters, "source_names"):
            functions.is_valid_text_filter(filters["source_names"])
            if filters["source_names"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["source_names"]["op"]]
                params.append(op(Source.name, filters["source_names"]["filter"]))
            else:
                filter = eval('Source.name.' + text_operators[filters["source_names"]["op"]])
                params.append(filter(filters["source_names"]["filter"]))
            # end if
            join_tables = True
        # end if

        # validity_start filters
        if check_key_in_filters(filters, "validity_start_filters"):
            functions.is_valid_date_filters(filters["validity_start_filters"])
            for validity_start_filter in filters["validity_start_filters"]:
                op = arithmetic_operators[validity_start_filter["op"]]
                params.append(op(Source.validity_start, validity_start_filter["date"]))
            # end for
            join_tables = True
        # end if

        # validity_stop filters
        if check_key_in_filters(filters, "validity_stop_filters"):
            functions.is_valid_date_filters(filters["validity_stop_filters"])
            for validity_stop_filter in filters["validity_stop_filters"]:
                op = arithmetic_operators[validity_stop_filter["op"]]
                params.append(op(Source.validity_stop, validity_stop_filter["date"]))
            # end for
            join_tables = True
        # end if

        # validity duration filters
        if check_key_in_filters(filters, "validity_duration_filters"):
            functions.is_valid_float_filters(filters["validity_duration_filters"])
            for validity_duration_filter in filters["validity_duration_filters"]:
                op = arithmetic_operators[validity_duration_filter["op"]]
                params.append(op((extract("epoch", Source.validity_stop) - extract("epoch", Source.validity_start)), validity_duration_filter["float"]))
            # end for
            join_tables = True
        # end if

        # reception_time filters
        if check_key_in_filters(filters, "reception_time_filters"):
            functions.is_valid_date_filters(filters["reception_time_filters"])
            for reception_time_filter in filters["reception_time_filters"]:
                op = arithmetic_operators[reception_time_filter["op"]]
                params.append(op(Source.reception_time, reception_time_filter["date"]))
            # end for
            join_tables = True
        # end if
        
        # generation_time filters
        if check_key_in_filters(filters, "generation_time_filters"):
            functions.is_valid_date_filters(filters["generation_time_filters"])
            for generation_time_filter in filters["generation_time_filters"]:
                op = arithmetic_operators[generation_time_filter["op"]]
                params.append(op(Source.generation_time, generation_time_filter["date"]))
            # end for
            join_tables = True
        # end if

        # source ingestion_time filters
        if check_key_in_filters(filters, "source_ingestion_time_filters"):
            functions.is_valid_date_filters(filters["source_ingestion_time_filters"])
            for ingestion_time_filter in filters["source_ingestion_time_filters"]:
                op = arithmetic_operators[ingestion_time_filter["op"]]
                params.append(op(Source.ingestion_time, ingestion_time_filter["date"]))
            # end for
            join_tables = True
        # end if

        # ingestion_duration filters
        if check_key_in_filters(filters, "ingestion_duration_filters"):
            functions.is_valid_float_filters(filters["ingestion_duration_filters"])
            for ingestion_duration_filter in filters["ingestion_duration_filters"]:
                op = arithmetic_operators[ingestion_duration_filter["op"]]
                params.append(op(Source.ingestion_duration, timedelta(seconds = ingestion_duration_filter["float"])))
            # end for
            join_tables = True
        # end if

        # ingested filter
        if check_key_in_filters(filters, "ingested"):
            functions.is_valid_bool_filter(filters["ingested"])
            params.append(Source.ingested == filters["ingested"])
            join_tables = True
        # end if

        # ingestion_error filter
        if check_key_in_filters(filters, "ingestion_error"):
            functions.is_valid_bool_filter_with_op(filters["ingestion_error"])
            op = arithmetic_operators[filters["ingestion_error"]["op"]]
            params.append(op(Source.ingestion_error, filters["ingestion_error"]["filter"]))
            join_tables = True
        # end if

        # Processors
        if check_key_in_filters(filters, "processors"):
            functions.is_valid_text_filter(filters["processors"])
            if filters["processors"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["processors"]["op"]]
                params.append(op(Source.processor, filters["processors"]["filter"]))
            else:
                filter = eval('Source.processor.' + text_operators[filters["processors"]["op"]])
                params.append(filter(filters["processors"]["filter"]))
            # end if
            join_tables = True
        # end if

        # processor_version filters
        if check_key_in_filters(filters, "processor_version_filters"):
            for processor_version_filter in filters["processor_version_filters"]:
                functions.is_valid_text_filter(processor_version_filter)

                if processor_version_filter["op"] in arithmetic_operators.keys():
                    op = arithmetic_operators[processor_version_filter["op"]]
                    params.append(op(Source.processor_version, processor_version_filter["filter"]))
                else:
                    filter = eval('Source.processor_version.' + text_operators[processor_version_filter["op"]])
                    params.append(filter(processor_version_filter["filter"]))
                # end if
            # end for
            join_tables = True
        # end if

        # status filters
        if check_key_in_filters(filters, "statuses"):
            functions.is_valid_text_filter(filters["statuses"])
            if filters["statuses"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["statuses"]["op"]]
                params.append(op(SourceStatus.status, filters["statuses"]["filter"]))
            else:
                filter = eval('SourceStatus.status.' + text_operators[filters["statuses"]["op"]])
                params.append(filter(filters["statuses"]["filter"]))
            # end if
            join_tables = True
            tables[SourceStatus] = SourceStatus.source_uuid==Source.source_uuid
        # end if

        # DIM signatures
        if check_key_in_filters(filters, "dim_signatures"):
            functions.is_valid_text_filter(filters["dim_signatures"])
            if filters["dim_signatures"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["dim_signatures"]["op"]]
                params.append(op(DimSignature.dim_signature, filters["dim_signatures"]["filter"]))
            else:
                filter = eval('DimSignature.dim_signature.' + text_operators[filters["dim_signatures"]["op"]])
                params.append(filter(filters["dim_signatures"]["filter"]))
            # end if
            join_tables = True
            tables[DimSignature] = DimSignature.dim_signature_uuid==Source.dim_signature_uuid
        # end if

        query = self.session.query(SourceAlert)
        if len(tables.keys()) > 0 or join_tables:
            query = query.join(Source, Source.source_uuid==SourceAlert.source_uuid)
            for table in tables:
                query = query.join(table, tables[table])
            # end for
        # end if

        join_tables = False
        tables = {}        

        # Alert configuration names
        if check_key_in_filters(filters, "names"):
            functions.is_valid_text_filter(filters["names"])
            if filters["names"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["names"]["op"]]
                params.append(op(Alert.name, filters["names"]["filter"]))
            else:
                filter = eval('Alert.name.' + text_operators[filters["names"]["op"]])
                params.append(filter(filters["names"]["filter"]))
            # end if
            join_tables = True
        # end if

        # Alert severities
        if check_key_in_filters(filters, "severities"):
            functions.is_valid_severity_filter(filters["severities"])
            if filters["severities"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["severities"]["op"]]
                params.append(op(Alert.severity, filters["severities"]["filter"]))
            else:
                filters = [alert_severity_codes[severity_filter] for severity_filter in filters["severities"]["filter"]]
                filter = eval('Alert.severity.' + text_operators[filters["severities"]["op"]])
                params.append(filter(filters))
            # end if
            join_tables = True
        # end if

        # Alert groups
        if check_key_in_filters(filters, "groups"):
            functions.is_valid_text_filter(filters["groups"])
            if filters["groups"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["groups"]["op"]]
                params.append(op(AlertGroup.name, filters["groups"]["filter"]))
            else:
                filter = eval('AlertGroup.name.' + text_operators[filters["groups"]["op"]])
                params.append(filter(filters["groups"]["filter"]))
            # end if
            join_tables = True
            tables[AlertGroup] = AlertGroup.alert_group_uuid==Alert.alert_group_uuid
        # end if
        
        if len(tables.keys()) > 0 or join_tables:
            query = query.join(Alert, Alert.alert_uuid==SourceAlert.alert_uuid)
            for table in tables:
                query = query.join(table, tables[table])
            # end for
        # end if
        
        # Alert UUIDs
        if check_key_in_filters(filters, "alert_uuids"):
            functions.is_valid_text_filter(filters["alert_uuids"])
            if filters["alert_uuids"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["alert_uuids"]["op"]]
                params.append(op(SourceAlert.alert_uuid, filters["alert_uuids"]["filter"]))
            else:
                filter = eval('SourceAlert.alert_uuid.' + text_operators[filters["alert_uuids"]["op"]])
                params.append(filter(filters["alert_uuids"]["filter"]))
            # end if
        # end if
        
        # validated filter
        if check_key_in_filters(filters, "validated"):
            functions.is_valid_bool_filter(filters["validated"])
            params.append(SourceAlert.validated == filters["validated"])
        # end if
        
        # source alert ingestion_time filters
        if check_key_in_filters(filters, "alert_ingestion_time_filters"):
            functions.is_valid_date_filters(filters["alert_ingestion_time_filters"])
            for ingestion_time_filter in filters["alert_ingestion_time_filters"]:
                op = arithmetic_operators[ingestion_time_filter["op"]]
                params.append(op(SourceAlert.ingestion_time, ingestion_time_filter["date"]))
            # end for
        # end if

        # Generators
        if check_key_in_filters(filters, "generators"):
            functions.is_valid_text_filter(filters["generators"])
            if filters["generators"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["generators"]["op"]]
                params.append(op(SourceAlert.generator, filters["generators"]["filter"]))
            else:
                filter = eval('SourceAlert.generator.' + text_operators[filters["generators"]["op"]])
                params.append(filter(filters["generators"]["filter"]))
            # end if
        # end if

        # notified filter
        if check_key_in_filters(filters, "notified"):
            functions.is_valid_bool_filter(filters["notified"])
            params.append(SourceAlert.notified == filters["notified"])
        # end if

        # solved filter
        if check_key_in_filters(filters, "solved"):
            functions.is_valid_bool_filter(filters["solved"])
            params.append(SourceAlert.solved == filters["solved"])
        # end if

        # source alert solved time filters
        if check_key_in_filters(filters, "solved_time_filters"):
            functions.is_valid_date_filters(filters["solved_time_filters"])
            for solved_time_filter in filters["solved_time_filters"]:
                op = arithmetic_operators[solved_time_filter["op"]]
                params.append(op(SourceAlert.solved_time, solved_time_filter["date"]))
            # end for
        # end if

        # source alert notification time filters
        if check_key_in_filters(filters, "notification_time_filters"):
            functions.is_valid_date_filters(filters["notification_time_filters"])
            for notification_time_filter in filters["notification_time_filters"]:
                op = arithmetic_operators[notification_time_filter["op"]]
                params.append(op(SourceAlert.notification_time, notification_time_filter["date"]))
            # end for
        # end if
                
        query = query.filter(*params)

        # Order by
        if check_key_in_filters(filters, "order_by"):
            functions.is_valid_order_by(filters["order_by"])
            if filters["order_by"]["descending"]:
                order_by_statement = eval("SourceAlert." + filters["order_by"]["field"] + ".desc()")
            else:
                order_by_statement = eval("SourceAlert." + filters["order_by"]["field"])
            # end if
            query = query.order_by(order_by_statement)
        # end if

        # Limit
        if check_key_in_filters(filters, "limit"):
            functions.is_valid_positive_integer(filters["limit"])
            query = query.limit(filters["limit"])
        # end if

        # Offset
        if check_key_in_filters(filters, "offset"):
            functions.is_valid_positive_integer(filters["offset"])
            query = query.offset(filters["offset"])
        # end if

        log_query(query)

        source_alerts = []
        if check_key_in_filters(filters, "delete") and filters["delete"]:
            self.delete(query)
        else:
            source_alerts = query.all()
        # end if

        return source_alerts

    def get_reports(self, names = None, generation_modes = None, validity_start_filters = None, validity_stop_filters = None, validity_duration_filters = None, triggering_time_filters = None, generation_start_filters = None, generation_stop_filters = None, metadata_ingestion_duration_filters = None, generated = None, compressed = None, generators = None, generator_version_filters = None, generation_error = None, report_group_uuids = None, report_uuids = None, report_groups = None, statuses = None, delete = False, synchronize_deletion = True, order_by = None, limit = None, offset = None):
        """
        Method to obtain the reports entities filtered by the received parameters

        :param report_uuids: list of report identifiers
        :type report_uuids: text_filter
        :param names: report name filters
        :type names: text_filter
        :param generation_modes: report name filters
        :type generation_modes: text_filter
        :param validity_start_filters: list of start filters
        :type validity_start_filters: date_filters
        :param validity_stop_filters: list of stop filters
        :type validity_stop_filters: date_filters
        :param validity_duration_filters: list of duration filters
        :type validity_duration_filters: float_filters
        :param triggering_time_filters: list of triggering time filters
        :type triggering_time_filters: date_filters
        :param generation_start_filters: list of generation start filters
        :type generation_start_filters: date_filters
        :param generation_stop_filters: list of generation stop filters
        :type generation_stop_filters: date_filters
        :param metadata_ingestion_duration_filters: list of metadata ingestion duration filters
        :type metadata_ingestion_duration_filters: float_filters
        :param generated: flag to indicate if the report was generated or not
        :type generated: boolean_filter
        :param compressed: flag to indicate if the report was compressed or not
        :type compressed: boolean_filter
        :param generators: generators filters
        :type generators: text_filter
        :param generator_version_filters: list of version filters
        :type generator_version_filters: text_filter
        :param generation_error: flag to indicate if the report generation ended in error or not
        :type generation_error: boolean_filter
        :param report_group_uuids: list of report group identifiers
        :type report_group_uuids: text_filter
        :param report_uuids: list of report identifiers
        :type report_uuids: text_filter
        :param report_groups: report group filters
        :type report_groups: text_filter
        :param statuses: status filters
        :type statuses: float_filter

        :return: found reports
        :rtype: list
        """
        params = []
        tables = []

        # Report names
        if names != None:
            functions.is_valid_text_filter(names)
            if names["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[names["op"]]
                params.append(op(Report.name, names["filter"]))
            else:
                filter = eval('Report.name.' + text_operators[names["op"]])
                params.append(filter(names["filter"]))
            # end if
        # end if

        # Report generation modes
        if generation_modes != None:
            functions.is_valid_text_filter(generation_modes)
            if generation_modes["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[generation_modes["op"]]
                params.append(op(Report.generation_mode, generation_modes["filter"]))
            else:
                filter = eval('Report.generation_mode.' + text_operators[generation_modes["op"]])
                params.append(filter(generation_modes["filter"]))
            # end if
        # end if

        # validity_start filters
        if validity_start_filters != None:
            functions.is_valid_date_filters(validity_start_filters)
            for validity_start_filter in validity_start_filters:
                op = arithmetic_operators[validity_start_filter["op"]]
                params.append(op(Report.validity_start, validity_start_filter["date"]))
            # end for
        # end if

        # validity_stop filters
        if validity_stop_filters != None:
            functions.is_valid_date_filters(validity_stop_filters)
            for validity_stop_filter in validity_stop_filters:
                op = arithmetic_operators[validity_stop_filter["op"]]
                params.append(op(Report.validity_stop, validity_stop_filter["date"]))
            # end for
        # end if

        # validity duration filters
        if validity_duration_filters != None:
            functions.is_valid_float_filters(validity_duration_filters)
            for validity_duration_filter in validity_duration_filters:
                op = arithmetic_operators[validity_duration_filter["op"]]
                params.append(op((extract("epoch", Report.validity_stop) - extract("epoch", Report.validity_start)), validity_duration_filter["float"]))
            # end for
        # end if

        # triggering_time filters
        if triggering_time_filters != None:
            functions.is_valid_date_filters(triggering_time_filters)
            for triggering_time_filter in triggering_time_filters:
                op = arithmetic_operators[triggering_time_filter["op"]]
                params.append(op(Report.triggering_time, triggering_time_filter["date"]))
            # end for
        # end if

        # generation_start filters
        if generation_start_filters != None:
            functions.is_valid_date_filters(generation_start_filters)
            for generation_start_filter in generation_start_filters:
                op = arithmetic_operators[generation_start_filter["op"]]
                params.append(op(Report.generation_start, generation_start_filter["date"]))
            # end for
        # end if

        # generation_stop filters
        if generation_stop_filters != None:
            functions.is_valid_date_filters(generation_stop_filters)
            for generation_stop_filter in generation_stop_filters:
                op = arithmetic_operators[generation_stop_filter["op"]]
                params.append(op(Report.generation_stop, generation_stop_filter["date"]))
            # end for
        # end if

        # generated filter
        if generated != None:
            functions.is_valid_bool_filter(generated)
            params.append(Report.generated == generated)
        # end if

        # compressed filter
        if compressed != None:
            functions.is_valid_bool_filter(compressed)
            params.append(Report.compressed == compressed)
        # end if

        # Generators
        if generators != None:
            functions.is_valid_text_filter(generators)
            if generators["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[generators["op"]]
                params.append(op(Report.generator, generators["filter"]))
            else:
                filter = eval('Report.generator.' + text_operators[generators["op"]])
                params.append(filter(generators["filter"]))
            # end if
        # end if

        # generator_version filters
        if generator_version_filters != None:
            for generator_version_filter in generator_version_filters:
                functions.is_valid_text_filter(generator_version_filter)

                if generator_version_filter["op"] in arithmetic_operators.keys():
                    op = arithmetic_operators[generator_version_filter["op"]]
                    params.append(op(Report.generator_version, generator_version_filter["filter"]))
                else:
                    filter = eval('Report.generator_version.' + text_operators[generator_version_filter["op"]])
                    params.append(filter(generator_version_filter["filter"]))
                # end if
            # end for
        # end if

        # generation_error filter
        if generation_error != None:
            functions.is_valid_bool_filter_with_op(generation_error)
            op = arithmetic_operators[generation_error["op"]]
            params.append(op(Report.generation_error, generation_error["filter"]))
        # end if

        # Report group UUIDs
        if report_group_uuids != None:
            functions.is_valid_text_filter(report_group_uuids)
            if report_group_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[report_group_uuids["op"]]
                params.append(op(Report.report_group_uuid, report_group_uuids["filter"]))
            else:
                filter = eval('Report.report_group_uuid.' + text_operators[report_group_uuids["op"]])
                params.append(filter(report_group_uuids["filter"]))
            # end if
        # end if

        # Report UUIDs
        if report_uuids != None:
            functions.is_valid_text_filter(report_uuids)
            if report_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[report_uuids["op"]]
                params.append(op(Report.report_uuid, report_uuids["filter"]))
            else:
                filter = eval('Report.report_uuid.' + text_operators[report_uuids["op"]])
                params.append(filter(report_uuids["filter"]))
            # end if
        # end if
        
        # Report groups
        if report_groups != None:
            functions.is_valid_text_filter(report_groups)
            if report_groups["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[report_groups["op"]]
                params.append(op(ReportGroup.name, report_groups["filter"]))
            else:
                filter = eval('ReportGroup.name.' + text_operators[report_groups["op"]])
                params.append(filter(report_groups["filter"]))
            # end if
            tables.append(ReportGroup)
        # end if

        # status filters
        if statuses != None:
            functions.is_valid_float_filters(statuses)
            for status in statuses:
                op = arithmetic_operators[status["op"]]
                params.append(op(ReportStatus.status, status["float"]))
            # end for
        # end if

        query = self.session.query(Report)
        for table in set(tables):
            query = query.join(table)
        # end for

        query = query.filter(*params)

        # Order by
        if order_by != None:
            functions.is_valid_order_by(order_by)
            if order_by["descending"]:
                order_by_statement = eval("Report." + order_by["field"] + ".desc()")
            else:
                order_by_statement = eval("Report." + order_by["field"])
            # end if
            query = query.order_by(order_by_statement)
        # end if

        # Limit
        if limit != None:
            functions.is_valid_positive_integer(limit)
            query = query.limit(limit)
        # end if

        # Offset
        if offset != None:
            functions.is_valid_positive_integer(offset)
            query = query.offset(offset)
        # end if

        log_query(query)

        reports = []
        if delete:
            if synchronize_deletion:
                reports = sorted(query.all(), key=lambda x:(x.name))
                for report in reports:
                    name = report.name
                    uuid = report.report_uuid
                    report_group_uuid = report.report_group_uuid
                    lock = "treat_data_" + report.name
                    @self.synchronized_rboa(lock, external=True, lock_path="/dev/shm")
                    def _delete_report(self, report):
                        self.delete(self.session.query(Report).with_for_update().filter(Report.report_uuid == report.report_uuid))
                    # end def
                    _delete_report(self, report)
                    logger.info("The report with name {}, UUID {} and report group UUID has been removed".format(name, uuid, report_group_uuid))
                # end for
            else:
                reports = query.all()
                self.delete(self.session.query(Report).with_for_update().filter(Report.report_uuid.in_([report.report_uuid for report in reports])))
            # end if
        else:
            reports = query.all()
        # end if

        return reports

    def get_report_alerts(self, filters = None):
        """
        Method to obtain the alerts associated to report entities filtered by the received filters

        :param filters: dictionary with the filters to apply to the query
        :type filters: dict

        :return: found report_alerts
        :rtype: list
        """
        params = []
        join_tables = False
        tables = {}

        # Report UUIDs
        if check_key_in_filters(filters, "report_uuids"):
            functions.is_valid_text_filter(filters["report_uuids"])
            if filters["report_uuids"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["report_uuids"]["op"]]
                params.append(op(ReportAlert.report_uuid, filters["report_uuids"]["filter"]))
            else:
                filter = eval('ReportAlert.report_uuid.' + text_operators[filters["report_uuids"]["op"]])
                params.append(filter(filters["report_uuids"]["filter"]))
            # end if
        # end if

        # Report group UUIDs
        if check_key_in_filters(filters, "report_group_uuids"):
            functions.is_valid_text_filter(filters["report_group_uuids"])
            if filters["report_group_uuids"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["report_group_uuids"]["op"]]
                params.append(op(Report.report_group_uuid, filters["report_group_uuids"]["filter"]))
            else:
                filter = eval('Report.report_group_uuid.' + text_operators[filters["report_group_uuids"]["op"]])
                params.append(filter(filters["report_group_uuids"]["filter"]))
            # end if
            join_tables = True
        # end if

        # Report names
        if check_key_in_filters(filters, "report_names"):
            functions.is_valid_text_filter(filters["report_names"])
            if filters["report_names"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["report_names"]["op"]]
                params.append(op(Report.name, filters["report_names"]["filter"]))
            else:
                filter = eval('Report.name.' + text_operators[filters["report_names"]["op"]])
                params.append(filter(filters["report_names"]["filter"]))
            # end if
            join_tables = True
        # end if

        # Report generation modes
        if check_key_in_filters(filters, "generation_modes"):
            functions.is_valid_text_filter(filters["generation_modes"])
            if filters["generation_modes"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["generation_modes"]["op"]]
                params.append(op(Report.generation_mode, filters["generation_modes"]["filter"]))
            else:
                filter = eval('Report.generation_mode.' + text_operators[filters["generation_modes"]["op"]])
                params.append(filter(filters["generation_modes"]["filter"]))
            # end if
        # end if

        # validity_start filters
        if check_key_in_filters(filters, "validity_start_filters"):
            functions.is_valid_date_filters(filters["validity_start_filters"])
            for validity_start_filter in filters["validity_start_filters"]:
                op = arithmetic_operators[validity_start_filter["op"]]
                params.append(op(Report.validity_start, validity_start_filter["date"]))
            # end for
            join_tables = True
        # end if

        # validity_stop filters
        if check_key_in_filters(filters, "validity_stop_filters"):
            functions.is_valid_date_filters(filters["validity_stop_filters"])
            for validity_stop_filter in filters["validity_stop_filters"]:
                op = arithmetic_operators[validity_stop_filter["op"]]
                params.append(op(Report.validity_stop, validity_stop_filter["date"]))
            # end for
            join_tables = True
        # end if

        # validity duration filters
        if check_key_in_filters(filters, "validity_duration_filters"):
            functions.is_valid_float_filters(filters["validity_duration_filters"])
            for validity_duration_filter in filters["validity_duration_filters"]:
                op = arithmetic_operators[validity_duration_filter["op"]]
                params.append(op((extract("epoch", Report.validity_stop) - extract("epoch", Report.validity_start)), validity_duration_filter["float"]))
            # end for
            join_tables = True
        # end if

        # triggering_time filters
        if check_key_in_filters(filters, "triggering_time_filters"):
            functions.is_valid_date_filters(filters["triggering_time_filters"])
            for triggering_time_filter in filters["triggering_time_filters"]:
                op = arithmetic_operators[triggering_time_filter["op"]]
                params.append(op(Report.triggering_time, triggering_time_filter["date"]))
            # end for
            join_tables = True
        # end if
        
        # generation_start filters
        if check_key_in_filters(filters, "generation_start_filters"):
            functions.is_valid_date_filters(filters["generation_start_filters"])
            for generation_start_filter in filters["generation_start_filters"]:
                op = arithmetic_operators[generation_start_filter["op"]]
                params.append(op(Report.generation_start, generation_start_filter["date"]))
            # end for
            join_tables = True
        # end if

        # generation_stop filters
        if check_key_in_filters(filters, "generation_stop_filters"):
            functions.is_valid_date_filters(filters["generation_stop_filters"])
            for generation_stop_filter in filters["generation_stop_filters"]:
                op = arithmetic_operators[generation_stop_filter["op"]]
                params.append(op(Report.generation_stop, generation_stop_filter["date"]))
            # end for
            join_tables = True
        # end if

        # generated filter
        if check_key_in_filters(filters, "generated"):
            functions.is_valid_bool_filter(filters["generated"])
            params.append(Report.generated == filters["generated"])
            join_tables = True
        # end if

        # compressed filter
        if check_key_in_filters(filters, "compressed"):
            functions.is_valid_bool_filter(filters["compressed"])
            params.append(Report.compressed == filters["compressed"])
            join_tables = True
        # end if

        # generation_error filter
        if check_key_in_filters(filters, "generation_error"):
            functions.is_valid_bool_filter_with_op(filters["generation_error"])
            op = arithmetic_operators[filters["generation_error"]["op"]]
            params.append(op(Report.generation_error, filters["generation_error"]["filter"]))
            join_tables = True
        # end if

        # Generators
        if check_key_in_filters(filters, "generators"):
            functions.is_valid_text_filter(filters["generators"])
            if filters["generators"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["generators"]["op"]]
                params.append(op(Report.generator, filters["generators"]["filter"]))
            else:
                filter = eval('Report.generator.' + text_operators[filters["generators"]["op"]])
                params.append(filter(filters["generators"]["filter"]))
            # end if
            join_tables = True
        # end if

        # generator_version filters
        if check_key_in_filters(filters, "generator_version_filters"):
            for generator_version_filter in filters["generator_version_filters"]:
                functions.is_valid_text_filter(generator_version_filter)

                if generator_version_filter["op"] in arithmetic_operators.keys():
                    op = arithmetic_operators[generator_version_filter["op"]]
                    params.append(op(Report.generator_version, generator_version_filter["filter"]))
                else:
                    filter = eval('Report.generator_version.' + text_operators[generator_version_filter["op"]])
                    params.append(filter(generator_version_filter["filter"]))
                # end if
            # end for
            join_tables = True
        # end if

        # status filters
        if check_key_in_filters(filters, "statuses"):
            functions.is_valid_text_filter(filters["statuses"])
            if filters["statuses"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["statuses"]["op"]]
                params.append(op(ReportStatus.status, filters["statuses"]["filter"]))
            else:
                filter = eval('ReportStatus.status.' + text_operators[filters["statuses"]["op"]])
                params.append(filter(filters["statuses"]["filter"]))
            # end if
            join_tables = True
            tables[ReportStatus] = ReportStatus.report_uuid==Report.report_uuid
        # end if

        # Report groups
        if check_key_in_filters(filters, "report_groups"):
            functions.is_valid_text_filter(filters["report_groups"])
            if filters["report_groups"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["report_groups"]["op"]]
                params.append(op(ReportGroup.name, filters["report_groups"]["filter"]))
            else:
                filter = eval('ReportGroup.name.' + text_operators[filters["report_groups"]["op"]])
                params.append(filter(filters["report_groups"]["filter"]))
            # end if
            join_tables = True
            tables[ReportGroup] = ReportGroup.report_group_uuid==Report.report_group_uuid
        # end if

        query = self.session.query(ReportAlert)
        if len(tables.keys()) > 0 or join_tables:
            query = query.join(Report, Report.report_uuid==ReportAlert.report_uuid)
            for table in tables:
                query = query.join(table, tables[table])
            # end for
        # end if

        join_tables = False
        tables = {}        

        # Alert configuration names
        if check_key_in_filters(filters, "names"):
            functions.is_valid_text_filter(filters["names"])
            if filters["names"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["names"]["op"]]
                params.append(op(Alert.name, filters["names"]["filter"]))
            else:
                filter = eval('Alert.name.' + text_operators[filters["names"]["op"]])
                params.append(filter(filters["names"]["filter"]))
            # end if
            join_tables = True
        # end if

        # Alert severities
        if check_key_in_filters(filters, "severities"):
            functions.is_valid_severity_filter(filters["severities"])
            if filters["severities"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["severities"]["op"]]
                params.append(op(Alert.severity, filters["severities"]["filter"]))
            else:
                filters = [alert_severity_codes[severity_filter] for severity_filter in filters["severities"]["filter"]]
                filter = eval('Alert.severity.' + text_operators[filters["severities"]["op"]])
                params.append(filter(filters))
            # end if
            join_tables = True
        # end if

        # Alert groups
        if check_key_in_filters(filters, "groups"):
            functions.is_valid_text_filter(filters["groups"])
            if filters["groups"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["groups"]["op"]]
                params.append(op(AlertGroup.name, filters["groups"]["filter"]))
            else:
                filter = eval('AlertGroup.name.' + text_operators[filters["groups"]["op"]])
                params.append(filter(filters["groups"]["filter"]))
            # end if
            join_tables = True
            tables[AlertGroup] = AlertGroup.alert_group_uuid==Alert.alert_group_uuid
        # end if
        
        if len(tables.keys()) > 0 or join_tables:
            query = query.join(Alert, Alert.alert_uuid==ReportAlert.alert_uuid)
            for table in tables:
                query = query.join(table, tables[table])
            # end for
        # end if
        
        # Alert UUIDs
        if check_key_in_filters(filters, "alert_uuids"):
            functions.is_valid_text_filter(filters["alert_uuids"])
            if filters["alert_uuids"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["alert_uuids"]["op"]]
                params.append(op(ReportAlert.alert_uuid, filters["alert_uuids"]["filter"]))
            else:
                filter = eval('ReportAlert.alert_uuid.' + text_operators[filters["alert_uuids"]["op"]])
                params.append(filter(filters["alert_uuids"]["filter"]))
            # end if
        # end if
        
        # validated filter
        if check_key_in_filters(filters, "validated"):
            functions.is_valid_bool_filter(filters["validated"])
            params.append(ReportAlert.validated == filters["validated"])
        # end if
        
        # report alert ingestion_time filters
        if check_key_in_filters(filters, "alert_ingestion_time_filters"):
            functions.is_valid_date_filters(filters["alert_ingestion_time_filters"])
            for ingestion_time_filter in filters["alert_ingestion_time_filters"]:
                op = arithmetic_operators[ingestion_time_filter["op"]]
                params.append(op(ReportAlert.ingestion_time, ingestion_time_filter["date"]))
            # end for
        # end if

        # Generators
        if check_key_in_filters(filters, "generators"):
            functions.is_valid_text_filter(filters["generators"])
            if filters["generators"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["generators"]["op"]]
                params.append(op(ReportAlert.generator, filters["generators"]["filter"]))
            else:
                filter = eval('ReportAlert.generator.' + text_operators[filters["generators"]["op"]])
                params.append(filter(filters["generators"]["filter"]))
            # end if
        # end if

        # notified filter
        if check_key_in_filters(filters, "notified"):
            functions.is_valid_bool_filter(filters["notified"])
            params.append(ReportAlert.notified == filters["notified"])
        # end if

        # solved filter
        if check_key_in_filters(filters, "solved"):
            functions.is_valid_bool_filter(filters["solved"])
            params.append(ReportAlert.solved == filters["solved"])
        # end if

        # report alert solved time filters
        if check_key_in_filters(filters, "solved_time_filters"):
            functions.is_valid_date_filters(filters["solved_time_filters"])
            for solved_time_filter in filters["solved_time_filters"]:
                op = arithmetic_operators[solved_time_filter["op"]]
                params.append(op(ReportAlert.solved_time, solved_time_filter["date"]))
            # end for
        # end if

        # report alert notification time filters
        if check_key_in_filters(filters, "notification_time_filters"):
            functions.is_valid_date_filters(filters["notification_time_filters"])
            for notification_time_filter in filters["notification_time_filters"]:
                op = arithmetic_operators[notification_time_filter["op"]]
                params.append(op(ReportAlert.notification_time, notification_time_filter["date"]))
            # end for
        # end if
                
        query = query.filter(*params)

        # Order by
        if check_key_in_filters(filters, "order_by"):
            functions.is_valid_order_by(filters["order_by"])
            if filters["order_by"]["descending"]:
                order_by_statement = eval("ReportAlert." + filters["order_by"]["field"] + ".desc()")
            else:
                order_by_statement = eval("ReportAlert." + filters["order_by"]["field"])
            # end if
            query = query.order_by(order_by_statement)
        # end if

        # Limit
        if check_key_in_filters(filters, "limit"):
            functions.is_valid_positive_integer(filters["limit"])
            query = query.limit(filters["limit"])
        # end if

        # Offset
        if check_key_in_filters(filters, "offset"):
            functions.is_valid_positive_integer(filters["offset"])
            query = query.offset(filters["offset"])
        # end if

        log_query(query)

        report_alerts = []
        if check_key_in_filters(filters, "delete") and filters["delete"]:
            self.delete(query)
        else:
            report_alerts = query.all()
        # end if

        return report_alerts

    def get_report_groups(self, report_group_uuids = None, names = None, order_by = None, limit = None, offset = None):
        """
        Method to obtain the DIM signature entities filtered by the received parameters

        :param report_group_uuids: list of DIM signature identifiers
        :type report_group_uuids: text_filter
        :param names: name filters
        :type names: text_filter

        :return: found DIM signatures
        :rtype: list
        """
        params = []

        # DIM signature UUIDs
        if report_group_uuids != None:
            functions.is_valid_text_filter(report_group_uuids)
            if report_group_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[report_group_uuids["op"]]
                params.append(op(ReportGroup.report_group_uuid, report_group_uuids["filter"]))
            else:
                filter = eval('ReportGroup.report_group_uuid.' + text_operators[report_group_uuids["op"]])
                params.append(filter(report_group_uuids["filter"]))
            # end if
        # end if

        # DIM signatures
        if names != None:
            functions.is_valid_text_filter(names)
            if names["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[names["op"]]
                params.append(op(ReportGroup.name, names["filter"]))
            else:
                filter = eval('ReportGroup.name.' + text_operators[names["op"]])
                params.append(filter(names["filter"]))
            # end if
        # end if

        query = self.session.query(ReportGroup).filter(*params)

        # Order by
        if order_by != None:
            functions.is_valid_order_by(order_by)
            if order_by["descending"]:
                order_by_statement = eval("ReportGroup." + order_by["field"] + ".desc()")
            else:
                order_by_statement = eval("ReportGroup." + order_by["field"])
            # end if
            query = query.order_by(order_by_statement)
        # end if

        # Limit
        if limit != None:
            functions.is_valid_positive_integer(limit)
            query = query.limit(limit)
        # end if

        # Offset
        if offset != None:
            functions.is_valid_positive_integer(offset)
            query = query.offset(offset)
        # end if
        
        log_query(query)
        report_groups = query.all()

        return report_groups
    
    def get_gauges(self, gauge_uuids = None, names = None, systems = None, dim_signature_uuids = None, dim_signatures = None, order_by = None, limit = None, offset = None):
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
            if gauge_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[gauge_uuids["op"]]
                params.append(op(Gauge.gauge_uuid, gauge_uuids["filter"]))
            else:
                filter = eval('Gauge.gauge_uuid.' + text_operators[gauge_uuids["op"]])
                params.append(filter(gauge_uuids["filter"]))
            # end if
        # end if

        # Gauge names
        if names != None:
            functions.is_valid_text_filter(names)
            if names["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[names["op"]]
                params.append(op(Gauge.name, names["filter"]))
            else:
                filter = eval('Gauge.name.' + text_operators[names["op"]])
                params.append(filter(names["filter"]))
            # end if
        # end if

        # Gauge systems
        if systems != None:
            functions.is_valid_text_filter(systems)
            if systems["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[systems["op"]]
                params.append(op(Gauge.system, systems["filter"]))
            else:
                filter = eval('Gauge.system.' + text_operators[systems["op"]])
                params.append(filter(systems["filter"]))
            # end if
        # end if

        # DIM signature UUIDs
        if dim_signature_uuids != None:
            functions.is_valid_text_filter(dim_signature_uuids)
            if dim_signature_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[dim_signature_uuids["op"]]
                params.append(op(Gauge.dim_signature_uuid, dim_signature_uuids["filter"]))
            else:
                filter = eval('Gauge.dim_signature_uuid.' + text_operators[dim_signature_uuids["op"]])
                params.append(filter(dim_signature_uuids["filter"]))
            # end if
        # end if

        # DIM signatures
        if dim_signatures != None:
            functions.is_valid_text_filter(dim_signatures)
            if dim_signatures["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[dim_signatures["op"]]
                params.append(op(DimSignature.dim_signature, dim_signatures["filter"]))
            else:
                filter = eval('DimSignature.dim_signature.' + text_operators[dim_signatures["op"]])
                params.append(filter(dim_signatures["filter"]))
            # end if
            tables.append(DimSignature)
        # end if

        query = self.session.query(Gauge)
        for table in set(tables):
            query = query.join(table)
        # end for

        query = query.filter(*params)

        # Order by
        if order_by != None:
            functions.is_valid_order_by(order_by)
            if order_by["descending"]:
                order_by_statement = eval("Gauge." + order_by["field"] + ".desc()")
            else:
                order_by_statement = eval("Gauge." + order_by["field"])
            # end if
            query = query.order_by(order_by_statement)
        # end if

        # Limit
        if limit != None:
            functions.is_valid_positive_integer(limit)
            query = query.limit(limit)
        # end if

        # Offset
        if offset != None:
            functions.is_valid_positive_integer(offset)
            query = query.offset(offset)
        # end if

        log_query(query)
        gauges = query.all()

        return gauges

    def prepare_query_values(self, value_filters, value_entities, params, tables = None):
        # value filters
        if value_filters != None:
            functions.is_valid_value_filters(value_filters)
            for value_filter in value_filters:
                # Type
                value_type = value_entities[value_filter["type"]]
                if tables != None:
                    tables.append(value_type)
                # end if

                # Name
                if value_filter["name"]["op"] in arithmetic_operators.keys():
                    op = arithmetic_operators[value_filter["name"]["op"]]
                    params.append(op(eval("value_type.name"), value_filter["name"]["filter"]))
                else:
                    filter = eval('value_type.name.' + text_operators[value_filter["name"]["op"]])
                    params.append(filter(value_filter["name"]["filter"]))
                # end if

                # Value
                if "value" in value_filter:
                    value = value_filter["value"]
                    if value["op"] in arithmetic_operators.keys():
                        op = arithmetic_operators[value["op"]]
                        params.append(op(value_type.value, value["filter"]))
                    else:
                        op = eval("value_type.value." + text_operators[value["op"]])
                        params.append(op(value["filter"]))
                    # end if
                # end if
            # end for
        # end if
    # end def

    def get_events(self, event_uuids = None, start_filters = None, stop_filters = None, duration_filters = None, ingestion_time_filters = None, value_filters = None, gauge_uuids = None, source_uuids = None, explicit_ref_uuids = None, sources = None, explicit_refs = None, gauge_names = None, gauge_systems = None, keys = None, order_by = None, limit = None, offset = None):
        """
        """
        params = []
        # Allow only obtain visible events
        params.append(Event.visible == True)

        tables = []

        # event_uuids
        if event_uuids != None:
            functions.is_valid_text_filter(event_uuids)
            if event_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[event_uuids["op"]]
                params.append(op(Event.event_uuid, event_uuids["filter"]))
            else:
                filter = eval('Event.event_uuid.' + text_operators[event_uuids["op"]])
                params.append(filter(event_uuids["filter"]))
            # end if
        # end if

        # source_uuids
        if source_uuids != None:
            functions.is_valid_text_filter(source_uuids)
            if source_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[source_uuids["op"]]
                params.append(op(Event.source_uuid, source_uuids["filter"]))
            else:
                filter = eval('Event.source_uuid.' + text_operators[source_uuids["op"]])
                params.append(filter(source_uuids["filter"]))
            # end if
        # end if

        # explicit_ref_uuids
        if explicit_ref_uuids != None:
            functions.is_valid_text_filter(explicit_ref_uuids)
            if explicit_ref_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[explicit_ref_uuids["op"]]
                params.append(op(Event.explicit_ref_uuid, explicit_ref_uuids["filter"]))
            else:
                filter = eval('Event.explicit_ref_uuid.' + text_operators[explicit_ref_uuids["op"]])
                params.append(filter(explicit_ref_uuids["filter"]))
            # end if
        # end if

        # gauge_uuids
        if gauge_uuids != None:
            functions.is_valid_text_filter(gauge_uuids)
            if gauge_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[gauge_uuids["op"]]
                params.append(op(Event.gauge_uuid, gauge_uuids["filter"]))
            else:
                filter = eval('Event.gauge_uuid.' + text_operators[gauge_uuids["op"]])
                params.append(filter(gauge_uuids["filter"]))
            # end if
        # end if

        # Sources
        if sources != None:
            functions.is_valid_text_filter(sources)
            if sources["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[sources["op"]]
                params.append(op(Source.name, sources["filter"]))
            else:
                filter = eval('Source.name.' + text_operators[sources["op"]])
                params.append(filter(sources["filter"]))
            # end if
            tables.append(Source)
        # end if

        # Explicit references
        if explicit_refs != None:
            functions.is_valid_text_filter(explicit_refs)
            if explicit_refs["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[explicit_refs["op"]]
                params.append(op(ExplicitRef.explicit_ref, explicit_refs["filter"]))
            elif explicit_refs["op"] in regex_operators.keys():
                params.append(text("explicit_refs.explicit_ref ~ '" + explicit_refs["filter"] + "'"))
            else:
                filter = eval('ExplicitRef.explicit_ref.' + text_operators[explicit_refs["op"]])
                params.append(filter(explicit_refs["filter"]))
            # end if
            tables.append(ExplicitRef)
        # end if

        # Gauge names
        if gauge_names != None:
            functions.is_valid_text_filter(gauge_names)
            if gauge_names["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[gauge_names["op"]]
                params.append(op(Gauge.name, gauge_names["filter"]))
            else:
                filter = eval('Gauge.name.' + text_operators[gauge_names["op"]])
                params.append(filter(gauge_names["filter"]))
            # end if
            tables.append(Gauge)
        # end if

        # Gauge systems
        if gauge_systems != None:
            functions.is_valid_text_filter(gauge_systems)
            if gauge_systems["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[gauge_systems["op"]]
                params.append(op(Gauge.system, gauge_systems["filter"]))
            else:
                filter = eval('Gauge.system.' + text_operators[gauge_systems["op"]])
                params.append(filter(gauge_systems["filter"]))
            # end if
            tables.append(Gauge)
        # end if

        # keys
        if keys != None:
            functions.is_valid_text_filter(keys)
            if keys["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[keys["op"]]
                params.append(op(EventKey.event_key, keys["filter"]))
            else:
                filter = eval('EventKey.event_key.' + text_operators[keys["op"]])
                params.append(filter(keys["filter"]))
            # end if
            tables.append(EventKey)
        # end if

        # start filters
        if start_filters != None:
            functions.is_valid_date_filters(start_filters)
            for start_filter in start_filters:
                op = arithmetic_operators[start_filter["op"]]
                params.append(op(Event.start, start_filter["date"]))
            # end for
        # end if

        # stop filters
        if stop_filters != None:
            functions.is_valid_date_filters(stop_filters)
            for stop_filter in stop_filters:
                op = arithmetic_operators[stop_filter["op"]]
                params.append(op(Event.stop, stop_filter["date"]))
            # end for
        # end if

        # duration filters
        if duration_filters != None:
            functions.is_valid_float_filters(duration_filters)
            for duration_filter in duration_filters:
                op = arithmetic_operators[duration_filter["op"]]
                params.append(op((extract("epoch", Event.stop) - extract("epoch", Event.start)), duration_filter["float"]))
            # end for
        # end if

        # ingestion_time filters
        if ingestion_time_filters != None:
            functions.is_valid_date_filters(ingestion_time_filters)
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

        query = query.filter(*params)

        # Order by
        if order_by != None:
            functions.is_valid_order_by(order_by)
            if order_by["descending"]:
                order_by_statement = eval("Event." + order_by["field"] + ".desc()")
            else:
                order_by_statement = eval("Event." + order_by["field"])
            # end if
            query = query.order_by(order_by_statement)
        # end if

        # Limit
        if limit != None:
            functions.is_valid_positive_integer(limit)
            query = query.limit(limit)
        # end if

        # Offset
        if offset != None:
            functions.is_valid_positive_integer(offset)
            query = query.offset(offset)
        # end if

        log_query(query)
        events = query.all()

        return events

    def get_event_alerts(self, filters = None):
        """
        Method to obtain the alerts associated to event entities filtered by the received filters

        :param filters: dictionary with the filters to apply to the query
        :type filters: dict

        :return: found event_alerts
        :rtype: list
        """

        # Check filters
        expected_filters = ["event_uuids", "source_uuids", "explicit_ref_uuids", "gauge_uuids", "sources", "explicit_refs", "gauge_names", "gauge_systems", "keys", "start_filters", "stop_filters", "duration_filters", "event_ingestion_time_filters", "value_filters", "names", "severities", "groups", "alert_uuids", "validated", "alert_ingestion_time_filters", "generators", "notified", "solved", "solved_time_filters", "notification_time_filters", "order_by", "limit", "offset", "delete"]
        parsing.check_filters(filters, expected_filters)

        params = []
        join_tables = False
        tables = {}

        # event_uuids
        if check_key_in_filters(filters, "event_uuids"):
            functions.is_valid_text_filter(filters["event_uuids"])
            if filters["event_uuids"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["event_uuids"]["op"]]
                params.append(op(EventAlert.event_uuid, filters["event_uuids"]["filter"]))
            else:
                filter = eval('EventAlert.event_uuid.' + text_operators[filters["event_uuids"]["op"]])
                params.append(filter(filters["event_uuids"]["filter"]))
            # end if
        # end if

        # source_uuids
        if check_key_in_filters(filters, "source_uuids"):
            functions.is_valid_text_filter(filters["source_uuids"])
            if filters["source_uuids"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["source_uuids"]["op"]]
                params.append(op(Event.source_uuid, filters["source_uuids"]["filter"]))
            else:
                filter = eval('Event.source_uuid.' + text_operators[filters["source_uuids"]["op"]])
                params.append(filter(filters["source_uuids"]["filter"]))
            # end if
            join_tables = True
        # end if

        # explicit_ref_uuids
        if check_key_in_filters(filters, "explicit_ref_uuids"):
            functions.is_valid_text_filter(filters["explicit_ref_uuids"])
            if filters["explicit_ref_uuids"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["explicit_ref_uuids"]["op"]]
                params.append(op(Event.explicit_ref_uuid, filters["explicit_ref_uuids"]["filter"]))
            else:
                filter = eval('Event.explicit_ref_uuid.' + text_operators[filters["explicit_ref_uuids"]["op"]])
                params.append(filter(filters["explicit_ref_uuids"]["filter"]))
            # end if
            join_tables = True
        # end if

        # gauge_uuids
        if check_key_in_filters(filters, "gauge_uuids"):
            functions.is_valid_text_filter(filters["gauge_uuids"])
            if filters["gauge_uuids"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["gauge_uuids"]["op"]]
                params.append(op(Event.gauge_uuid, filters["gauge_uuids"]["filter"]))
            else:
                filter = eval('Event.gauge_uuid.' + text_operators[filters["gauge_uuids"]["op"]])
                params.append(filter(filters["gauge_uuids"]["filter"]))
            # end if
            join_tables = True
        # end if

        # Sources
        if check_key_in_filters(filters, "sources"):
            functions.is_valid_text_filter(filters["sources"])
            if filters["sources"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["sources"]["op"]]
                params.append(op(Source.name, filters["sources"]["filter"]))
            else:
                filter = eval('Source.name.' + text_operators[filters["sources"]["op"]])
                params.append(filter(filters["sources"]["filter"]))
            # end if
            join_tables = True
            tables[Source] = Source.source_uuid==Event.source_uuid
        # end if

        # Explicit references
        if check_key_in_filters(filters, "explicit_refs"):
            functions.is_valid_text_filter(filters["explicit_refs"])
            if filters["explicit_refs"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["explicit_refs"]["op"]]
                params.append(op(ExplicitRef.explicit_ref, filters["explicit_refs"]["filter"]))
            elif filters["explicit_refs"]["op"] in regex_operators.keys():
                params.append(text("explicit_refs.explicit_ref ~ '" + filters["explicit_refs"]["filter"] + "'"))
            else:
                filter = eval('ExplicitRef.explicit_ref.' + text_operators[filters["explicit_refs"]["op"]])
                params.append(filter(filters["explicit_refs"]["filter"]))
            # end if
            join_tables = True
            tables[ExplicitRef] = ExplicitRef.explicit_ref_uuid==Event.explicit_ref_uuid
        # end if

        # Gauge names
        if check_key_in_filters(filters, "gauge_names"):
            functions.is_valid_text_filter(filters["gauge_names"])
            if filters["gauge_names"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["gauge_names"]["op"]]
                params.append(op(Gauge.name, filters["gauge_names"]["filter"]))
            else:
                filter = eval('Gauge.name.' + text_operators[filters["gauge_names"]["op"]])
                params.append(filter(filters["gauge_names"]["filter"]))
            # end if
            join_tables = True
            tables[Gauge] = Gauge.gauge_uuid==Event.gauge_uuid
        # end if

        # Gauge systems
        if check_key_in_filters(filters, "gauge_systems"):
            functions.is_valid_text_filter(filters["gauge_systems"])
            if filters["gauge_systems"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["gauge_systems"]["op"]]
                params.append(op(Gauge.system, filters["gauge_systems"]["filter"]))
            else:
                filter = eval('Gauge.system.' + text_operators[filters["gauge_systems"]["op"]])
                params.append(filter(filters["gauge_systems"]["filter"]))
            # end if
            join_tables = True
            tables[Gauge] = Gauge.gauge_uuid==Event.gauge_uuid
        # end if

        # keys
        if check_key_in_filters(filters, "keys"):
            functions.is_valid_text_filter(filters["keys"])
            if filters["keys"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["keys"]["op"]]
                params.append(op(EventKey.event_key, filters["keys"]["filter"]))
            else:
                filter = eval('EventKey.event_key.' + text_operators[filters["keys"]["op"]])
                params.append(filter(filters["keys"]["filter"]))
            # end if
            join_tables = True
            tables[EventKey] = EventKey.event_uuid==Event.event_uuid
        # end if

        # start filters
        if check_key_in_filters(filters, "start_filters"):
            functions.is_valid_date_filters(filters["start_filters"])
            for start_filter in filters["start_filters"]:
                op = arithmetic_operators[start_filter["op"]]
                params.append(op(Event.start, start_filter["date"]))
            # end for
            join_tables = True
        # end if

        # stop filters
        if check_key_in_filters(filters, "stop_filters"):
            functions.is_valid_date_filters(filters["stop_filters"])
            for stop_filter in filters["stop_filters"]:
                op = arithmetic_operators[stop_filter["op"]]
                params.append(op(Event.stop, stop_filter["date"]))
            # end for
            join_tables = True
        # end if

        # duration filters
        if check_key_in_filters(filters, "duration_filters"):
            functions.is_valid_float_filters(filters["duration_filters"])
            for duration_filter in filters["duration_filters"]:
                op = arithmetic_operators[duration_filter["op"]]
                params.append(op((extract("epoch", Event.stop) - extract("epoch", Event.start)), duration_filter["float"]))
            # end for
            join_tables = True
        # end if

        # ingestion_time filters
        if check_key_in_filters(filters, "event_ingestion_time_filters"):
            functions.is_valid_date_filters(filters["event_ingestion_time_filters"])
            for ingestion_time_filter in filters["event_ingestion_time_filters"]:
                op = arithmetic_operators[ingestion_time_filter["op"]]
                params.append(op(Event.ingestion_time, ingestion_time_filter["date"]))
            # end for
            join_tables = True
        # end if

        # value filters
        if check_key_in_filters(filters, "value_filters"):
            tables_values = []
            self.prepare_query_values(filters["value_filters"], event_value_entities, params, tables_values)
            for table in set(tables_values):
                tables[table] = Event.event_uuid==table.event_uuid
            # end for
        # end if

        query = self.session.query(EventAlert)
        if len(tables.keys()) > 0 or join_tables:
            query = query.join(Event, Event.event_uuid==EventAlert.event_uuid)
            for table in tables:
                query = query.join(table, tables[table])
            # end for
        # end if

        join_tables = False
        tables = {}        

        # Alert configuration names
        if check_key_in_filters(filters, "names"):
            functions.is_valid_text_filter(filters["names"])
            if filters["names"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["names"]["op"]]
                params.append(op(Alert.name, filters["names"]["filter"]))
            else:
                filter = eval('Alert.name.' + text_operators[filters["names"]["op"]])
                params.append(filter(filters["names"]["filter"]))
            # end if
            join_tables = True
        # end if

        # Alert severities
        if check_key_in_filters(filters, "severities"):
            functions.is_valid_severity_filter(filters["severities"])
            if filters["severities"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["severities"]["op"]]
                params.append(op(Alert.severity, alert_severity_codes[filters["severities"]["filter"]]))
            else:
                if type(filters["severities"]["filter"]) == list:
                    filters_to_apply = [alert_severity_codes[severity_filter] for severity_filter in filters["severities"]["filter"]]
                else:
                    filters_to_apply = filters["severities"]["filter"]
                # end if
                filter = eval('Alert.severity.' + text_operators[filters["severities"]["op"]])
                params.append(filter(filters_to_apply))
            # end if
            join_tables = True
        # end if

        # Alert groups
        if check_key_in_filters(filters, "groups"):
            functions.is_valid_text_filter(filters["groups"])
            if filters["groups"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["groups"]["op"]]
                params.append(op(AlertGroup.name, filters["groups"]["filter"]))
            else:
                filter = eval('AlertGroup.name.' + text_operators[filters["groups"]["op"]])
                params.append(filter(filters["groups"]["filter"]))
            # end if
            join_tables = True
            tables[AlertGroup] = AlertGroup.alert_group_uuid==Alert.alert_group_uuid
        # end if
        
        if len(tables.keys()) > 0 or join_tables:
            query = query.join(Alert, Alert.alert_uuid==EventAlert.alert_uuid)
            for table in tables:
                query = query.join(table, tables[table])
            # end for
        # end if
        
        # Alert UUIDs
        if check_key_in_filters(filters, "alert_uuids"):
            functions.is_valid_text_filter(filters["alert_uuids"])
            if filters["alert_uuids"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["alert_uuids"]["op"]]
                params.append(op(EventAlert.alert_uuid, filters["alert_uuids"]["filter"]))
            else:
                filter = eval('EventAlert.alert_uuid.' + text_operators[filters["alert_uuids"]["op"]])
                params.append(filter(filters["alert_uuids"]["filter"]))
            # end if
        # end if
        
        # validated filter
        if check_key_in_filters(filters, "validated"):
            functions.is_valid_bool_filter(filters["validated"])
            params.append(EventAlert.validated == filters["validated"])
        # end if
        
        # event alert ingestion_time filters
        if check_key_in_filters(filters, "alert_ingestion_time_filters"):
            functions.is_valid_date_filters(filters["alert_ingestion_time_filters"])
            for ingestion_time_filter in filters["alert_ingestion_time_filters"]:
                op = arithmetic_operators[ingestion_time_filter["op"]]
                params.append(op(EventAlert.ingestion_time, ingestion_time_filter["date"]))
            # end for
        # end if

        # Generators
        if check_key_in_filters(filters, "generators"):
            functions.is_valid_text_filter(filters["generators"])
            if filters["generators"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["generators"]["op"]]
                params.append(op(EventAlert.generator, filters["generators"]["filter"]))
            else:
                filter = eval('EventAlert.generator.' + text_operators[filters["generators"]["op"]])
                params.append(filter(filters["generators"]["filter"]))
            # end if
        # end if

        # notified filter
        if check_key_in_filters(filters, "notified"):
            functions.is_valid_bool_filter(filters["notified"])
            params.append(EventAlert.notified == filters["notified"])
        # end if

        # solved filter
        if check_key_in_filters(filters, "solved"):
            functions.is_valid_bool_filter(filters["solved"])
            params.append(EventAlert.solved == filters["solved"])
        # end if

        # event alert solved time filters
        if check_key_in_filters(filters, "solved_time_filters"):
            functions.is_valid_date_filters(filters["solved_time_filters"])
            for solved_time_filter in filters["solved_time_filters"]:
                op = arithmetic_operators[solved_time_filter["op"]]
                params.append(op(EventAlert.solved_time, solved_time_filter["date"]))
            # end for
        # end if

        # event alert notification time filters
        if check_key_in_filters(filters, "notification_time_filters"):
            functions.is_valid_date_filters(filters["notification_time_filters"])
            for notification_time_filter in filters["notification_time_filters"]:
                op = arithmetic_operators[notification_time_filter["op"]]
                params.append(op(EventAlert.notification_time, notification_time_filter["date"]))
            # end for
        # end if
                
        query = query.filter(*params)

        # Order by
        if check_key_in_filters(filters, "order_by"):
            functions.is_valid_order_by(filters["order_by"])
            if filters["order_by"]["descending"]:
                order_by_statement = eval("EventAlert." + filters["order_by"]["field"] + ".desc()")
            else:
                order_by_statement = eval("EventAlert." + filters["order_by"]["field"])
            # end if
            query = query.order_by(order_by_statement)
        # end if

        # Limit
        if check_key_in_filters(filters, "limit"):
            functions.is_valid_positive_integer(filters["limit"])
            query = query.limit(filters["limit"])
        # end if

        # Offset
        if check_key_in_filters(filters, "offset"):
            functions.is_valid_positive_integer(filters["offset"])
            query = query.offset(filters["offset"])
        # end if

        log_query(query)

        event_alerts = []
        if check_key_in_filters(filters, "delete") and filters["delete"]:
            self.delete(query)
        else:
            event_alerts = query.all()
        # end if

        return event_alerts
    
    def get_event_keys(self, event_uuids = None, dim_signature_uuids = None, keys = None, order_by = None, limit = None, offset = None):
        """
        """
        params = []

        # DIM signature UUIDs
        if dim_signature_uuids != None:
            functions.is_valid_text_filter(dim_signature_uuids)
            if dim_signature_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[dim_signature_uuids["op"]]
                params.append(op(EventKey.dim_signature_uuid, dim_signature_uuids["filter"]))
            else:
                filter = eval('EventKey.dim_signature_uuid.' + text_operators[dim_signature_uuids["op"]])
                params.append(filter(dim_signature_uuids["filter"]))
            # end if
        # end if

        # event_uuids
        if event_uuids != None:
            functions.is_valid_text_filter(event_uuids)
            if event_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[event_uuids["op"]]
                params.append(op(EventKey.event_uuid, event_uuids["filter"]))
            else:
                filter = eval('EventKey.event_uuid.' + text_operators[event_uuids["op"]])
                params.append(filter(event_uuids["filter"]))
            # end if
        # end if

        # keys
        if keys != None:
            functions.is_valid_text_filter(keys)
            if keys["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[keys["op"]]
                params.append(op(EventKey.event_key, keys["filter"]))
            else:
                filter = eval('EventKey.event_key.' + text_operators[keys["op"]])
                params.append(filter(keys["filter"]))
            # end if
        # end if

        query = self.session.query(EventKey).filter(*params)
        
        # Order by
        if order_by != None:
            functions.is_valid_order_by(order_by)
            if order_by["descending"]:
                order_by_statement = eval("EventKey." + order_by["field"] + ".desc()")
            else:
                order_by_statement = eval("EventKey." + order_by["field"])
            # end if
            query = query.order_by(order_by_statement)
        # end if

        # Limit
        if limit != None:
            functions.is_valid_positive_integer(limit)
            query = query.limit(limit)
        # end if

        # Offset
        if offset != None:
            functions.is_valid_positive_integer(offset)
            query = query.offset(offset)
        # end if

        log_query(query)
        event_keys = query.all()

        return event_keys

    def get_event_links(self, event_uuid_links = None, event_uuids = None, link_names = None):
        """
        """
        params = []
        if event_uuid_links:
            functions.is_valid_text_filter(event_uuid_links)
            if event_uuid_links["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[event_uuid_links["op"]]
                params.append(op(EventLink.event_uuid_link, event_uuid_links["filter"]))
            else:
                filter = eval('EventLink.event_uuid_link.' + text_operators[event_uuid_links["op"]])
                params.append(filter(event_uuid_links["filter"]))
            # end if
        # end if

        if event_uuids:
            functions.is_valid_text_filter(event_uuids)
            if event_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[event_uuids["op"]]
                params.append(op(EventLink.event_uuid, event_uuids["filter"]))
            else:
                filter = eval('EventLink.event_uuid.' + text_operators[event_uuids["op"]])
                params.append(filter(event_uuids["filter"]))
            # end if
        # end if

        if link_names:
            functions.is_valid_text_filter(link_names)
            if link_names["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[link_names["op"]]
                params.append(op(EventLink.name, link_names["filter"]))
            else:
                filter = eval('EventLink.name.' + text_operators[link_names["op"]])
                params.append(filter(link_names["filter"]))
            # end if
        # end if

        query = self.session.query(EventLink).filter(*params)
        log_query(query)
        links = query.all()

        return links

    def get_linked_events(self, event_uuids = None, source_uuids = None, explicit_ref_uuids = None, gauge_uuids = None, start_filters = None, stop_filters = None, link_names = None, sources = None, explicit_refs = None, gauge_names = None, gauge_systems = None, value_filters = None, return_prime_events = True, keys = None, back_ref = False, order_by = None, limit = None, offset = None):

        # Obtain prime events
        prime_events = self.get_events(event_uuids = event_uuids, source_uuids = source_uuids, explicit_ref_uuids = explicit_ref_uuids, gauge_uuids = gauge_uuids, sources = sources, explicit_refs = explicit_refs, gauge_names = gauge_names, gauge_systems = gauge_systems, keys = keys, start_filters = start_filters, stop_filters = stop_filters, value_filters = value_filters, order_by = order_by, limit = limit, offset = offset)

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
        for link in links:
            linked_event = [event for event in linked_events if event.event_uuid == link.event_uuid][0]
            events["linked_events"].append({"link_name": link.name,
                                            "event": linked_event})
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
            for link in links:
                event_linking = [event for event in events_linking if event.event_uuid == link.event_uuid_link][0]
                events["events_linking"].append({"link_name": link.name,
                                                "event": event_linking})
            # end for
        # end if

        return events

    def get_linking_events(self, event_uuids = None, source_uuids = None, explicit_ref_uuids = None, gauge_uuids = None, start_filters = None, stop_filters = None, link_names = None, sources = None, explicit_refs = None, gauge_names = None, gauge_systems = None, value_filters = None, return_prime_events = True, keys = None, back_ref = False, order_by = None, limit = None, offset = None):

        # Obtain prime events
        prime_events = self.get_events(event_uuids = event_uuids, source_uuids = source_uuids, explicit_ref_uuids = explicit_ref_uuids, gauge_uuids = gauge_uuids, sources = sources, explicit_refs = explicit_refs, gauge_names = gauge_names, gauge_systems = gauge_systems, keys = keys, start_filters = start_filters, stop_filters = stop_filters, value_filters = value_filters, order_by = order_by, limit = limit, offset = offset)

        prime_event_uuids = [str(event.__dict__["event_uuid"]) for event in prime_events]

        # Obtain the links to the prime events from other events
        links = []
        if len(prime_event_uuids) > 0:
            links = self.get_event_links(event_uuids = {"filter": prime_event_uuids, "op": "in"}, link_names = link_names)
        # end if

        # Obtain the events linking the prime events
        event_linking_uuids = [str(link.event_uuid_link) for link in links]
        events_linking = []
        if len(event_linking_uuids) > 0:
            events_linking = self.get_events(event_uuids = {"filter": event_linking_uuids, "op": "in"})
        # end if

        events = {}
        if return_prime_events:
            events["prime_events"] = prime_events
        # end if
        events["linking_events"] = events_linking

        if back_ref:
            # Obtain the events linked by the prime events
            links = self.get_event_links(event_uuid_links = {"filter": prime_event_uuids, "op": "in"})
            linked_event_uuids = [str(link.event_uuid) for link in links]
            linked_events = []
            if len(linked_event_uuids) > 0:
                linked_events = self.get_events(event_uuids = {"filter": linked_event_uuids, "op": "in"})
            # end if

            events["linked_events"] = linked_events
        # end if

        return events

    def get_linking_events_group_by_link_name(self, event_uuids = None, source_uuids = None, explicit_ref_uuids = None, gauge_uuids = None, start_filters = None, stop_filters = None, link_names = None, sources = None, explicit_refs = None, gauge_names = None, gauge_systems = None, value_filters = None, return_prime_events = True, keys = None, back_ref = False, order_by = None, limit = None, offset = None):

        # Obtain prime events
        prime_events = self.get_events(event_uuids = event_uuids, source_uuids = source_uuids, explicit_ref_uuids = explicit_ref_uuids, gauge_uuids = gauge_uuids, sources = sources, explicit_refs = explicit_refs, gauge_names = gauge_names, gauge_systems = gauge_systems, keys = keys, start_filters = start_filters, stop_filters = stop_filters, value_filters = value_filters, order_by = order_by, limit = limit, offset = offset)

        prime_event_uuids = [str(event.__dict__["event_uuid"]) for event in prime_events]
        events_linking = {}
        for link_name in link_names["filter"]:
            # Obtain the links to the prime events from other events
            links = []
            if len(prime_event_uuids) > 0:
                links = self.get_event_links(event_uuids = {"filter": prime_event_uuids, "op": "in"}, link_names = {"filter": [link_name], "op": link_names["op"]})
            # end if

            # Obtain the events linking the prime events
            event_linking_uuids = [str(link.event_uuid_link) for link in links]
            events_linking[link_name] = []
            if len(event_linking_uuids) > 0:
                events_linking[link_name] = self.get_events(event_uuids = {"filter": event_linking_uuids, "op": "in"})
            # end if
        # end for
        
        events = {}
        if return_prime_events:
            events["prime_events"] = prime_events
        # end if
        events["linking_events"] = events_linking

        if back_ref:
            # Obtain the events linking the prime events
            links = self.get_event_links(event_uuid_links = {"filter": prime_event_uuids, "op": "in"})
            linked_event_uuids = [str(link.event_uuid) for link in links]
            linked_events = []
            if len(event_linking_uuids) > 0:
                linked_events = self.get_events(event_uuids = {"filter": linked_event_uuids, "op": "in"})
            # end if

            events["linked_events"] = linked_events
        # end if

        return events

    def get_annotation_cnfs(self, dim_signature_uuids = None, annotation_cnf_uuids = None, names = None, systems = None, dim_signatures = None, order_by = None, limit = None, offset = None):
        """
        """
        params = []
        tables = []
        # DIM signature UUIDs
        if dim_signature_uuids != None:
            functions.is_valid_text_filter(dim_signature_uuids)
            if dim_signature_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[dim_signature_uuids["op"]]
                params.append(op(AnnotationCnf.dim_signature_uuid, dim_signature_uuids["filter"]))
            else:
                filter = eval('AnnotationCnf.dim_signature_uuid.' + text_operators[dim_signature_uuids["op"]])
                params.append(filter(dim_signature_uuids["filter"]))
            # end if
        # end if
        # DIM signatures
        if dim_signatures != None:
            functions.is_valid_text_filter(dim_signatures)
            if dim_signatures["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[dim_signatures["op"]]
                params.append(op(DimSignature.dim_signature, dim_signatures["filter"]))
            else:
                filter = eval('DimSignature.dim_signature.' + text_operators[dim_signatures["op"]])
                params.append(filter(dim_signatures["filter"]))
            # end if
            tables.append(DimSignature)
        # end if

        # AnnotationCnf UUIDs
        if annotation_cnf_uuids != None:
            functions.is_valid_text_filter(annotation_cnf_uuids)
            if annotation_cnf_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[annotation_cnf_uuids["op"]]
                params.append(op(AnnotationCnf.annotation_cnf_uuid, annotation_cnf_uuids["filter"]))
            else:
                filter = eval('AnnotationCnf.annotation_cnf_uuid.' + text_operators[annotation_cnf_uuids["op"]])
                params.append(filter(annotation_cnf_uuids["filter"]))
            # end if
        # end if

        # AnnotationCnf names
        if names != None:
            functions.is_valid_text_filter(names)
            if names["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[names["op"]]
                params.append(op(AnnotationCnf.name, names["filter"]))
            else:
                filter = eval('AnnotationCnf.name.' + text_operators[names["op"]])
                params.append(filter(names["filter"]))
            # end if
        # end if

        # AnnotationCnf systems
        if systems != None:
            functions.is_valid_text_filter(systems)
            if systems["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[systems["op"]]
                params.append(op(AnnotationCnf.system, systems["filter"]))
            else:
                filter = eval('AnnotationCnf.system.' + text_operators[systems["op"]])
                params.append(filter(systems["filter"]))
            # end if
        # end if

        query = self.session.query(AnnotationCnf)
        for table in set(tables):
            query = query.join(table)
        # end for

        query = query.filter(*params)

        # Order by
        if order_by != None:
            functions.is_valid_order_by(order_by)
            if order_by["descending"]:
                order_by_statement = eval("AnnotationCnf." + order_by["field"] + ".desc()")
            else:
                order_by_statement = eval("AnnotationCnf." + order_by["field"])
            # end if
            query = query.order_by(order_by_statement)
        # end if

        # Limit
        if limit != None:
            functions.is_valid_positive_integer(limit)
            query = query.limit(limit)
        # end if

        # Offset
        if offset != None:
            functions.is_valid_positive_integer(offset)
            query = query.offset(offset)
        # end if

        log_query(query)
        annotation_cnfs = query.all()

        return annotation_cnfs

    def get_annotations(self, source_uuids = None, explicit_ref_uuids = None, annotation_cnf_uuids = None, ingestion_time_filters = None, annotation_uuids = None, sources = None, explicit_refs = None, annotation_cnf_names = None, annotation_cnf_systems = None, value_filters = None, order_by = None, limit = None, offset = None):
        """
        """
        params = []
        tables = []
        # Allow only obtain visible annotations
        params.append(Annotation.visible == True)

        # source_uuids
        if source_uuids != None:
            functions.is_valid_text_filter(source_uuids)
            if source_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[source_uuids["op"]]
                params.append(op(Annotation.source_uuid, source_uuids["filter"]))
            else:
                filter = eval('Annotation.source_uuid.' + text_operators[source_uuids["op"]])
                params.append(filter(source_uuids["filter"]))
            # end if
        # end if

        # explicit_ref_uuids
        if explicit_ref_uuids != None:
            functions.is_valid_text_filter(explicit_ref_uuids)
            if explicit_ref_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[explicit_ref_uuids["op"]]
                params.append(op(Annotation.explicit_ref_uuid, explicit_ref_uuids["filter"]))
            else:
                filter = eval('Annotation.explicit_ref_uuid.' + text_operators[explicit_ref_uuids["op"]])
                params.append(filter(explicit_ref_uuids["filter"]))
            # end if
        # end if

        # annotation_cnf_uuids
        if annotation_cnf_uuids != None:
            functions.is_valid_text_filter(annotation_cnf_uuids)
            if annotation_cnf_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[annotation_cnf_uuids["op"]]
                params.append(op(Annotation.annotation_cnf_uuid, annotation_cnf_uuids["filter"]))
            else:
                filter = eval('Annotation.annotation_cnf_uuid.' + text_operators[annotation_cnf_uuids["op"]])
                params.append(filter(annotation_cnf_uuids["filter"]))
            # end if
        # end if

        # annotation_uuids
        if annotation_uuids != None:
            functions.is_valid_text_filter(annotation_uuids)
            if annotation_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[annotation_uuids["op"]]
                params.append(op(Annotation.annotation_uuid, annotation_uuids["filter"]))
            else:
                filter = eval('Annotation.annotation_uuid.' + text_operators[annotation_uuids["op"]])
                params.append(filter(annotation_uuids["filter"]))
            # end if
        # end if

        # ingestion_time filters
        if ingestion_time_filters != None:
            functions.is_valid_date_filters(ingestion_time_filters)
            for ingestion_time_filter in ingestion_time_filters:
                op = arithmetic_operators[ingestion_time_filter["op"]]
                params.append(op(Annotation.ingestion_time, ingestion_time_filter["date"]))
            # end for
        # end if

        # Sources
        if sources != None:
            functions.is_valid_text_filter(sources)
            if sources["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[sources["op"]]
                params.append(op(Source.name, sources["filter"]))
            else:
                filter = eval('Source.name.' + text_operators[sources["op"]])
                params.append(filter(sources["filter"]))
            # end if
            tables.append(Source)
        # end if

        # Explicit references
        if explicit_refs != None:
            functions.is_valid_text_filter(explicit_refs)
            if explicit_refs["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[explicit_refs["op"]]
                params.append(op(ExplicitRef.explicit_ref, explicit_refs["filter"]))
            else:
                filter = eval('ExplicitRef.explicit_ref.' + text_operators[explicit_refs["op"]])
                params.append(filter(explicit_refs["filter"]))
            # end if
            tables.append(ExplicitRef)
        # end if

        # Annotation configuration names
        if annotation_cnf_names != None:
            functions.is_valid_text_filter(annotation_cnf_names)
            if annotation_cnf_names["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[annotation_cnf_names["op"]]
                params.append(op(AnnotationCnf.name, annotation_cnf_names["filter"]))
            else:
                filter = eval('AnnotationCnf.name.' + text_operators[annotation_cnf_names["op"]])
                params.append(filter(annotation_cnf_names["filter"]))
            # end if
            tables.append(AnnotationCnf)
        # end if

        # Annotation configuration systems
        if annotation_cnf_systems != None:
            functions.is_valid_text_filter(annotation_cnf_systems)
            if annotation_cnf_systems["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[annotation_cnf_systems["op"]]
                params.append(op(AnnotationCnf.system, annotation_cnf_systems["filter"]))
            else:
                filter = eval('AnnotationCnf.system.' + text_operators[annotation_cnf_systems["op"]])
                params.append(filter(annotation_cnf_systems["filter"]))
            # end if
            tables.append(AnnotationCnf)
        # end if

        # value filters
        self.prepare_query_values(value_filters, annotation_value_entities, params, tables)

        query = self.session.query(Annotation)
        for table in set(tables):
            query = query.join(table)
        # end for

        query = query.filter(*params)

        # Order by
        if order_by != None:
            functions.is_valid_order_by(order_by)
            if order_by["descending"]:
                order_by_statement = eval("Annotation." + order_by["field"] + ".desc()")
            else:
                order_by_statement = eval("Annotation." + order_by["field"])
            # end if
            query = query.order_by(order_by_statement)
        # end if

        # Limit
        if limit != None:
            functions.is_valid_positive_integer(limit)
            query = query.limit(limit)
        # end if

        # Offset
        if offset != None:
            functions.is_valid_positive_integer(offset)
            query = query.offset(offset)
        # end if
        
        log_query(query)
        annotations = query.all()

        return annotations

    def get_annotation_alerts(self, filters = None):
        """
        Method to obtain the alerts associated to annotation entities filtered by the received filters

        :param filters: dictionary with the filters to apply to the query
        :type filters: dict

        :return: found annotation_alerts
        :rtype: list
        """
        params = []
        join_tables = False
        tables = {}

        # annotation_uuids
        if check_key_in_filters(filters, "annotation_uuids"):
            functions.is_valid_text_filter(filters["annotation_uuids"])
            if filters["annotation_uuids"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["annotation_uuids"]["op"]]
                params.append(op(AnnotationAlert.annotation_uuid, filters["annotation_uuids"]["filter"]))
            else:
                filter = eval('AnnotationAlert.annotation_uuid.' + text_operators[filters["annotation_uuids"]["op"]])
                params.append(filter(filters["annotation_uuids"]["filter"]))
            # end if
        # end if

        # source_uuids
        if check_key_in_filters(filters, "source_uuids"):
            functions.is_valid_text_filter(filters["source_uuids"])
            if filters["source_uuids"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["source_uuids"]["op"]]
                params.append(op(Annotation.source_uuid, filters["source_uuids"]["filter"]))
            else:
                filter = eval('Annotation.source_uuid.' + text_operators[filters["source_uuids"]["op"]])
                params.append(filter(filters["source_uuids"]["filter"]))
            # end if
            join_tables = True
        # end if

        # explicit_ref_uuids
        if check_key_in_filters(filters, "explicit_ref_uuids"):
            functions.is_valid_text_filter(filters["explicit_ref_uuids"])
            if filters["explicit_ref_uuids"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["explicit_ref_uuids"]["op"]]
                params.append(op(Annotation.explicit_ref_uuid, filters["explicit_ref_uuids"]["filter"]))
            else:
                filter = eval('Annotation.explicit_ref_uuid.' + text_operators[filters["explicit_ref_uuids"]["op"]])
                params.append(filter(filters["explicit_ref_uuids"]["filter"]))
            # end if
            join_tables = True
        # end if

        # annotation_cnf_uuids
        if check_key_in_filters(filters, "annotation_cnf_uuids"):
            functions.is_valid_text_filter(filters["annotation_cnf_uuids"])
            if filters["annotation_cnf_uuids"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["annotation_cnf_uuids"]["op"]]
                params.append(op(Annotation.annotation_cnf_uuid, filters["annotation_cnf_uuids"]["filter"]))
            else:
                filter = eval('Annotation.annotation_cnf_uuid.' + text_operators[filters["annotation_cnf_uuids"]["op"]])
                params.append(filter(filters["annotation_cnf_uuids"]["filter"]))
            # end if
            join_tables = True
        # end if

        # Annotation ingestion_time filters
        if check_key_in_filters(filters, "annotation_ingestion_time_filters"):
            functions.is_valid_date_filters(filters["annotation_ingestion_time_filters"])
            for ingestion_time_filter in filters["annotation_ingestion_time_filters"]:
                op = arithmetic_operators[ingestion_time_filter["op"]]
                params.append(op(Annotation.ingestion_time, ingestion_time_filter["date"]))
            # end for
            join_tables = True
        # end if

        # Sources
        if check_key_in_filters(filters, "sources"):
            functions.is_valid_text_filter(filters["sources"])
            if filters["sources"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["sources"]["op"]]
                params.append(op(Source.name, filters["sources"]["filter"]))
            else:
                filter = eval('Source.name.' + text_operators[filters["sources"]["op"]])
                params.append(filter(filters["sources"]["filter"]))
            # end if
            join_tables = True
            tables[Source] = Source.source_uuid==Annotation.source_uuid
        # end if

        # Explicit references
        if check_key_in_filters(filters, "explicit_refs"):
            functions.is_valid_text_filter(filters["explicit_refs"])
            if filters["explicit_refs"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["explicit_refs"]["op"]]
                params.append(op(ExplicitRef.explicit_ref, filters["explicit_refs"]["filter"]))
            else:
                filter = eval('ExplicitRef.explicit_ref.' + text_operators[filters["explicit_refs"]["op"]])
                params.append(filter(filters["explicit_refs"]["filter"]))
            # end if
            join_tables = True
            tables[ExplicitRef] = ExplicitRef.explicit_ref_uuid==Annotation.explicit_ref_uuid
        # end if

        # Annotation configuration names
        if check_key_in_filters(filters, "annotation_cnf_names"):
            functions.is_valid_text_filter(filters["annotation_cnf_names"])
            if filters["annotation_cnf_names"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["annotation_cnf_names"]["op"]]
                params.append(op(AnnotationCnf.name, filters["annotation_cnf_names"]["filter"]))
            else:
                filter = eval('AnnotationCnf.name.' + text_operators[filters["annotation_cnf_names"]["op"]])
                params.append(filter(filters["annotation_cnf_names"]["filter"]))
            # end if
            join_tables = True
            tables[AnnotationCnf] = AnnotationCnf.annotation_cnf_uuid==Annotation.annotation_cnf_uuid
        # end if

        # Annotation configuration systems
        if check_key_in_filters(filters, "annotation_cnf_systems"):
            functions.is_valid_text_filter(filters["annotation_cnf_systems"])
            if filters["annotation_cnf_systems"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["annotation_cnf_systems"]["op"]]
                params.append(op(AnnotationCnf.system, filters["annotation_cnf_systems"]["filter"]))
            else:
                filter = eval('AnnotationCnf.system.' + text_operators[filters["annotation_cnf_systems"]["op"]])
                params.append(filter(filters["annotation_cnf_systems"]["filter"]))
            # end if
            join_tables = True
            tables[AnnotationCnf] = AnnotationCnf.annotation_cnf_uuid==Annotation.annotation_cnf_uuid
        # end if

        # value filters
        if check_key_in_filters(filters, "value_filters"):
            tables_values = []
            self.prepare_query_values(filters["value_filters"], annotation_value_entities, params, tables_values)
            for table in set(tables_values):
                tables[table] = Annotation.annotation_uuid==table.annotation_uuid
            # end for
        # end if

        query = self.session.query(AnnotationAlert)
        if len(tables.keys()) > 0 or join_tables:
            query = query.join(Annotation, Annotation.annotation_uuid==AnnotationAlert.annotation_uuid)
            for table in tables:
                query = query.join(table, tables[table])
            # end for
        # end if

        join_tables = False
        tables = {}        

        # Alert configuration names
        if check_key_in_filters(filters, "names"):
            functions.is_valid_text_filter(filters["names"])
            if filters["names"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["names"]["op"]]
                params.append(op(Alert.name, filters["names"]["filter"]))
            else:
                filter = eval('Alert.name.' + text_operators[filters["names"]["op"]])
                params.append(filter(filters["names"]["filter"]))
            # end if
            join_tables = True
        # end if

        # Alert severities
        if check_key_in_filters(filters, "severities"):
            functions.is_valid_severity_filter(filters["severities"])
            if filters["severities"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["severities"]["op"]]
                params.append(op(Alert.severity, alert_severity_codes[filters["severities"]["filter"]]))
            else:
                if type(filters["severities"]["filter"]) == list:
                    filters_to_apply = [alert_severity_codes[severity_filter] for severity_filter in filters["severities"]["filter"]]
                else:
                    filters_to_apply = filters["severities"]["filter"]
                # end if
                filter = eval('Alert.severity.' + text_operators[filters["severities"]["op"]])
                params.append(filter(filters_to_apply))
            # end if
            join_tables = True
        # end if

        # Alert groups
        if check_key_in_filters(filters, "groups"):
            functions.is_valid_text_filter(filters["groups"])
            if filters["groups"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["groups"]["op"]]
                params.append(op(AlertGroup.name, filters["groups"]["filter"]))
            else:
                filter = eval('AlertGroup.name.' + text_operators[filters["groups"]["op"]])
                params.append(filter(filters["groups"]["filter"]))
            # end if
            join_tables = True
            tables[AlertGroup] = AlertGroup.alert_group_uuid==Alert.alert_group_uuid
        # end if
        
        if len(tables.keys()) > 0 or join_tables:
            query = query.join(Alert, Alert.alert_uuid==AnnotationAlert.alert_uuid)
            for table in tables:
                query = query.join(table, tables[table])
            # end for
        # end if
        
        # Alert UUIDs
        if check_key_in_filters(filters, "alert_uuids"):
            functions.is_valid_text_filter(filters["alert_uuids"])
            if filters["alert_uuids"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["alert_uuids"]["op"]]
                params.append(op(AnnotationAlert.alert_uuid, filters["alert_uuids"]["filter"]))
            else:
                filter = eval('AnnotationAlert.alert_uuid.' + text_operators[filters["alert_uuids"]["op"]])
                params.append(filter(filters["alert_uuids"]["filter"]))
            # end if
        # end if
        
        # validated filter
        if check_key_in_filters(filters, "validated"):
            functions.is_valid_bool_filter(filters["validated"])
            params.append(AnnotationAlert.validated == filters["validated"])
        # end if
        
        # annotation alert ingestion_time filters
        if check_key_in_filters(filters, "alert_ingestion_time_filters"):
            functions.is_valid_date_filters(filters["alert_ingestion_time_filters"])
            for ingestion_time_filter in filters["alert_ingestion_time_filters"]:
                op = arithmetic_operators[ingestion_time_filter["op"]]
                params.append(op(AnnotationAlert.ingestion_time, ingestion_time_filter["date"]))
            # end for
        # end if

        # Generators
        if check_key_in_filters(filters, "generators"):
            functions.is_valid_text_filter(filters["generators"])
            if filters["generators"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["generators"]["op"]]
                params.append(op(AnnotationAlert.generator, filters["generators"]["filter"]))
            else:
                filter = eval('AnnotationAlert.generator.' + text_operators[filters["generators"]["op"]])
                params.append(filter(filters["generators"]["filter"]))
            # end if
        # end if

        # notified filter
        if check_key_in_filters(filters, "notified"):
            functions.is_valid_bool_filter(filters["notified"])
            params.append(AnnotationAlert.notified == filters["notified"])
        # end if

        # solved filter
        if check_key_in_filters(filters, "solved"):
            functions.is_valid_bool_filter(filters["solved"])
            params.append(AnnotationAlert.solved == filters["solved"])
        # end if

        # annotation alert solved time filters
        if check_key_in_filters(filters, "solved_time_filters"):
            functions.is_valid_date_filters(filters["solved_time_filters"])
            for solved_time_filter in filters["solved_time_filters"]:
                op = arithmetic_operators[solved_time_filter["op"]]
                params.append(op(AnnotationAlert.solved_time, solved_time_filter["date"]))
            # end for
        # end if

        # annotation alert notification time filters
        if check_key_in_filters(filters, "notification_time_filters"):
            functions.is_valid_date_filters(filters["notification_time_filters"])
            for notification_time_filter in filters["notification_time_filters"]:
                op = arithmetic_operators[notification_time_filter["op"]]
                params.append(op(AnnotationAlert.notification_time, notification_time_filter["date"]))
            # end for
        # end if
                
        query = query.filter(*params)

        # Order by
        if check_key_in_filters(filters, "order_by"):
            functions.is_valid_order_by(filters["order_by"])
            if filters["order_by"]["descending"]:
                order_by_statement = eval("AnnotationAlert." + filters["order_by"]["field"] + ".desc()")
            else:
                order_by_statement = eval("AnnotationAlert." + filters["order_by"]["field"])
            # end if
            query = query.order_by(order_by_statement)
        # end if

        # Limit
        if check_key_in_filters(filters, "limit"):
            functions.is_valid_positive_integer(filters["limit"])
            query = query.limit(filters["limit"])
        # end if

        # Offset
        if check_key_in_filters(filters, "offset"):
            functions.is_valid_positive_integer(filters["offset"])
            query = query.offset(filters["offset"])
        # end if

        log_query(query)

        annotation_alerts = []
        if check_key_in_filters(filters, "delete") and filters["delete"]:
            self.delete(query)
        else:
            annotation_alerts = query.all()
        # end if

        return annotation_alerts

    def get_explicit_refs(self, group_ids = None, explicit_ref_uuids = None, explicit_ref_ingestion_time_filters = None, explicit_refs = None, groups = None, sources = None, source_uuids = None, event_uuids = None, gauge_names = None, gauge_systems = None, gauge_uuids = None, start_filters = None, stop_filters = None, duration_filters = None, event_ingestion_time_filters = None, event_value_filters = None, keys = None, annotation_ingestion_time_filters = None, annotation_uuids = None, annotation_cnf_names = None, annotation_cnf_systems = None, annotation_cnf_uuids = None, annotation_value_filters = None, order_by = None, limit = None, offset = None):
        """
        """
        params = []

        tables = {}

        # group_ids
        if group_ids != None:
            functions.is_valid_text_filter(group_ids)
            if group_ids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[group_ids["op"]]
                params.append(op(ExplicitRef.expl_ref_cnf_uuid, group_ids["filter"]))
            else:
                filter = eval('ExplicitRef.expl_ref_cnf_uuid.' + text_operators[group_ids["op"]])
                params.append(filter(group_ids["filter"]))
            # end if
        # end if

        # explicit_ref_uuids
        if explicit_ref_uuids != None:
            functions.is_valid_text_filter(explicit_ref_uuids)
            if explicit_ref_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[explicit_ref_uuids["op"]]
                params.append(op(ExplicitRef.explicit_ref_uuid, explicit_ref_uuids["filter"]))
            else:
                filter = eval('ExplicitRef.explicit_ref_uuid.' + text_operators[explicit_ref_uuids["op"]])
                params.append(filter(explicit_ref_uuids["filter"]))
            # end if
        # end if

        # Explicit references
        if explicit_refs != None:
            functions.is_valid_text_filter(explicit_refs)
            if explicit_refs["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[explicit_refs["op"]]
                params.append(op(ExplicitRef.explicit_ref, explicit_refs["filter"]))
            else:
                filter = eval('ExplicitRef.explicit_ref.' + text_operators[explicit_refs["op"]])
                params.append(filter(explicit_refs["filter"]))
            # end if
        # end if

        # Groups
        if groups != None:
            functions.is_valid_text_filter(groups)
            if groups["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[groups["op"]]
                params.append(op(ExplicitRefGrp.name, groups["filter"]))
            else:
                filter = eval('ExplicitRefGrp.name.' + text_operators[groups["op"]])
                params.append(filter(groups["filter"]))
            # end if
            tables[ExplicitRefGrp] = ExplicitRefGrp.expl_ref_cnf_uuid==ExplicitRef.expl_ref_cnf_uuid
        # end if

        # explicit references ingestion_time filters
        if explicit_ref_ingestion_time_filters != None:
            functions.is_valid_date_filters(explicit_ref_ingestion_time_filters)
            for ingestion_time_filter in explicit_ref_ingestion_time_filters:
                op = arithmetic_operators[ingestion_time_filter["op"]]
                params.append(op(ExplicitRef.ingestion_time, ingestion_time_filter["date"]))
            # end for
        # end if

        query = self.session.query(ExplicitRef)
        for table in tables:
            query = query.join(table, tables[table])
        # end for

        join_tables = False
        tables = {}
        
        # Events
        # event_uuids
        if event_uuids != None:
            functions.is_valid_text_filter(event_uuids)
            if event_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[event_uuids["op"]]
                params.append(op(Event.event_uuid, event_uuids["filter"]))
            else:
                filter = eval('Event.event_uuid.' + text_operators[event_uuids["op"]])
                params.append(filter(event_uuids["filter"]))
            # end if
            join_tables = True
        # end if

        # source_uuids
        if source_uuids != None:
            functions.is_valid_text_filter(source_uuids)
            if source_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[source_uuids["op"]]
                params.append(op(Event.source_uuid, source_uuids["filter"]))
            else:
                filter = eval('Event.source_uuid.' + text_operators[source_uuids["op"]])
                params.append(filter(source_uuids["filter"]))
            # end if
            join_tables = True
        # end if
        
        # Sources
        if sources != None:
            functions.is_valid_text_filter(sources)
            if sources["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[sources["op"]]
                params.append(op(Source.name, sources["filter"]))
            else:
                filter = eval('Source.name.' + text_operators[sources["op"]])
                params.append(filter(sources["filter"]))
            # end if
            join_tables = True
            tables[Source] = Source.source_uuid==Event.source_uuid
        # end if

        # gauge_uuids
        if gauge_uuids != None:
            functions.is_valid_text_filter(gauge_uuids)
            if gauge_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[gauge_uuids["op"]]
                params.append(op(Event.gauge_uuid, gauge_uuids["filter"]))
            else:
                filter = eval('Event.gauge_uuid.' + text_operators[gauge_uuids["op"]])
                params.append(filter(gauge_uuids["filter"]))
            # end if
            join_tables = True
        # end if

        # Gauge names
        if gauge_names != None:
            functions.is_valid_text_filter(gauge_names)
            if gauge_names["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[gauge_names["op"]]
                params.append(op(Gauge.name, gauge_names["filter"]))
            else:
                filter = eval('Gauge.name.' + text_operators[gauge_names["op"]])
                params.append(filter(gauge_names["filter"]))
            # end if
            join_tables = True
            tables[Gauge] = Gauge.gauge_uuid==Event.gauge_uuid
        # end if

        # Gauge systems
        if gauge_systems != None:
            functions.is_valid_text_filter(gauge_systems)
            if gauge_systems["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[gauge_systems["op"]]
                params.append(op(Gauge.system, gauge_systems["filter"]))
            else:
                filter = eval('Gauge.system.' + text_operators[gauge_systems["op"]])
                params.append(filter(gauge_systems["filter"]))
            # end if
            join_tables = True
            tables[Gauge] = Gauge.gauge_uuid==Event.gauge_uuid
        # end if

        # keys
        if keys != None:
            functions.is_valid_text_filter(keys)
            if keys["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[keys["op"]]
                params.append(op(EventKey.event_key, keys["filter"]))
            else:
                filter = eval('EventKey.event_key.' + text_operators[keys["op"]])
                params.append(filter(keys["filter"]))
            # end if
            join_tables = True
            tables[EventKey] = EventKey.event_uuid==Event.event_uuid
        # end if

        # start filters
        if start_filters != None:
            functions.is_valid_date_filters(start_filters)
            for start_filter in start_filters:
                op = arithmetic_operators[start_filter["op"]]
                params.append(op(Event.start, start_filter["date"]))
            # end for
            join_tables = True
        # end if

        # stop filters
        if stop_filters != None:
            functions.is_valid_date_filters(stop_filters)
            for stop_filter in stop_filters:
                op = arithmetic_operators[stop_filter["op"]]
                params.append(op(Event.stop, stop_filter["date"]))
            # end for
            join_tables = True
        # end if

        # duration filters
        if duration_filters != None:
            functions.is_valid_float_filters(duration_filters)
            for duration_filter in duration_filters:
                op = arithmetic_operators[duration_filter["op"]]
                params.append(op((extract("epoch", Event.stop) - extract("epoch", Event.start)), duration_filter["float"]))
            # end for
            join_tables = True
        # end if

        # ingestion_time filters
        if event_ingestion_time_filters != None:
            functions.is_valid_date_filters(event_ingestion_time_filters)
            for ingestion_time_filter in event_ingestion_time_filters:
                op = arithmetic_operators[ingestion_time_filter["op"]]
                params.append(op(Event.ingestion_time, ingestion_time_filter["date"]))
            # end for
            join_tables = True
        # end if

        # Event value filters
        tables_values = []
        self.prepare_query_values(event_value_filters, event_value_entities, params, tables_values)
        for table in set(tables_values):
            tables[table] = Event.event_uuid==table.event_uuid
        # end for

        if len(tables.keys()) > 0 or join_tables:
            query = query.join(Event, ExplicitRef.explicit_ref_uuid==Event.explicit_ref_uuid)
            for table in tables:
                query = query.join(table, tables[table])
            # end for
        # end if

        join_tables = False
        tables = {}
        
        # Annotations
        # annotation_uuids
        if annotation_uuids != None:
            functions.is_valid_text_filter(annotation_uuids)
            if annotation_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[annotation_uuids["op"]]
                params.append(op(Annotation.annotation_uuid, annotation_uuids["filter"]))
            else:
                filter = eval('Annotation.annotation_uuid.' + text_operators[annotation_uuids["op"]])
                params.append(filter(annotation_uuids["filter"]))
            # end if
            join_tables = True
        # end if

        # annotation_cnf_uuids
        if annotation_cnf_uuids != None:
            functions.is_valid_text_filter(annotation_cnf_uuids)
            if annotation_cnf_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[annotation_cnf_uuids["op"]]
                params.append(op(Annotation.annotation_cnf_uuid, annotation_cnf_uuids["filter"]))
            else:
                filter = eval('Annotation.annotation_cnf_uuid.' + text_operators[annotation_cnf_uuids["op"]])
                params.append(filter(annotation_cnf_uuids["filter"]))
            # end if
            join_tables = True
        # end if

        # ingestion_time filters
        if annotation_ingestion_time_filters != None:
            functions.is_valid_date_filters(annotation_ingestion_time_filters)
            for ingestion_time_filter in annotation_ingestion_time_filters:
                op = arithmetic_operators[ingestion_time_filter["op"]]
                params.append(op(Annotation.ingestion_time, ingestion_time_filter["date"]))
            # end for
            join_tables = True
        # end if

        # Annotation configuration names
        if annotation_cnf_names != None:
            functions.is_valid_text_filter(annotation_cnf_names)
            if annotation_cnf_names["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[annotation_cnf_names["op"]]
                params.append(op(AnnotationCnf.name, annotation_cnf_names["filter"]))
            else:
                filter = eval('AnnotationCnf.name.' + text_operators[annotation_cnf_names["op"]])
                params.append(filter(annotation_cnf_names["filter"]))
            # end if
            join_tables = True
            tables[AnnotationCnf] = AnnotationCnf.annotation_cnf_uuid==Annotation.annotation_cnf_uuid
        # end if

        # Annotation configuration systems
        if annotation_cnf_systems != None:
            functions.is_valid_text_filter(annotation_cnf_systems)
            if annotation_cnf_systems["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[annotation_cnf_systems["op"]]
                params.append(op(AnnotationCnf.system, annotation_cnf_systems["filter"]))
            else:
                filter = eval('AnnotationCnf.system.' + text_operators[annotation_cnf_systems["op"]])
                params.append(filter(annotation_cnf_systems["filter"]))
            # end if
            join_tables = True
            tables[AnnotationCnf] = AnnotationCnf.annotation_cnf_uuid==Annotation.annotation_cnf_uuid
        # end if

        # Annotation value filters
        tables_values = []
        self.prepare_query_values(annotation_value_filters, annotation_value_entities, params, tables_values)
        for table in set(tables_values):
            tables[table] = Annotation.annotation_uuid==table.annotation_uuid
        # end for

        if len(tables.keys()) > 0 or join_tables:
            query = query.join(Annotation, ExplicitRef.explicit_ref_uuid==Annotation.explicit_ref_uuid)
            for table in tables:
                query = query.join(table, tables[table])
            # end for
        # end if

        query = query.filter(*params)

        # Order by
        if order_by != None:
            functions.is_valid_order_by(order_by)
            if order_by["descending"]:
                order_by_statement = eval("ExplicitRef." + order_by["field"] + ".desc()")
            else:
                order_by_statement = eval("ExplicitRef." + order_by["field"])
            # end if
            query = query.order_by(order_by_statement)
        # end if

        # Limit
        if limit != None:
            functions.is_valid_positive_integer(limit)
            query = query.limit(limit)
        # end if

        # Offset
        if offset != None:
            functions.is_valid_positive_integer(offset)
            query = query.offset(offset)
        # end if
        
        log_query(query)
        explicit_refs = query.all()

        return explicit_refs

    def get_explicit_ref_alerts(self, filters = None):
        """
        Method to obtain the alerts associated to explicit reference entities filtered by the received filters

        :param filters: dictionary with the filters to apply to the query
        :type filters: dict

        :return: found er_alerts
        :rtype: list
        """
        params = []
        join_tables = False
        tables = {}

        # explicit_ref_uuids
        if check_key_in_filters(filters, "explicit_ref_uuids"):
            functions.is_valid_text_filter(filters["explicit_ref_uuids"])
            if filters["explicit_ref_uuids"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["explicit_ref_uuids"]["op"]]
                params.append(op(ExplicitRefAlert.explicit_ref_uuid, filters["explicit_ref_uuids"]["filter"]))
            else:
                filter = eval('ExplicitRefAlert.explicit_ref_uuid.' + text_operators[filters["explicit_ref_uuids"]["op"]])
                params.append(filter(filters["explicit_ref_uuids"]["filter"]))
            # end if
        # end if
        
        # explicit_ref_group_ids
        if check_key_in_filters(filters, "explicit_ref_group_ids"):
            functions.is_valid_text_filter(filters["explicit_ref_group_ids"])
            if filters["explicit_ref_group_ids"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["explicit_ref_group_ids"]["op"]]
                params.append(op(ExplicitRef.expl_ref_cnf_uuid, filters["explicit_ref_group_ids"]["filter"]))
            else:
                filter = eval('ExplicitRef.expl_ref_cnf_uuid.' + text_operators[filters["explicit_ref_group_ids"]["op"]])
                params.append(filter(filters["explicit_ref_group_ids"]["filter"]))
            # end if
            join_tables = True
        # end if

        # Explicit references
        if check_key_in_filters(filters, "explicit_refs"):
            functions.is_valid_text_filter(filters["explicit_refs"])
            if filters["explicit_refs"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["explicit_refs"]["op"]]
                params.append(op(ExplicitRef.explicit_ref, filters["explicit_refs"]["filter"]))
            else:
                filter = eval('ExplicitRef.explicit_ref.' + text_operators[filters["explicit_refs"]["op"]])
                params.append(filter(filters["explicit_refs"]["filter"]))
            # end if
            join_tables = True
        # end if

        # Groups
        if check_key_in_filters(filters, "explicit_ref_groups"):
            functions.is_valid_text_filter(filters["explicit_ref_groups"])
            if filters["explicit_ref_groups"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["explicit_ref_groups"]["op"]]
                params.append(op(ExplicitRefGrp.name, filters["explicit_ref_groups"]["filter"]))
            else:
                filter = eval('ExplicitRefGrp.name.' + text_operators[filters["explicit_ref_groups"]["op"]])
                params.append(filter(filters["explicit_ref_groups"]["filter"]))
            # end if
            join_tables = True
            tables[ExplicitRefGrp] = ExplicitRefGrp.expl_ref_cnf_uuid==ExplicitRef.expl_ref_cnf_uuid
        # end if

        # explicit references ingestion_time filters
        if check_key_in_filters(filters, "explicit_ref_ingestion_time_filters"):
            functions.is_valid_date_filters(filters["explicit_ref_ingestion_time_filters"])
            for ingestion_time_filter in filters["explicit_ref_ingestion_time_filters"]:
                op = arithmetic_operators[ingestion_time_filter["op"]]
                params.append(op(ExplicitRef.ingestion_time, ingestion_time_filter["date"]))
            # end for
            join_tables = True
        # end if

        query = self.session.query(ExplicitRefAlert)
        if len(tables.keys()) > 0 or join_tables:
            query = query.join(ExplicitRef, ExplicitRef.explicit_ref_uuid==ExplicitRefAlert.explicit_ref_uuid)
            for table in tables:
                query = query.join(table, tables[table])
            # end for
        # end if

        join_tables = False
        tables = {}
        
        # Events
        # event_uuids
        if check_key_in_filters(filters, "event_uuids"):
            functions.is_valid_text_filter(filters["event_uuids"])
            if filters["event_uuids"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["event_uuids"]["op"]]
                params.append(op(Event.event_uuid, filters["event_uuids"]["filter"]))
            else:
                filter = eval('Event.event_uuid.' + text_operators[filters["event_uuids"]["op"]])
                params.append(filter(filters["event_uuids"]["filter"]))
            # end if
            join_tables = True
        # end if

        # source_uuids
        if check_key_in_filters(filters, "source_uuids"):
            functions.is_valid_text_filter(filters["source_uuids"])
            if filters["source_uuids"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["source_uuids"]["op"]]
                params.append(op(Event.source_uuid, filters["source_uuids"]["filter"]))
            else:
                filter = eval('Event.source_uuid.' + text_operators[filters["source_uuids"]["op"]])
                params.append(filter(filters["source_uuids"]["filter"]))
            # end if
            join_tables = True
        # end if
        
        # Sources
        if check_key_in_filters(filters, "sources"):
            functions.is_valid_text_filter(filters["sources"])
            if filters["sources"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["sources"]["op"]]
                params.append(op(Source.name, filters["sources"]["filter"]))
            else:
                filter = eval('Source.name.' + text_operators[filters["sources"]["op"]])
                params.append(filter(filters["sources"]["filter"]))
            # end if
            join_tables = True
            tables[Source] = Source.source_uuid==Event.source_uuid
        # end if

        # gauge_uuids
        if check_key_in_filters(filters, "gauge_uuids"):
            functions.is_valid_text_filter(filters["gauge_uuids"])
            if filters["gauge_uuids"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["gauge_uuids"]["op"]]
                params.append(op(Event.gauge_uuid, filters["gauge_uuids"]["filter"]))
            else:
                filter = eval('Event.gauge_uuid.' + text_operators[filters["gauge_uuids"]["op"]])
                params.append(filter(filters["gauge_uuids"]["filter"]))
            # end if
            join_tables = True
        # end if

        # Gauge names
        if check_key_in_filters(filters, "gauge_names"):
            functions.is_valid_text_filter(filters["gauge_names"])
            if filters["gauge_names"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["gauge_names"]["op"]]
                params.append(op(Gauge.name, filters["gauge_names"]["filter"]))
            else:
                filter = eval('Gauge.name.' + text_operators[filters["gauge_names"]["op"]])
                params.append(filter(filters["gauge_names"]["filter"]))
            # end if
            join_tables = True
            tables[Gauge] = Gauge.gauge_uuid==Event.gauge_uuid
        # end if

        # Gauge systems
        if check_key_in_filters(filters, "gauge_systems"):
            functions.is_valid_text_filter(filters["gauge_systems"])
            if filters["gauge_systems"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["gauge_systems"]["op"]]
                params.append(op(Gauge.system, filters["gauge_systems"]["filter"]))
            else:
                filter = eval('Gauge.system.' + text_operators[filters["gauge_systems"]["op"]])
                params.append(filter(filters["gauge_systems"]["filter"]))
            # end if
            join_tables = True
            tables[Gauge] = Gauge.gauge_uuid==Event.gauge_uuid
        # end if

        # keys
        if check_key_in_filters(filters, "keys"):
            functions.is_valid_text_filter(filters["keys"])
            if filters["keys"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["keys"]["op"]]
                params.append(op(EventKey.event_key, filters["keys"]["filter"]))
            else:
                filter = eval('EventKey.event_key.' + text_operators[filters["keys"]["op"]])
                params.append(filter(filters["keys"]["filter"]))
            # end if
            join_tables = True
            tables[EventKey] = EventKey.event_uuid==Event.event_uuid
        # end if

        # start filters
        if check_key_in_filters(filters, "start_filters"):
            functions.is_valid_date_filters(filters["start_filters"])
            for start_filter in filters["start_filters"]:
                op = arithmetic_operators[start_filter["op"]]
                params.append(op(Event.start, start_filter["date"]))
            # end for
            join_tables = True
        # end if

        # stop filters
        if check_key_in_filters(filters, "stop_filters"):
            functions.is_valid_date_filters(filters["stop_filters"])
            for stop_filter in filters["stop_filters"]:
                op = arithmetic_operators[stop_filter["op"]]
                params.append(op(Event.stop, stop_filter["date"]))
            # end for
            join_tables = True
        # end if

        # duration filters
        if check_key_in_filters(filters, "duration_filters"):
            functions.is_valid_float_filters(filters["duration_filters"])
            for duration_filter in filters["duration_filters"]:
                op = arithmetic_operators[duration_filter["op"]]
                params.append(op((extract("epoch", Event.stop) - extract("epoch", Event.start)), duration_filter["float"]))
            # end for
            join_tables = True
        # end if

        # ingestion_time filters
        if check_key_in_filters(filters, "event_ingestion_time_filters"):
            functions.is_valid_date_filters(filters["event_ingestion_time_filters"])
            for ingestion_time_filter in filters["event_ingestion_time_filters"]:
                op = arithmetic_operators[ingestion_time_filter["op"]]
                params.append(op(Event.ingestion_time, ingestion_time_filter["date"]))
            # end for
            join_tables = True
        # end if

        # value filters
        if check_key_in_filters(filters, "event_value_filters"):
            tables_values = []
            self.prepare_query_values(filters["event_value_filters"], event_value_entities, params, tables_values)
            for table in set(tables_values):
                tables[table] = Event.event_uuid==table.event_uuid
            # end for
        # end if
        
        if len(tables.keys()) > 0 or join_tables:
            query = query.join(Event, Event.explicit_ref_uuid==ExplicitRefAlert.explicit_ref_uuid)
            for table in tables:
                query = query.join(table, tables[table])
            # end for
        # end if

        join_tables = False
        tables = {}

        # Annotations
        # annotation_uuids
        if check_key_in_filters(filters, "annotation_uuids"):
            functions.is_valid_text_filter(filters["annotation_uuids"])
            if filters["annotation_uuids"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["annotation_uuids"]["op"]]
                params.append(op(Annotation.annotation_uuid, filters["annotation_uuids"]["filter"]))
            else:
                filter = eval('Annotation.annotation_uuid.' + text_operators[filters["annotation_uuids"]["op"]])
                params.append(filter(filters["annotation_uuids"]["filter"]))
            # end if
            join_tables = True
        # end if

        # annotation_cnf_uuids
        if check_key_in_filters(filters, "annotation_cnf_uuids"):
            functions.is_valid_text_filter(filters["annotation_cnf_uuids"])
            if filters["annotation_cnf_uuids"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["annotation_cnf_uuids"]["op"]]
                params.append(op(Annotation.annotation_cnf_uuid, filters["annotation_cnf_uuids"]["filter"]))
            else:
                filter = eval('Annotation.annotation_cnf_uuid.' + text_operators[filters["annotation_cnf_uuids"]["op"]])
                params.append(filter(filters["annotation_cnf_uuids"]["filter"]))
            # end if
            join_tables = True
        # end if

        # ingestion_time filters
        if check_key_in_filters(filters, "annotation_ingestion_time_filters"):
            functions.is_valid_date_filters(filters["annotation_ingestion_time_filters"])
            for ingestion_time_filter in filters["annotation_ingestion_time_filters"]:
                op = arithmetic_operators[ingestion_time_filter["op"]]
                params.append(op(Annotation.ingestion_time, ingestion_time_filter["date"]))
            # end for
            join_tables = True
        # end if

        # Annotation configuration names
        if check_key_in_filters(filters, "annotation_cnf_names"):
            functions.is_valid_text_filter(filters["annotation_cnf_names"])
            if filters["annotation_cnf_names"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["annotation_cnf_names"]["op"]]
                params.append(op(AnnotationCnf.name, filters["annotation_cnf_names"]["filter"]))
            else:
                filter = eval('AnnotationCnf.name.' + text_operators[filters["annotation_cnf_names"]["op"]])
                params.append(filter(filters["annotation_cnf_names"]["filter"]))
            # end if
            join_tables = True
            tables[AnnotationCnf] = AnnotationCnf.annotation_cnf_uuid==Annotation.annotation_cnf_uuid
        # end if

        # Annotation configuration systems
        if check_key_in_filters(filters, "annotation_cnf_systems"):
            functions.is_valid_text_filter(filters["annotation_cnf_systems"])
            if filters["annotation_cnf_systems"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["annotation_cnf_systems"]["op"]]
                params.append(op(AnnotationCnf.system, filters["annotation_cnf_systems"]["filter"]))
            else:
                filter = eval('AnnotationCnf.system.' + text_operators[filters["annotation_cnf_systems"]["op"]])
                params.append(filter(filters["annotation_cnf_systems"]["filter"]))
            # end if
            join_tables = True
            tables[AnnotationCnf] = AnnotationCnf.annotation_cnf_uuid==Annotation.annotation_cnf_uuid
        # end if

        # value filters
        if check_key_in_filters(filters, "annotation_value_filters"):
            tables_values = []
            self.prepare_query_values(filters["annotation_value_filters"], annotation_value_entities, params, tables_values)
            for table in set(tables_values):
                tables[table] = Annotation.annotation_uuid==table.annotation_uuid
            # end for
        # end if

        if len(tables.keys()) > 0 or join_tables:
            query = query.join(Annotation, Annotation.explicit_ref_uuid==ExplicitRefAlert.explicit_ref_uuid)
            for table in tables:
                query = query.join(table, tables[table])
            # end for
        # end if

        join_tables = False
        tables = {}        

        # Alert configuration names
        if check_key_in_filters(filters, "names"):
            functions.is_valid_text_filter(filters["names"])
            if filters["names"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["names"]["op"]]
                params.append(op(Alert.name, filters["names"]["filter"]))
            else:
                filter = eval('Alert.name.' + text_operators[filters["names"]["op"]])
                params.append(filter(filters["names"]["filter"]))
            # end if
            join_tables = True
        # end if

        # Alert severities
        if check_key_in_filters(filters, "severities"):
            functions.is_valid_severity_filter(filters["severities"])
            if filters["severities"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["severities"]["op"]]
                params.append(op(Alert.severity, alert_severity_codes[filters["severities"]["filter"]]))
            else:
                if type(filters["severities"]["filter"]) == list:
                    filters_to_apply = [alert_severity_codes[severity_filter] for severity_filter in filters["severities"]["filter"]]
                else:
                    filters_to_apply = filters["severities"]["filter"]
                # end if
                filter = eval('Alert.severity.' + text_operators[filters["severities"]["op"]])
                params.append(filter(filters_to_apply))
            # end if
            join_tables = True
        # end if

        # Alert groups
        if check_key_in_filters(filters, "groups"):
            functions.is_valid_text_filter(filters["groups"])
            if filters["groups"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["groups"]["op"]]
                params.append(op(AlertGroup.name, filters["groups"]["filter"]))
            else:
                filter = eval('AlertGroup.name.' + text_operators[filters["groups"]["op"]])
                params.append(filter(filters["groups"]["filter"]))
            # end if
            join_tables = True
            tables[AlertGroup] = AlertGroup.alert_group_uuid==Alert.alert_group_uuid
        # end if
        
        if len(tables.keys()) > 0 or join_tables:
            query = query.join(Alert, Alert.alert_uuid==ExplicitRefAlert.alert_uuid)
            for table in tables:
                query = query.join(table, tables[table])
            # end for
        # end if
        
        # Alert UUIDs
        if check_key_in_filters(filters, "alert_uuids"):
            functions.is_valid_text_filter(filters["alert_uuids"])
            if filters["alert_uuids"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["alert_uuids"]["op"]]
                params.append(op(ExplicitRefAlert.alert_uuid, filters["alert_uuids"]["filter"]))
            else:
                filter = eval('ExplicitRefAlert.alert_uuid.' + text_operators[filters["alert_uuids"]["op"]])
                params.append(filter(filters["alert_uuids"]["filter"]))
            # end if
        # end if
        
        # validated filter
        if check_key_in_filters(filters, "validated"):
            functions.is_valid_bool_filter(filters["validated"])
            params.append(ExplicitRefAlert.validated == filters["validated"])
        # end if
        
        # explicit reference alert ingestion_time filters
        if check_key_in_filters(filters, "alert_ingestion_time_filters"):
            functions.is_valid_date_filters(filters["alert_ingestion_time_filters"])
            for ingestion_time_filter in filters["alert_ingestion_time_filters"]:
                op = arithmetic_operators[ingestion_time_filter["op"]]
                params.append(op(ExplicitRefAlert.ingestion_time, ingestion_time_filter["date"]))
            # end for
        # end if

        # Generators
        if check_key_in_filters(filters, "generators"):
            functions.is_valid_text_filter(filters["generators"])
            if filters["generators"]["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[filters["generators"]["op"]]
                params.append(op(ExplicitRefAlert.generator, filters["generators"]["filter"]))
            else:
                filter = eval('ExplicitRefAlert.generator.' + text_operators[filters["generators"]["op"]])
                params.append(filter(filters["generators"]["filter"]))
            # end if
        # end if

        # notified filter
        if check_key_in_filters(filters, "notified"):
            functions.is_valid_bool_filter(filters["notified"])
            params.append(ExplicitRefAlert.notified == filters["notified"])
        # end if

        # solved filter
        if check_key_in_filters(filters, "solved"):
            functions.is_valid_bool_filter(filters["solved"])
            params.append(ExplicitRefAlert.solved == filters["solved"])
        # end if

        # explicit reference alert solved time filters
        if check_key_in_filters(filters, "solved_time_filters"):
            functions.is_valid_date_filters(filters["solved_time_filters"])
            for solved_time_filter in filters["solved_time_filters"]:
                op = arithmetic_operators[solved_time_filter["op"]]
                params.append(op(ExplicitRefAlert.solved_time, solved_time_filter["date"]))
            # end for
        # end if

        # explicit reference alert notification time filters
        if check_key_in_filters(filters, "notification_time_filters"):
            functions.is_valid_date_filters(filters["notification_time_filters"])
            for notification_time_filter in filters["notification_time_filters"]:
                op = arithmetic_operators[notification_time_filter["op"]]
                params.append(op(ExplicitRefAlert.notification_time, notification_time_filter["date"]))
            # end for
        # end if
                
        query = query.filter(*params)

        # Order by
        if check_key_in_filters(filters, "order_by"):
            functions.is_valid_order_by(filters["order_by"])
            if filters["order_by"]["descending"]:
                order_by_statement = eval("ExplicitRefAlert." + filters["order_by"]["field"] + ".desc()")
            else:
                order_by_statement = eval("ExplicitRefAlert." + filters["order_by"]["field"])
            # end if
            query = query.order_by(order_by_statement)
        # end if

        # Limit
        if check_key_in_filters(filters, "limit"):
            functions.is_valid_positive_integer(filters["limit"])
            query = query.limit(filters["limit"])
        # end if

        # Offset
        if check_key_in_filters(filters, "offset"):
            functions.is_valid_positive_integer(filters["offset"])
            query = query.offset(filters["offset"])
        # end if

        log_query(query)

        explicit_ref_alerts = []
        if check_key_in_filters(filters, "delete") and filters["delete"]:
            self.delete(query)
        else:
            explicit_ref_alerts = query.all()
        # end if

        return explicit_ref_alerts
    
    def get_explicit_ref_links(self, explicit_ref_uuid_links = None, explicit_ref_uuids = None, link_names = None):
        """
        """
        params = []
        if explicit_ref_uuid_links:
            functions.is_valid_text_filter(explicit_ref_uuid_links)
            if explicit_ref_uuid_links["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[explicit_ref_uuid_links["op"]]
                params.append(op(DimSignature.explicit_ref_uuid_link, explicit_ref_uuid_links["filter"]))
            else:
                filter = eval('ExplicitRefLink.explicit_ref_uuid_link.' + text_operators[explicit_ref_uuid_links["op"]])
                params.append(filter(explicit_ref_uuid_links["filter"]))
            # end if
        # end if

        if explicit_ref_uuids:
            functions.is_valid_text_filter(explicit_ref_uuids)
            if explicit_ref_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[explicit_ref_uuids["op"]]
                params.append(op(ExplicitRefLink.explicit_ref_uuid, explicit_ref_uuids["filter"]))
            else:
                filter = eval('ExplicitRefLink.explicit_ref_uuid.' + text_operators[explicit_ref_uuids["op"]])
                params.append(filter(explicit_ref_uuids["filter"]))
            # end if
        # end if

        if link_names:
            functions.is_valid_text_filter(link_names)
            if link_names["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[link_names["op"]]
                params.append(op(ExplicitRefLink.name, link_names["filter"]))
            else:
                filter = eval('ExplicitRefLink.name.' + text_operators[link_names["op"]])
                params.append(filter(link_names["filter"]))
            # end if
        # end if

        query = self.session.query(ExplicitRefLink).filter(*params)
        log_query(query)
        links = query.all()

        return links

    def get_linked_explicit_refs(self, group_ids = None, explicit_ref_uuids = None, explicit_refs = None, ingestion_time_filters = None, link_names = None, groups = None, return_prime_explicit_refs = True, back_ref = False, order_by = None, limit = None, offset = None):

        # Obtain prime explicit_refs
        prime_explicit_refs = self.get_explicit_refs(group_ids = group_ids, groups = groups, explicit_ref_uuids = explicit_ref_uuids, explicit_refs = explicit_refs, explicit_ref_ingestion_time_filters = ingestion_time_filters, order_by = order_by, limit = limit, offset = offset)

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

    def get_linking_explicit_refs(self, group_ids = None, explicit_ref_uuids = None, explicit_refs = None, ingestion_time_filters = None, link_names = None, groups = None, return_prime_explicit_refs = True, back_ref = False, order_by = None, limit = None, offset = None):

        # Obtain prime explicit_refs
        prime_explicit_refs = self.get_explicit_refs(group_ids = group_ids, groups = groups, explicit_ref_uuids = explicit_ref_uuids, explicit_refs = explicit_refs, explicit_ref_ingestion_time_filters = ingestion_time_filters, order_by = order_by, limit = limit, offset = offset)

        prime_explicit_ref_uuids = [str(explicit_ref.explicit_ref_uuid) for explicit_ref in prime_explicit_refs]

        # Obtain the links to the prime explicit_refs from other explicit_refs
        links = []
        if len(prime_explicit_ref_uuids) > 0:
            links = self.get_explicit_ref_links(explicit_ref_uuids = {"filter": prime_explicit_ref_uuids, "op": "in"}, link_names = link_names)
        # end if

        # Obtain the explicit_refs linking the prime explicit_refs
        explicit_ref_linking_uuids = [str(link.explicit_ref_uuid_link) for link in links]
        explicit_refs_linking = []
        if len(explicit_ref_linking_uuids) > 0:
            explicit_refs_linking = self.get_explicit_refs(explicit_ref_uuids = {"filter": explicit_ref_linking_uuids, "op": "in"})
        # end if

        explicit_refs = {}
        if return_prime_explicit_refs:
            explicit_refs["prime_explicit_refs"] = prime_explicit_refs
        # end if
        explicit_refs["linking_explicit_refs"] = explicit_refs_linking

        if back_ref:
            # Obtain the explicit_refs linked by the prime explicit_refs
            links = self.get_explicit_ref_links(explicit_ref_uuid_links = {"filter": prime_explicit_ref_uuids, "op": "in"})
            linked_explicit_ref_uuids = [str(link.explicit_ref_uuid) for link in links]
            linked_explicit_refs = []
            if len(linked_explicit_ref_uuids) > 0:
                linked_explicit_refs = self.get_explicit_refs(explicit_ref_uuids = {"filter": linked_explicit_ref_uuids, "op": "in"})
            # end if

            explicit_refs["linked_explicit_refs"] = linked_explicit_refs
        # end if

        return explicit_refs

    def get_linking_explicit_refs_group_by_link_name(self, group_ids = None, explicit_ref_uuids = None, explicit_refs = None, ingestion_time_filters = None, link_names = None, groups = None, return_prime_explicit_refs = True, back_ref = False, order_by = None, limit = None, offset = None):

        # Obtain prime explicit_refs
        prime_explicit_refs = self.get_explicit_refs(group_ids = group_ids, groups = groups, explicit_ref_uuids = explicit_ref_uuids, explicit_refs = explicit_refs, explicit_ref_ingestion_time_filters = ingestion_time_filters, order_by = order_by, limit = limit, offset = offset)

        prime_explicit_ref_uuids = [str(explicit_ref.explicit_ref_uuid) for explicit_ref in prime_explicit_refs]

        explicit_refs_linking = {}
        for link_name in link_names["filter"]:
            # Obtain the links to the prime explicit_refs from other explicit_refs
            links = []
            if len(prime_explicit_ref_uuids) > 0:
                links = self.get_explicit_ref_links(explicit_ref_uuids = {"filter": prime_explicit_ref_uuids, "op": "in"}, link_names = {"filter": [link_name], "op": link_names["op"]})
            # end if

            # Obtain the explicit_refs linking the prime explicit_refs
            explicit_ref_linking_uuids = [str(link.explicit_ref_uuid_link) for link in links]
            explicit_refs_linking[link_name] = []
            if len(explicit_ref_linking_uuids) > 0:
                explicit_refs_linking[link_name] = self.get_explicit_refs(explicit_ref_uuids = {"filter": explicit_ref_linking_uuids, "op": "in"})
            # end if
        # end for
        
        explicit_refs = {}
        if return_prime_explicit_refs:
            explicit_refs["prime_explicit_refs"] = prime_explicit_refs
        # end if
        explicit_refs["linking_explicit_refs"] = explicit_refs_linking

        if back_ref:
            # Obtain the explicit_refs linked by the prime explicit_refs
            links = self.get_explicit_ref_links(explicit_ref_uuid_links = {"filter": prime_explicit_ref_uuids, "op": "in"})
            linked_explicit_ref_uuids = [str(link.explicit_ref_uuid) for link in links]
            linked_explicit_refs = []
            if len(linked_explicit_ref_uuids) > 0:
                linked_explicit_refs = self.get_explicit_refs(explicit_ref_uuids = {"filter": linked_explicit_ref_uuids, "op": "in"})
            # end if

            explicit_refs["linked_explicit_refs"] = linked_explicit_refs
        # end if

        return explicit_refs

    def get_explicit_refs_groups(self, group_ids = None, names = None, order_by = None, limit = None, offset = None):
        """
        """
        params = []
        # Group UUIDs
        if group_ids != None:
            functions.is_valid_text_filter(group_ids)
            if group_ids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[group_ids["op"]]
                params.append(op(ExplicitRefGrp.expl_ref_cnf_uuid, group_ids["filter"]))
            else:
                filter = eval('ExplicitRefGrp.expl_ref_cnf_uuid.' + text_operators[group_ids["op"]])
                params.append(filter(group_ids["filter"]))
            # end if
        # end if

        # Gauge names
        if names != None:
            functions.is_valid_text_filter(names)
            if names["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[names["op"]]
                params.append(op(ExplicitRefGrp.name, names["filter"]))
            else:
                filter = eval('ExplicitRefGrp.name.' + text_operators[names["op"]])
                params.append(filter(names["filter"]))
            # end if
        # end if

        query = self.session.query(ExplicitRefGrp).filter(*params)

        # Order by
        if order_by != None:
            functions.is_valid_order_by(order_by)
            if order_by["descending"]:
                order_by_statement = eval("ExplicitRefGrp." + order_by["field"] + ".desc()")
            else:
                order_by_statement = eval("ExplicitRefGrp." + order_by["field"])
            # end if
            query = query.order_by(order_by_statement)
        # end if

        # Limit
        if limit != None:
            functions.is_valid_positive_integer(limit)
            query = query.limit(limit)
        # end if

        # Offset
        if offset != None:
            functions.is_valid_positive_integer(offset)
            query = query.offset(offset)
        # end if

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
            if event_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[event_uuids["op"]]
                params.append(op(event_value_entities[value_type].event_uuid, event_uuids["filter"]))
            else:
                filter = eval('event_value_entities[value_type].event_uuid.' + text_operators[event_uuids["op"]])
                params.append(filter(event_uuids["filter"]))
            # end if
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
            if annotation_uuids["op"] in arithmetic_operators.keys():
                op = arithmetic_operators[annotation_uuids["op"]]
                params.append(op(annotation_value_entities[value_type].annotation_uuid, annotation_uuids["filter"]))
            else:
                filter = eval('annotation_value_entities[value_type].annotation_uuid.' + text_operators[annotation_uuids["op"]])
                params.append(filter(annotation_uuids["filter"]))
            # end if
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
