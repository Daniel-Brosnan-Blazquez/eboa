"""
Engine definition

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import datetime
import uuid
import random
import os
from dateutil import parser
from itertools import chain
from oslo_concurrency import lockutils
import json
import jsonschema
import re
from importlib import import_module
from distutils import util

# Import SQLalchemy entities
from sqlalchemy import or_, and_
from sqlalchemy.exc import IntegrityError, InternalError
from sqlalchemy.sql import func
from sqlalchemy.orm import scoped_session

# Import GEOalchemy entities
from geoalchemy2 import functions
from geoalchemy2.shape import to_shape

# Import exceptions
from eboa.engine.errors import LinksInconsistency, UndefinedEventLink, DuplicatedEventLinkRef, WrongPeriod, SourceAlreadyIngested, WrongValue, OddNumberOfCoordinates, EboaResourcesPathNotAvailable, WrongGeometry, ErrorParsingDictionary, DuplicatedValues, UndefinedEntityReference, PriorityNotDefined, WrongReportedValidityPeriod

# Import datamodel
from eboa.datamodel.base import Session
from eboa.datamodel.dim_signatures import DimSignature
from eboa.datamodel.alerts import Alert, AlertGroup, EventAlert, AnnotationAlert, SourceAlert, ExplicitRefAlert
from eboa.datamodel.events import Event, EventLink, EventKey, EventText, EventDouble, EventObject, EventGeometry, EventBoolean, EventTimestamp
from eboa.datamodel.gauges import Gauge
from eboa.datamodel.sources import Source, SourceStatus
from eboa.datamodel.explicit_refs import ExplicitRef, ExplicitRefGrp, ExplicitRefLink
from eboa.datamodel.annotations import Annotation, AnnotationCnf, AnnotationText, AnnotationDouble, AnnotationObject, AnnotationGeometry, AnnotationBoolean, AnnotationTimestamp

# Import query interface
from eboa.engine.query import Query

# Import xml parser
from lxml import etree

# Import parsing module
import eboa.engine.parsing as parsing

# Import logging
from eboa.logging import Log

# Import debugging
from eboa.debugging import debug, race_condition, race_condition2, race_condition3

# Import auxiliary functions
from eboa.engine.functions import get_resources_path, get_schemas_path, read_configuration
from eboa.engine.common_functions import insert_values, insert_alert_groups, insert_alert_cnfs

config = read_configuration()

logging = Log(name = __name__)
logger = logging.logger

exit_codes = {
    "OK": {
        "status": 0,
        "message": "The source file {} associated to the DIM signature {} and DIM processing {} with version {} has ingested correctly {} event/s, {} annotation/s and {} alert/s"
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
    "UNDEFINED_EVENT_LINK_REF": {
        "status": 6,
        "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} (or any other DIM in the same batch) contains an event which links to an undefined reference identifier {}"
    },
    "WRONG_VALUE": {
        "status": 7,
        "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} contains an event/annotation defining a wrong value. The error was: {}"
    },
    "ODD_NUMBER_OF_COORDINATES": {
        "status": 8,
        "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} contains an event/annotation defining a wrong value. The error was: {}"
    },
    "FILE_NOT_VALID": {
        "status": 9,
        "message": "The source file with name {} does not pass the schema verification"
    },
    "WRONG_GEOMETRY": {
        "status": 10,
        "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} contains an event/annotation which defines a wrong geometry. The exception raised has been the following: {}"
    },
    "DUPLICATED_EVENT_LINK_REF": {
        "status": 11,
        "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} contains more than one event which defines the same link reference identifier {}"
    },
    "LINKS_INCONSISTENCY": {
        "status": 12,
        "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} (or any other DIM in the same batch) defines links between events which lead to clashing unique values into the DDBB. The exception raised has been the following: {}"
    },
    "DUPLICATED_VALUES": {
        "status": 13,
        "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} defines duplicated values inside the same entity. The exception raised has been the following: {}"
    },
    "UNDEFINED_ENTITY_REF": {
        "status": 14,
        "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} contains an alert which links to an undefined reference identifier {}"
    },
    "PROCESSING_DURATION_NOT_TIMEDELTA": {
        "status": 15,
        "message": "The source file with name {} has been received to be processed with a processing duration of type different than datetime.timedelta"
    },
    "PROCESSOR_DOES_NOT_EXIST": {
        "status": 16,
        "message": "The source file with path {} was going to be processed with the processor {} which does not exist. Reported error: {}"
    },
    "PROCESSING_ENDED_UNEXPECTEDLY": {
        "status": 17,
        "message": "The source file with name {} was going to be processed by the processor {} but the processing ended unexpectedly with the error {}"
    },
    "INGESTION_ENDED_UNEXPECTEDLY": {
        "status": 18,
        "message": "The source file with name {} was going to be ingested with the processor {} but the ingestion ended unexpectedly with the error {}"
    },
    "FILE_DOES_NOT_EXIST": {
        "status": 19,
        "message": "The source file with path {} does not exist"
    },
    "FILE_DOES_NOT_HAVE_A_TRIGGERING_RULE": {
        "status": 20,
        "message": "The source file with path {} does not have any triggering rule associated"
    },
    "PRIORITY_NOT_DEFINED": {
        "status": 21,
        "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} defines the operation mode as 'insert_and_erase_with_priority' or 'insert_and_erase_with_equal_or_lower_priority' or contains an event or an annotation with insertion_type 'INSERT_and_ERASE_with_PRIORITY' or 'INSERT_and_ERASE_with_EQUAL_or_LOWER_PRIORITY' or 'INSERT_and_ERASE_INTERSECTED_EVENTS_with_PRIORITY' or 'INSERT_and_ERASE_per_EVENT_with_PRIORITY' or 'EVENT_KEYS_with_PRIORITY' but the corresponding priority has not been defined inside the source structure"
    },
    "WRONG_SOURCE_REPORTED_VALIDITY_PERIOD": {
        "status": 22,
        "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} has a reported validity period which its stop ({}) is lower than its start ({})"
    },
    "LINK_TO_NOT_AVAILABLE_EVENT": {
        "status": 23,
        "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} defines the link with event_uuid_link '{}', name '{}' and event_uuid '{}' for which the event_uuid is not available. The exception raised has been the following: {}\nThis error is consider normal business and so the link is skipped but the ingestion continues."
    },
    "FILE_IS_NOT_XML_OR_XML_CONTENT_INCORRECT": {
        "status": 24,
        "message": "The file {} is not an XML or its content is not correct."
    },
    "FILE_DOES_NOT_PASS_SCHEMA": {
        "status": 25,
        "message": "The file {} does not pass the schema {} with the following errors and/or warnings:\n{}"
    },
    "TRIGGERING_COMMAND_ENDED_UNEXPECTEDLY": {
        "status": 26,
        "message": "The triggered command associated to the source file with name {} ended unexpectedly with the following errors:\n{}"
    },
    "SCHEMA_FILE_DOES_NOT_EXIST": {
        "status": 27,
        "message": "The schema file with path {} does not exist"
    }
}

class Engine():
    """Class for communicating with the engine of the eboa module

    Provides access to the logic for inserting, deleting and updating
    the information stored into the DDBB
    """
    # Set the synchronized module
    synchronized = lockutils.synchronized_with_prefix('eboa-')

    def __init__(self, data = None):
        """
        Instantiation method

        :param data: data provided to be treat by the engine (default None)
        :type data: dict
        """
        if data == None:
            data = {}
        # end if
        self.data = data
        self.Scoped_session = scoped_session(Session)
        self.session = self.Scoped_session()
        self.session_progress = self.Scoped_session()
        self.query = Query(self.session)
        self.operation = None
    
        return

    ###################
    # PARSING METHODS #
    ###################
    @debug
    def parse_data_from_json(self, json_path, check_schema = True):
        """
        Method to parse a json file for later treatment of its content
        
        :param json_path: path to the json file to be parsed
        :type json_path: str
        :param check_schema: indicates whether to pass a schema over the json file or not
        :type check_schema: bool
        """
        json_name = os.path.basename(json_path)
        # Parse data from the json file
        try:
            with open(json_path) as input_file:
                data = json.load(input_file)

        except ValueError:
            message = exit_codes["FILE_NOT_VALID"]["message"].format(json_name)
            self._insert_source_without_dim_signature(json_name)
            self._insert_source_status(exit_codes["FILE_NOT_VALID"]["status"], error = True, message = message)
            self.source.parse_error = "The json file cannot be loaded as it has a wrong structure"
            # Insert the content of the file into the DDBB
            with open(json_path) as input_file:
                self.source.content_text = input_file.read()
            self.session.commit()
            # Log the error
            logger.error(message)
            return exit_codes["FILE_NOT_VALID"]["status"]
        # end try

        if check_schema:
            try:
                parsing.validate_data_dictionary(data)
            except ErrorParsingDictionary as e:
                self._insert_source_without_dim_signature(json_name)
                self._insert_source_status(exit_codes["FILE_NOT_VALID"]["status"], error = True, message = str(e))
                self.source.parse_error = str(e)
                # Insert the content of the file into the DDBB
                with open(json_path) as input_file:
                    self.source.content_text = input_file.read()
                self.session.commit()
                # Log the error
                logger.error(str(e))
                logger.error(exit_codes["FILE_NOT_VALID"]["message"].format(json_name))
                return exit_codes["FILE_NOT_VALID"]["status"]
            # end if

        # end if

        self.data=data

        return 

    @debug
    def parse_data_from_xml(self, xml_path, check_schema = True):
        """
        Method to parse an xml file for later treatment of its content
        
        :param xml_path: path to the xml file to be parsed
        :type xml_path: str
        :param check_schema: indicates whether to pass a schema over the xml file or not
        :type check_schema: bool
        """
        xml_name = os.path.basename(xml_path)
        # Parse data from the xml file
        try:
            parsed_xml = etree.parse(xml_path)
        except etree.XMLSyntaxError as e:
            self._insert_source_without_dim_signature(xml_name)
            self._insert_source_status(exit_codes["FILE_NOT_VALID"]["status"], error = True, message = str(e))
            # Insert the parse error into the DDBB
            self.source.parse_error = str(e)
            # Insert the content of the file into the DDBB
            with open(xml_path,"r") as xml_file:
                self.source.content_text = xml_file.read()
            self.session.commit()
            # Log the error
            logger.error(exit_codes["FILE_NOT_VALID"]["message"].format(xml_name))
            return exit_codes["FILE_NOT_VALID"]["status"]
        # end try

        xpath_xml = etree.XPathEvaluator(parsed_xml)

        # Pass schema
        if check_schema:
            schema_path = get_schemas_path() + "/eboa_schema.xsd"
            parsed_schema = etree.parse(schema_path)
            schema = etree.XMLSchema(parsed_schema)
            valid = schema.validate(parsed_xml)
            if not valid:
                message = exit_codes["FILE_NOT_VALID"]["message"].format(xml_name)
                self._insert_source_without_dim_signature(xml_name)
                self._insert_source_status(exit_codes["FILE_NOT_VALID"]["status"], error = True, message = message)
                # Insert the parse error into the DDBB
                self.source.parse_error = str(schema.error_log.last_error)
                # Insert the content of the file into the DDBB
                with open(xml_path,"r") as xml_file:
                    self.source.content_text = xml_file.read()
                self.session.commit()
                # Log the error
                logger.error(message)
                return exit_codes["FILE_NOT_VALID"]["status"]
            # end if
        # end if
        self.data["operations"] = []
        for operation in xpath_xml("/ops/child::*"):
            if operation.tag == "insert" or operation.tag == "insert_and_erase":
                self._parse_insert_operation_from_xml(operation)
            # end if
        # end for

        return 

    @debug
    def _parse_insert_operation_from_xml(self, operation):
        """
        Method to parse the insert element inside an xml file

        :param operation: xml element pointing to the insert tag
        :type operation: xml element
        """
        data = {}
        if operation.tag == "insert":
            data["mode"] = "insert"
        else:
            data["mode"] = "insert_and_erase"
        # end if
        # Extract dim_signature
        data["dim_signature"] = {"name": operation.xpath("dim_signature")[0].get("name"),
                                 "version": operation.xpath("dim_signature")[0].get("version"),
                                 "exec": operation.xpath("dim_signature")[0].get("exec")}
        # end if

        # Extract source
        if len (operation.xpath("source")) == 1:
            data["source"] = {"name": operation.xpath("source")[0].get("name"),
                              "reception_time": operation.xpath("source")[0].get("reception_time"),
                              "generation_time": operation.xpath("source")[0].get("generation_time"),
                              "validity_start": operation.xpath("source")[0].get("validity_start"),
                              "validity_stop": operation.xpath("source")[0].get("validity_stop")}
            if operation.xpath("source")[0].get("processing_duration") != None:
                data["source"]["processing_duration"] = operation.xpath("source")[0].get("processing_duration")
            # end if
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
                if event.get("link_ref"):
                    event_info["link_ref"] = event.get("link_ref")
                # end if
                event_info["gauge"] = {"name": event.xpath("gauge")[0].get("name"),
                                       "system": event.xpath("gauge")[0].get("system"),
                                       "description": event.xpath("gauge")[0].get("description"),
                                       "insertion_type": event.xpath("gauge")[0].get("insertion_type")}
                # end if
                # Add links
                if len (event.xpath("links/link")) > 0:
                    links = []
                    for link in event.xpath("links/link"):
                        link_dict = {"name": link.get("name"),
                                      "link": link.text,
                                      "link_mode": link.get("link_mode")}
                        if "back_ref" in link.keys():
                            link_dict["back_ref"] = link.get("back_ref")
                        # end if
                        links.append(link_dict)
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
                                                     "system": annotation.xpath("annotation_cnf")[0].get("system"),
                                                     "description": annotation.xpath("annotation_cnf")[0].get("description")}
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
        Method to parse the values element inside an xml file

        :param node: xml element pointing to the values element
        :type node: xml element
        :param parent: list of values
        :type parent: list
        """
        for child_node in node.xpath("child::*"):
            if child_node.tag == "value":
                parent.append({"name": child_node.get("name"),
                               "type": child_node.get("type"),
                               "value": child_node.text
                })
            else:
                parent_object = {"name": child_node.get("name"),
                                 "type": "object",
                                 "values": []
                }
                parent.append(parent_object)
                self._parse_values_from_xml(child_node, parent_object["values"])
            # end if
        # end for
        return

    #####################
    # INSERTION METHODS #
    #####################
    @debug
    def _validate_data(self, data, source = None):
        """
        Method to validate the data structure

        :param data: structure of data to validate
        :type data: dict 
        :param source: name of the source of the data
        :type source: str
       """

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary as e:
            if source != None:
                self._insert_source_without_dim_signature(source)
                self._insert_source_status(exit_codes["FILE_NOT_VALID"]["status"], error = True, message = str(e))
                self.source.parse_error = str(e)
                self.session.commit()
            # end if
            logger.error(str(e))
            return False
        # end try

        return True

    @debug
    def treat_data(self, data = None, source = None, validate = True, processing_duration = None):
        """
        Method to treat the data stored in self.data

        :param data: structure of data to treat
        :type data: dict 
        :param source: name of the source of the data
        :type source: str
        :param validate: flag to indicate if the schema check has to be performed
        :type validate: bool
        :param processing_duration: duration of the processing which generated the data to be treated
        :type processing_duration: datetime.timedelta

        :return: exit_codes for every operation with the associated information (DIM signature, processor and source)
        :rtype: list of dictionaries
        """

        # Initialize context
        self._initialize_context_treat_data()

        if data != None:
            self.data = data
        # end if

        if processing_duration != None and not type(processing_duration) == datetime.timedelta:
            logger.error(exit_codes["PROCESSING_DURATION_NOT_TIMEDELTA"]["message"].format(source))
            processing_duration = None
        # end if
        
        if validate:
            is_valid = self._validate_data(self.data, source = source)
            if not is_valid:
                # Log the error
                logger.error(exit_codes["FILE_NOT_VALID"]["message"].format(source))
                returned_information = {
                    "source": source,
                    "dim_signature": None,
                    "processor": None,
                    "status": exit_codes["FILE_NOT_VALID"]["status"]
                }
                return [returned_information]
            # end if
        # end if

        returned_values = []
        for self.operation in self.data.get("operations") or []:

            lock = "treat_data_" + self.operation.get("source").get("name")
            @self.synchronized(lock, external=True, lock_path="/dev/shm")
            def treat_operation_data(self, processing_duration, returned_values):
                returned_value = -1
                self.all_gauges_for_insert_and_erase = False
                self.all_gauges_for_insert_and_erase_with_priority = False
                self.all_gauges_for_insert_and_erase_with_equal_or_lower_priority = False
                if self.operation.get("mode") == "insert_and_erase":
                    self.all_gauges_for_insert_and_erase = True
                elif self.operation.get("mode") == "insert_and_erase_with_priority":
                    self.all_gauges_for_insert_and_erase_with_priority = True
                elif self.operation.get("mode") == "insert_and_erase_with_equal_or_lower_priority":
                    self.all_gauges_for_insert_and_erase_with_equal_or_lower_priority = True
                # end if

                if self.operation.get("mode") in ["insert", "insert_and_erase", "insert_and_erase_with_priority", "insert_and_erase_with_equal_or_lower_priority"]:
                    returned_value = self._insert_data(processing_duration = processing_duration)
                    returned_information = {
                        "source": self.operation.get("source").get("name"),
                        "dim_signature": self.operation.get("dim_signature").get("name"),
                        "processor": self.operation.get("dim_signature").get("exec"),
                        "status": returned_value
                    }
                    returned_values.append(returned_information)
                # end if
            # end def
            treat_operation_data(self, processing_duration, returned_values)
        # end for

        # Insert event links at the end of the process to allow having references to events in DIMs which are going to be inserted later
        # Associate any error to the source of the last operation
        self.session.begin_nested()        
        try:
            self._insert_event_links()
        except LinksInconsistency as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["LINKS_INCONSISTENCY"]["status"], error = True, message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            # Log the error
            logger.error(e)
            returned_information = {
                "source": self.operation.get("source").get("name"),
                "dim_signature": self.operation.get("dim_signature").get("name"),
                "processor": self.operation.get("dim_signature").get("exec"),
                "status": exit_codes["LINKS_INCONSISTENCY"]["status"]
            }
            returned_values.append(returned_information)
        except UndefinedEventLink as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["UNDEFINED_EVENT_LINK_REF"]["status"], error = True, message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            # Log the error
            logger.error(e)
            returned_information = {
                "source": self.operation.get("source").get("name"),
                "dim_signature": self.operation.get("dim_signature").get("name"),
                "processor": self.operation.get("dim_signature").get("exec"),
                "status": exit_codes["UNDEFINED_EVENT_LINK_REF"]["status"]
            }
            returned_values.append(returned_information)
        # end try

        # Close nested operations and commit
        self.session.commit()
        self.session.commit()
        
        return returned_values

    def _initialize_context_treat_data(self):
        # Initialize context
        self.event_link_refs = {}
        self.list_event_links_by_ref = []
        self.list_event_links_by_uuid = []
        self.list_event_links_to_check_by_event_uuid_link = []
        self.dict_event_uuids_aliases = {}

        return

    def _initialize_context_insert_data(self):
        # Initialize context
        self.dim_signature = None
        self.source = None
        self.gauges = {}
        self.annotation_cnfs = {}
        self.expl_groups = {}
        self.explicit_refs = {}
        self.insert_and_erase_gauges = {}
        self.insert_and_erase_per_event_gauges = {}
        self.insert_and_erase_with_priority_gauges = {}
        self.insert_and_erase_with_equal_or_lower_priority_gauges = {}
        self.insert_and_erase_intersected_events_with_priority_gauges = {}
        self.insert_and_erase_per_event_with_priority_gauges = {}
        self.annotation_cnfs_explicit_refs = []
        self.annotation_cnfs_explicit_refs_insert_and_erase_with_priority = []
        self.annotations = {}
        self.keys_events = {}
        self.keys_events_with_priority = {}
        self.alert_cnfs = {}
        self.alert_groups = {}

        # This is not necessarily needed, just here to avoid a lot of modifications on tests
        if not hasattr(self, "event_link_refs"):
            self.event_link_refs = {}
        # end if
        if not hasattr(self, "list_event_links_by_ref"):
            self.list_event_links_by_ref = []
        # end if
        if not hasattr(self, "list_event_links_by_uuid"):
            self.list_event_links_by_uuid = []
        # end if
        if not hasattr(self, "list_event_links_to_check_by_event_uuid_link"):
            self.list_event_links_to_check_by_event_uuid_link = []
        # end if
        if not hasattr(self, "dict_event_uuids_aliases"):
            self.dict_event_uuids_aliases = {}
        # end if

        return

    @debug
    def _insert_data(self, processing_duration = None):
        """
        Method to insert the data into the DDBB for an operation of mode insert
        :param processing_duration: duration of the processing which generated the data to be treated
        :type processing_duration: datetime.timedelta
        """
        # Initialize context
        self._initialize_context_insert_data()
        
        # Insert the DIM signature
        self._insert_dim_signature()

        # Insert source
        try:
            self._insert_source(processing_duration = processing_duration)
            self.ingestion_start = datetime.datetime.now()
            log_info = exit_codes["INGESTION_STARTED"]["message"].format(
                self.source.name,
                self.dim_signature.dim_signature,
                self.source.processor, 
                self.source.processor_version)
            self._insert_source_status(exit_codes["INGESTION_STARTED"]["status"],
                                       message = log_info)
            # Log that the ingestion of the source file has been started
            logger.info(log_info)
        except SourceAlreadyIngested as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["SOURCE_ALREADY_INGESTED"]["status"], message = str(e))
            # Log that the source file has been already been processed
            logger.error(e)
            self.session.commit()
            return exit_codes["SOURCE_ALREADY_INGESTED"]["status"]
        except WrongPeriod as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["WRONG_SOURCE_PERIOD"]["status"], error = True, message = str(e))
            # Log that the source file has a wrong specified period as the stop is lower than the start
            logger.error(e)
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            return exit_codes["WRONG_SOURCE_PERIOD"]["status"]
        except PriorityNotDefined as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["PRIORITY_NOT_DEFINED"]["status"], error = True, message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            # Log the error
            logger.error(e)
            return exit_codes["PRIORITY_NOT_DEFINED"]["status"]
        # end try

        # Get the general source entry (processor = None, version = None, DIM signature = PENDING_SOURCES)
        # This is when using the command eboa_triggering.py for ingestion control purposes
        self.general_source_progress = self.session_progress.query(Source).join(DimSignature).filter(Source.name == self.operation.get("source").get("name"),
                                                                                  DimSignature.dim_signature == "PENDING_SOURCES",
                                                                                  Source.processor_version == "",
                                                                                  Source.processor == "").first()

        if not self.general_source_progress:
            self.general_source_progress = self.source_progress
        # end if

        if not self.operation.get("source").get("ingested") == "false":
            self.general_source_progress.processor_progress = 100
        # end if

        self._insert_ingestion_progress(10)
        
        # Insert gauges
        self._insert_gauges()

        logger.debug("Gauges inserted for the source file {} associated to the DIM signature {} and DIM processing {} with version {}".format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version))

        self._insert_ingestion_progress(15)
        
        # Insert annotation configuration
        self._insert_annotation_cnfs()

        self._insert_ingestion_progress(20)

        logger.debug("Annotation configurations inserted for the source file {} associated to the DIM signature {} and DIM processing {} with version {}".format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version))
        
        # Insert explicit reference groups
        self._insert_expl_groups()

        self._insert_ingestion_progress(25)

        logger.debug("Explicit reference groups inserted for the source file {} associated to the DIM signature {} and DIM processing {} with version {}".format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version))
        
        # Insert explicit references
        self._insert_explicit_refs()

        self._insert_ingestion_progress(30)

        logger.debug("Explicit references inserted for the source file {} associated to the DIM signature {} and DIM processing {} with version {}".format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version))
        
        # Insert links between explicit references
        self._insert_links_explicit_refs()

        self._insert_ingestion_progress(35)

        logger.debug("Explicit reference links inserted for the source file {} associated to the DIM signature {} and DIM processing {} with version {}".format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version))

        # Insert alert groups
        self._insert_alert_groups()

        self._insert_ingestion_progress(40)

        logger.debug("Alert groups inserted for the source file {} associated to the DIM signature {} and DIM processing {} with version {}".format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version))
        
        # Insert alert configuration
        self._insert_alert_cnfs()

        self._insert_ingestion_progress(45)

        logger.debug("Alert configurations inserted for the source file {} associated to the DIM signature {} and DIM processing {} with version {}".format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version))
        
        self.session.begin_nested()
        # Insert events
        try:
            self._insert_events()
        except DuplicatedEventLinkRef as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["DUPLICATED_EVENT_LINK_REF"]["status"], error = True, message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            # Log the error
            logger.error(e)
            return exit_codes["DUPLICATED_EVENT_LINK_REF"]["status"]
        except WrongPeriod as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["WRONG_EVENT_PERIOD"]["status"], error = True, message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            # Log the error
            logger.error(e)
            return exit_codes["WRONG_EVENT_PERIOD"]["status"]
        except WrongValue as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["WRONG_VALUE"]["status"], error = True, message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            # Log the error
            logger.error(e)
            return exit_codes["WRONG_VALUE"]["status"]
        except OddNumberOfCoordinates as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["ODD_NUMBER_OF_COORDINATES"]["status"], error = True, message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            # Log the error
            logger.error(e)
            return exit_codes["ODD_NUMBER_OF_COORDINATES"]["status"]
        except WrongGeometry as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["WRONG_GEOMETRY"]["status"], error = True, message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            # Log the error
            logger.error(e)
            return exit_codes["WRONG_GEOMETRY"]["status"]
        except DuplicatedValues as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["DUPLICATED_VALUES"]["status"], error = True, message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            # Log the error
            logger.error(e)
            return exit_codes["DUPLICATED_VALUES"]["status"]
        except PriorityNotDefined as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["PRIORITY_NOT_DEFINED"]["status"], error = True, message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            # Log the error
            logger.error(e)
            return exit_codes["PRIORITY_NOT_DEFINED"]["status"]
        # end try

        self._insert_ingestion_progress(65)

        logger.debug("Events inserted for the source file {} associated to the DIM signature {} and DIM processing {} with version {}".format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version))
        
        # Insert annotations
        try:
            self._insert_annotations()
        except WrongValue as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["WRONG_VALUE"]["status"], error = True, message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            # Log the error
            logger.error(e)
            return exit_codes["WRONG_VALUE"]["status"]
        except OddNumberOfCoordinates as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["ODD_NUMBER_OF_COORDINATES"]["status"], error = True, message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            # Log the error
            logger.error(e)
            return exit_codes["ODD_NUMBER_OF_COORDINATES"]["status"]
        except WrongGeometry as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["WRONG_GEOMETRY"]["status"], error = True, message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            # Log the error
            logger.error(e)
            return exit_codes["WRONG_GEOMETRY"]["status"]
        except DuplicatedValues as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["DUPLICATED_VALUES"]["status"], error = True, message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            # Log the error
            logger.error(e)
            return exit_codes["DUPLICATED_VALUES"]["status"]
        except PriorityNotDefined as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["PRIORITY_NOT_DEFINED"]["status"], error = True, message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            # Log the error
            logger.error(e)
            return exit_codes["PRIORITY_NOT_DEFINED"]["status"]
        # end try

        logger.debug("Annotations inserted for the source file {} associated to the DIM signature {} and DIM processing {} with version {}".format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version))

        self._insert_ingestion_progress(80)

        # Insert alerts
        try:
            self._insert_alerts()
        except UndefinedEntityReference as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["UNDEFINED_ENTITY_REF"]["status"], error = True, message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            # Log the error
            logger.error(e)
            return exit_codes["UNDEFINED_ENTITY_REF"]["status"]
        # end try

        self._insert_ingestion_progress(85)

        logger.debug("Alerts inserted for the source file {} associated to the DIM signature {} and DIM processing {} with version {}".format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version))

        # At this point all the information has been inserted, commit data twice as there was a begin nested initiated
        self.session.commit()
        self.session.commit()
        self.session.commit()

        # Review the inserted events and annotations for removing the
        # information that is deprecated
        self._remove_deprecated_data()

        self._insert_ingestion_progress(90)

        logger.debug("Deprecated data removed for the source file {} associated to the DIM signature {} and DIM processing {} with version {}".format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version))
        
        n_events = 0
        n_event_alerts = 0
        if "events" in self.operation:
            events = self.operation.get("events")
            n_events = len(events)

            # Get the alerts associated to the events
            for event in events:
                if "alerts" in event:
                    event_alerts = [alert for alert in event.get("alerts")]
                    n_event_alerts = n_event_alerts + len(event_alerts)
                # end if
            # end for
        # end if

        n_annotations = 0
        n_annotation_alerts = 0
        if "annotations" in self.operation:
            annotations = self.operation.get("annotations")
            n_annotations = len(annotations)

            # Get the alerts associated to the annotations
            for annotation in annotations:
                if "alerts" in annotation:
                    annotation_alerts = [alert for alert in annotation.get("alerts")]
                    n_annotation_alerts = n_annotation_alerts + len(annotation_alerts)
                # end if
            # end for
        # end if

        n_explicit_references = 0
        n_explicit_reference_alerts = 0
        if "explicit_references" in self.operation:
            explicit_references = self.operation.get("explicit_references")
            n_explicit_references = len(explicit_references)

            # Get the alerts associated to the explicit_references
            for explicit_reference in explicit_references:
                if "alerts" in explicit_reference:
                    explicit_reference_alerts = [alert for alert in explicit_reference.get("alerts")]
                    n_explicit_reference_alerts = n_explicit_reference_alerts + len(explicit_reference_alerts)
                # end if
            # end for
        # end if

        n_source_alerts = 0        
        if "source" in self.operation:
            source = self.operation.get("source")

            # Get the alerts associated to the sources
            if "alerts" in source:
                source_alerts = [alert for alert in source.get("alerts")]
                n_source_alerts = len(source_alerts)
            # end if
        # end if

        n_alerts = n_event_alerts + n_annotation_alerts + n_explicit_reference_alerts + n_source_alerts
        if "alerts" in self.operation:
            n_alerts = n_alerts + len(self.operation.get("alerts"))
        # end if

        self._insert_ingestion_progress(100)

        # Log that the file has been ingested correctly
        log = exit_codes["OK"]["message"].format(
            self.source.name,
            self.dim_signature.dim_signature,
            self.source.processor, 
            self.source.processor_version,
            n_events,
            n_annotations,
            n_alerts)
        self._insert_source_status(exit_codes["OK"]["status"],True, message = log)
        logger.info(log)

        # Remove if the content was inserted due to errors processing the input
        self.source.content_json = None
        
        # Commit data
        self.session.commit()

        return exit_codes["OK"]["status"]

    def _bulk_insert_mappings(self, entity, mappings):
        self.session.begin_nested()
        try:
            self.session.bulk_insert_mappings(entity, mappings)
        except IntegrityError:
            self.session.rollback()
            for mapping in mappings:
                self.session.begin_nested()
                try:
                    self.session.bulk_insert_mappings(entity, [mapping])
                except IntegrityError as e:
                    logger.info("Mapping ##{}## is skipped because the following exception was raised ##{}##".format(mapping, str(e)))
                    self.session.rollback()
                # end try
                self.session.commit()
            # end for
        # end try
        self.session.commit()
    # end def

    @debug
    def _insert_dim_signature(self):
        """
        Method to insert the DIM signature
        """
        dim_signature = self.operation.get("dim_signature")
        dim_name = dim_signature.get("name")
        self.dim_signature = self.session.query(DimSignature).filter(DimSignature.dim_signature == dim_name).first()
        if not self.dim_signature:
            id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
            self.dim_signature = DimSignature(id, dim_name)
            self.session.add(self.dim_signature)
            try:
                race_condition()
                self.session.commit()
            except IntegrityError:
                # The DIM signature has been inserted between the
                # query and the insertion. Roll back transaction for
                # re-using the session
                self.session.rollback()
                # Get the stored DIM signature
                self.dim_signature = self.session.query(DimSignature).filter(DimSignature.dim_signature == dim_name).first()
                pass
            # end try
        # end if
        
        return

    def _insert_ingestion_progress(self, progress):
        """
        Method to insert the ingestion progress of a source
        :param progress: value of progress
        :type progress: float
        """
        if not self.operation.get("source").get("ingested") == "false":
            self.source_progress.ingestion_progress = progress
            self.general_source_progress.ingestion_progress = progress
            self.session_progress.commit()
        # end if

        return
    
    @debug
    def _insert_source(self, processing_duration = None):
        """
        Method to insert the DIM processing
        :param processing_duration: duration of the processing which generated the data to be treated
        :type processing_duration: interval
        """
        # Obtain all attributes of the source
        version = self.operation.get("dim_signature").get("version")
        processor = self.operation.get("dim_signature").get("exec")
        operation_mode = self.operation.get("mode")
        source = self.operation.get("source")
        name = source.get("name")
        reception_time = source.get("reception_time")
        generation_time = source.get("generation_time")
        reported_generation_time = source.get("reported_generation_time")
        if not reported_generation_time:
            reported_generation_time = generation_time
        # end if
        validity_start = source.get("validity_start")
        validity_stop = source.get("validity_stop")
        reported_validity_start = source.get("reported_validity_start")
        if not reported_validity_start:
            reported_validity_start = validity_start
        # end if
        reported_validity_stop = source.get("reported_validity_stop")
        if not reported_validity_stop:
            reported_validity_stop = validity_stop
        # end if
        processing_duration_from_input_data = source.get("processing_duration")
        priority = source.get("priority")
        ingestion_completeness = source.get("ingestion_completeness")
        ingestion_completeness_check = True
        ingestion_completeness_message = None
        if ingestion_completeness:
            ingestion_completeness_check = bool(util.strtobool(ingestion_completeness.get("check")))
            ingestion_completeness_message = ingestion_completeness.get("message")
        # end if
        
        if processing_duration_from_input_data:
            processing_duration = datetime.timedelta(seconds=float(processing_duration_from_input_data))
        # end if

        # Set the processor progress to 100 if the flag ingested is not set to false
        processor_progress = None
        if not self.operation.get("source").get("ingested") == "false":
            processor_progress = 100
        # end if
        
        id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        if parser.parse(validity_stop).replace(tzinfo=None) < parser.parse(validity_start).replace(tzinfo=None) or parser.parse(reported_validity_stop).replace(tzinfo=None) < parser.parse(reported_validity_start).replace(tzinfo=None):
            # The validity period is not correct (stop > start)
            # Create Source for registering the error in the DDBB
            self.source = Source(id, name, reception_time, generation_time,
                                 version, self.dim_signature, processor = processor,
                                 processing_duration = processing_duration, processor_progress = processor_progress,
                                 reported_generation_time = reported_generation_time)
            self.session.add(self.source)
            try:
                race_condition()
                self.session.commit()
            except IntegrityError:
                # The DIM processing was already ingested
                self.session.rollback()
                self.source = self.session.query(Source).filter(Source.name == name,
                                                                       Source.dim_signature_uuid == self.dim_signature.dim_signature_uuid,
                                                                       Source.processor_version == version,
                Source.processor == processor).first()
            # end try
            if parser.parse(validity_stop).replace(tzinfo=None) < parser.parse(validity_start).replace(tzinfo=None):
                raise WrongPeriod(exit_codes["WRONG_SOURCE_PERIOD"]["message"].format(name, self.dim_signature.dim_signature, processor, version, validity_stop, validity_start))
            else:
                raise WrongReportedValidityPeriod(exit_codes["WRONG_SOURCE_REPORTED_VALIDITY_PERIOD"]["message"].format(name, self.dim_signature.dim_signature, processor, version, reported_validity_stop, reported_validity_start))
            # end if
        # end if

        self.source = self.session.query(Source).filter(Source.name == name,
                                                               Source.dim_signature_uuid == self.dim_signature.dim_signature_uuid,
                                                               Source.processor_version == version,
                Source.processor == processor).first()
        if self.source and self.source.ingested:
            # The source has been already ingested
            raise SourceAlreadyIngested(exit_codes["SOURCE_ALREADY_INGESTED"]["message"].format(name,
                                                                                                     self.dim_signature.dim_signature,
                                                                                                     processor, 
                                                                                                     version))
        elif self.source:
            # Source available in DDBB but with flag ingested equal to False. Upadte the information
            self.source.validity_start = parser.parse(validity_start)
            self.source.validity_stop = parser.parse(validity_stop)
            self.source.reported_validity_start = parser.parse(reported_validity_start)
            self.source.reported_validity_stop = parser.parse(reported_validity_stop)
            self.source.reception_time = parser.parse(reception_time)
            self.source.generation_time = parser.parse(generation_time)
            self.source.reported_generation_time = parser.parse(reported_generation_time)
            self.source.processing_duration = processing_duration
            self.source.priority = priority
            self.source.ingestion_completeness = ingestion_completeness_check
            self.source.ingestion_completeness_message = ingestion_completeness_message
        else:
            # Source not available in DDBB. Insert the information
            self.source = Source(id, name, parser.parse(reception_time),
                                 parser.parse(generation_time), version, self.dim_signature,
                                 parser.parse(validity_start), parser.parse(validity_stop),
                                 processor = processor, processing_duration = processing_duration,
                                 processor_progress = processor_progress, reported_generation_time = reported_generation_time,
                                 reported_validity_start = reported_validity_start,
                                 reported_validity_stop = reported_validity_stop, priority = priority,
                                 ingestion_completeness = ingestion_completeness_check, ingestion_completeness_message = ingestion_completeness_message)
            self.session.add(self.source)
            try:
                race_condition()
                self.session.commit()
            except IntegrityError:
                # The source has been ingested between the query and the insertion
                self.session.rollback()
                self.source = self.session.query(Source).filter(Source.name == name,
                                                                       Source.dim_signature_uuid == self.dim_signature.dim_signature_uuid,
                                                                       Source.processor_version == version,
                    Source.processor == processor).first()
                raise SourceAlreadyIngested(exit_codes["SOURCE_ALREADY_INGESTED"]["message"].format(name,
                                                                  self.dim_signature.dim_signature,
                                                                  processor, 
                                                                  version))
            # end try
        # end if

        # Check if the source has to define the priority
        if operation_mode in ["insert_and_erase_with_priority", "insert_and_erase_with_equal_or_lower_priority"] and not self.source.priority:
            raise PriorityNotDefined(exit_codes["PRIORITY_NOT_DEFINED"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version))
        # end if
            
        
        self.source_progress = self.session_progress.query(Source).filter(Source.name == name,
                                                        Source.dim_signature_uuid == self.dim_signature.dim_signature_uuid,
                                                        Source.processor_version == version,
                                                        Source.processor == processor).first()
        
        list_alerts = []
        
        # Manage alerts
        if "alerts" in source:
            alert_groups = insert_alert_groups(self.session, source)
            alert_cnfs = insert_alert_cnfs(self.session, source, alert_groups)
            for alert in source["alerts"]:
                alert_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                alert_cnf = alert_cnfs[alert.get("alert_cnf").get("name")]
                kwargs = {}
                kwargs["message"] = alert.get("message")
                kwargs["ingestion_time"] = datetime.datetime.now()
                kwargs["generator"] = alert.get("generator")
                kwargs["notification_time"] = alert.get("notification_time")
                kwargs["alert_uuid"] = alert_cnf.alert_uuid
                kwargs["source_alert_uuid"] = alert_uuid
                kwargs["source_uuid"] = self.source.source_uuid
                list_alerts.append(dict(kwargs))
            # end for
        # end if

        # Bulk insert alerts
        if len(list_alerts) > 0:
            self._bulk_insert_mappings(SourceAlert, list_alerts)
        # end if

        return

    def _insert_source_without_dim_signature(self, name):
        """
        Method to insert the DIM processing but without an associated dim_signature

        :param name: name of the DIM processing
        :type name: str
        """
        id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        self.source = self.session.query(Source).filter(Source.name == name).first()
        if not self.source:
            self.source = Source(id, name, datetime.datetime.now().isoformat())
            self.session.add(self.source)
            # If there is a race condition here the eboa will insert a
            # new row with the same name as the unique constraint is
            # not violated with NULL values in the associated columns
            # (not really important as this is a function for
            # registering the wrong usage of the API)
            self.session.commit()
        # end if

        return
        
    @debug
    def _insert_gauges(self):
        """
        Method to insert the gauges
        """
        description = None
        gauges = [(event.get("gauge").get("name"), event.get("gauge").get("system"), event.get("gauge").get("description"))  for event in self.operation.get("events") or []]
        unique_gauges = sorted(set(gauges))
        for gauge in unique_gauges:
            name = gauge[0]
            system = gauge[1]
            description = gauge[2]
            self.gauges[(name,system)] = self.session.query(Gauge).filter(Gauge.name == name, Gauge.system == system, Gauge.dim_signature_uuid == self.dim_signature.dim_signature_uuid).first()
            if not self.gauges[(name,system)]:
                self.session.begin_nested()
                id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                self.gauges[(name,system)] = Gauge(id, name, self.dim_signature, system, description)
                self.session.add(self.gauges[(name,system)])
                try:
                    race_condition()
                    self.session.commit()
                except IntegrityError:
                    # The gauge has been inserted between the query and the insertion. Roll back transaction for
                    # re-using the session
                    self.session.rollback()
                    # Get the stored gauge
                    self.gauges[(name,system)] = self.session.query(Gauge).filter(Gauge.name == name, Gauge.system == system, Gauge.dim_signature_uuid == self.dim_signature.dim_signature_uuid).first()
                    pass
                # end try
            # end if
        # end for
        return

    @debug        
    def _insert_annotation_cnfs(self):
        """
        Method to insert the annotation configurations
        """
        annotation_cnfs = [(annotation.get("annotation_cnf").get("name"), annotation.get("annotation_cnf").get("system"), annotation.get("annotation_cnf").get("description"))  for annotation in self.operation.get("annotations") or []]
        unique_annotation_cnfs = sorted(set(annotation_cnfs))
        for annotation in unique_annotation_cnfs:
            name = annotation[0]
            system = annotation[1]
            description = annotation[2]
            self.annotation_cnfs[(name,system)] = self.session.query(AnnotationCnf).filter(AnnotationCnf.name == name, AnnotationCnf.system == system, AnnotationCnf.dim_signature_uuid == self.dim_signature.dim_signature_uuid).first()
            if not self.annotation_cnfs[(name,system)]:
                self.session.begin_nested()
                id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                self.annotation_cnfs[(name,system)] = AnnotationCnf(id, name, self.dim_signature, system, description)
                self.session.add(self.annotation_cnfs[(name,system)])
                try:
                    race_condition()
                    self.session.commit()
                except IntegrityError:
                    # The annotation has been inserted between the query and the insertion. Roll back transaction for
                    # re-using the session
                    self.session.rollback()
                    # Get the stored annotation configuration
                    self.annotation_cnfs[(name,system)] = self.session.query(AnnotationCnf).filter(AnnotationCnf.name == name, AnnotationCnf.system == system, AnnotationCnf.dim_signature_uuid == self.dim_signature.dim_signature_uuid).first()
                    pass
                # end try
            # end if
        # end for

        return

    @debug
    def _insert_expl_groups(self):
        """
        Method to insert the groups of explicit references
        """
        explicit_ref_groups = [explicit_ref.get("group") for explicit_ref in self.operation.get("explicit_references") or [] if explicit_ref.get("group")]
        unique_explicit_ref_groups = sorted(set(explicit_ref_groups))
        
        for explicit_ref_group in unique_explicit_ref_groups or []:
            self.session.begin_nested()
            id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
            expl_group_ddbb = ExplicitRefGrp(id, explicit_ref_group)
            self.session.add(expl_group_ddbb)
            try:
                race_condition()
                self.session.commit()
            except IntegrityError:
                # The explicit reference group exists already into DDBB
                self.session.rollback()
                expl_group_ddbb = self.session.query(ExplicitRefGrp).filter(ExplicitRefGrp.name == explicit_ref_group).first()
                pass
            # end try
            self.expl_groups[explicit_ref_group] = expl_group_ddbb
        # end for

        return

    @debug
    def _insert_explicit_refs(self):
        """
        Method to insert the explicit references
        """

        # Join all sources of explicit references
        events_explicit_refs = [event.get("explicit_reference") for event in self.operation.get("events") or [] if event.get("explicit_reference")]
        annotations_explicit_refs = [annotation.get("explicit_reference") for annotation in self.operation.get("annotations") or [] if annotation.get("explicit_reference")]
        declared_explicit_refs = [i.get("name") for i in self.operation.get("explicit_references") or []]
        linked_explicit_refs = [link.get("link") for explicit_ref in self.operation.get("explicit_references") or [] if explicit_ref.get("links") for link in explicit_ref.get("links")]
        explicit_references = sorted(set(events_explicit_refs + annotations_explicit_refs + declared_explicit_refs + linked_explicit_refs))

        for explicit_ref in explicit_references:
            self.explicit_refs[explicit_ref] = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == explicit_ref).first()

            explicit_ref_grp = None
            # Get associated group if exists from the declared explicit references
            declared_explicit_reference = next(iter([i for i in self.operation.get("explicit_references") or [] if i.get("name") == explicit_ref]), None)
            if declared_explicit_reference:
                explicit_ref_grp = self.expl_groups.get(declared_explicit_reference.get("group"))
            # end if
            
            if not self.explicit_refs[explicit_ref]:
                self.session.begin_nested()
                id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                self.explicit_refs[explicit_ref] = ExplicitRef(id, datetime.datetime.now(), explicit_ref, explicit_ref_grp)
                self.session.add(self.explicit_refs[explicit_ref])
                try:
                    race_condition()
                    self.session.commit()
                except IntegrityError:
                    # The explicit reference has been inserted between the query and the insertion. Roll back transaction for
                    # re-using the session
                    self.session.rollback()
                    self.explicit_refs[explicit_ref] = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == explicit_ref).first()
                    pass
                # end try
            # end if


            # Update group information if available (this is to reduce the effort at ingestion level of synchronizing groups.
            # The ingestion developer has to take into account using always the same groups accross ingestion
            # processors for the same explicit references to avoid clashes)
            if explicit_ref_grp != None:
                self.explicit_refs[explicit_ref].group = explicit_ref_grp
            # end if
        # end for

        # Manage alerts
        list_alerts = []
        explicit_refs_with_alerts = [explicit_ref for explicit_ref in self.operation.get("explicit_references") or [] if "alerts" in explicit_ref]
        for explicit_ref in explicit_refs_with_alerts:
                alert_groups = insert_alert_groups(self.session, explicit_ref)
                alert_cnfs = insert_alert_cnfs(self.session, explicit_ref, alert_groups)
                for alert in explicit_ref["alerts"]:
                    alert_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                    alert_cnf = alert_cnfs[alert.get("alert_cnf").get("name")]
                    kwargs = {}
                    kwargs["message"] = alert.get("message")
                    kwargs["ingestion_time"] = datetime.datetime.now()
                    kwargs["generator"] = alert.get("generator")
                    kwargs["notification_time"] = alert.get("notification_time")
                    kwargs["alert_uuid"] = alert_cnf.alert_uuid
                    kwargs["explicit_ref_alert_uuid"] = alert_uuid
                    kwargs["explicit_ref_uuid"] = id
                    list_alerts.append(dict(kwargs))
                # end for
            # end if
        # end for
        # Bulk insert alerts
        if len(list_alerts) > 0:
            self._bulk_insert_mappings(ExplicitRefAlert, list_alerts)
        # end if

        return
        
    @debug
    def _insert_links_explicit_refs(self):
        """
        Method to insert the links between explicit references
        """
        list_explicit_reference_links = []
        for explicit_ref in [i for i in self.operation.get("explicit_references") or [] if i.get("links")]:
            for link in explicit_ref.get("links") or []:
                list_explicit_reference_links.append((explicit_ref.get("name"), link.get("link"), link.get("name")))
                
                # Insert the back ref if specified
                if link.get("back_ref"):
                    list_explicit_reference_links.append((link.get("link"), explicit_ref.get("name"), link.get("back_ref")))
                # end if
            # end for
        # end for

        unique_sorted_links = sorted(set(list_explicit_reference_links))
        
        for link in unique_sorted_links:
            explicit_ref1 = self.explicit_refs[link[0]]
            explicit_ref2 = self.explicit_refs[link[1]]
            link_name = link[2]
            link_ddbb = self.session.query(ExplicitRefLink).filter(ExplicitRefLink.explicit_ref_uuid_link == explicit_ref1.explicit_ref_uuid, ExplicitRefLink.name == link_name, ExplicitRefLink.explicit_ref_uuid == explicit_ref2.explicit_ref_uuid).first()
            if not link_ddbb:
                self.session.begin_nested()
                self.session.add(ExplicitRefLink(explicit_ref1.explicit_ref_uuid, link_name, explicit_ref2))
                try:
                    race_condition()
                    self.session.commit()
                except IntegrityError:
                    # The link exists already into DDBB
                    self.session.rollback()
                    pass
                # end try
            # end if
        # end for
        
        return

    def _insert_event(self, list_events, id, start, stop, gauge_uuid, explicit_ref_uuid, visible, source = None, source_id = None):
        """
        Method to insert an event

        :param list_events: list to add the created events for later buld ingestion
        :type list_events: list
        :param id: identifier of the event
        :type id: uuid
        :param start: datetime
        :type start: start date of the period of the event
        :param stop: datetime
        :type stop: stop date of the period of the event
        :param gauge_uuid: reference to the associated gauge
        :type gauge_uuid: int
        :param explicit_ref_uuid: identifier of the associated explicit reference
        :type explicit_ref_uuid: int
        :param visible: indicator of the visibility of the event
        :type visible: bool
        :param source: associated DIM processing (default None). Provide this parameter or source_id
        :type source: sqlalchemy object
        :param source_id: identifier of the associated DIM processing (default None). Provide this parameter or source
        :type source_id: uuid
        """
        if not type(start) == datetime.datetime:
            start = parser.parse(start).replace(tzinfo=None)
        # end if
        if not type(stop) == datetime.datetime:
            stop = parser.parse(stop).replace(tzinfo=None)
        # end if
        if stop < start:
            # The period of the event is not correct (stop > start)
            self.session.rollback()
            raise WrongPeriod(exit_codes["WRONG_EVENT_PERIOD"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version, stop, start))
        # end if
        
        if source == None:
            source = self.session.query(Source).filter(Source.source_uuid == source_id).first()
        # end if

        source_start = source.validity_start
        source_stop = source.validity_stop
        if start < source_start or stop > source_stop:
            # The period of the event is not inside the validity period of the input
            self.session.rollback()
            raise WrongPeriod(exit_codes["EVENT_PERIOD_NOT_IN_SOURCE_PERIOD"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version, start, stop, source_start, source_stop))
        # end if
            
        list_events.append(dict(event_uuid = id, start = start, stop = stop,
                                ingestion_time = datetime.datetime.now(),
                                gauge_uuid = gauge_uuid,
                                explicit_ref_uuid = explicit_ref_uuid,
                                source_uuid = source.source_uuid,
                                visible = visible))
        return

    @debug
    def _insert_events(self):
        """
        Method to insert the events
        """
        list_events = []
        list_keys = []
        list_alerts = []
        
        list_values = {}
        for event in self.operation.get("events") or []:
            id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
            self.dict_event_uuids_aliases[id] = []
            start = event.get("start")
            stop = event.get("stop")
            gauge_info = event.get("gauge")
            gauge = self.gauges[(gauge_info.get("name"), gauge_info.get("system"))]
            explicit_ref = None
            if event.get("explicit_reference") in self.explicit_refs:
                explicit_ref = self.explicit_refs[event.get("explicit_reference")]
            # end if
            key = event.get("key")
            visible = False
            if gauge_info["insertion_type"] in ["INSERT_and_ERASE_with_PRIORITY", "INSERT_and_ERASE_with_EQUAL_or_LOWER_PRIORITY", "INSERT_and_ERASE_INTERSECTED_EVENTS_with_PRIORITY", "INSERT_and_ERASE_per_EVENT_with_PRIORITY", "EVENT_KEYS_with_PRIORITY"] and not self.source.priority:
                raise PriorityNotDefined(exit_codes["PRIORITY_NOT_DEFINED"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version))
            # end if
            if gauge_info["insertion_type"] == "SIMPLE_UPDATE":
                visible = True
            elif gauge_info["insertion_type"] == "INSERT_and_ERASE":
                self.insert_and_erase_gauges[gauge.gauge_uuid] = None
            elif gauge_info["insertion_type"] == "INSERT_and_ERASE_per_EVENT":
                if gauge.gauge_uuid not in self.insert_and_erase_per_event_gauges:
                    # Initialize the list of segments to be reviewed
                    self.insert_and_erase_per_event_gauges[gauge.gauge_uuid] = []
                # end if
                self.insert_and_erase_per_event_gauges[gauge.gauge_uuid].append((start, stop))
            elif gauge_info["insertion_type"] == "INSERT_and_ERASE_with_PRIORITY":
                self.insert_and_erase_with_priority_gauges[gauge.gauge_uuid] = None
            elif gauge_info["insertion_type"] == "INSERT_and_ERASE_with_EQUAL_or_LOWER_PRIORITY":
                self.insert_and_erase_with_equal_or_lower_priority_gauges[gauge.gauge_uuid] = None
            elif gauge_info["insertion_type"] == "INSERT_and_ERASE_INTERSECTED_EVENTS_with_PRIORITY":
                self.insert_and_erase_intersected_events_with_priority_gauges[gauge.gauge_uuid] = None
            elif gauge_info["insertion_type"] == "INSERT_and_ERASE_per_EVENT_with_PRIORITY":
                if gauge.gauge_uuid not in self.insert_and_erase_per_event_with_priority_gauges:
                    # Initialize the list of segments to be reviewed
                    self.insert_and_erase_per_event_with_priority_gauges[gauge.gauge_uuid] = []
                # end if
                self.insert_and_erase_per_event_with_priority_gauges[gauge.gauge_uuid].append((start, stop))
            elif gauge_info["insertion_type"] == "EVENT_KEYS":
                self.keys_events[(key, str(self.dim_signature.dim_signature_uuid))] = None
            elif gauge_info["insertion_type"] == "EVENT_KEYS_with_PRIORITY":
                self.keys_events_with_priority[(key, str(self.dim_signature.dim_signature_uuid))] = None
            # end if
            explicit_ref_uuid = None
            if explicit_ref != None:
                explicit_ref_uuid = explicit_ref.explicit_ref_uuid
            # end if
            # Insert the event into the list for bulk ingestion
            self._insert_event(list_events, id, start, stop, gauge.gauge_uuid, explicit_ref_uuid,
                                    visible, source = self.source)

            # Insert the key into the list for bulk ingestion
            if key != None:
                list_keys.append(dict(event_key = key, event_uuid = id,
                                      visible = visible,
                                      dim_signature_uuid = self.dim_signature.dim_signature_uuid))
            # end if
            # Insert values
            if "values" in event:
                entity_uuid = {"name": "event_uuid",
                               "id": id
                }
                self._insert_values(event.get("values"), entity_uuid, list_values)
            # end if
            # Build links by reference for later ingestion
            if "links" in event:
                for link in event["links"]:
                    back_ref = False
                    back_ref_name = ""
                    if "back_ref" in link:
                        back_ref = True
                        back_ref_name = link["back_ref"]
                    # end if
                    
                    if link["link_mode"] == "by_ref":
                        self.list_event_links_by_ref.append((id,
                                                        link["name"],
                                                        link["link"],
                                                        back_ref,
                                                        back_ref_name))
                    else:
                        self.list_event_links_by_uuid.append((str(id),
                                                         link["name"],
                                                         str(link["link"])))
                        if back_ref:
                            self.list_event_links_by_uuid.append((str(link["link"]),
                                                             back_ref_name,
                                                             str(id)))
                        # end if
                    # end if
                # end for
            # end if
            if "link_ref" in event:
                if event["link_ref"] in self.event_link_refs:
                    # The same link identifier has been specified in more than one event
                    self.session.rollback()
                    raise DuplicatedEventLinkRef(exit_codes["DUPLICATED_EVENT_LINK_REF"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version, event["link_ref"]))
                # end if
                self.event_link_refs[event["link_ref"]] = id
            # end if

            # Manage alerts
            if "alerts" in event:
                alert_groups = insert_alert_groups(self.session, event)
                alert_cnfs = insert_alert_cnfs(self.session, event, alert_groups)
                for alert in event["alerts"]:
                    ####
                    # IMPORTANT NOTE: Remember to modify method
                    # _replicate_event_alerts when modifying the
                    # fields of the alerts
                    ####
                    alert_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                    alert_cnf = alert_cnfs[alert.get("alert_cnf").get("name")]
                    kwargs = {}
                    kwargs["message"] = alert.get("message")
                    kwargs["ingestion_time"] = datetime.datetime.now()
                    kwargs["generator"] = alert.get("generator")
                    kwargs["notification_time"] = alert.get("notification_time")
                    kwargs["alert_uuid"] = alert_cnf.alert_uuid
                    kwargs["event_alert_uuid"] = alert_uuid
                    kwargs["event_uuid"] = id
                    list_alerts.append(dict(kwargs))
                # end for
            # end if

        # end for

        # Bulk insert events
        self.session.bulk_insert_mappings(Event, list_events)
        # Bulk insert keys
        self.session.bulk_insert_mappings(EventKey, list_keys)

        # Bulk insert values
        try:
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
                try:
                    self.session.bulk_insert_mappings(EventGeometry, list_values["geometries"])
                except InternalError as e:
                    self.session.rollback()
                    raise WrongGeometry(exit_codes["WRONG_GEOMETRY"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version, e))
            # end if
        except IntegrityError as e:
            self.session.rollback()
            raise DuplicatedValues(exit_codes["DUPLICATED_VALUES"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version, e))
        # end try

        # Bulk insert alerts
        if len(list_alerts) > 0:
            self._bulk_insert_mappings(EventAlert, list_alerts)
        # end if
        
        return

    def _get_event_uuid_aliases(self, event_uuid):
        """
        Method to get the final UUIDs associated to a specific UUID
        :param event_uuid: event UUID to find the aliases
        :type event_uuid: UUID

        :return: event UUID aliases
        :rtype: list
        """

        event_uuid_aliases = []
        if event_uuid in self.dict_event_uuids_aliases and len(self.dict_event_uuids_aliases[event_uuid]) > 0:
            for event_uuid_alias in self.dict_event_uuids_aliases[event_uuid]:
                if event_uuid_alias in self.dict_event_uuids_aliases and len(self.dict_event_uuids_aliases[event_uuid_alias]) > 0:
                    event_uuid_aliases.extend(list(set(self._get_event_uuid_aliases(event_uuid_alias))))
                else:
                    event_uuid_aliases.append(event_uuid_alias)
                # end if
            # end for
        else:
            event_uuid_aliases.append(event_uuid)
        # end if
            
        return event_uuid_aliases
    
    @debug
    def _insert_event_links(self):
        """
        Method to insert the links between events
        """

        list_event_links_by_ref_sorted = []
        list_event_links_by_ref_ddbb = []
        list_event_links_by_uuid_sorted = []
        list_event_links_by_uuid_ddbb = []

        # Insert links by reference
        list_event_links_by_ref_sorted = sorted(set(self.list_event_links_by_ref))
        for link in list_event_links_by_ref_sorted:
            link_ref = link[2]
            if not link_ref in self.event_link_refs:
                # There has not been defined this link reference in any event
                self.session.rollback()
                raise UndefinedEventLink(exit_codes["UNDEFINED_EVENT_LINK_REF"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version, link_ref))
            # end if

            event_uuid_link = link[0]
            event_uuid_links = self._get_event_uuid_aliases(event_uuid_link)

            for event_uuid_link in event_uuid_links:
                link_name = link[1]
                event_uuid = self.event_link_refs[link_ref]
                event_uuids = self._get_event_uuid_aliases(event_uuid)

                for event_uuid in event_uuids:
                    self.list_event_links_to_check_by_event_uuid_link.append(str(event_uuid_link))
                    list_event_links_by_ref_ddbb.append(dict(event_uuid_link = event_uuid_link,
                                                             name = link_name,
                                                             event_uuid = event_uuid))
                    back_ref = link[3]
                    back_ref_name = link[4]
                    if back_ref:
                        self.list_event_links_to_check_by_event_uuid_link.append(str(event_uuid))
                        list_event_links_by_ref_ddbb.append(dict(event_uuid_link = event_uuid,
                                                                 name = back_ref_name,
                                                                 event_uuid = event_uuid_link))
                    # end if
                # end for
            # end for
        # end for

        list_event_links_by_uuid_sorted = sorted(set(self.list_event_links_by_uuid))
        for link in list_event_links_by_uuid_sorted:
            event_uuid_link = link[0]
            event_uuid_links = self._get_event_uuid_aliases(event_uuid_link)
            
            for event_uuid_link in event_uuid_links:
                link_name = link[1]
                event_uuid = link[2]
                event_uuids = self._get_event_uuid_aliases(event_uuid)

                for event_uuid in event_uuids:
                    self.list_event_links_to_check_by_event_uuid_link.append(str(event_uuid_link))

                    list_event_links_by_uuid_ddbb.append(dict(event_uuid_link = event_uuid_link,
                                                              name = link_name,
                                                              event_uuid = event_uuid))
                # end for
            # end for
        # end for

        # Bulk insert links
        list_event_links_ddbb = list_event_links_by_uuid_ddbb + list_event_links_by_ref_ddbb
        if len(list_event_links_ddbb) > 0:
            self.session.begin_nested()
            try:
                race_condition2()
                self.session.bulk_insert_mappings(EventLink, list_event_links_ddbb)
                self.session.commit()
            except IntegrityError as e:
                self.session.rollback()
                if "psycopg2.errors.ForeignKeyViolation" in str(e):
                    for link in list_event_links_ddbb:
                        self.session.begin_nested()
                        try:
                            self.session.bulk_insert_mappings(EventLink, [link])
                            self.session.commit()
                        except IntegrityError as e:
                            self.session.rollback()
                            if "psycopg2.errors.ForeignKeyViolation" in str(e):
                                logger.error(exit_codes["LINK_TO_NOT_AVAILABLE_EVENT"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version, link["event_uuid_link"], link["name"], link["event_uuid"], e))
                                pass
                            else:
                                self.session.rollback()
                                raise LinksInconsistency(exit_codes["LINKS_INCONSISTENCY"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version, e))
                            # end if
                        # end try
                else:
                    self.session.rollback()
                    raise LinksInconsistency(exit_codes["LINKS_INCONSISTENCY"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version, e))
                # end if
            # end try
        # end if

        # Review inserted links to check if some of them are obsolete (mainly the ones linking to the inserted events)
        if len(self.list_event_links_to_check_by_event_uuid_link) > 0:
            events_with_event_uuids_equal_to_event_uuid_links = self.session.query(Event).filter(Event.event_uuid.in_(self.list_event_links_to_check_by_event_uuid_link)).all()
            event_uuids_existing_in_ddbb = {str(event.event_uuid) for event in events_with_event_uuids_equal_to_event_uuid_links}
            event_uuid_links_to_be_removed = [event_uuid for event_uuid in self.list_event_links_to_check_by_event_uuid_link if event_uuid not in event_uuids_existing_in_ddbb]
            if len(event_uuid_links_to_be_removed) > 0:
                self.session.query(EventLink).filter(EventLink.event_uuid_link.in_(event_uuid_links_to_be_removed)).delete(synchronize_session=False)
            # end if
        # end if

        return
    
    def _insert_values(self, values, entity_uuid, list_values, position = 0, parent_level = -1, parent_position = 0, positions = None):
        """
        Method to insert the values associated to events or annotations

        :param values: list of values to be inserted
        :type values: list
        :param entity_uuid: identifier of the event or annotation
        :type entity_uuid: uuid
        :param list_values: list with the inserted values for later bulk ingestion
        :type list_values: list
        :param position: value position inside the structure of values
        :type position: int
        :param parent_level: level of the parent value inside the structure of values
        :type parent_level: int
        :param parent_position: position of the parent value inside the correspoding level of the structure of values
        :type parent_position: int
        :param positions: counter of the positions per level
        :type positions: dict
        """
        try:
            insert_values(values, entity_uuid, list_values, position, parent_level, parent_position, positions)
        except WrongValue as e:
            self.session.rollback()
            raise WrongValue(exit_codes["WRONG_VALUE"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version, str(e)))
        except OddNumberOfCoordinates as e:
            self.session.rollback()
            raise OddNumberOfCoordinates(exit_codes["ODD_NUMBER_OF_COORDINATES"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version, str(e)))
        # end try

        return
        
    @debug
    def _insert_annotations(self):
        """
        Method to insert the annotations
        """
        list_annotations = []
        list_alerts = []
        list_values = {}
        for annotation in self.operation.get("annotations") or []:
            id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
            annotation_cnf_info = annotation.get("annotation_cnf")
            annotation_cnf = self.annotation_cnfs[(annotation_cnf_info.get("name"), annotation_cnf_info.get("system"))]
            explicit_ref = self.explicit_refs[annotation.get("explicit_reference")]

            visible = True
            if "insertion_type" in annotation_cnf_info:
                if annotation_cnf_info["insertion_type"] == "INSERT_and_ERASE":
                    self.annotation_cnfs_explicit_refs.append({"explicit_ref": explicit_ref,
                                                           "annotation_cnf": annotation_cnf
                                                               })
                    visible = False
                elif annotation_cnf_info["insertion_type"] == "INSERT_and_ERASE_with_PRIORITY":
                    if not self.source.priority:
                        raise PriorityNotDefined(exit_codes["PRIORITY_NOT_DEFINED"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version))
                    # end if
                    self.annotation_cnfs_explicit_refs_insert_and_erase_with_priority.append({"explicit_ref": explicit_ref,
                                                               "annotation_cnf": annotation_cnf
                                                               })
                    visible = False
                # end if
            # end if

            # Insert the annotation into the list for bulk ingestion
            list_annotations.append(dict(annotation_uuid = id, ingestion_time = datetime.datetime.now(),
                                         annotation_cnf_uuid = annotation_cnf.annotation_cnf_uuid,
                                         explicit_ref_uuid = explicit_ref.explicit_ref_uuid,
                                         source_uuid = self.source.source_uuid,
                                         visible = visible))
            # Insert values
            if "values" in annotation:
                entity_uuid = {"name": "annotation_uuid",
                               "id": id
                }
                self._insert_values(annotation.get("values"), entity_uuid, list_values)
            # end if

            if not (explicit_ref, annotation_cnf.annotation_cnf_uuid) in self.annotations:
                self.annotations[(explicit_ref, annotation_cnf.annotation_cnf_uuid)] = None
            # end if

            # Manage alerts
            if "alerts" in annotation:
                alert_groups = insert_alert_groups(self.session, annotation)
                alert_cnfs = insert_alert_cnfs(self.session, annotation, alert_groups)
                for alert in annotation["alerts"]:
                    alert_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                    alert_cnf = alert_cnfs[alert.get("alert_cnf").get("name")]
                    kwargs = {}
                    kwargs["message"] = alert.get("message")
                    kwargs["ingestion_time"] = datetime.datetime.now()
                    kwargs["generator"] = alert.get("generator")
                    kwargs["notification_time"] = alert.get("notification_time")
                    kwargs["alert_uuid"] = alert_cnf.alert_uuid
                    kwargs["annotation_alert_uuid"] = alert_uuid
                    kwargs["annotation_uuid"] = id
                    list_alerts.append(dict(kwargs))
                # end for
            # end if

        # end for
            
        # Bulk insert annotations
        self.session.bulk_insert_mappings(Annotation, list_annotations)

        # Bulk insert values
        try:
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
                try:
                    self.session.bulk_insert_mappings(AnnotationGeometry, list_values["geometries"])
                except InternalError as e:
                    self.session.rollback()
                    raise WrongGeometry(exit_codes["WRONG_GEOMETRY"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version, e))
            # end if
        except IntegrityError as e:
            self.session.rollback()
            raise DuplicatedValues(exit_codes["DUPLICATED_VALUES"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version, e))
        # end try

        # Bulk insert alerts
        if len(list_alerts) > 0:
            self._bulk_insert_mappings(AnnotationAlert, list_alerts)
        # end if

        return

    @debug
    def _insert_alert_groups(self):
        """
        Method to insert the groups of alerts
        """
        self.alert_groups = insert_alert_groups(self.session, self.operation)
        
        return
    
    @debug        
    def _insert_alert_cnfs(self):
        """
        Method to insert the alert configurations
        """
        self.alert_cnfs = insert_alert_cnfs(self.session, self.operation, self.alert_groups)
        
        return
    
    @debug
    def _insert_alerts(self):
        """
        Method to insert the alerts
        """
        list_event_alerts = []
        list_source_alerts = []
        list_explicit_ref_alerts = []
        for alert in self.operation.get("alerts") or []:
            id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
            alert_cnf = self.alert_cnfs[alert.get("alert_cnf").get("name")]
            message = alert.get("message")
            generator = alert.get("generator")
            notification_time = alert.get("notification_time")
            entity_ref = alert.get("entity").get("reference")
            entity_ref_mode = alert.get("entity").get("reference_mode")
            entity_type = alert.get("entity").get("type")

            kwargs = {}
            kwargs["message"] = message
            kwargs["ingestion_time"] = datetime.datetime.now()
            kwargs["generator"] = generator
            kwargs["notification_time"] = notification_time
            kwargs["alert_uuid"] = alert_cnf.alert_uuid

            if entity_ref_mode == "by_uuid":
                entity_uuid = entity_ref
            # end if

            if entity_type == "event":
                list_alerts = list_event_alerts
                if entity_ref_mode == "by_ref":
                    if not entity_ref in self.event_link_refs:
                        raise UndefinedEntityReference(exit_codes["UNDEFINED_ENTITY_REF"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version, entity_ref))
                    # end if
                    entity_uuid = self.event_link_refs[entity_ref]
                # end if
                kwargs["event_alert_uuid"] = id
                kwargs["event_uuid"] = entity_uuid
            elif entity_type == "source":
                list_alerts = list_source_alerts
                if entity_ref_mode == "by_ref":
                    if entity_ref != self.source.name:
                        raise UndefinedEntityReference(exit_codes["UNDEFINED_ENTITY_REF"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version, entity_ref))
                    # end if
                    entity_uuid = self.source.source_uuid
                # end if
                kwargs["source_alert_uuid"] = id
                kwargs["source_uuid"] = entity_uuid
            else:
                list_alerts = list_explicit_ref_alerts
                if entity_ref_mode == "by_ref":
                    if not entity_ref in self.explicit_refs:
                        raise UndefinedEntityReference(exit_codes["UNDEFINED_ENTITY_REF"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version, entity_ref))
                    # end if
                    entity_uuid = self.explicit_refs[entity_ref].explicit_ref_uuid
                # end if
                kwargs["explicit_ref_alert_uuid"] = id
                kwargs["explicit_ref_uuid"] = entity_uuid
            # end if

            # Insert the alert into the list for bulk ingestion
            list_alerts.append(dict(kwargs))

        # end for
            
        # Bulk insert alerts
        if len(list_event_alerts) > 0:
            self._bulk_insert_mappings(EventAlert, list_event_alerts)
        # end if
        if len(list_source_alerts) > 0:
            self._bulk_insert_mappings(SourceAlert, list_source_alerts)
        # end if
        if len(list_explicit_ref_alerts) > 0:
            self._bulk_insert_mappings(ExplicitRefAlert, list_explicit_ref_alerts)
        # end if

        return
    
    @debug
    def _insert_source_status(self, status, final = False, error = False, message = None):
        """
        Method to insert the DIM processing status

        :param status: code indicating the status of the processing of the file
        :type status: int
        :param message: error message generated when the ingestion does not finish correctly
        :type message: str
        :param final: boolean indicated whether it is a final status or not. This is to insert the ingestion duration in case of final = True
        :type final: bool
        """
        id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        # Insert processing status
        status = SourceStatus(id,datetime.datetime.now(),status,self.source)
        self.session.add(status)

        if message:
            # Insert the error message
            status.log = message
        # end if

        # Flag the ingestion error
        self.source.ingestion_error = error

        if final:
            
            ingested = True
            if self.operation.get("source").get("ingested") == "false":
                ingested = False
            else:
                self._insert_ingestion_progress(100)
            # end if
            self.source.ingested = ingested

            if ingested:
                # Insert ingestion duration and ingestion time
                self.source.ingestion_duration = datetime.datetime.now() - self.ingestion_start
                self.source.ingestion_time = datetime.datetime.now()
            # end if
        # end if

        self.session.commit()

        return

    @debug
    def _remove_deprecated_data(self):
        """
        Method to remove the events and annotations that were overwritten by other data
        """
        # Make this method process and thread safe
        lock = "remove_deprecated_data" + self.dim_signature.dim_signature
        @self.synchronized(lock, external=True, lock_path="/dev/shm")
        def _remove_deprecated_data_synchronize(self):

            if hasattr(self, "all_gauges_for_insert_and_erase") and self.all_gauges_for_insert_and_erase:
                # Remove events due to insert_and_erase insertion mode
                self._remove_deprecated_events_by_insert_and_erase_at_dim_signature_level()
            elif hasattr(self, "all_gauges_for_insert_and_erase_with_priority") and self.all_gauges_for_insert_and_erase_with_priority:
                # Remove events due to insert_and_erase_with_priority insertion mode
                self._remove_deprecated_events_by_insert_and_erase_with_priority_at_dim_signature_level()
            elif hasattr(self, "all_gauges_for_insert_and_erase_with_equal_or_lower_priority") and self.all_gauges_for_insert_and_erase_with_equal_or_lower_priority:
                # Remove events due to insert_and_erase_with_equal_or_lower_priority insertion mode
                self._remove_deprecated_events_by_insert_and_erase_with_equal_or_lower_priority_at_dim_signature_level()
            else:
                # Remove events due to INSERT_and_ERASE insertion mode
                self._remove_deprecated_events_by_insert_and_erase_at_event_level()

                # Remove events due to INSERT_and_ERASE_with_PRIORITY insertion mode
                self._remove_deprecated_events_by_insert_and_erase_with_priority_at_event_level()

                # Remove events due to INSERT_and_ERASE_with_EQUAL_or_LOWER_PRIORITY insertion mode
                self._remove_deprecated_events_by_insert_and_erase_with_equal_or_lower_priority_at_event_level()

                # Remove events due to INSERT_and_ERASE_INTERSECTED_EVENTS_with_PRIORITY insertion mode
                self._remove_deprecated_events_by_insert_and_erase_intersected_events_with_priority_at_event_level()

                # Remove events due to INSERT_and_ERASE_per_EVENT insertion mode
                self._remove_deprecated_events_by_insert_and_erase_per_event()

                # Remove events due to INSERT_and_ERASE_per_EVENT_with_PRIORITY insertion mode
                self._remove_deprecated_events_by_insert_and_erase_per_event_with_priority()

            # end if
            
            # Remove events due to EVENT_KEYS insertion mode
            self._remove_deprecated_events_event_keys()

            # Remove events due to EVENT_KEYS_with_PRIORITY insertion mode
            self._remove_deprecated_events_event_keys_with_priority()

            # Remove annotations due to INSERT_and_ERASE insertion mode
            self._remove_deprecated_annotations()

            # Remove annotations due to INSERT_and_ERASE_with_PRIORITY insertion mode
            self._remove_deprecated_annotations_insert_and_erase_with_priority()

            # Commit data
            self.session.commit()

        # end def

        _remove_deprecated_data_synchronize(self)

        return

    @debug
    def _remove_deprecated_events_by_insert_and_erase_at_dim_signature_level(self):
        """
        Method to remove events that were overwritten by other events due to insert and erase at dim signature level insertion mode
        """
        list_events_to_be_created = {"events": [],
                                     "values": {},
                                     "alerts": [],
                                     "keys": [],
                                     "links": []}
        list_event_uuids_aliases = {}
        list_events_to_be_removed = []

        # Prepare gauges for removing deprecated events
        for gauge in self.dim_signature.gauges:
            self.insert_and_erase_gauges[gauge.gauge_uuid] = None
        # end for

        for gauge_uuid in self.insert_and_erase_gauges:
            list_events_to_be_created_not_ending_on_period = {}
            list_split_events = {}

            # Get the sources of events intersecting the validity period
            dim_signature = self.session.query(DimSignature).join(Gauge).filter(Gauge.gauge_uuid == gauge_uuid).first()
            sources = self.session.query(Source).filter(Source.dim_signature_uuid == dim_signature.dim_signature_uuid,
                                                        or_(and_(Source.validity_start < self.source.validity_stop,
                                                                 Source.validity_stop > self.source.validity_start),
                                                            and_(Source.validity_start == self.source.validity_start,
                                                                 Source.validity_stop == self.source.validity_stop))).order_by(Source.validity_start).all()
            events = sorted(set([event for source in sources for event in source.events if
                                 (event.gauge_uuid == gauge_uuid) and
                                 ((event.start < self.source.validity_stop and
                                  event.stop > self.source.validity_start) or
                                 (event.start == self.source.validity_start and
                                  event.stop == self.source.validity_stop))]), key=lambda event: event.start)
                      
            # Get the timeline of validity periods intersecting
            timeline_points = set(list(chain.from_iterable([[source.validity_start,source.validity_stop] for source in sources])))

            filtered_timeline_points = [timestamp for timestamp in timeline_points if timestamp >= self.source.validity_start and timestamp <= self.source.validity_stop]

            sources_duration_0 = [source for source in sources if source.validity_start == source.validity_stop]
            sources_duration_0_added_stop = {}
            for source in sources_duration_0:
                # Insert only once the stop of the sources with duration 0
                if source.validity_stop not in sources_duration_0_added_stop:
                    sources_duration_0_added_stop[source.validity_stop] = None
                    filtered_timeline_points.append(source.validity_stop)
                # end if
            # end for
            
            # Sort list
            filtered_timeline_points.sort()

            events_per_timestamp = get_events_per_timestamp(events, filtered_timeline_points)

            sources_per_timestamp = get_sources_per_timestamp(sources, filtered_timeline_points)

            # Iterate through the periods
            next_timestamp = 1
            for timestamp in filtered_timeline_points:
                race_condition3()

                events_during_period = []
                if timestamp in events_per_timestamp:
                    events_during_period = events_per_timestamp[timestamp]
                # end if
                sources_during_period = []
                if timestamp in sources_per_timestamp:
                    sources_during_period = sources_per_timestamp[timestamp]
                # end if

                # Check if for the last period there are pending splits or events to be created not ending on previous periods
                if next_timestamp == len(filtered_timeline_points):
                    for event_uuid in list_split_events:
                        event = list_split_events[event_uuid]
                        self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                               list_event_uuids_aliases,
                                                                               timestamp,
                                                                               event.stop,
                                                                               event)

                        # Remove event
                        list_events_to_be_removed.append(event_uuid)
                    # end for

                    events_to_be_created_not_ending_on_period = [event for event in events_during_period if event.event_uuid in list_events_to_be_created_not_ending_on_period]

                    for event_uuid in list_events_to_be_created_not_ending_on_period:
                        # The event has to be created
                        event = [event for event in events_to_be_created_not_ending_on_period if event.event_uuid == event_uuid][0]
                        start = list_events_to_be_created_not_ending_on_period[event_uuid]
                        self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                               list_event_uuids_aliases,
                                                                               start,
                                                                               event.stop,
                                                                               event)

                        # Remove the original event
                        list_events_to_be_removed.append(event.event_uuid)

                    # end for

                    break
                # end if

                validity_start = timestamp
                validity_stop = filtered_timeline_points[next_timestamp]
                next_timestamp += 1
                # Get the maximum generation time at this moment
                max_generation_time = max([source.generation_time for source in sources_during_period
                                           if (source.validity_start < validity_stop and
                                               source.validity_stop > validity_start)
                                           or
                                           (source.validity_start == validity_start and
                                            source.validity_stop == validity_stop)])
                
                # Get the related source
                source_max_generation_time = sorted([source for source in sources_during_period
                                           if source.generation_time == max_generation_time and
                                              ((source.validity_start < validity_stop and
                                               source.validity_stop > validity_start)
                                              or
                                              (source.validity_start == validity_start and
                                               source.validity_stop == validity_stop))], key=lambda source: (source.ingestion_time is None, source.ingestion_time))[0]

                # Events related to the DIM processing with the maximum generation time
                events_max_generation_time = [event for event in events_during_period if event.source.source_uuid == source_max_generation_time.source_uuid and
                                              ((event.start < validity_stop and event.stop > validity_start)
                                               or
                                               (event.start == validity_start and event.stop == validity_stop))]
                                    
                # Review events with higher generation time in the period
                for event in events_max_generation_time:
                    if event.event_uuid in list_split_events:
                        if event.stop <= validity_stop:
                            self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                   list_event_uuids_aliases,
                                                                                   validity_start,
                                                                                   event.stop,
                                                                                   event)
                            list_events_to_be_removed.append(event.event_uuid)
                        else:
                            list_events_to_be_created_not_ending_on_period[event.event_uuid] = validity_start
                        # end if
                        del list_split_events[event.event_uuid]
                    else:
                        if event.event_uuid in list_events_to_be_created_not_ending_on_period:
                            if event.stop <= validity_stop:
                                # The event has to be created
                                start = list_events_to_be_created_not_ending_on_period[event.event_uuid]
                                self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                       list_event_uuids_aliases,
                                                                                       start,
                                                                                       event.stop,
                                                                                       event)

                                del list_events_to_be_created_not_ending_on_period[event.event_uuid]

                                # Remove the original event
                                list_events_to_be_removed.append(event.event_uuid)
                            # end if
                        else:
                            event.visible = True
                        # end if
                    # end if
                # end for

                # Delete deprecated events fully contained into the validity period
                event_uuids_to_be_removed = [event.event_uuid for event in events_during_period
                                             if event.source.generation_time <= max_generation_time and
                                             event.source.source_uuid != source_max_generation_time.source_uuid and
                                             event.start >= validity_start and event.stop <= validity_stop]
                                
                list_events_to_be_removed.extend(event_uuids_to_be_removed)
                
                # Get the events ending on the current period to be removed
                events_not_staying_ending_on_period = [event for event in events_during_period
                                                       if event.source.generation_time <= max_generation_time and
                                                       event.start <= validity_start and
                                                       event.stop > validity_start and
                                                       event.stop <= validity_stop and
                                                       event.source_uuid != source_max_generation_time.source_uuid]

                # Get the events ending on the current period to be removed
                events_not_staying_not_ending_on_period = [event for event in events_during_period if event.source.generation_time <= max_generation_time and
                                                           event.start < validity_stop and
                                                           event.stop > validity_stop and
                                                           event.source_uuid != source_max_generation_time.source_uuid]

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
                            self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                   list_event_uuids_aliases,
                                                                                   start,
                                                                                   validity_start,
                                                                                   event)
                        # end if
                        if event.stop > validity_stop:
                            event.visible = False
                            list_split_events[event.event_uuid] = event
                        else:
                            list_events_to_be_removed.append(event.event_uuid)
                        # end if
                    elif event.event_uuid in list_split_events and event.stop <= validity_stop:
                        list_events_to_be_removed.append(event.event_uuid)
                        del list_split_events[event.event_uuid]
                    # end if
                # end for
            # end for

        # end for
        # Bulk insert events
        self.session.bulk_insert_mappings(Event, list_events_to_be_created["events"])
        # Bulk insert keys
        self.session.bulk_insert_mappings(EventKey, list_events_to_be_created["keys"])
        # Bulk insert links
        self._replicate_event_links(list_event_uuids_aliases, list_events_to_be_created["links"])
        self.session.bulk_insert_mappings(EventLink, list_events_to_be_created["links"])

        # Remove the events that were partially affected by the insert and erase operation
        self.session.query(EventLink).filter(EventLink.event_uuid_link.in_(list_events_to_be_removed)).delete(synchronize_session=False)
        self.session.query(Event).filter(Event.event_uuid.in_(list_events_to_be_removed)).delete(synchronize_session=False)

        # Bulk insert alerts
        self.session.bulk_insert_mappings(EventAlert, list_events_to_be_created["alerts"])

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

    @debug
    def _remove_deprecated_events_by_insert_and_erase_with_priority_at_dim_signature_level(self):
        """
        Method to remove events that were overwritten by other events due to insert and erase with priority at dim signature level insertion mode
        """
        list_events_to_be_created = {"events": [],
                                     "values": {},
                                     "alerts": [],
                                     "keys": [],
                                     "links": []}
        list_event_uuids_aliases = {}
        list_events_to_be_removed = []

        # Prepare gauges for removing deprecated events
        for gauge in self.dim_signature.gauges:
            self.insert_and_erase_with_priority_gauges[gauge.gauge_uuid] = None
        # end for

        for gauge_uuid in self.insert_and_erase_with_priority_gauges:
            list_events_to_be_created_not_ending_on_period = {}
            list_split_events = {}

            # Get the sources of events intersecting the validity period defined for the current source
            dim_signature = self.session.query(DimSignature).join(Gauge).filter(Gauge.gauge_uuid == gauge_uuid).first()
            sources = self.session.query(Source).filter(Source.dim_signature_uuid == dim_signature.dim_signature_uuid,
                                                        Source.priority != None,
                                                        or_(and_(Source.validity_start < self.source.validity_stop,
                                                                 Source.validity_stop > self.source.validity_start),
                                                            and_(Source.validity_start == self.source.validity_start,
                                                                 Source.validity_stop == self.source.validity_stop))).order_by(Source.validity_start).all()
            events = sorted(set([event for source in sources for event in source.events if
                                 (event.gauge_uuid == gauge_uuid) and
                                 ((event.start < self.source.validity_stop and
                                  event.stop > self.source.validity_start) or
                                 (event.start == self.source.validity_start and
                                  event.stop == self.source.validity_stop))]), key=lambda event: event.start)

            # Get the timeline of validity periods intersecting
            timeline_points = set(list(chain.from_iterable([[source.validity_start,source.validity_stop] for source in sources])))

            filtered_timeline_points = [timestamp for timestamp in timeline_points if timestamp >= self.source.validity_start and timestamp <= self.source.validity_stop]

            sources_duration_0 = [source for source in sources if source.validity_start == source.validity_stop]
            sources_duration_0_added_stop = {}
            for source in sources_duration_0:
                # Insert only once the stop of the sources with duration 0
                if source.validity_stop not in sources_duration_0_added_stop:
                    sources_duration_0_added_stop[source.validity_stop] = None
                    filtered_timeline_points.append(source.validity_stop)
                # end if
            # end for
            
            # Sort list
            filtered_timeline_points.sort()

            events_per_timestamp = get_events_per_timestamp(events, filtered_timeline_points)

            sources_per_timestamp = get_sources_per_timestamp(sources, filtered_timeline_points)

            # Iterate through the periods
            next_timestamp = 1
            for timestamp in filtered_timeline_points:
                race_condition3()

                events_during_period = []
                if timestamp in events_per_timestamp:
                    events_during_period = events_per_timestamp[timestamp]
                # end if
                sources_during_period = []
                if timestamp in sources_per_timestamp:
                    sources_during_period = sources_per_timestamp[timestamp]
                # end if
                
                # Check if for the last period there are pending splits or events to be created not ending on previous periods
                if next_timestamp == len(filtered_timeline_points):
                    for event_uuid in list_split_events:
                        event = list_split_events[event_uuid]
                        self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                               list_event_uuids_aliases,
                                                                               timestamp,
                                                                               event.stop,
                                                                               event)

                        # Remove event
                        list_events_to_be_removed.append(event_uuid)
                    # end for
                    events_to_be_created_not_ending_on_period = [event for event in events_during_period if event.event_uuid in list_events_to_be_created_not_ending_on_period]
                    
                    for event_uuid in list_events_to_be_created_not_ending_on_period:
                        # The event has to be created
                        event = [event for event in events_to_be_created_not_ending_on_period if event.event_uuid == event_uuid][0]
                        start = list_events_to_be_created_not_ending_on_period[event_uuid]
                        self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                               list_event_uuids_aliases,
                                                                               start,
                                                                               event.stop,
                                                                               event)

                        # Remove the original event
                        list_events_to_be_removed.append(event.event_uuid)

                    # end for

                    break
                # end if

                validity_start = timestamp
                validity_stop = filtered_timeline_points[next_timestamp]
                next_timestamp += 1

                # Get the maximum priority at this moment
                max_priority = max([source.priority for source in sources_during_period
                                           if source.priority >= self.source.priority and
                                           ((source.validity_start < validity_stop and
                                            source.validity_stop > validity_start)
                                           or
                                           (source.validity_start == validity_start and
                                            source.validity_stop == validity_stop))])

                # Get the maximum generation time at this moment
                max_generation_time = max([source.generation_time for source in sources_during_period
                                           if source.priority == max_priority and
                                           ((source.validity_start < validity_stop and
                                               source.validity_stop > validity_start)
                                           or
                                           (source.validity_start == validity_start and
                                            source.validity_stop == validity_stop))])

                # Get the related source
                source_max_generation_time = sorted(
                    [source for source in sources_during_period
                     if source.generation_time == max_generation_time and
                     source.priority == max_priority and
                     ((source.validity_start < validity_stop and
                       source.validity_stop > validity_start)
                      or
                      (source.validity_start == validity_start and
                       source.validity_stop == validity_stop))], key=lambda source: (source.ingestion_time is None, source.ingestion_time))[0]
                
                # Events related to the DIM processing with the maximum generation time
                events_max_generation_time = [event for event in events_during_period if event.source.source_uuid == source_max_generation_time.source_uuid and
                                              ((event.start < validity_stop and event.stop > validity_start)
                                               or
                                               (event.start == validity_start and event.stop == validity_stop))]
                
                # Review events with higher generation time in the period
                for event in events_max_generation_time:
                    if event.event_uuid in list_split_events:
                        if event.stop <= validity_stop:
                            self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                   list_event_uuids_aliases,
                                                                                   validity_start,
                                                                                   event.stop,
                                                                                   event)
                            list_events_to_be_removed.append(event.event_uuid)
                        else:
                            list_events_to_be_created_not_ending_on_period[event.event_uuid] = validity_start
                        # end if
                        del list_split_events[event.event_uuid]
                    else:
                        if event.event_uuid in list_events_to_be_created_not_ending_on_period:
                            if event.stop <= validity_stop:
                                # The event has to be created
                                start = list_events_to_be_created_not_ending_on_period[event.event_uuid]
                                self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                       list_event_uuids_aliases,
                                                                                       start,
                                                                                       event.stop,
                                                                                       event)

                                del list_events_to_be_created_not_ending_on_period[event.event_uuid]

                                # Remove the original event
                                list_events_to_be_removed.append(event.event_uuid)
                            # end if
                        else:
                            event.visible = True
                        # end if
                    # end if
                # end for

                # Delete deprecated events fully contained into the validity period
                event_uuids_to_be_removed = [event.event_uuid for event in events_during_period
                                             if ((event.source.generation_time <= max_generation_time and event.source.priority <= max_priority)
                                                 or
                                                 (event.source.priority < max_priority)) and
                                             event.source.source_uuid != source_max_generation_time.source_uuid and
                                             event.start >= validity_start and event.stop <= validity_stop]

                list_events_to_be_removed.extend(event_uuids_to_be_removed)

                # Get the events ending on the current period to be removed
                events_not_staying_ending_on_period = [event for event in events_during_period
                                                       if ((event.source.generation_time <= max_generation_time and event.source.priority <= max_priority)
                                                           or
                                                           (event.source.priority < max_priority)) and

                                                       event.start <= validity_start and
                                                       event.stop > validity_start and
                                                       event.stop <= validity_stop and
                                                       event.source_uuid != source_max_generation_time.source_uuid]
                
                # Get the events ending on the current period to be removed
                events_not_staying_not_ending_on_period = [event for event in events_during_period
                                                           if ((event.source.generation_time <= max_generation_time and event.source.priority <= max_priority)
                                                               or
                                                               (event.source.priority < max_priority)) and
                                                           event.start < validity_stop and
                                                           event.stop > validity_stop and
                                                           event.source_uuid != source_max_generation_time.source_uuid]
                
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
                            self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                   list_event_uuids_aliases,
                                                                                   start,
                                                                                   validity_start,
                                                                                   event)
                        # end if
                        if event.stop > validity_stop:
                            event.visible = False
                            list_split_events[event.event_uuid] = event
                        else:
                            list_events_to_be_removed.append(event.event_uuid)
                        # end if
                    elif event.event_uuid in list_split_events and event.stop <= validity_stop:
                        list_events_to_be_removed.append(event.event_uuid)
                        del list_split_events[event.event_uuid]
                    # end if
                # end for
            # end for

        # end for
        # Bulk insert events
        self.session.bulk_insert_mappings(Event, list_events_to_be_created["events"])
        # Bulk insert keys
        self.session.bulk_insert_mappings(EventKey, list_events_to_be_created["keys"])
        # Bulk insert links
        self._replicate_event_links(list_event_uuids_aliases, list_events_to_be_created["links"])
        self.session.bulk_insert_mappings(EventLink, list_events_to_be_created["links"])

        # Remove the events that were partially affected by the insert and erase operation
        self.session.query(EventLink).filter(EventLink.event_uuid_link.in_(list_events_to_be_removed)).delete(synchronize_session=False)
        self.session.query(Event).filter(Event.event_uuid.in_(list_events_to_be_removed)).delete(synchronize_session=False)

        # Bulk insert alerts
        self.session.bulk_insert_mappings(EventAlert, list_events_to_be_created["alerts"])

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

    @debug
    def _remove_deprecated_events_by_insert_and_erase_with_equal_or_lower_priority_at_dim_signature_level(self):
        """
        Method to remove events that were overwritten by other events due to insert and erase with equal or lower priority at dim signature level insertion mode
        """
        list_events_to_be_created = {"events": [],
                                     "values": {},
                                     "alerts": [],
                                     "keys": [],
                                     "links": []}
        list_event_uuids_aliases = {}
        list_events_to_be_removed = []

        # Prepare gauges for removing deprecated events
        for gauge in self.dim_signature.gauges:
            self.insert_and_erase_with_equal_or_lower_priority_gauges[gauge.gauge_uuid] = None
        # end for

        for gauge_uuid in self.insert_and_erase_with_equal_or_lower_priority_gauges:
            list_events_to_be_created_not_ending_on_period = {}
            list_split_events = {}

            # Get the sources of events intersecting the validity period defined for the current source
            dim_signature = self.session.query(DimSignature).join(Gauge).filter(Gauge.gauge_uuid == gauge_uuid).first()
            sources = self.session.query(Source).filter(Source.dim_signature_uuid == dim_signature.dim_signature_uuid,
                                                                           Source.priority != None,
                                                                                  or_(and_(Source.validity_start < self.source.validity_stop,
                                                                                           Source.validity_stop > self.source.validity_start),
                                                                                      and_(Source.validity_start == self.source.validity_start,
                                                                                           Source.validity_stop == self.source.validity_stop))).order_by(Source.validity_start).all()
            events = sorted(set([event for source in sources for event in source.events if
                                 (event.gauge_uuid == gauge_uuid) and
                                 ((event.start < self.source.validity_stop and
                                  event.stop > self.source.validity_start) or
                                 (event.start == self.source.validity_start and
                                  event.stop == self.source.validity_stop))]), key=lambda event: event.start)

            # Get the timeline of validity periods intersecting
            timeline_points = set(list(chain.from_iterable([[source.validity_start,source.validity_stop] for source in sources])))

            filtered_timeline_points = [timestamp for timestamp in timeline_points if timestamp >= self.source.validity_start and timestamp <= self.source.validity_stop]

            sources_duration_0 = [source for source in sources if source.validity_start == source.validity_stop]
            sources_duration_0_added_stop = {}
            for source in sources_duration_0:
                # Insert only once the stop of the sources with duration 0
                if source.validity_stop not in sources_duration_0_added_stop:
                    sources_duration_0_added_stop[source.validity_stop] = None
                    filtered_timeline_points.append(source.validity_stop)
                # end if
            # end for
            
            # Sort list
            filtered_timeline_points.sort()

            events_per_timestamp = get_events_per_timestamp(events, filtered_timeline_points)

            sources_per_timestamp = get_sources_per_timestamp(sources, filtered_timeline_points)

            # Iterate through the periods
            next_timestamp = 1
            for timestamp in filtered_timeline_points:
                race_condition3()

                events_during_period = []
                if timestamp in events_per_timestamp:
                    events_during_period = events_per_timestamp[timestamp]
                # end if
                sources_during_period = []
                if timestamp in sources_per_timestamp:
                    sources_during_period = sources_per_timestamp[timestamp]
                # end if

                # Check if for the last period there are pending splits or events to be created not ending on previous periods
                if next_timestamp == len(filtered_timeline_points):
                    for event_uuid in list_split_events:
                        event = list_split_events[event_uuid]
                        self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                               list_event_uuids_aliases,
                                                                               timestamp,
                                                                               event.stop,
                                                                               event)

                        # Remove event
                        list_events_to_be_removed.append(event_uuid)
                    # end for

                    events_to_be_created_not_ending_on_period = [event for event in events_during_period if event.event_uuid in list_events_to_be_created_not_ending_on_period]
                    
                    for event_uuid in list_events_to_be_created_not_ending_on_period:
                        # The event has to be created
                        event = [event for event in events_to_be_created_not_ending_on_period if event.event_uuid == event_uuid][0]
                        start = list_events_to_be_created_not_ending_on_period[event_uuid]
                        self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                               list_event_uuids_aliases,
                                                                               start,
                                                                               event.stop,
                                                                               event)

                        # Remove the original event
                        list_events_to_be_removed.append(event.event_uuid)

                    # end for

                    break
                # end if

                validity_start = timestamp
                validity_stop = filtered_timeline_points[next_timestamp]
                next_timestamp += 1

                # Get the maximum priority at this moment
                max_priority = max([source.priority for source in sources_during_period
                                           if source.priority >= self.source.priority and
                                           ((source.validity_start < validity_stop and
                                            source.validity_stop > validity_start)
                                           or
                                           (source.validity_start == validity_start and
                                            source.validity_stop == validity_stop))])
                
                # Get the maximum generation time at this moment
                max_generation_time = max([source.generation_time for source in sources_during_period
                                           if source.priority == max_priority and
                                           ((source.validity_start < validity_stop and
                                               source.validity_stop > validity_start)
                                           or
                                           (source.validity_start == validity_start and
                                            source.validity_stop == validity_stop))])
                
                # Get the related source
                source_max_generation_time = sorted(
                    [source for source in sources_during_period
                     if source.generation_time == max_generation_time and
                     source.priority == max_priority and
                     ((source.validity_start < validity_stop and
                       source.validity_stop > validity_start)
                      or
                      (source.validity_start == validity_start and
                       source.validity_stop == validity_stop))], key=lambda source: (source.ingestion_time is None, source.ingestion_time))[0]
                
                # Events related to the source with the maximum generation time if the priority is lower or equal that the defined for the current source
                # Note this list will be empty if the source with maximum priority has a priority higher than the one defined by the current source
                events_max_generation_time = [event for event in events_during_period if
                                              event.source.priority <= self.source.priority and
                                              event.source.source_uuid == source_max_generation_time.source_uuid and
                                              ((event.start < validity_stop and event.stop > validity_start)
                                               or
                                               (event.start == validity_start and event.stop == validity_stop))]

                # Review events with higher generation time in the period
                for event in events_max_generation_time:
                    if event.event_uuid in list_split_events:
                        if event.stop <= validity_stop:
                            self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                   list_event_uuids_aliases,
                                                                                   validity_start,
                                                                                   event.stop,
                                                                                   event)
                            list_events_to_be_removed.append(event.event_uuid)
                        else:
                            list_events_to_be_created_not_ending_on_period[event.event_uuid] = validity_start
                        # end if
                        del list_split_events[event.event_uuid]
                    else:
                        if event.event_uuid in list_events_to_be_created_not_ending_on_period:
                            if event.stop <= validity_stop:
                                # The event has to be created
                                start = list_events_to_be_created_not_ending_on_period[event.event_uuid]
                                self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                       list_event_uuids_aliases,
                                                                                       start,
                                                                                       event.stop,
                                                                                       event)

                                del list_events_to_be_created_not_ending_on_period[event.event_uuid]

                                # Remove the original event
                                list_events_to_be_removed.append(event.event_uuid)
                            # end if
                        else:
                            event.visible = True
                        # end if
                    # end if
                # end for

                # Delete deprecated events fully contained into the validity period
                event_uuids_to_be_removed = [event.event_uuid for event in events_during_period
                                             if event.source.priority <= self.source.priority and
                                             ((event.source.generation_time <= max_generation_time and event.source.priority <= max_priority)
                                                 or
                                                 (event.source.priority < max_priority)) and
                                             event.source.source_uuid != source_max_generation_time.source_uuid and
                                             event.start >= validity_start and event.stop <= validity_stop]
                
                list_events_to_be_removed.extend(event_uuids_to_be_removed)

                # Get the events ending on the current period to be removed
                events_not_staying_ending_on_period = [event for event in events_during_period
                                                       if event.source.priority <= self.source.priority and
                                                       ((event.source.generation_time <= max_generation_time and event.source.priority <= max_priority)
                                                           or
                                                           (event.source.priority < max_priority)) and

                                                       event.start <= validity_start and
                                                       event.stop > validity_start and
                                                       event.stop <= validity_stop and
                                                       event.source_uuid != source_max_generation_time.source_uuid]
                
                # Get the events ending on the current period to be removed
                events_not_staying_not_ending_on_period = [event for event in events_during_period
                                                           if event.source.priority <= self.source.priority and
                                                           ((event.source.generation_time <= max_generation_time and event.source.priority <= max_priority)
                                                               or
                                                               (event.source.priority < max_priority)) and
                                                           event.start < validity_stop and
                                                           event.stop > validity_stop and
                                                           event.source_uuid != source_max_generation_time.source_uuid]
                
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
                            self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                   list_event_uuids_aliases,
                                                                                   start,
                                                                                   validity_start,
                                                                                   event)
                        # end if
                        if event.stop > validity_stop:
                            event.visible = False
                            list_split_events[event.event_uuid] = event
                        else:
                            list_events_to_be_removed.append(event.event_uuid)
                        # end if
                    elif event.event_uuid in list_split_events and event.stop <= validity_stop:
                        list_events_to_be_removed.append(event.event_uuid)
                        del list_split_events[event.event_uuid]
                    # end if
                # end for
            # end for

        # end for
        # Bulk insert events
        self.session.bulk_insert_mappings(Event, list_events_to_be_created["events"])
        # Bulk insert keys
        self.session.bulk_insert_mappings(EventKey, list_events_to_be_created["keys"])
        # Bulk insert links
        self._replicate_event_links(list_event_uuids_aliases, list_events_to_be_created["links"])
        self.session.bulk_insert_mappings(EventLink, list_events_to_be_created["links"])

        # Remove the events that were partially affected by the insert and erase operation
        self.session.query(EventLink).filter(EventLink.event_uuid_link.in_(list_events_to_be_removed)).delete(synchronize_session=False)
        self.session.query(Event).filter(Event.event_uuid.in_(list_events_to_be_removed)).delete(synchronize_session=False)

        # Bulk insert alerts
        self.session.bulk_insert_mappings(EventAlert, list_events_to_be_created["alerts"])

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

    @debug
    def _create_event_for_remove_deprecated_events_method(self, list_events_to_be_created, list_event_uuids_aliases, start, stop, event):
        """
        Method to create an event given the start and stop values

        :param list_events_to_be_created: list of events to be created
        :type list_events_to_be_created: dict with the following structure
                list_events_to_be_created = {"events": [],
                                     "values": {},
                                     "alerts": [],
                                     "keys": [],
                                     "links": []}
        :param list_event_uuids_aliases: list of event uuids aliases
        :type list_event_uuids_aliases: dict
        :param start: start time to assigned to the new event
        :type start: datetime
        :param stop: stop time to assigned to the new event
        :type stop: datetime
        :param event: original event to copy
        :type event: event

        """
        id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        self._insert_event(list_events_to_be_created["events"], id, start, stop,
                           event.gauge_uuid, event.explicit_ref_uuid, True, source_id = event.source_uuid)
        self._replicate_event_values(event.event_uuid, id, list_events_to_be_created["events"][-1], list_events_to_be_created["values"])
        self._replicate_event_alerts(event.event_uuid, id, list_events_to_be_created["alerts"])
        self._create_event_uuid_alias(event.event_uuid, id, list_event_uuids_aliases)
        self._replicate_event_keys(event.event_uuid, id, list_events_to_be_created["keys"])

        return

    @debug
    def _remove_deprecated_events_by_insert_and_erase_at_event_level(self):
        """
        Method to remove events that were overwritten by other events due to insert and erase at event level insertion mode
        """
        list_events_to_be_created = {"events": [],
                                     "values": {},
                                     "alerts": [],
                                     "keys": [],
                                     "links": []}
        list_event_uuids_aliases = {}
        list_events_to_be_removed = []
        for gauge_uuid in self.insert_and_erase_gauges:
            list_events_to_be_created_not_ending_on_period = {}
            list_split_events = {}

            # Get the sources of events intersecting the validity period
            sources = self.session.query(Source).join(Event).filter(Event.gauge_uuid == gauge_uuid,
                                                                    or_(and_(Source.validity_start < self.source.validity_stop,
                                                                             Source.validity_stop > self.source.validity_start),
                                                                        and_(Source.validity_start == self.source.validity_start,
                                                                             Source.validity_stop == self.source.validity_stop))).order_by(Source.validity_start).all()
            events = sorted(set([event for source in sources for event in source.events if
                                 (event.gauge_uuid == gauge_uuid) and
                                 ((event.start < self.source.validity_stop and
                                  event.stop > self.source.validity_start) or
                                 (event.start == self.source.validity_start and
                                  event.stop == self.source.validity_stop))]), key=lambda event: event.start)

            # Get the timeline of validity periods intersecting
            timeline_points = set(list(chain.from_iterable([[source.validity_start,source.validity_stop] for source in sources])))

            filtered_timeline_points = [timestamp for timestamp in timeline_points if timestamp >= self.source.validity_start and timestamp <= self.source.validity_stop]

            sources_duration_0 = [source for source in sources if source.validity_start == source.validity_stop]
            sources_duration_0_added_stop = {}
            for source in sources_duration_0:
                # Insert only once the stop of the sources with duration 0
                if source.validity_stop not in sources_duration_0_added_stop:
                    sources_duration_0_added_stop[source.validity_stop] = None
                    filtered_timeline_points.append(source.validity_stop)
                # end if
            # end for
            
            # Sort list
            filtered_timeline_points.sort()

            events_per_timestamp = get_events_per_timestamp(events, filtered_timeline_points)

            sources_per_timestamp = get_sources_per_timestamp(sources, filtered_timeline_points)

            # Iterate through the periods
            next_timestamp = 1
            for timestamp in filtered_timeline_points:
                race_condition3()

                events_during_period = []
                if timestamp in events_per_timestamp:
                    events_during_period = events_per_timestamp[timestamp]
                # end if
                sources_during_period = []
                if timestamp in sources_per_timestamp:
                    sources_during_period = sources_per_timestamp[timestamp]
                # end if

                # Check if for the last period there are pending splits or events to be created not ending on previous periods
                if next_timestamp == len(filtered_timeline_points):
                    for event_uuid in list_split_events:
                        event = list_split_events[event_uuid]
                        self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                               list_event_uuids_aliases,
                                                                               timestamp,
                                                                               event.stop,
                                                                               event)
                        # Remove event
                        list_events_to_be_removed.append(event_uuid)
                    # end for
                    
                    events_to_be_created_not_ending_on_period = [event for event in events_during_period if event.event_uuid in list_events_to_be_created_not_ending_on_period]
                    
                    for event_uuid in list_events_to_be_created_not_ending_on_period:
                        # The event has to be created
                        event = [event for event in events_to_be_created_not_ending_on_period if event.event_uuid == event_uuid][0]
                        start = list_events_to_be_created_not_ending_on_period[event_uuid]
                        self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                               list_event_uuids_aliases,
                                                                               start,
                                                                               event.stop,
                                                                               event)

                        # Remove the original event
                        list_events_to_be_removed.append(event.event_uuid)

                    # end for
                    break
                # end if

                validity_start = timestamp
                validity_stop = filtered_timeline_points[next_timestamp]
                next_timestamp += 1

                # Check if there are no events for this timestamp
                if len(events_during_period) < 1:
                    continue
                # end if

                # Get the maximum generation time at this moment
                max_generation_time = max([source.generation_time for source in sources_during_period
                                           if ((source.validity_start < validity_stop and
                                               source.validity_stop > validity_start)
                                           or
                                           (source.validity_start == validity_start and
                                            source.validity_stop == validity_stop))])
                
                # Get the related source
                source_max_generation_time = sorted([source for source in sources_during_period
                                                     if source.generation_time == max_generation_time],
                                                    key=lambda source: (source.ingestion_time is None, source.ingestion_time))[0]

                # Check if the period contains sources with the relevant events if not continue with the following period
                if not source_max_generation_time:
                    continue
                # end if
                
                # Events related to the DIM processing with the maximum generation time
                events_max_generation_time = [event for event in events_during_period if event.source.source_uuid == source_max_generation_time.source_uuid and
                                              ((event.start < validity_stop and event.stop > validity_start)
                                               or
                                               (event.start == validity_start and event.stop == validity_stop))]

                # Review events with higher generation time in the period
                for event in events_max_generation_time:
                    if event.event_uuid in list_split_events:
                        if event.stop <= validity_stop:
                            self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                   list_event_uuids_aliases,
                                                                                   validity_start,
                                                                                   event.stop,
                                                                                   event)
                            list_events_to_be_removed.append(event.event_uuid)
                        else:
                            list_events_to_be_created_not_ending_on_period[event.event_uuid] = validity_start
                        # end if
                        del list_split_events[event.event_uuid]
                    else:
                        if event.event_uuid in list_events_to_be_created_not_ending_on_period:
                            if event.stop <= validity_stop:
                                # The event has to be created
                                start = list_events_to_be_created_not_ending_on_period[event.event_uuid]
                                self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                       list_event_uuids_aliases,
                                                                                       start,
                                                                                       event.stop,
                                                                                       event)

                                del list_events_to_be_created_not_ending_on_period[event.event_uuid]

                                # Remove the original event
                                list_events_to_be_removed.append(event.event_uuid)
                            # end if
                        else:
                            event.visible = True
                        # end if
                    # end if
                # end for

                # Delete deprecated events fully contained into the validity period
                event_uuids_to_be_removed = [event.event_uuid for event in events_during_period
                                             if event.source.generation_time <= max_generation_time and
                                             event.source.source_uuid != source_max_generation_time.source_uuid and
                                             event.start >= validity_start and event.stop <= validity_stop]
                
                list_events_to_be_removed.extend(event_uuids_to_be_removed)

                # Get the events ending on the current period to be removed
                events_not_staying_ending_on_period = [event for event in events_during_period
                                                       if event.source.generation_time <= max_generation_time and
                                                       event.start <= validity_start and
                                                       event.stop > validity_start and
                                                       event.stop <= validity_stop and
                                                       event.source_uuid != source_max_generation_time.source_uuid]
                
                # Get the events ending on the current period to be removed
                events_not_staying_not_ending_on_period = [event for event in events_during_period if event.source.generation_time <= max_generation_time and
                                                           event.start < validity_stop and
                                                           event.stop > validity_stop and
                                                           event.source_uuid != source_max_generation_time.source_uuid]
                
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
                            self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                   list_event_uuids_aliases,
                                                                                   start,
                                                                                   validity_start,
                                                                                   event)
                        # end if
                        if event.stop > validity_stop:
                            event.visible = False
                            list_split_events[event.event_uuid] = event
                        else:
                            list_events_to_be_removed.append(event.event_uuid)
                        # end if
                    elif event.event_uuid in list_split_events and event.stop <= validity_stop:
                        list_events_to_be_removed.append(event.event_uuid)
                        del list_split_events[event.event_uuid]
                    # end if
                # end for
            # end for

        # end for
        # Bulk insert events
        self.session.bulk_insert_mappings(Event, list_events_to_be_created["events"])
        # Bulk insert keys
        self.session.bulk_insert_mappings(EventKey, list_events_to_be_created["keys"])
        # Bulk insert links
        self._replicate_event_links(list_event_uuids_aliases, list_events_to_be_created["links"])
        self.session.bulk_insert_mappings(EventLink, list_events_to_be_created["links"])

        # Remove the events that were partially affected by the insert and erase operation
        self.session.query(EventLink).filter(EventLink.event_uuid_link.in_(list_events_to_be_removed)).delete(synchronize_session=False)
        self.session.query(Event).filter(Event.event_uuid.in_(list_events_to_be_removed)).delete(synchronize_session=False)

        # Bulk insert alerts
        self.session.bulk_insert_mappings(EventAlert, list_events_to_be_created["alerts"])

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

    @debug
    def _remove_deprecated_events_by_insert_and_erase_with_priority_at_event_level(self):
        """
        Method to remove events that were overwritten by other events due to insert and erase with priority at event level insertion mode
        """
        list_events_to_be_created = {"events": [],
                                     "values": {},
                                     "alerts": [],
                                     "keys": [],
                                     "links": []}
        list_event_uuids_aliases = {}
        list_events_to_be_removed = []
        for gauge_uuid in self.insert_and_erase_with_priority_gauges:
            list_events_to_be_created_not_ending_on_period = {}
            list_split_events = {}

            # Get the sources of events intersecting the validity period defined for the current source
            sources = self.session.query(Source).join(Event).filter(Event.gauge_uuid == gauge_uuid,
                                                                       Source.priority != None,
                                                                    or_(and_(Source.validity_start < self.source.validity_stop,
                                                                             Source.validity_stop > self.source.validity_start),
                                                                        and_(Source.validity_start == self.source.validity_start,
                                                                             Source.validity_stop == self.source.validity_stop))).order_by(Event.start).all()
            events = sorted(set([event for source in sources for event in source.events if
                                 (event.gauge_uuid == gauge_uuid) and
                                 ((event.start < self.source.validity_stop and
                                  event.stop > self.source.validity_start) or
                                 (event.start == self.source.validity_start and
                                  event.stop == self.source.validity_stop))]), key=lambda event: event.start)

            # Get the timeline of validity periods intersecting
            timeline_points = set(list(chain.from_iterable([[source.validity_start,source.validity_stop] for source in sources])))

            filtered_timeline_points = [timestamp for timestamp in timeline_points if timestamp >= self.source.validity_start and timestamp <= self.source.validity_stop]

            sources_duration_0 = [source for source in sources if source.validity_start == source.validity_stop]
            sources_duration_0_added_stop = {}
            for source in sources_duration_0:
                # Insert only once the stop of the sources with duration 0
                if source.validity_stop not in sources_duration_0_added_stop:
                    sources_duration_0_added_stop[source.validity_stop] = None
                    filtered_timeline_points.append(source.validity_stop)
                # end if
            # end for
            
            # Sort list
            filtered_timeline_points.sort()

            events_per_timestamp = get_events_per_timestamp(events, filtered_timeline_points)

            sources_per_timestamp = get_sources_per_timestamp(sources, filtered_timeline_points)

            # Iterate through the periods
            next_timestamp = 1
            for timestamp in filtered_timeline_points:
                race_condition3()

                events_during_period = []
                if timestamp in events_per_timestamp:
                    events_during_period = events_per_timestamp[timestamp]
                # end if
                sources_during_period = []
                if timestamp in sources_per_timestamp:
                    sources_during_period = sources_per_timestamp[timestamp]
                # end if
                
                # Check if for the last period there are pending splits or events to be created not ending on previous periods
                if next_timestamp == len(filtered_timeline_points):
                    for event_uuid in list_split_events:
                        event = list_split_events[event_uuid]
                        self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                               list_event_uuids_aliases,
                                                                               timestamp,
                                                                               event.stop,
                                                                               event)
                        # Remove event
                        list_events_to_be_removed.append(event_uuid)
                    # end for

                    events_to_be_created_not_ending_on_period = [event for event in events_during_period if event.event_uuid in list_events_to_be_created_not_ending_on_period]

                    for event_uuid in list_events_to_be_created_not_ending_on_period:
                        # The event has to be created
                        event = [event for event in events_to_be_created_not_ending_on_period if event.event_uuid == event_uuid][0]
                        start = list_events_to_be_created_not_ending_on_period[event_uuid]
                        self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                               list_event_uuids_aliases,
                                                                               start,
                                                                               event.stop,
                                                                               event)

                        # Remove the original event
                        list_events_to_be_removed.append(event.event_uuid)

                    # end for
                    break
                # end if

                validity_start = timestamp
                validity_stop = filtered_timeline_points[next_timestamp]
                next_timestamp += 1

                # Get the maximum priority at this moment
                max_priority = max([source.priority for source in sources_during_period
                                           if source.priority >= self.source.priority and
                                           ((source.validity_start < validity_stop and
                                            source.validity_stop > validity_start)
                                           or
                                           (source.validity_start == validity_start and
                                            source.validity_stop == validity_stop))])
                
                # Get the maximum generation time at this moment
                max_generation_time = max([source.generation_time for source in sources_during_period
                                           if source.priority == max_priority and
                                           ((source.validity_start < validity_stop and
                                               source.validity_stop > validity_start)
                                           or
                                           (source.validity_start == validity_start and
                                            source.validity_stop == validity_stop))])
                
                # Get the related source
                source_max_generation_time = sorted(
                    [source for source in sources_during_period
                     if source.generation_time == max_generation_time and
                     source.priority == max_priority and
                     ((source.validity_start < validity_stop and
                       source.validity_stop > validity_start)
                      or
                      (source.validity_start == validity_start and
                       source.validity_stop == validity_stop))], key=lambda source: (source.ingestion_time is None, source.ingestion_time))[0]

                # Check if the period contains sources with the relevant events if not continue with the following period
                if not source_max_generation_time:
                    continue
                # end if
                
                # Events related to the DIM processing with the maximum generation time
                events_max_generation_time = [event for event in events_during_period if event.source.source_uuid == source_max_generation_time.source_uuid and
                                              ((event.start < validity_stop and event.stop > validity_start)
                                               or
                                               (event.start == validity_start and event.stop == validity_stop))]
                
                # Review events with higher generation time in the period
                for event in events_max_generation_time:
                    if event.event_uuid in list_split_events:
                        if event.stop <= validity_stop:
                            self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                   list_event_uuids_aliases,
                                                                                   validity_start,
                                                                                   event.stop,
                                                                                   event)
                            list_events_to_be_removed.append(event.event_uuid)
                        else:
                            list_events_to_be_created_not_ending_on_period[event.event_uuid] = validity_start
                        # end if
                        del list_split_events[event.event_uuid]
                    else:
                        if event.event_uuid in list_events_to_be_created_not_ending_on_period:
                            if event.stop <= validity_stop:
                                # The event has to be created
                                start = list_events_to_be_created_not_ending_on_period[event.event_uuid]
                                self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                       list_event_uuids_aliases,
                                                                                       start,
                                                                                       event.stop,
                                                                                       event)

                                del list_events_to_be_created_not_ending_on_period[event.event_uuid]

                                # Remove the original event
                                list_events_to_be_removed.append(event.event_uuid)
                            # end if
                        else:
                            event.visible = True
                        # end if
                    # end if
                # end for

                # Delete deprecated events fully contained into the validity period
                event_uuids_to_be_removed = [event.event_uuid for event in events_during_period
                                             if ((event.source.generation_time <= max_generation_time and event.source.priority <= max_priority)
                                                 or
                                                 (event.source.priority < max_priority)) and
                                             event.source.source_uuid != source_max_generation_time.source_uuid and
                                             event.start >= validity_start and event.stop <= validity_stop]
                
                list_events_to_be_removed.extend(event_uuids_to_be_removed)

                # Get the events ending on the current period to be removed
                events_not_staying_ending_on_period = [event for event in events_during_period
                                                       if ((event.source.generation_time <= max_generation_time and event.source.priority <= max_priority)
                                                           or
                                                           (event.source.priority < max_priority)) and
                                                       event.start <= validity_start and
                                                       event.stop > validity_start and
                                                       event.stop <= validity_stop and
                                                       event.source_uuid != source_max_generation_time.source_uuid]
                
                # Get the events ending on the current period to be removed
                events_not_staying_not_ending_on_period = [event for event in events_during_period
                                                           if ((event.source.generation_time <= max_generation_time and event.source.priority <= max_priority)
                                                               or
                                                               (event.source.priority < max_priority)) and
                                                           event.start < validity_stop and
                                                           event.stop > validity_stop and
                                                           event.source_uuid != source_max_generation_time.source_uuid]
                
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
                            self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                   list_event_uuids_aliases,
                                                                                   start,
                                                                                   validity_start,
                                                                                   event)
                        # end if
                        if event.stop > validity_stop:
                            event.visible = False
                            list_split_events[event.event_uuid] = event
                        else:
                            list_events_to_be_removed.append(event.event_uuid)
                        # end if
                    elif event.event_uuid in list_split_events and event.stop <= validity_stop:
                        list_events_to_be_removed.append(event.event_uuid)
                        del list_split_events[event.event_uuid]
                    # end if
                # end for
            # end for

        # end for
        # Bulk insert events
        self.session.bulk_insert_mappings(Event, list_events_to_be_created["events"])
        # Bulk insert keys
        self.session.bulk_insert_mappings(EventKey, list_events_to_be_created["keys"])
        # Bulk insert links
        self._replicate_event_links(list_event_uuids_aliases, list_events_to_be_created["links"])
        self.session.bulk_insert_mappings(EventLink, list_events_to_be_created["links"])

        # Remove the events that were partially affected by the insert and erase operation
        self.session.query(EventLink).filter(EventLink.event_uuid_link.in_(list_events_to_be_removed)).delete(synchronize_session=False)
        self.session.query(Event).filter(Event.event_uuid.in_(list_events_to_be_removed)).delete(synchronize_session=False)

        # Bulk insert alerts
        self.session.bulk_insert_mappings(EventAlert, list_events_to_be_created["alerts"])

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

    @debug
    def _remove_deprecated_events_by_insert_and_erase_with_equal_or_lower_priority_at_event_level(self):
        """
        Method to remove events that were overwritten by other events due to insert and erase with equal or lower priority at event level insertion mode
        """
        list_events_to_be_created = {"events": [],
                                     "values": {},
                                     "alerts": [],
                                     "keys": [],
                                     "links": []}
        list_event_uuids_aliases = {}
        list_events_to_be_removed = []
        for gauge_uuid in self.insert_and_erase_with_equal_or_lower_priority_gauges:
            list_events_to_be_created_not_ending_on_period = {}
            list_split_events = {}

            # Get the sources of events intersecting the validity period defined for the current source
            sources = self.session.query(Source).join(Event).filter(Event.gauge_uuid == gauge_uuid,
                                                                       Source.priority != None,
                                                                    or_(and_(Source.validity_start < self.source.validity_stop,
                                                                             Source.validity_stop > self.source.validity_start),
                                                                        and_(Source.validity_start == self.source.validity_start,
                                                                             Source.validity_stop == self.source.validity_stop))).order_by(Event.start).all()
            events = sorted(set([event for source in sources for event in source.events if
                                 (event.gauge_uuid == gauge_uuid) and
                                 ((event.start < self.source.validity_stop and
                                  event.stop > self.source.validity_start) or
                                 (event.start == self.source.validity_start and
                                  event.stop == self.source.validity_stop))]), key=lambda event: event.start)

            # Get the timeline of validity periods intersecting
            timeline_points = set(list(chain.from_iterable([[source.validity_start,source.validity_stop] for source in sources])))

            filtered_timeline_points = [timestamp for timestamp in timeline_points if timestamp >= self.source.validity_start and timestamp <= self.source.validity_stop]

            sources_duration_0 = [source for source in sources if source.validity_start == source.validity_stop]
            sources_duration_0_added_stop = {}
            for source in sources_duration_0:
                # Insert only once the stop of the sources with duration 0
                if source.validity_stop not in sources_duration_0_added_stop:
                    sources_duration_0_added_stop[source.validity_stop] = None
                    filtered_timeline_points.append(source.validity_stop)
                # end if
            # end for
            
            # Sort list
            filtered_timeline_points.sort()

            events_per_timestamp = get_events_per_timestamp(events, filtered_timeline_points)

            sources_per_timestamp = get_sources_per_timestamp(sources, filtered_timeline_points)

            # Iterate through the periods
            next_timestamp = 1
            for timestamp in filtered_timeline_points:
                race_condition3()

                events_during_period = []
                if timestamp in events_per_timestamp:
                    events_during_period = events_per_timestamp[timestamp]
                # end if
                sources_during_period = []
                if timestamp in sources_per_timestamp:
                    sources_during_period = sources_per_timestamp[timestamp]
                # end if

                # Check if for the last period there are pending splits or events to be created not ending on previous periods
                if next_timestamp == len(filtered_timeline_points):
                    for event_uuid in list_split_events:
                        event = list_split_events[event_uuid]
                        self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                               list_event_uuids_aliases,
                                                                               timestamp,
                                                                               event.stop,
                                                                               event)
                        # Remove event
                        list_events_to_be_removed.append(event_uuid)
                    # end for

                    events_to_be_created_not_ending_on_period = [event for event in events_during_period if event.event_uuid in list_events_to_be_created_not_ending_on_period]

                    for event_uuid in list_events_to_be_created_not_ending_on_period:
                        # The event has to be created
                        event = [event for event in events_to_be_created_not_ending_on_period if event.event_uuid == event_uuid][0]
                        start = list_events_to_be_created_not_ending_on_period[event_uuid]
                        self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                               list_event_uuids_aliases,
                                                                               start,
                                                                               event.stop,
                                                                               event)

                        # Remove the original event
                        list_events_to_be_removed.append(event.event_uuid)

                    # end for
                    break
                # end if

                validity_start = timestamp
                validity_stop = filtered_timeline_points[next_timestamp]
                next_timestamp += 1

                # Get the maximum priority at this moment
                max_priority = max([source.priority for source in sources_during_period
                                           if source.priority >= self.source.priority and
                                           ((source.validity_start < validity_stop and
                                            source.validity_stop > validity_start)
                                           or
                                           (source.validity_start == validity_start and
                                            source.validity_stop == validity_stop))])
                
                # Get the maximum generation time at this moment
                max_generation_time = max([source.generation_time for source in sources_during_period
                                           if source.priority == max_priority and
                                           ((source.validity_start < validity_stop and
                                               source.validity_stop > validity_start)
                                           or
                                           (source.validity_start == validity_start and
                                            source.validity_stop == validity_stop))])
                
                # Get the related source
                source_max_generation_time = sorted(
                    [source for source in sources_during_period
                     if source.generation_time == max_generation_time and
                     source.priority == max_priority and
                     ((source.validity_start < validity_stop and
                       source.validity_stop > validity_start)
                      or
                      (source.validity_start == validity_start and
                       source.validity_stop == validity_stop))], key=lambda source: (source.ingestion_time is None, source.ingestion_time))[0]

                # Check if the period contains sources with the relevant events if not continue with the following period
                if not source_max_generation_time:
                    continue
                # end if
                
                # Events related to the source with the maximum generation time if the priority is lower or equal that the defined for the current source
                # Note this list will be empty if the source with maximum priority has a priority higher than the one defined by the current source
                events_max_generation_time = [event for event in events_during_period if event.source.source_uuid == source_max_generation_time.source_uuid and
                                              ((event.start < validity_stop and event.stop > validity_start)
                                               or
                                               (event.start == validity_start and event.stop == validity_stop))]
                
                # Review events with higher generation time in the period
                for event in events_max_generation_time:
                    if event.event_uuid in list_split_events:
                        if event.stop <= validity_stop:
                            self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                   list_event_uuids_aliases,
                                                                                   validity_start,
                                                                                   event.stop,
                                                                                   event)
                            list_events_to_be_removed.append(event.event_uuid)
                        else:
                            list_events_to_be_created_not_ending_on_period[event.event_uuid] = validity_start
                        # end if
                        del list_split_events[event.event_uuid]
                    else:
                        if event.event_uuid in list_events_to_be_created_not_ending_on_period:
                            if event.stop <= validity_stop:
                                # The event has to be created
                                start = list_events_to_be_created_not_ending_on_period[event.event_uuid]
                                self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                       list_event_uuids_aliases,
                                                                                       start,
                                                                                       event.stop,
                                                                                       event)

                                del list_events_to_be_created_not_ending_on_period[event.event_uuid]

                                # Remove the original event
                                list_events_to_be_removed.append(event.event_uuid)
                            # end if
                        else:
                            event.visible = True
                        # end if
                    # end if
                # end for

                # Delete deprecated events fully contained into the validity period
                event_uuids_to_be_removed = [event.event_uuid for event in events_during_period
                                             if event.source.priority <= self.source.priority and
                                             ((event.source.generation_time <= max_generation_time and event.source.priority <= max_priority)
                                                 or
                                                 (event.source.priority < max_priority)) and
                                             event.source.source_uuid != source_max_generation_time.source_uuid and
                                             event.start >= validity_start and event.stop <= validity_stop]
                
                list_events_to_be_removed.extend(event_uuids_to_be_removed)

                # Get the events ending on the current period to be removed
                events_not_staying_ending_on_period = [event for event in events_during_period
                                                       if event.source.priority <= self.source.priority and
                                                       ((event.source.generation_time <= max_generation_time and event.source.priority <= max_priority)
                                                           or
                                                           (event.source.priority < max_priority)) and

                                                       event.start <= validity_start and
                                                       event.stop > validity_start and
                                                       event.stop <= validity_stop and
                                                       event.source_uuid != source_max_generation_time.source_uuid]

                # Get the events ending on the current period to be removed
                events_not_staying_not_ending_on_period = [event for event in events_during_period
                                                           if event.source.priority <= self.source.priority and
                                                           ((event.source.generation_time <= max_generation_time and event.source.priority <= max_priority)
                                                               or
                                                               (event.source.priority < max_priority)) and
                                                           event.start < validity_stop and
                                                           event.stop > validity_stop and
                                                           event.source_uuid != source_max_generation_time.source_uuid]
                
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
                            self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                   list_event_uuids_aliases,
                                                                                   start,
                                                                                   validity_start,
                                                                                   event)
                        # end if
                        if event.stop > validity_stop:
                            event.visible = False
                            list_split_events[event.event_uuid] = event
                        else:
                            list_events_to_be_removed.append(event.event_uuid)
                        # end if
                    elif event.event_uuid in list_split_events and event.stop <= validity_stop:
                        list_events_to_be_removed.append(event.event_uuid)
                        del list_split_events[event.event_uuid]
                    # end if
                # end for
            # end for

        # end for
        # Bulk insert events
        self.session.bulk_insert_mappings(Event, list_events_to_be_created["events"])
        # Bulk insert keys
        self.session.bulk_insert_mappings(EventKey, list_events_to_be_created["keys"])
        # Bulk insert links
        self._replicate_event_links(list_event_uuids_aliases, list_events_to_be_created["links"])
        self.session.bulk_insert_mappings(EventLink, list_events_to_be_created["links"])

        # Remove the events that were partially affected by the insert and erase operation
        self.session.query(EventLink).filter(EventLink.event_uuid_link.in_(list_events_to_be_removed)).delete(synchronize_session=False)
        self.session.query(Event).filter(Event.event_uuid.in_(list_events_to_be_removed)).delete(synchronize_session=False)

        # Bulk insert alerts
        self.session.bulk_insert_mappings(EventAlert, list_events_to_be_created["alerts"])

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

    @debug
    def _remove_deprecated_events_by_insert_and_erase_intersected_events_with_priority_at_event_level(self):
        """
        Method to remove events that were overwritten by other events due to insert and erase with equal or lower priority and intersected by greater priority at event level insertion mode
        """
        list_events_to_be_created = {"events": [],
                                     "values": {},
                                     "alerts": [],
                                     "keys": [],
                                     "links": []}
        list_event_uuids_aliases = {}
        list_events_to_be_removed = []
        for gauge_uuid in self.insert_and_erase_intersected_events_with_priority_gauges:
            list_events_to_be_created_not_ending_on_period = {}
            list_split_events = {}

            # Get the events intersecting the validity period defined for the current source with priority set
            events = self.session.query(Event).join(Source).filter(Event.gauge_uuid == gauge_uuid,
                                                                   Source.priority != None,
                                                                   or_(and_(Event.start < self.source.validity_stop,
                                                                            Event.stop > self.source.validity_start),
                                                                       and_(Event.start == self.source.validity_start,
                                                                            Event.stop == self.source.validity_stop))).order_by(Event.start).all()
            sources = sorted(set([event.source for event in events]), key=lambda source: source.validity_start)
            
            # Get the timeline of event periods intersecting
            timeline_points = set(list(chain.from_iterable([[event.start,event.stop] for event in events])))

            filtered_timeline_points = [timestamp for timestamp in timeline_points if timestamp >= self.source.validity_start and timestamp <= self.source.validity_stop]

            events_duration_0 = [event for event in events if event.start == event.stop]
            events_duration_0_added_stop = {}
            for event in events_duration_0:
                # Insert only once the stop of the events with duration 0
                if event.stop not in events_duration_0_added_stop:
                    events_duration_0_added_stop[event.stop] = None
                    filtered_timeline_points.append(event.stop)
                # end if
            # end for
            
            # Sort list
            filtered_timeline_points.sort()

            events_per_timestamp = get_events_per_timestamp(events, filtered_timeline_points)
            sources_per_timestamp = get_sources_per_timestamp(sources, filtered_timeline_points, events_duration_0)
            
            # Iterate through the periods
            next_timestamp = 1
            for timestamp in filtered_timeline_points:
                events_during_period = []
                if timestamp in events_per_timestamp:
                    events_during_period = events_per_timestamp[timestamp]
                # end if
                sources_during_period = []
                if timestamp in sources_per_timestamp:
                    sources_during_period = sources_per_timestamp[timestamp]
                # end if
                
                # Check if for the last period there are pending splits or events to be created not ending on previous periods
                if next_timestamp == len(filtered_timeline_points):
                    for event_uuid in list_split_events:
                        event = list_split_events[event_uuid]
                        self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                               list_event_uuids_aliases,
                                                                               timestamp,
                                                                               event.stop,
                                                                               event)
                        # Remove event
                        list_events_to_be_removed.append(event_uuid)
                    # end for

                    events_to_be_created_not_ending_on_period = [event for event in events_during_period if event.event_uuid in list_events_to_be_created_not_ending_on_period]
                    for event_uuid in list_events_to_be_created_not_ending_on_period:
                        # The event has to be created
                        event = [event for event in events_to_be_created_not_ending_on_period if event.event_uuid == event_uuid][0]
                        start = list_events_to_be_created_not_ending_on_period[event_uuid]
                        self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                               list_event_uuids_aliases,
                                                                               start,
                                                                               event.stop,
                                                                               event)

                        # Remove the original event
                        list_events_to_be_removed.append(event.event_uuid)

                    # end for
                    break
                # end if

                validity_start = timestamp
                validity_stop = filtered_timeline_points[next_timestamp]
                next_timestamp += 1

                # Check if there are no events for this timestamp
                if len(events_during_period) < 1:
                    continue
                # end if

                # Get the maximum priority at this moment
                max_priority = max([source.priority for source in sources_during_period
                                    if
                                    (source.priority <= self.source.priority and
                                     ((source.validity_start < validity_stop and source.validity_stop > validity_start)
                                      or
                                      (source.validity_start == validity_start and source.validity_stop == validity_stop)))
                                    or
                                    (source.priority > self.source.priority and
                                     ((len([event for event in events_during_period if event.source.source_uuid == source.source_uuid and event.start < validity_stop and event.stop > validity_start]) > 0)
                                      or
                                      (len([event for event in events_during_period if event.source.source_uuid == source.source_uuid and event.start == validity_start and event.stop == validity_stop]) > 0)))])
                
                if max_priority and max_priority <= self.source.priority:
                    # Get the maximum generation time at this moment
                    max_generation_time = max([source.generation_time for source in sources_during_period if source.priority == max_priority and
                                               ((source.validity_start < validity_stop and source.validity_stop > validity_start)
                                               or
                                                (source.validity_start == validity_start and source.validity_stop == validity_stop))])

                    # Get the related source (sorting by ingestion time and setting sources with ingestion time None at the end)
                    source_max_generation_time = sorted([source for source in sources_during_period if source.generation_time == max_generation_time and
                                                  source.priority == max_priority and
                                                  ((source.validity_start < validity_stop and source.validity_stop > validity_start)
                                                   or
                                                   (source.validity_start == validity_start and source.validity_stop == validity_stop))], key=lambda source: (source.ingestion_time is None, source.ingestion_time))[0]
                elif max_priority and max_priority > self.source.priority:
                    # Get the maximum generation time at this moment
                    max_generation_time = max([source.generation_time for source in sources_during_period if source.priority == max_priority and
                                               ((len([event for event in events_during_period if event.source.source_uuid == source.source_uuid and event.start < validity_stop and event.stop > validity_start]) > 0)
                                                or
                                                (len([event for event in events_during_period if event.source.source_uuid == source.source_uuid and event.start == validity_start and event.stop == validity_stop]) > 0))])

                    # Get the related source
                    source_max_generation_time = sorted([source for source in sources_during_period if source.generation_time == max_generation_time and
                                                         source.priority == max_priority and
                                                         ((len([event for event in events_during_period if event.source.source_uuid == source.source_uuid and event.start < validity_stop and event.stop > validity_start]) > 0)
                                                          or
                                                          (len([event for event in events_during_period if event.source.source_uuid == source.source_uuid and event.start == validity_start and event.stop == validity_stop]) > 0))], key=lambda source: (source.ingestion_time is None, source.ingestion_time))[0]

                # end if

                # Check if there were no events to define the maximum priority in this period
                if not max_priority:
                    continue
                # end if
                                
                # Events related to the source with the maximum generation time if the priority is lower or equal that the defined for the current source
                # Note this list will be empty if the source with maximum priority has a priority higher than the one defined by the current source
                events_max_generation_time = [event for event in events_during_period if event.source.priority <= self.source.priority and
                                              event.source.source_uuid == source_max_generation_time.source_uuid and
                                              ((event.start < validity_stop and event.stop > validity_start)
                                              or
                                               (event.start == validity_start and event.stop == validity_stop))]

                # Review events with higher generation time in the period
                for event in events_max_generation_time:
                    if event.event_uuid in list_split_events:
                        if event.stop <= validity_stop:
                            self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                   list_event_uuids_aliases,
                                                                                   validity_start,
                                                                                   event.stop,
                                                                                   event)
                            list_events_to_be_removed.append(event.event_uuid)
                        else:
                            list_events_to_be_created_not_ending_on_period[event.event_uuid] = validity_start
                        # end if
                        del list_split_events[event.event_uuid]
                    else:
                        if event.event_uuid in list_events_to_be_created_not_ending_on_period:
                            if event.stop <= validity_stop:
                                # The event has to be created
                                start = list_events_to_be_created_not_ending_on_period[event.event_uuid]
                                self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                       list_event_uuids_aliases,
                                                                                       start,
                                                                                       event.stop,
                                                                                       event)

                                del list_events_to_be_created_not_ending_on_period[event.event_uuid]

                                # Remove the original event
                                list_events_to_be_removed.append(event.event_uuid)
                            # end if
                        else:
                            event.visible = True
                        # end if
                    # end if
                # end for

                # Delete deprecated events fully contained into the validity period
                event_uuids_to_be_removed = [event.event_uuid for event in events_during_period
                                             if event.source.priority <= self.source.priority and
                                             ((event.source.generation_time <= max_generation_time and event.source.priority <= max_priority)
                                                 or
                                                 (event.source.priority < max_priority)) and
                                             event.source.source_uuid != source_max_generation_time.source_uuid and
                                             event.start >= validity_start and event.stop <= validity_stop]
                
                list_events_to_be_removed.extend(event_uuids_to_be_removed)

                # Get the events ending on the current period to be removed
                events_not_staying_ending_on_period = [event for event in events_during_period if event.source.priority <= self.source.priority and
                                                       ((event.source.generation_time <= max_generation_time and event.source.priority <= max_priority)
                                                        or
                                                        (event.source.priority < max_priority)) and
                                                       event.start <= validity_start and
                                                       event.stop > validity_start and
                                                       event.stop <= validity_stop and
                                                       event.source_uuid != source_max_generation_time.source_uuid]

                # Get the events ending on the current period to be removed
                events_not_staying_not_ending_on_period = [event for event in events_during_period if event.source.priority <= self.source.priority and
                                                       ((event.source.generation_time <= max_generation_time and event.source.priority <= max_priority)
                                                        or
                                                        (event.source.priority < max_priority)) and
                                                       event.start < validity_stop and
                                                       event.stop > validity_stop and
                                                       event.source_uuid != source_max_generation_time.source_uuid]
                
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
                            self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                   list_event_uuids_aliases,
                                                                                   start,
                                                                                   validity_start,
                                                                                   event)
                        # end if
                        if event.stop > validity_stop:
                            event.visible = False
                            list_split_events[event.event_uuid] = event
                        else:
                            list_events_to_be_removed.append(event.event_uuid)
                        # end if
                    elif event.event_uuid in list_split_events and event.stop <= validity_stop:
                        list_events_to_be_removed.append(event.event_uuid)
                        del list_split_events[event.event_uuid]
                    # end if
                # end for
            # end for

        # end for
        # Bulk insert events
        self.session.bulk_insert_mappings(Event, list_events_to_be_created["events"])
        # Bulk insert keys
        self.session.bulk_insert_mappings(EventKey, list_events_to_be_created["keys"])
        # Bulk insert links
        self._replicate_event_links(list_event_uuids_aliases, list_events_to_be_created["links"])
        self.session.bulk_insert_mappings(EventLink, list_events_to_be_created["links"])

        # Remove the events that were partially affected by the insert and erase operation
        self.session.query(EventLink).filter(EventLink.event_uuid_link.in_(list_events_to_be_removed)).delete(synchronize_session=False)
        self.session.query(Event).filter(Event.event_uuid.in_(list_events_to_be_removed)).delete(synchronize_session=False)

        # Bulk insert alerts
        self.session.bulk_insert_mappings(EventAlert, list_events_to_be_created["alerts"])

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

    @debug
    def _replicate_event_values(self, from_event_uuid, to_event_uuid, to_event, list_values_to_be_created):
        """
        Method to replicate the values associated to events that were overwritten partially by other events

        :param from_event_uuid: original event where to get the associated values from
        :type from_event_uuid: uuid
        :param to_event_uuid: new event UUID to associate the values
        :type to_event_uuid: uuid
        :param to_event: new event to associate the values
        :type to_event: Event
        :param list_values_to_be_created: list of values to be stored later inside the DDBB
        :type list_values_to_be_created: list
        """
        if "REPLICATE_EVENT_VALUES_MODULE" in config and config["REPLICATE_EVENT_VALUES_MODULE"] != "":
            try:
                processor_module = import_module(config["REPLICATE_EVENT_VALUES_MODULE"])
                # Use an auxiliar list to be resiliant to failures when calling to replicate with no external module
                list_values_to_be_created_aux = {}                
                processor_module.replicate_event_values(self.query, from_event_uuid, to_event_uuid, to_event, list_values_to_be_created_aux)
                for type in list_values_to_be_created_aux:
                    if not type in list_values_to_be_created:
                        list_values_to_be_created[type] = []
                    # end if
                    for value in list_values_to_be_created_aux[type]:
                        list_values_to_be_created[type].append(value)
                    # end for
                # end for
            except ImportError as e:
                logger.error("The specified module {} for replicating the values of events does not exist. Returned error: {}".format(config["REPLICATE_EVENT_VALUES_MODULE"], str(e)))
                self._replicate_event_values_no_external_module(from_event_uuid, to_event_uuid, list_values_to_be_created)
            except Exception as e:
                logger.error("An error occurred when calling to the method replicate_event_values of the specified module {}. Returned error: {}".format(config["REPLICATE_EVENT_VALUES_MODULE"], str(e)))
                self._replicate_event_values_no_external_module(from_event_uuid, to_event_uuid, list_values_to_be_created)
        else:
            self._replicate_event_values_no_external_module(from_event_uuid, to_event_uuid, list_values_to_be_created)
        # end if
        
        return


    @debug
    def _replicate_event_values_no_external_module(self, from_event_uuid, to_event_uuid, list_values_to_be_created):
        """
        Method to replicate the values associated to events that were overwritten partially by other events

        :param from_event_uuid: original event where to get the associated values from
        :type from_event_uuid: uuid
        :param to_event_uuid: new event to associate the values
        :type to_event_uuid: uuid
        :param list_values_to_be_created: list of values to be stored later inside the DDBB
        :type list_values_to_be_created: list
        """
        values = self.query.get_event_values(event_uuids = [from_event_uuid])
        for value in values:
            if not type(value) in list_values_to_be_created:
                list_values_to_be_created[type(value)] = []
            # end if
            value_to_insert = {"event_uuid": to_event_uuid,
                               "name": value.name,
                               "position": value.position,
                               "parent_level": value.parent_level,
                               "parent_position": value.parent_position
            }
            if not type(value) in (EventObject, EventGeometry):
                value_to_insert["value"] = value.value
            elif type(value) == EventGeometry:
                value_to_insert["value"] = to_shape(value.value).wkt
            # end if
            list_values_to_be_created[type(value)].append(value_to_insert)
        # end for
        
        return

    @debug
    def _replicate_event_alerts(self, from_event_uuid, to_event_uuid, list_alerts_to_be_created):
        """
        Method to replicate the alerts associated to events that were overwritten partially by other events

        :param from_event_uuid: original event where to get the associated values from
        :type from_event_uuid: uuid
        :param to_event_uuid: new event UUID to associate the values
        :type to_event_uuid: uuid
        :param list_alerts_to_be_created: list of alerts to be stored later inside the DDBB
        :type list_alerts_to_be_created: list
        """
        alerts = self.query.get_event_alerts(event_uuids = {"filter": [from_event_uuid], "op": "in"})
        for alert in alerts:
            id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
            alert_to_insert = {
                "event_alert_uuid": id,
                "message": alert.message,
                "validated": alert.validated,
                "ingestion_time": alert.ingestion_time,
                "generator": alert.generator,
                "notified": alert.notified,
                "solved": alert.solved,
                "solved_time": alert.solved_time,
                "notification_time": alert.notification_time,
                "justification": alert.justification,
                "alert_uuid": alert.alertDefinition.alert_uuid,
                "event_uuid": to_event_uuid
            }
            list_alerts_to_be_created.append(alert_to_insert)
        # end for
        
        return
    
    def _create_event_uuid_alias(self, event_uuid, alias, list_event_uuids_aliases):
        """
        """
        if not event_uuid in list_event_uuids_aliases:
            list_event_uuids_aliases[event_uuid] = []
        # end if
        list_event_uuids_aliases[event_uuid].append(alias)
        
        return

    def _replicate_event_links(self, list_event_uuids_aliases, list_links_to_be_created):
        """
        Method to replicate the links associated to events that were overwritten partially by other events

        :param list_event_uuids_aliases: list of the aliases of the event_uuids
        :type list_event_uuids_aliases: list
        """
        for event_uuid in list_event_uuids_aliases:

            # Annotate aliases for links creation at the end of the ingestion of all DIMs
            if event_uuid not in self.dict_event_uuids_aliases:
                self.dict_event_uuids_aliases[event_uuid] = []
            # end if
            self.dict_event_uuids_aliases[event_uuid].extend(list_event_uuids_aliases[event_uuid])
            
            # Get links that point to it
            links = self.query.get_event_links(event_uuids = {"filter": [event_uuid], "op": "in"})
            for link in links:
                for alias in list_event_uuids_aliases[event_uuid]:
                    if link.event_uuid_link in list_event_uuids_aliases:
                        aliases_to_point = list_event_uuids_aliases[link.event_uuid_link]
                    else:
                        aliases_to_point = [link.event_uuid_link]
                    # end if
                    for alias_to_point in aliases_to_point:
                        list_links_to_be_created.append(dict(name = link.name,
                                                             event_uuid_link = alias_to_point,
                                                             event_uuid = alias))
                    # end for
                # end for
            # end for

            # Get links that point from it
            links = self.session.query(EventLink).filter(EventLink.event_uuid_link == event_uuid,
                                                         EventLink.event_uuid.notin_(list(list_event_uuids_aliases.keys()))).all()
            for link in links:
                for alias in list_event_uuids_aliases[event_uuid]:
                    list_links_to_be_created.append(dict(name = link.name,
                                                         event_uuid_link = alias,
                                                         event_uuid = link.event_uuid))
                # end for
                # Remove the link as the event_uuid_link is going to disappear
                self.session.delete(link)
            # end for
            
        # end for
        return

    def _replicate_event_keys(self, from_event_uuid, to_event_uuid, list_keys_to_be_created):
        """
        Method to replicate the keys associated to events that were overwritten partially by other events

        :param from_event_uuid: original event where to get the associated values from
        :type from_event_uuid: uuid
        :param to_event_uuid: new event to associate the values
        :type to_event_uuid: uuid
        :param list_keys_to_be_created: list of keys to be stored later inside the DDBB
        :type list_keys_to_be_created: list
        """
        keys = self.query.get_event_keys(event_uuids = {"filter": [from_event_uuid], "op": "in"})
        for key in keys:
            list_keys_to_be_created.append(dict(event_key = key.event_key,
                                                event_uuid = to_event_uuid,
                                                visible = True,
                                                dim_signature_uuid = key.dim_signature_uuid))
        # end for
        
        return

    @debug
    def _remove_deprecated_events_by_insert_and_erase_per_event(self):
        """
        Method to remove events that were overwritten by other events due to INSERT and ERASE per EVENT insertion mode
        """

        for gauge_uuid in self.insert_and_erase_per_event_gauges:
            for segment in self.insert_and_erase_per_event_gauges[gauge_uuid]:
                list_events_to_be_created = {"events": [],
                                             "values": {},
                                             "alerts": [],
                                             "keys": [],
                                             "links": []}
                list_event_uuids_aliases = {}
                list_events_to_be_removed = []

                list_events_to_be_created_not_ending_on_period = {}
                list_split_events = {}

                segment_start = parser.parse(segment[0])
                segment_stop = parser.parse(segment[1])
                # Get the events intersecting the segment
                events = self.session.query(Event).filter(Event.gauge_uuid == gauge_uuid,
                                                          or_(and_(Event.start < segment_stop,
                                                                   Event.stop > segment_start),
                                                              and_(Event.start == segment_start,
                                                                   Event.stop == segment_stop))).order_by(Event.start).all()
                sources = sorted(set([event.source for event in events]), key=lambda source: source.validity_start)
                # Get the timeline of validity periods intersecting
                timeline_points = set(list(chain.from_iterable([[event.start,event.stop] for event in events])))

                filtered_timeline_points = [timestamp for timestamp in timeline_points if timestamp >= segment_start and timestamp <= segment_stop]

                events_duration_0 = [event for event in events if event.start == event.stop]
                events_duration_0_added_stop = {}
                for event in events_duration_0:
                    # Insert only once the stop of the events with duration 0
                    if event.stop not in events_duration_0_added_stop:
                        events_duration_0_added_stop[event.stop] = None
                        filtered_timeline_points.append(event.stop)
                    # end if
                # end for

                # Sort list
                filtered_timeline_points.sort()

                events_per_timestamp = get_events_per_timestamp(events, filtered_timeline_points)

                sources_per_timestamp = get_sources_per_timestamp(sources, filtered_timeline_points, events_duration_0)

                # Iterate through the periods
                next_timestamp = 1
                for timestamp in filtered_timeline_points:
                    race_condition3()

                    events_during_period = []
                    if timestamp in events_per_timestamp:
                        events_during_period = events_per_timestamp[timestamp]
                    # end if
                    sources_during_period = []
                    if timestamp in sources_per_timestamp:
                        sources_during_period = sources_per_timestamp[timestamp]
                    # end if
                    
                    # Check if for the last period there are pending splits
                    if next_timestamp == len(filtered_timeline_points):
                        for event_uuid in list_split_events:
                            event = list_split_events[event_uuid]
                            self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                   list_event_uuids_aliases,
                                                                                   timestamp,
                                                                                   event.stop,
                                                                                   event)
                            # Remove event
                            list_events_to_be_removed.append(event_uuid)
                        # end for
                        events_to_be_created_not_ending_on_period = [event for event in events_during_period if event.event_uuid in list_events_to_be_created_not_ending_on_period]
                        for event_uuid in list_events_to_be_created_not_ending_on_period:
                            # The event has to be created
                            event = [event for event in events_to_be_created_not_ending_on_period if event.event_uuid == event_uuid][0]
                            start = list_events_to_be_created_not_ending_on_period[event_uuid]
                            self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                   list_event_uuids_aliases,
                                                                                   start,
                                                                                   event.stop,
                                                                                   event)

                            # Remove the original event
                            list_events_to_be_removed.append(event.event_uuid)

                        # end for
                        break
                    # end if

                    validity_start = timestamp
                    validity_stop = filtered_timeline_points[next_timestamp]
                    next_timestamp += 1
                    # Get the maximum generation time at this moment
                    max_generation_time = max([source.generation_time for source in sources_during_period
                                        if ((len([event for event in events_during_period if event.source.source_uuid == source.source_uuid and event.start < validity_stop and event.stop > validity_start]) > 0)
                                            or
                                            (len([event for event in events_during_period if event.source.source_uuid == source.source_uuid and event.start == validity_start and event.stop == validity_stop]) > 0))])

                    # Get the related source
                    source_max_generation_time = sorted([source for source in sources_during_period if source.generation_time == max_generation_time and
                                                         ((len([event for event in events_during_period if event.source.source_uuid == source.source_uuid and event.start < validity_stop and event.stop > validity_start]) > 0)
                                                          or
                                                          (len([event for event in events_during_period if event.source.source_uuid == source.source_uuid and event.start == validity_start and event.stop == validity_stop]) > 0))], key=lambda source: (source.ingestion_time is None, source.ingestion_time))[0]

                    # Events related to the DIM processing with the maximum generation time
                    events_max_generation_time = [event for event in events_during_period if event.source.source_uuid == source_max_generation_time.source_uuid and
                                                  ((event.start < validity_stop and event.stop > validity_start)
                                                   or
                                                   (event.start == validity_start and event.stop == validity_stop))]

                    # Review events with higher generation time in the period
                    for event in events_max_generation_time:
                        if event.event_uuid in list_split_events:
                            if event.stop <= validity_stop:
                                self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                       list_event_uuids_aliases,
                                                                                       validity_start,
                                                                                       event.stop,
                                                                                       event)
                                list_events_to_be_removed.append(event.event_uuid)
                            else:
                                list_events_to_be_created_not_ending_on_period[event.event_uuid] = validity_start
                            # end if
                            del list_split_events[event.event_uuid]
                        else:
                            if event.event_uuid in list_events_to_be_created_not_ending_on_period:
                                if event.stop <= validity_stop:
                                    # The event has to be created
                                    start = list_events_to_be_created_not_ending_on_period[event.event_uuid]
                                    self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                           list_event_uuids_aliases,
                                                                                           start,
                                                                                           event.stop,
                                                                                           event)

                                    del list_events_to_be_created_not_ending_on_period[event.event_uuid]

                                    # Remove the original event
                                    list_events_to_be_removed.append(event.event_uuid)
                                # end if
                            else:
                                event.visible = True
                            # end if
                        # end if
                    # end for

                    # Delete deprecated events fully contained into the validity period
                    event_uuids_to_be_removed = [event.event_uuid for event in events_during_period
                                                 if event.source.generation_time <= max_generation_time and
                                                 event.source.source_uuid != source_max_generation_time.source_uuid and
                                                 event.start >= validity_start and event.stop <= validity_stop]
                
                    list_events_to_be_removed.extend(event_uuids_to_be_removed)

                    # Get the events ending on the current period to be removed
                    events_not_staying_ending_on_period = [event for event in events_during_period
                                                           if event.source.generation_time <= max_generation_time and
                                                           event.start <= validity_start and
                                                           event.stop > validity_start and
                                                           event.stop <= validity_stop and
                                                           event.source_uuid != source_max_generation_time.source_uuid]
                                        
                    # Get the events not ending on the current period to be removed
                    events_not_staying_not_ending_on_period = [event for event in events_during_period if event.source.generation_time <= max_generation_time and
                                                               event.start < validity_stop and
                                                               event.stop > validity_stop and
                                                               event.source_uuid != source_max_generation_time.source_uuid]

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
                                self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                       list_event_uuids_aliases,
                                                                                       start,
                                                                                       validity_start,
                                                                                       event)
                            # end if
                            if event.stop > validity_stop:
                                event.visible = False
                                list_split_events[event.event_uuid] = event
                            else:
                                list_events_to_be_removed.append(event.event_uuid)
                            # end if
                        elif event.event_uuid in list_split_events and event.stop <= validity_stop:
                            list_events_to_be_removed.append(event.event_uuid)
                            del list_split_events[event.event_uuid]
                        # end if
                    # end for
                # end for

                # Bulk insert events
                self.session.bulk_insert_mappings(Event, list_events_to_be_created["events"])
                # Bulk insert keys
                self.session.bulk_insert_mappings(EventKey, list_events_to_be_created["keys"])
                # Bulk insert links
                self._replicate_event_links(list_event_uuids_aliases, list_events_to_be_created["links"])
                self.session.bulk_insert_mappings(EventLink, list_events_to_be_created["links"])

                # Remove the events that were partially affected by the insert and erase operation
                self.session.query(EventLink).filter(EventLink.event_uuid_link.in_(list_events_to_be_removed)).delete(synchronize_session=False)
                self.session.query(Event).filter(Event.event_uuid.in_(list_events_to_be_removed)).delete(synchronize_session=False)

                # Bulk insert alerts
                self.session.bulk_insert_mappings(EventAlert, list_events_to_be_created["alerts"])

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

            # end for
        # end for

        return

    @debug
    def _remove_deprecated_events_by_insert_and_erase_per_event_with_priority(self):
        """
        Method to remove events that were overwritten by other events due to INSERT and ERASE per EVENT with priority insertion mode
        """

        for gauge_uuid in self.insert_and_erase_per_event_with_priority_gauges:
            for segment in self.insert_and_erase_per_event_with_priority_gauges[gauge_uuid]:
                list_events_to_be_created = {"events": [],
                                             "values": {},
                                             "alerts": [],
                                             "keys": [],
                                             "links": []}
                list_event_uuids_aliases = {}
                list_events_to_be_removed = []

                list_events_to_be_created_not_ending_on_period = {}
                list_split_events = {}

                segment_start = parser.parse(segment[0])
                segment_stop = parser.parse(segment[1])
                # Get the events intersecting the segment
                events = self.session.query(Event).join(Source).filter(Event.gauge_uuid == gauge_uuid,
                                                                       Source.priority != None,
                                                                       or_(and_(Event.start < segment_stop,
                                                                                Event.stop > segment_start),
                                                                           and_(Event.start == segment_start,
                                                                                Event.stop == segment_stop))).order_by(Event.start).all()
                sources = sorted(set([event.source for event in events]), key=lambda source: source.validity_start)
                
                # Get the timeline of validity periods intersecting
                timeline_points = set(list(chain.from_iterable([[event.start,event.stop] for event in events])))

                filtered_timeline_points = [timestamp for timestamp in timeline_points if timestamp >= segment_start and timestamp <= segment_stop]

                events_duration_0 = [event for event in events if event.start == event.stop]
                events_duration_0_added_stop = {}
                for event in events_duration_0:
                    # Insert only once the stop of the events with duration 0
                    if event.stop not in events_duration_0_added_stop:
                        events_duration_0_added_stop[event.stop] = None
                        filtered_timeline_points.append(event.stop)
                    # end if
                # end for

                # Sort list
                filtered_timeline_points.sort()

                events_per_timestamp = get_events_per_timestamp(events, filtered_timeline_points)
                sources_per_timestamp = get_sources_per_timestamp(sources, filtered_timeline_points, events_duration_0)

                # Iterate through the periods
                next_timestamp = 1
                for timestamp in filtered_timeline_points:
                    race_condition3()

                    events_during_period = []
                    if timestamp in events_per_timestamp:
                        events_during_period = events_per_timestamp[timestamp]
                    # end if
                    sources_during_period = []
                    if timestamp in sources_per_timestamp:
                        sources_during_period = sources_per_timestamp[timestamp]
                    # end if
                    
                    # Check if for the last period there are pending splits
                    if next_timestamp == len(filtered_timeline_points):
                        for event_uuid in list_split_events:
                            event = list_split_events[event_uuid]
                            self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                   list_event_uuids_aliases,
                                                                                   timestamp,
                                                                                   event.stop,
                                                                                   event)
                            # Remove event
                            list_events_to_be_removed.append(event_uuid)
                        # end for
                        events_to_be_created_not_ending_on_period = [event for event in events_during_period if event.event_uuid in list_events_to_be_created_not_ending_on_period]

                        for event_uuid in list_events_to_be_created_not_ending_on_period:
                            # The event has to be created
                            event = [event for event in events_to_be_created_not_ending_on_period if event.event_uuid == event_uuid][0]
                            start = list_events_to_be_created_not_ending_on_period[event_uuid]
                            self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                   list_event_uuids_aliases,
                                                                                   start,
                                                                                   event.stop,
                                                                                   event)

                            # Remove the original event
                            list_events_to_be_removed.append(event.event_uuid)

                        # end for
                        break
                    # end if

                    validity_start = timestamp
                    validity_stop = filtered_timeline_points[next_timestamp]
                    next_timestamp += 1

                    # Get the maximum priority at this moment
                    max_priority = max([source.priority for source in sources_during_period
                                               if source.priority >= self.source.priority and
                                               ((len([event for event in events_during_period if event.source.source_uuid == source.source_uuid and event.start < validity_stop and event.stop > validity_start]) > 0)
                                                or
                                                (len([event for event in events_during_period if event.source.source_uuid == source.source_uuid and event.start == validity_start and event.stop == validity_stop]) > 0))])
                    
                    # Get the maximum generation time at this moment
                    max_generation_time = max([source.generation_time for source in sources_during_period
                                               if source.priority == max_priority and
                                               ((len([event for event in events_during_period if event.source.source_uuid == source.source_uuid and event.start < validity_stop and event.stop > validity_start]) > 0)
                                                or
                                                (len([event for event in events_during_period if event.source.source_uuid == source.source_uuid and event.start == validity_start and event.stop == validity_stop]) > 0))])

                    # Get the related source
                    source_max_generation_time = sorted([source for source in sources_during_period if source.generation_time == max_generation_time and
                                                          source.priority == max_priority and
                                                         ((len([event for event in events_during_period if event.source.source_uuid == source.source_uuid and event.start < validity_stop and event.stop > validity_start]) > 0)
                                                          or
                                                          (len([event for event in events_during_period if event.source.source_uuid == source.source_uuid and event.start == validity_start and event.stop == validity_stop]) > 0))], key=lambda source: (source.ingestion_time is None, source.ingestion_time))[0]

                    # Events related to the DIM processing with the maximum generation time
                    events_max_generation_time = [event for event in events_during_period if event.source.source_uuid == source_max_generation_time.source_uuid and
                                                  ((event.start < validity_stop and event.stop > validity_start)
                                                   or
                                                   (event.start == validity_start and event.stop == validity_stop))]

                    # Review events with higher generation time in the period
                    for event in events_max_generation_time:
                        if event.event_uuid in list_split_events:
                            if event.stop <= validity_stop:
                                self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                       list_event_uuids_aliases,
                                                                                       validity_start,
                                                                                       event.stop,
                                                                                       event)
                                list_events_to_be_removed.append(event.event_uuid)
                            else:
                                list_events_to_be_created_not_ending_on_period[event.event_uuid] = validity_start
                            # end if
                            del list_split_events[event.event_uuid]
                        else:
                            if event.event_uuid in list_events_to_be_created_not_ending_on_period:
                                if event.stop <= validity_stop:
                                    # The event has to be created
                                    start = list_events_to_be_created_not_ending_on_period[event.event_uuid]
                                    self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                           list_event_uuids_aliases,
                                                                                           start,
                                                                                           event.stop,
                                                                                           event)

                                    del list_events_to_be_created_not_ending_on_period[event.event_uuid]

                                    # Remove the original event
                                    list_events_to_be_removed.append(event.event_uuid)
                                # end if
                            else:
                                event.visible = True
                            # end if
                        # end if
                    # end for

                    # Delete deprecated events fully contained into the validity period
                    event_uuids_to_be_removed = [event.event_uuid for event in events_during_period
                                                 if ((event.source.generation_time <= max_generation_time and event.source.priority <= max_priority)
                                                     or
                                                     (event.source.priority < max_priority)) and
                                                 event.source.source_uuid != source_max_generation_time.source_uuid and
                                                 event.start >= validity_start and event.stop <= validity_stop]

                    list_events_to_be_removed.extend(event_uuids_to_be_removed)

                    # Get the events ending on the current period to be removed
                    events_not_staying_ending_on_period = [event for event in events_during_period
                                                           if ((event.source.generation_time <= max_generation_time and event.source.priority <= max_priority)
                                                               or
                                                               (event.source.priority < max_priority)) and
                                                           event.start <= validity_start and
                                                           event.stop > validity_start and
                                                           event.stop <= validity_stop and
                                                           event.source_uuid != source_max_generation_time.source_uuid]

                    # Get the events not ending on the current period to be removed
                    events_not_staying_not_ending_on_period = [event for event in events_during_period
                                                               if ((event.source.generation_time <= max_generation_time and event.source.priority <= max_priority)
                                                                   or
                                                                   (event.source.priority < max_priority)) and
                                                               event.start < validity_stop and
                                                               event.stop > validity_stop and
                                                               event.source_uuid != source_max_generation_time.source_uuid]
                    
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
                                self._create_event_for_remove_deprecated_events_method(list_events_to_be_created,
                                                                                       list_event_uuids_aliases,
                                                                                       start,
                                                                                       validity_start,
                                                                                       event)
                            # end if
                            if event.stop > validity_stop:
                                event.visible = False
                                list_split_events[event.event_uuid] = event
                            else:
                                list_events_to_be_removed.append(event.event_uuid)
                            # end if
                        elif event.event_uuid in list_split_events and event.stop <= validity_stop:
                            list_events_to_be_removed.append(event.event_uuid)
                            del list_split_events[event.event_uuid]
                        # end if
                    # end for
                # end for

                # Bulk insert events
                self.session.bulk_insert_mappings(Event, list_events_to_be_created["events"])
                # Bulk insert keys
                self.session.bulk_insert_mappings(EventKey, list_events_to_be_created["keys"])
                # Bulk insert links
                self._replicate_event_links(list_event_uuids_aliases, list_events_to_be_created["links"])
                self.session.bulk_insert_mappings(EventLink, list_events_to_be_created["links"])

                # Remove the events that were partially affected by the insert and erase operation
                self.session.query(EventLink).filter(EventLink.event_uuid_link.in_(list_events_to_be_removed)).delete(synchronize_session=False)
                self.session.query(Event).filter(Event.event_uuid.in_(list_events_to_be_removed)).delete(synchronize_session=False)

                # Bulk insert alerts
                self.session.bulk_insert_mappings(EventAlert, list_events_to_be_created["alerts"])

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

            # end for
        # end for

        return

    @debug
    def _remove_deprecated_events_event_keys(self):
        """
        Method to remove events that were overwritten by other events due to EVENT_KEYS insertion mode
        """
        for key_pair in self.keys_events:
            key = key_pair[0]
            dim_signature_uuid = key_pair[1]

            max_generation_time = self.session.query(func.max(Source.generation_time)).join(Event).join(EventKey).filter(EventKey.event_key == key, Source.dim_signature_uuid == dim_signature_uuid).first()

            event_max_generation_time = self.session.query(Event).join(Source).join(EventKey).filter(Source.generation_time == max_generation_time,
                                                                                                            EventKey.event_key == key,
                                                                                                            Source.dim_signature_uuid == dim_signature_uuid).order_by(Source.ingestion_time.nullslast()).first()

            # Delete deprecated events
            events_uuids_to_delete = self.session.query(Event.event_uuid).join(Source).join(EventKey).filter(Event.source_uuid != event_max_generation_time.source_uuid,
                                                                                                                    Source.generation_time <= max_generation_time,
                                                                                                                    EventKey.event_key == key,
                                                                                                                    Source.dim_signature_uuid == dim_signature_uuid)
            self.session.query(EventLink).filter(EventLink.event_uuid_link.in_(events_uuids_to_delete)).delete(synchronize_session=False)
            self.session.query(Event).filter(Event.event_uuid.in_(events_uuids_to_delete)).delete(synchronize_session=False)

            # Make events visible
            events_uuids_to_update = self.session.query(Event.event_uuid).join(EventKey).filter(Event.source_uuid == event_max_generation_time.source_uuid,
                                                                                                EventKey.event_key == key)
            self.session.query(EventKey).filter(EventKey.event_uuid.in_(events_uuids_to_update)).update({"visible": True}, synchronize_session=False)
            self.session.query(Event).filter(Event.event_uuid.in_(events_uuids_to_update)).update({"visible": True}, synchronize_session=False)
        # end for

        return

    @debug
    def _remove_deprecated_events_event_keys_with_priority(self):
        """
        Method to remove events that were overwritten by other events due to EVENT_KEYS_with_PRIORITY insertion mode
        """
        for key_pair in self.keys_events_with_priority:
            key = key_pair[0]
            dim_signature_uuid = key_pair[1]

            max_priority = self.session.query(func.max(Source.priority)).join(Event).join(EventKey).filter(Source.priority >= self.source.priority,
                                                                                                                  EventKey.event_key == key,
                                                                                                                  Source.dim_signature_uuid == dim_signature_uuid).first()
            max_generation_time = self.session.query(func.max(Source.generation_time)).join(Event).join(EventKey).filter(Source.priority == max_priority,
                                                                                                                         EventKey.event_key == key,
                                                                                                                         Source.dim_signature_uuid == dim_signature_uuid).first()

            event_max_generation_time = self.session.query(Event).join(Source).join(EventKey).filter(Source.priority == max_priority,
                                                                                                     Source.generation_time == max_generation_time,
                                                                                                     EventKey.event_key == key,
                                                                                                     Source.dim_signature_uuid == dim_signature_uuid).order_by(Source.ingestion_time.nullslast()).first()

            # Delete deprecated events
            events_uuids_to_delete = self.session.query(Event.event_uuid).join(Source).join(EventKey).filter(Event.source_uuid != event_max_generation_time.source_uuid,
                                                                                                             Source.priority <= max_priority,
                                                                                                             Source.generation_time <= max_generation_time,
                                                                                                             EventKey.event_key == key,
                                                                                                             Source.dim_signature_uuid == dim_signature_uuid)
            self.session.query(EventLink).filter(EventLink.event_uuid_link.in_(events_uuids_to_delete)).delete(synchronize_session=False)
            self.session.query(Event).filter(Event.event_uuid.in_(events_uuids_to_delete)).delete(synchronize_session=False)

            # Make events visible
            events_uuids_to_update = self.session.query(Event.event_uuid).join(EventKey).filter(Event.source_uuid == event_max_generation_time.source_uuid,
                                                                                                EventKey.event_key == key)
            self.session.query(EventKey).filter(EventKey.event_uuid.in_(events_uuids_to_update)).update({"visible": True}, synchronize_session=False)
            self.session.query(Event).filter(Event.event_uuid.in_(events_uuids_to_update)).update({"visible": True}, synchronize_session=False)
        # end for

        return

    @debug
    def _remove_deprecated_annotations(self):
        """
        Method to remove annotations that were overwritten by other annotations
        """
        annotations_uuids_to_delete = []
        annotations_uuids_to_update = []
        for annotation_cnf_explicit_ref in self.annotation_cnfs_explicit_refs:
            annotation_cnf = annotation_cnf_explicit_ref["annotation_cnf"]
            explicit_ref = annotation_cnf_explicit_ref["explicit_ref"]

            max_generation_time = self.session.query(func.max(Source.generation_time)).join(Annotation) \
                                                                                      .join(AnnotationCnf). \
                                                                                      join(ExplicitRef).filter(AnnotationCnf.annotation_cnf_uuid == annotation_cnf.annotation_cnf_uuid,
                                                                                                               ExplicitRef.explicit_ref_uuid == explicit_ref.explicit_ref_uuid).first()

            source_max_generation_time = self.session.query(Source).join(Annotation) \
                                                                        .join(AnnotationCnf). \
                                                                        join(ExplicitRef).filter(AnnotationCnf.annotation_cnf_uuid == annotation_cnf.annotation_cnf_uuid,
                                                                                            ExplicitRef.explicit_ref_uuid == explicit_ref.explicit_ref_uuid,
                                                                                            Source.generation_time == max_generation_time).order_by(Source.ingestion_time.nullslast()).first()

            annotations_uuids_to_update += self.session.query(Annotation.annotation_uuid) \
                                                      .join(AnnotationCnf). \
                                                      join(ExplicitRef).filter(AnnotationCnf.annotation_cnf_uuid == annotation_cnf.annotation_cnf_uuid,
                                                                               ExplicitRef.explicit_ref_uuid == explicit_ref.explicit_ref_uuid,
                                                                               Annotation.source_uuid == source_max_generation_time.source_uuid).all()

            annotations_uuids_to_delete += self.session.query(Annotation.annotation_uuid) \
                                                      .join(AnnotationCnf). \
                                                      join(ExplicitRef).filter(AnnotationCnf.annotation_cnf_uuid == annotation_cnf.annotation_cnf_uuid,
                                                                               ExplicitRef.explicit_ref_uuid == explicit_ref.explicit_ref_uuid,
                                                                               Annotation.source_uuid != source_max_generation_time.source_uuid).all()

        # end for
        if len(annotations_uuids_to_delete) > 0:
            self.session.query(Annotation).filter(Annotation.annotation_uuid.in_(annotations_uuids_to_delete)).delete(synchronize_session=False)
        # end if

        if len(annotations_uuids_to_update) > 0:
            self.session.query(Annotation).filter(Annotation.annotation_uuid.in_(annotations_uuids_to_update)).update({"visible": True}, synchronize_session=False)
        # end if

        return

    @debug
    def _remove_deprecated_annotations_insert_and_erase_with_priority(self):
        """
        Method to remove annotations that were overwritten by other annotations using INSERT_and_ERASE_with_PRIORITY
        """
        annotations_uuids_to_delete = []
        annotations_uuids_to_update = []
        for annotation_cnf_explicit_ref in self.annotation_cnfs_explicit_refs_insert_and_erase_with_priority:
            annotation_cnf = annotation_cnf_explicit_ref["annotation_cnf"]
            explicit_ref = annotation_cnf_explicit_ref["explicit_ref"]

            max_priority = self.session.query(func.max(Source.priority)).join(Annotation) \
                                                                        .join(AnnotationCnf). \
                                                                        join(ExplicitRef).filter(Source.priority >= self.source.priority,
                                                                                                 AnnotationCnf.annotation_cnf_uuid == annotation_cnf.annotation_cnf_uuid,
                                                                                                 ExplicitRef.explicit_ref_uuid == explicit_ref.explicit_ref_uuid).first()
            
            max_generation_time = self.session.query(func.max(Source.generation_time)).join(Annotation) \
                                                                                      .join(AnnotationCnf). \
                                                                                      join(ExplicitRef).filter(Source.priority == max_priority,
                                                                                                               AnnotationCnf.annotation_cnf_uuid == annotation_cnf.annotation_cnf_uuid,
                                                                                                               ExplicitRef.explicit_ref_uuid == explicit_ref.explicit_ref_uuid).first()

            source_max_generation_time = self.session.query(Source).join(Annotation) \
                                                                        .join(AnnotationCnf). \
                                                                        join(ExplicitRef).filter(Source.generation_time == max_generation_time,
                                                                                               Source.priority == max_priority,
                                                                                               AnnotationCnf.annotation_cnf_uuid == annotation_cnf.annotation_cnf_uuid,
                                                                                            ExplicitRef.explicit_ref_uuid == explicit_ref.explicit_ref_uuid).order_by(Source.ingestion_time.nullslast()).first()

            annotations_uuids_to_update += self.session.query(Annotation.annotation_uuid) \
                                                      .join(AnnotationCnf). \
                                                      join(ExplicitRef).filter(AnnotationCnf.annotation_cnf_uuid == annotation_cnf.annotation_cnf_uuid,
                                                                               ExplicitRef.explicit_ref_uuid == explicit_ref.explicit_ref_uuid,
                                                                               Annotation.source_uuid == source_max_generation_time.source_uuid).all()

            annotations_uuids_to_delete += self.session.query(Annotation.annotation_uuid) \
                                                      .join(AnnotationCnf). \
                                                      join(ExplicitRef). \
                                                      join(Source).filter(Source.priority <= max_priority,
                                                                          AnnotationCnf.annotation_cnf_uuid == annotation_cnf.annotation_cnf_uuid,
                                                                               ExplicitRef.explicit_ref_uuid == explicit_ref.explicit_ref_uuid,
                                                                               Annotation.source_uuid != source_max_generation_time.source_uuid).all()

        # end for
        if len(annotations_uuids_to_delete) > 0:
            self.session.query(Annotation).filter(Annotation.annotation_uuid.in_(annotations_uuids_to_delete)).delete(synchronize_session=False)
        # end if

        if len(annotations_uuids_to_update) > 0:
            self.session.query(Annotation).filter(Annotation.annotation_uuid.in_(annotations_uuids_to_update)).update({"visible": True}, synchronize_session=False)
        # end if

        return
    
    def insert_event_value(self, event_uuid, value):
        """
        Method to associate a value structure to an event related to the UUID received in the position specified

        IMPORTANT!!! This method performs the commit

        :param event_uuid: event to assiciate the value to
        :type from_event_uuid: uuid
        :param value: structure of a value to be inserted
        :type value: dict

        """
        exit_status = {
            "error": False,
            "inserted": False,
            "status": "VALUE_WAS_INGESTED_BY_OTHER_PROCESS"
        }

        # Validate the structure of the value received
        try:
            parsing.validate_values([value])
        except ErrorParsingDictionary as e:
            logger.error(str(e))
            exit_status = {
                "error": True,
                "inserted": False,
                "status": "ERROR_PARSING"
            }
            return exit_status
        # end try

        entity_uuid = {"name": "event_uuid",
                       "id": event_uuid
                   }

        value_name = value["name"]
        value_entity = EventObject
        if value["type"] == "boolean":
            value_entity = EventBoolean
        elif value["type"] == "text":
            value_entity = EventText
        elif value["type"] == "double":
            value_entity = EventDouble
        elif value["type"] == "timestamp":
            value_entity = EventTimestamp
        elif value["type"] == "geometrie":
            value_entity = EventGeometry
        # end if
        event = self.session.query(Event).filter(Event.event_uuid == event_uuid).first()
        item = self.session.query(value_entity).filter(value_entity.parent_level == -1, value_entity.parent_position == 0, value_entity.name == value_name, value_entity.event_uuid == event_uuid).first()
        while not item and event:
            list_values = {}
            event_values = self.query.get_event_values(event_uuids = [event_uuid])
            event_values_first_level = [value for value in event_values if value.parent_level == -1 and value.parent_position == 0]
            self._insert_values([value], entity_uuid, list_values, position = len(event_values_first_level), parent_level = -1, parent_position = 0)

            continue_with_values_ingestion = True
            self.session.begin_nested()
            # Bulk insert values
            if "objects" in list_values:
                try:
                    race_condition()
                    self.session.bulk_insert_mappings(EventObject, list_values["objects"])
                except IntegrityError:
                    continue_with_values_ingestion = False
                    # The object has not been ingested because of two possible reasons:
                    # 1. The values with the same name have been inserted by another process
                    # 2. The position specified has been taken by another process
                    self.session.rollback()
                # end try
            # end if
            try:
                if "booleans" in list_values and continue_with_values_ingestion:
                    self.session.bulk_insert_mappings(EventBoolean, list_values["booleans"])
                # end if
                if "texts" in list_values and continue_with_values_ingestion:
                    self.session.bulk_insert_mappings(EventText, list_values["texts"])
                # end if
                if "doubles" in list_values and continue_with_values_ingestion:
                    self.session.bulk_insert_mappings(EventDouble, list_values["doubles"])
                # end if
                if "timestamps" in list_values and continue_with_values_ingestion:
                    self.session.bulk_insert_mappings(EventTimestamp, list_values["timestamps"])
                # end if
                if "geometries" in list_values and continue_with_values_ingestion:
                    try:
                        self.session.bulk_insert_mappings(EventGeometry, list_values["geometries"])
                    except InternalError as e:
                        self.session.rollback()
                        logger.error("The values specified define a wrong geometry. The exception raised has been the following {}".format(e))
                        exit_status = {
                            "error": True,
                            "inserted": False,
                            "status": "VALUE_DEFINES_WRONG_GEOMETRY"
                        }
                        return exit_status
                # end if
            except IntegrityError as e:
                self.session.rollback()
                logger.error("The values specified define duplicated entities inside the same value. The exception raised has been the following {}".format(e))
                exit_status = {
                    "error": True,
                    "inserted": False,
                    "status": "DUPLICATED_VALUES"
                }
                return exit_status
            # end try

            if continue_with_values_ingestion:
                # Two commits because of nested
                self.session.commit()
                self.session.commit()
                exit_status = {
                    "error": False,
                    "inserted": True,
                    "status": "OK"
                }
            # end if

            event = self.session.query(Event).filter(Event.event_uuid == event_uuid).first()
            item = self.session.query(value_entity).filter(value_entity.parent_level == -1, value_entity.parent_position == 0, value_entity.name == value_name, value_entity.event_uuid == event_uuid).first()
        # end while

        if not event:
            exit_status = {
                "error": False,
                "inserted": False,
                "status": "EVENT_DOES_NOT_EXIST"
            }
        # end if
        
        return exit_status

    def close_session (self):
        """
        Method to close the session
        """
        self.session.close()
        self.session_progress.close()
        return

    def geometries_to_wkt(self, geometries):
        """
        Method to return the WKT values of the received geometries
        """
        returned_geometries = []
        for geometry in geometries:
            returned_geometries.append({
                "value": to_shape(geometry.value).wkt,
                "name": geometry.name
            })
        # end for

        return returned_geometries

def insert_source_status(session, source, status, error = False, message = None):
    """
    Method to insert the DIM processing status

    :param status: code indicating the status of the processing of the file
    :type status: int
    :param message: error message generated when the ingestion does not finish correctly
    :type message: str
    """
    id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
    # Insert processing status
    status = SourceStatus(id,datetime.datetime.now(),status,source)
    session.add(status)

    if message:
        # Insert the error message
        status.log = message
    # end if

    if error:
        # Flag the ingestion error
        source.ingestion_error = True
    # end if

    session.commit()

    return

def get_events_per_timestamp(events, timestamps):
    """
    Method to associate events to the timestamps (starts of segments specified by the timestamps).
    An event is associated to a timestamp:
    if (start < next timestamp and stop > timestamp) or
    (start == timestamp and stop == next timestamp)

    PRE:
    - The list of events is ordered by start
    - The list of timestamps is the list of start and stop values associated to the events without duplications except for the case of having events with duration 0 in which case there will be only one duplication

    Note:
    1. Given the following scenario:
    Timestamps: |         |         |
                          |
    Event1:     [         ]
    Event2:               |
    Event3:               [         ]
    Event4:     [                   ]

    Result:
    Timestamp:  |
    Events:     Event1, Event4
    Timestamp:            |
    Events:     Event2, Event3, Event4

    :param events: list of events to associate to the timestamps
    :type events: list
    :param timestamps: list of timestamps corresponding to the start and stop values of the received events
    :type timestamps: list

    :return: events_per_timestamp
    :rtype: dictionary (key: timestamp, value: list of events associated)
    """

    events_per_timestamp = {}
    timeline_points_iterator = 0
    for event in events:
        specific_timeline_points_iterator = timeline_points_iterator
        specific_covered_timestamps = {}
        while specific_timeline_points_iterator < len(timestamps):
            timestamp = timestamps[specific_timeline_points_iterator]

            # Check if the event has to be assigned to the penultimate timestamp (latest timestamp is not used)
            if specific_timeline_points_iterator == len(timestamps) - 1:
                if event.stop > timestamp:
                    if timestamp not in events_per_timestamp:
                        events_per_timestamp[timestamp] = []
                    # end if
                    if timestamp not in specific_covered_timestamps:
                        events_per_timestamp[timestamp].append(event)
                        specific_covered_timestamps[timestamp] = None
                    # end if
                # end if
                break
            # end if

            next_timestamp = timestamps[specific_timeline_points_iterator + 1]
            specific_timeline_points_iterator += 1
            # Improve performances by moving to the next timestamp for all events if the current event start is already >= next_timestamp
            if event.start > next_timestamp:
                timeline_points_iterator += 1
            # end if

            # Check that the event can still be allocated in the timeline if not continue with the following event
            if event.stop <= timestamp and event.start < timestamp:
                break
            # end if

            if (event.start < next_timestamp and event.stop > timestamp) or \
               (event.start == timestamp and event.stop == next_timestamp):
                if timestamp not in events_per_timestamp:
                    events_per_timestamp[timestamp] = []
                # end if
                if timestamp not in specific_covered_timestamps:
                    events_per_timestamp[timestamp].append(event)
                    specific_covered_timestamps[timestamp] = None
                # end if
            # end if

        # end while
    # end for

    return events_per_timestamp

def get_sources_per_timestamp(sources, timestamps, events_duration_0 = None):
    """
    Method to associate sources to the timestamps (starts of segments specified by the timestamps).
    A source is associated to a timestamp:
    if (validity_start < next timestamp and validity_stop > timestamp) or
    (validity_start == timestamp and validity_stop == next timestamp) or
    there is an associated event with duration 0 and event.start == timestamp

    PRE:
    - The list of sources is ordered by validity_start
    - The list of timestamps is the list of validity_start and validity_stop values associated to the sources without duplications except for the case of having sources with duration 0 in which case there will be only one duplication

    Note:
    1. Given the following scenario (if events_duration_0 != None)):
    Timestamps: |         |         |
                          |
    Source1:    [         ]
    Event1:               |           --> This will associate Source1 to the second timestamp
    Source2:              |
    Source3:              [         ]
    Source4:    [                   ]

    Result:
    Timestamp:  |
    Sources:     Source1, Source4
    Timestamp:            |
    Sources:     Source2, Source3, Source4

    :param sources: list of sources to associate to the timestamps
    :type sources: list
    :param timestamps: list of timestamps corresponding to the start and stop values of the received sources
    :type timestamps: list
    :param events_duration_0: list of events related to the sources with duration 0
    :type events_duration_0: list

    :return: sources_per_timestamp
    :rtype: dictionary (key: timestamp, value: list of sources associated)
    """

    sources_per_timestamp = {}
    timeline_points_iterator = 0
    for source in sources:
        specific_timeline_points_iterator = timeline_points_iterator
        specific_covered_timestamps = {}
        while specific_timeline_points_iterator < len(timestamps):
            timestamp = timestamps[specific_timeline_points_iterator]

            if events_duration_0 != None:
                # Associate source to the timestamp if it has an event with duration 0 and start == timestamp
                associated_events_with_duration_0 = [event for event in events_duration_0 if event.source.source_uuid == source.source_uuid and event.start == timestamp]
                if len(associated_events_with_duration_0) > 0:
                    if timestamp not in sources_per_timestamp:
                        sources_per_timestamp[timestamp] = []
                    # end if
                    if timestamp not in specific_covered_timestamps:
                        sources_per_timestamp[timestamp].append(source)
                        specific_covered_timestamps[timestamp] = None
                    # end if
                # end if
            # end if

            # Check if the source has to be assigned to the penultimate timestamp (latest timestamp is not used)
            if specific_timeline_points_iterator == len(timestamps) - 1:
                if source.validity_stop > timestamp:
                    if timestamp not in sources_per_timestamp:
                        sources_per_timestamp[timestamp] = []
                    # end if
                    if timestamp not in specific_covered_timestamps:
                        sources_per_timestamp[timestamp].append(source)
                        specific_covered_timestamps[timestamp] = None
                    # end if
                # end if
                break
            # end if

            next_timestamp = timestamps[specific_timeline_points_iterator + 1]
            specific_timeline_points_iterator += 1
            # Improve performances by moving to the next timestamp for all events if the current event start is already >= next_timestamp
            if source.validity_start > next_timestamp:
                timeline_points_iterator += 1
            # end if

            # Check that the source can still be allocated in the timeline if not continue with the following source
            if source.validity_stop <= timestamp and source.validity_start < timestamp:
                break
            # end if

            if (source.validity_start < next_timestamp and source.validity_stop > timestamp) or \
               (source.validity_start == timestamp and source.validity_stop == next_timestamp):
                if timestamp not in sources_per_timestamp:
                    sources_per_timestamp[timestamp] = []
                # end if
                if timestamp not in specific_covered_timestamps:
                    sources_per_timestamp[timestamp].append(source)
                    specific_covered_timestamps[timestamp] = None
                # end if
            # end if

        # end while
    # end for
    
    return sources_per_timestamp
