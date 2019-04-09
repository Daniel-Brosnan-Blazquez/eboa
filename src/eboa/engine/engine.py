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

# Import SQLalchemy entities
from sqlalchemy.exc import IntegrityError, InternalError
from sqlalchemy.sql import func

# Import GEOalchemy entities
from geoalchemy2 import functions
from geoalchemy2.shape import to_shape

# Import exceptions
from eboa.engine.errors import LinksInconsistency, UndefinedEventLink, DuplicatedEventLinkRef, WrongPeriod, SourceAlreadyIngested, WrongValue, OddNumberOfCoordinates, EboaResourcesPathNotAvailable, WrongGeometry, ErrorParsingDictionary, DuplicatedValues

# Import datamodel
from eboa.datamodel.base import Session
from eboa.datamodel.dim_signatures import DimSignature
from eboa.datamodel.alerts import Alert
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
from eboa.debugging import debug, race_condition

# Import auxiliary functions
from eboa.engine.functions import get_resources_path, read_configuration

config = read_configuration()
eboa_resources_path = get_resources_path()

logging = Log()
logger = logging.logger

exit_codes = {
    "OK": {
        "status": 0,
        "message": "The source file {} associated to the DIM signature {} and DIM processing {} with version {} has ingested correctly {} event/s and {} annotation/s"
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
        "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} contains an event which links to an undefined reference identifier {}"
    },
    "WRONG_VALUE": {
        "status": 7,
        "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} contains an event/annotation which defines the value {} that cannot be converted to the specified type {}"
    },
    "ODD_NUMBER_OF_COORDINATES": {
        "status": 8,
        "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} contains an event/annotation which defines the geometry value {} with an odd number of coordinates"
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
        "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} defines links between events which lead to clashing unique values into the DDBB. The exception raised has been the following: {}"
    },
    "DUPLICATED_VALUES": {
        "status": 13,
        "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} defines duplicated values inside the same entity. The exception raised has been the following: {}"
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
        self.session = Session()
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
            error_message = exit_codes["FILE_NOT_VALID"]["message"].format(json_name)
            self._insert_source_without_dim_signature(json_name)
            self._insert_source_status(exit_codes["FILE_NOT_VALID"]["status"], error_message = error_message)
            self.source.parse_error = "The json file cannot be loaded as it has a wrong structure"
            # Insert the content of the file into the DDBB
            with open(json_path) as input_file:
                self.source.content_text = input_file.read()
            self.session.commit()
            # Log the error
            logger.error(error_message)
            return exit_codes["FILE_NOT_VALID"]["status"]
        # end try

        if check_schema:
            try:
                parsing.validate_data_dictionary(data)
            except ErrorParsingDictionary as e:
                self._insert_source_without_dim_signature(json_name)
                self._insert_source_status(exit_codes["FILE_NOT_VALID"]["status"], error_message = str(e))
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
            self._insert_source_status(exit_codes["FILE_NOT_VALID"]["status"], error_message = str(e))
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
            schema_path = eboa_resources_path + "/" + config["RELATIVE_XML_SCHEMA_PATH"]
            parsed_schema = etree.parse(schema_path)
            schema = etree.XMLSchema(parsed_schema)
            valid = schema.validate(parsed_xml)
            if not valid:
                error_message = exit_codes["FILE_NOT_VALID"]["message"].format(xml_name)
                self._insert_source_without_dim_signature(xml_name)
                self._insert_source_status(exit_codes["FILE_NOT_VALID"]["status"], error_message = error_message)
                # Insert the parse error into the DDBB
                self.source.parse_error = str(schema.error_log.last_error)
                # Insert the content of the file into the DDBB
                with open(xml_path,"r") as xml_file:
                    self.source.content_text = xml_file.read()
                self.session.commit()
                # Log the error
                logger.error(error_message)
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

    #####################
    # INSERTION METHODS #
    #####################
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
                self._insert_source_status(exit_codes["FILE_NOT_VALID"]["status"], error_message = str(e))
                self.source.parse_error = str(e)
                self.session.commit()
            # end if
            logger.error(str(e))
            return False
        # end try

        return True

    def treat_data(self, data = None, source = None, validate = True):
        """
        Method to treat the data stored in self.data

        :param data: structure of data to treat
        :type data: dict 
        :param source: name of the source of the data
        :type source: str
        :param validate: flag to indicate if the schema check has to be performed
        :type validate: bool

        :return: exit_codes for every operation with the associated information (DIM signature, processor and source)
        :rtype: list of dictionaries
        """
        if data != None:
            self.data = data
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
            returned_value = -1
            self.all_gauges_for_insert_and_erase = False
            if self.operation.get("mode") == "insert_and_erase":
                self.all_gauges_for_insert_and_erase = True
            # end if

            if self.operation.get("mode") == "insert" or self.operation.get("mode") == "insert_and_erase":
                returned_value = self._insert_data()
                returned_information = {
                    "source": self.operation.get("source").get("name"),
                    "dim_signature": self.operation.get("dim_signature").get("name"),
                    "processor": self.operation.get("dim_signature").get("exec"),
                    "status": returned_value
                }
                returned_values.append(returned_information)
            # end if
        # end for
        return returned_values

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
        self.annotation_cnfs_explicit_refs = []
        self.annotations = {}
        self.keys_events = {}

        return

    @debug
    def _insert_data(self):
        """
        Method to insert the data into the DDBB for an operation of mode insert
        """
        # Initialize context
        self._initialize_context_insert_data()
        
        # Insert the DIM signature
        self._insert_dim_signature()
        try:
            self._insert_source()
            self.ingestion_start = datetime.datetime.now()
            self._insert_source_status(exit_codes["INGESTION_STARTED"]["status"])
            # Log that the ingestion of the source file has been started
            logger.info(exit_codes["INGESTION_STARTED"]["message"].format(
                self.source.name,
                self.dim_signature.dim_signature,
                self.source.processor, 
                self.source.processor_version))
        except SourceAlreadyIngested as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["SOURCE_ALREADY_INGESTED"]["status"], error_message = str(e))
            # Log that the source file has been already been processed
            logger.error(e)
            self.session.commit()
            return exit_codes["SOURCE_ALREADY_INGESTED"]["status"]
        except WrongPeriod as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["WRONG_SOURCE_PERIOD"]["status"], error_message = str(e))
            # Log that the source file has a wrong specified period as the stop is lower than the start
            logger.error(e)
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            return exit_codes["WRONG_SOURCE_PERIOD"]["status"]
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

        self.session.begin_nested()
        # Insert events
        try:
            self._insert_events()
        except DuplicatedEventLinkRef as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["DUPLICATED_EVENT_LINK_REF"]["status"], error_message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            # Log the error
            logger.error(e)
            return exit_codes["DUPLICATED_EVENT_LINK_REF"]["status"]
        except LinksInconsistency as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["LINKS_INCONSISTENCY"]["status"], error_message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            # Log the error
            logger.error(e)
            return exit_codes["LINKS_INCONSISTENCY"]["status"]
        except UndefinedEventLink as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["UNDEFINED_EVENT_LINK_REF"]["status"], error_message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            # Log the error
            logger.error(e)
            return exit_codes["UNDEFINED_EVENT_LINK_REF"]["status"]
        except WrongPeriod as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["WRONG_EVENT_PERIOD"]["status"], error_message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            # Log the error
            logger.error(e)
            return exit_codes["WRONG_EVENT_PERIOD"]["status"]
        except WrongValue as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["WRONG_VALUE"]["status"], error_message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            # Log the error
            logger.error(e)
            return exit_codes["WRONG_VALUE"]["status"]
        except OddNumberOfCoordinates as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["ODD_NUMBER_OF_COORDINATES"]["status"], error_message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            # Log the error
            logger.error(e)
            return exit_codes["ODD_NUMBER_OF_COORDINATES"]["status"]
        except WrongGeometry as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["WRONG_GEOMETRY"]["status"], error_message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            # Log the error
            logger.error(e)
            return exit_codes["WRONG_GEOMETRY"]["status"]
        except DuplicatedValues as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["DUPLICATED_VALUES"]["status"], error_message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            # Log the error
            logger.error(e)
            return exit_codes["DUPLICATED_VALUES"]["status"]
        # end try

        # Insert annotations
        try:
            self._insert_annotations()
        except WrongValue as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["WRONG_VALUE"]["status"], error_message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            # Log the error
            logger.error(e)
            return exit_codes["WRONG_VALUE"]["status"]
        except OddNumberOfCoordinates as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["ODD_NUMBER_OF_COORDINATES"]["status"], error_message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            # Log the error
            logger.error(e)
            return exit_codes["ODD_NUMBER_OF_COORDINATES"]["status"]
        except WrongGeometry as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["WRONG_GEOMETRY"]["status"], error_message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            # Log the error
            logger.error(e)
            return exit_codes["WRONG_GEOMETRY"]["status"]
        except DuplicatedValues as e:
            self.session.rollback()
            self._insert_source_status(exit_codes["DUPLICATED_VALUES"]["status"], error_message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            # Log the error
            logger.error(e)
            return exit_codes["DUPLICATED_VALUES"]["status"]
        # end try

        # Review the inserted events and annotations for removing the
        # information that is deprecated
        self._remove_deprecated_data()

        n_events = 0
        if "events" in self.operation:
            n_events = len(self.operation.get("events"))
        # end if

        n_annotations = 0
        if "annotations" in self.operation:
            n_annotations = len(self.operation.get("annotations"))
        # end if

        # Log that the file has been ingested correctly
        self._insert_source_status(exit_codes["OK"]["status"],True)
        logger.info(exit_codes["OK"]["message"].format(
            self.source.name,
            self.dim_signature.dim_signature,
            self.source.processor, 
            self.source.processor_version,
            n_events,
            n_annotations))

        # Remove if the content was inserted due to errors processing the input
        self.source.content_json = None
        
        # Commit data
        self.session.commit()
        return exit_codes["OK"]["status"]
        
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

        if hasattr(self, "all_gauges_for_insert_and_erase") and self.all_gauges_for_insert_and_erase:
            for gauge in self.dim_signature.gauges:
                self.insert_and_erase_gauges[gauge.gauge_uuid] = None
            # end for
        # end if

        return

    @debug
    def _insert_source(self):
        """
        Method to insert the DIM processing
        """
        version = self.operation.get("dim_signature").get("version")
        processor = self.operation.get("dim_signature").get("exec")
        source = self.operation.get("source")
        name = source.get("name")
        generation_time = source.get("generation_time")
        validity_start = source.get("validity_start")
        validity_stop = source.get("validity_stop")

        id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        if parser.parse(validity_stop) < parser.parse(validity_start):
            # The validity period is not correct (stop > start)
            # Create Source for registering the error in the DDBB
            self.source = Source(id, name, generation_time,
                                        version, self.dim_signature, processor = processor)
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
            raise WrongPeriod(exit_codes["WRONG_SOURCE_PERIOD"]["message"].format(name, self.dim_signature.dim_signature, processor, version, validity_stop, validity_start))
        # end if

        self.source = self.session.query(Source).filter(Source.name == name,
                                                               Source.dim_signature_uuid == self.dim_signature.dim_signature_uuid,
                                                               Source.processor_version == version,
                Source.processor == processor).first()
        if self.source and self.source.ingestion_duration:
            # The source has been already ingested
            raise SourceAlreadyIngested(exit_codes["SOURCE_ALREADY_INGESTED"]["message"].format(name,
                                                                                                     self.dim_signature.dim_signature,
                                                                                                     processor, 
                                                                                                     version))
        elif self.source:
            # Upadte the information
            self.source.validity_start = validity_start
            self.source.validity_stop = validity_stop
            self.source.generation_time = generation_time
            return
        # end if

        self.source = Source(id, name,
                                    generation_time, version, self.dim_signature,
                                    validity_start, validity_stop, processor = processor)
        self.session.add(self.source)
        try:
            race_condition()
            self.session.commit()
        except IntegrityError:
            # The DIM processing has been ingested between the query and the insertion
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
            self.source = Source(id, name)
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
        gauges = [(event.get("gauge").get("name"), event.get("gauge").get("system"))  for event in self.operation.get("events") or []]
        unique_gauges = set(gauges)
        for gauge in unique_gauges:
            name = gauge[0]
            system = gauge[1]
            self.gauges[(name,system)] = self.session.query(Gauge).filter(Gauge.name == name, Gauge.system == system, Gauge.dim_signature_uuid == self.dim_signature.dim_signature_uuid).first()
            if not self.gauges[(name,system)]:
                self.session.begin_nested()
                id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                descriptions = [event.get("gauge").get("description") for event in self.operation.get("events") or [] if event.get("gauge").get("name") == name and event.get("gauge").get("system") == system]
                if len(descriptions) > 0:
                    description = descriptions[0]
                # end if
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
        annotation_cnfs = [(annotation.get("annotation_cnf").get("name"), annotation.get("annotation_cnf").get("system"))  for annotation in self.operation.get("annotations") or []]
        unique_annotation_cnfs = set(annotation_cnfs)
        for annotation in unique_annotation_cnfs:
            name = annotation[0]
            system = annotation[1]
            self.annotation_cnfs[(name,system)] = self.session.query(AnnotationCnf).filter(AnnotationCnf.name == name, AnnotationCnf.system == system, AnnotationCnf.dim_signature_uuid == self.dim_signature.dim_signature_uuid).first()
            if not self.annotation_cnfs[(name,system)]:
                self.session.begin_nested()
                id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                descriptions = [event.get("annotation_cnf").get("description") for event in self.operation.get("annotations") or [] if event.get("annotation_cnf").get("name") == name and event.get("annotation_cnf").get("system") == system]
                if len(descriptions) > 0:
                    description = descriptions[0]
                # end if
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
        for explicit_ref in self.operation.get("explicit_references") or []:
            if "group" in explicit_ref:
                self.session.begin_nested()
                id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                expl_group_ddbb = ExplicitRefGrp(id, explicit_ref.get("group"))
                self.session.add(expl_group_ddbb)
                try:
                    race_condition()
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

    @debug
    def _insert_explicit_refs(self):
        """
        Method to insert the explicit references
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
        # end for

        return
        
    @debug
    def _insert_links_explicit_refs(self):
        """
        Method to insert the links between explicit references
        """
        list_explicit_reference_links = []
        for explicit_ref in [i for i in self.operation.get("explicit_references") or [] if i.get("links")]:
            for link in explicit_ref.get("links") or []:
                explicit_ref1 = self.explicit_refs[explicit_ref.get("name")]
                explicit_ref2 = self.explicit_refs[link.get("link")]
                link_ddbb = self.session.query(ExplicitRefLink).filter(ExplicitRefLink.explicit_ref_uuid_link == explicit_ref1.explicit_ref_uuid, ExplicitRefLink.name == link.get("name"), ExplicitRefLink.explicit_ref_uuid == explicit_ref2.explicit_ref_uuid).first()
                if not link_ddbb:
                    self.session.begin_nested()
                    self.session.add(ExplicitRefLink(explicit_ref1.explicit_ref_uuid, link.get("name"), explicit_ref2))
                    try:
                        race_condition()
                        self.session.commit()
                    except IntegrityError:
                        # The link exists already into DDBB
                        self.session.rollback()
                        pass
                    # end try
                # end if
                # Insert the back ref if specified
                if link.get("back_ref"):
                    link_ddbb = self.session.query(ExplicitRefLink).filter(ExplicitRefLink.explicit_ref_uuid_link == explicit_ref2.explicit_ref_uuid, ExplicitRefLink.name == link.get("back_ref"), ExplicitRefLink.explicit_ref_uuid == explicit_ref1.explicit_ref_uuid).first()
                    if not link_ddbb:
                        self.session.begin_nested()
                        self.session.add(ExplicitRefLink(explicit_ref2.explicit_ref_uuid, link.get("back_ref"), explicit_ref1))
                        try:
                            race_condition()
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
            start = parser.parse(start)
        # end if
        if not type(stop) == datetime.datetime:
            stop = parser.parse(stop)
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
        list_event_links_by_ref = {}
        list_event_link_refs = {}
        list_event_links_by_uuid_ddbb = []
        list_values = {}
        for event in self.operation.get("events") or []:
            id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
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
            elif gauge_info["insertion_type"] == "EVENT_KEYS":
                self.keys_events[(key, str(self.dim_signature.dim_signature_uuid))] = None
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
                self._insert_values(event.get("values")[0], entity_uuid, list_values)
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
                        if not link["link"] in list_event_links_by_ref:
                            list_event_links_by_ref[link["link"]] = []
                        # end if
                        list_event_links_by_ref[link["link"]].append({"name": link["name"],
                                                                      "event_uuid": id,
                                                                      "back_ref": back_ref,
                                                                      "back_ref_name": back_ref_name})
                    else:
                        list_event_links_by_uuid_ddbb.append(dict(event_uuid_link = id,
                                                                  name = link["name"],
                                                                  event_uuid = link["link"]))
                        if back_ref:
                            list_event_links_by_uuid_ddbb.append(dict(event_uuid_link = link["link"],
                                                                      name = back_ref_name,
                                                                      event_uuid = id))
                        # end if
                    # end if
                # end for
            # end if
            if "link_ref" in event:
                if event["link_ref"] in list_event_link_refs:
                    # The same link identifier has been specified in more than one event
                    self.session.rollback()
                    raise DuplicatedEventLinkRef(exit_codes["DUPLICATED_EVENT_LINK_REF"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version, event["link_ref"]))
                # end if
                list_event_link_refs[event["link_ref"]] = id
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

        # Insert links by reference
        list_event_links_by_ref_ddbb = []
        for link_ref in list_event_links_by_ref:
            if not link_ref in list_event_link_refs:
                # There has not been defined this link reference in any event
                self.session.rollback()
                raise UndefinedEventLink(exit_codes["UNDEFINED_EVENT_LINK_REF"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version, link_ref))
            # end if
            event_uuid = list_event_link_refs[link_ref]
            
            for link in list_event_links_by_ref[link_ref]:
                list_event_links_by_ref_ddbb.append(dict(event_uuid_link = link["event_uuid"],
                                                         name = link["name"],
                                                         event_uuid = event_uuid))
                if link["back_ref"]:
                    list_event_links_by_ref_ddbb.append(dict(event_uuid_link = event_uuid,
                                                             name = link["back_ref_name"],
                                                             event_uuid = link["event_uuid"]))
                # end if
            # end for
        # end for

        # Bulk insert links
        list_event_links_ddbb = list_event_links_by_ref_ddbb + list_event_links_by_uuid_ddbb
        try:
            self.session.bulk_insert_mappings(EventLink, list_event_links_ddbb)
        except IntegrityError as e:
            self.session.rollback()
            raise LinksInconsistency(exit_codes["LINKS_INCONSISTENCY"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version, e))
        # end if

        # Commit data
        self.session.commit()

        return

    def _insert_values(self, values, entity_uuid, list_values, level_position = 0, parent_level = -1, parent_level_position = 0, level_positions = None):
        """
        Method to insert the values associated to events or annotations

        :param values: list of values to be inserted
        :type values: list
        :param entity_uuid: identifier of the event or annotation
        :type entity_uuid: uuid
        :param list_values: list with the inserted values for later bulk ingestion
        :type list_values: list
        :param level_position: value position inside the structure of values
        :type level_position: int
        :param parent_level: level of the parent value inside the structure of values
        :type parent_level: int
        :param parent_level_position: position of the parent value inside the correspoding level of the structure of values
        :type parent_level_position: int
        :param level_positions: counter of the positions per level
        :type level_positions: dict
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
                        raise WrongValue(exit_codes["WRONG_VALUE"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version, item.get("value"), item["type"]))
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
                        raise WrongValue(exit_codes["WRONG_VALUE"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version, item.get("value"), item["type"]))
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
                        raise WrongValue(exit_codes["WRONG_VALUE"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version, item.get("value"), item["type"]))
                    # end try
                    if not "timestamps" in list_values:
                        list_values["timestamps"] = []
                    # end if
                    list_to_use = list_values["timestamps"]
                elif item["type"] == "geometry":
                    list_coordinates = item.get("value").split(" ")
                    if len (list_coordinates) % 2 != 0:
                        self.session.rollback()
                        raise OddNumberOfCoordinates(exit_codes["ODD_NUMBER_OF_COORDINATES"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version, item.get("value")))
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
                            raise WrongValue(exit_codes["WRONG_VALUE"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version, coordinate, "float"))
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
        
    @debug
    def _insert_annotations(self):
        """
        Method to insert the annotations
        """
        list_annotations = []
        list_values = {}
        for annotation in self.operation.get("annotations") or []:
            id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
            annotation_cnf_info = annotation.get("annotation_cnf")
            annotation_cnf = self.annotation_cnfs[(annotation_cnf_info.get("name"), annotation_cnf_info.get("system"))]
            explicit_ref = self.explicit_refs[annotation.get("explicit_reference")]

            if not (explicit_ref, annotation_cnf.annotation_cnf_uuid) in self.annotations:
                self.annotation_cnfs_explicit_refs.append({"explicit_ref": explicit_ref,
                                                           "annotation_cnf": annotation_cnf
                                                       })

                # Insert the annotation into the list for bulk ingestion
                list_annotations.append(dict(annotation_uuid = id, ingestion_time = datetime.datetime.now(),
                                             annotation_cnf_uuid = annotation_cnf.annotation_cnf_uuid,
                                             explicit_ref_uuid = explicit_ref.explicit_ref_uuid,
                                             source_uuid = self.source.source_uuid,
                                             visible = False))
                # Insert values
                if "values" in annotation:
                    entity_uuid = {"name": "annotation_uuid",
                                   "id": id
                    }
                    self._insert_values(annotation.get("values")[0], entity_uuid, list_values)
                # end if

                self.annotations[(explicit_ref, annotation_cnf.annotation_cnf_uuid)] = None
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

        return

    @debug
    def _insert_source_status(self, status, final = False, error_message = None):
        """
        Method to insert the DIM processing status

        :param status: code indicating the status of the processing of the file
        :type status: int
        :param error_message: error message generated when the ingestion does not finish correctly
        :type error_message: str
        :param final: boolean indicated whether it is a final status or not. This is to insert the ingestion duration in case of final = True
        :type final: bool
        """
        # Insert processing status
        status = SourceStatus(datetime.datetime.now(),status,self.source)
        self.session.add(status)

        if error_message:
            # Insert the error message
            status.log = error_message
        # end if

        if final:
            # Insert processing duration
            self.source.ingestion_duration = datetime.datetime.now() - self.ingestion_start
            self.source.ingestion_time = datetime.datetime.now()
        # end if

        self.session.commit()

        return

    @debug
    def _remove_deprecated_data(self):
        """
        Method to remove the events and annotations that were overwritten by other data
        """
        # Remove events due to INSERT_and_ERASE insertion mode
        self._remove_deprecated_events_by_insert_and_erase()

        # Remove events due to INSERT_and_ERASE_per_EVENT insertion mode
        self._remove_deprecated_events_by_insert_and_erase_per_event()

        # Remove events due to EVENT_KEYS insertion mode
        self._remove_deprecated_events_event_keys()

        # Remove annotations due to the generation time
        self._remove_deprecated_annotations()

        return

    @debug
    def _remove_deprecated_events_by_insert_and_erase(self):
        """
        Method to remove events that were overwritten by other events due to INSERT and ERASE insertion mode
        """
        list_events_to_be_created = {"events": [],
                                     "values": {},
                                     "keys": [],
                                     "links": []}
        list_event_uuids_aliases = {}
        list_events_to_be_removed = []
        for gauge_uuid in self.insert_and_erase_gauges:
            # Make this method process and thread safe (lock_path -> where the lockfile will be stored)
            # /dev/shm is shared memory (RAM)
            lock = "insert_and_erase" + str(gauge_uuid)
            @self.synchronized(lock, external=True, lock_path="/dev/shm")
            def _remove_deprecated_events_by_insert_and_erase_per_gauge(self, gauge_uuid, list_events_to_be_created, list_events_to_be_created_not_ending_on_period, list_split_events):
                # Get the sources of events intersecting the validity period
                dim_signature = self.session.query(DimSignature).join(Gauge).filter(Gauge.gauge_uuid == gauge_uuid).first()
                sources = self.session.query(Source).join(DimSignature).filter(DimSignature.dim_signature_uuid == dim_signature.dim_signature_uuid,
                                                                                      Source.validity_start < self.source.validity_stop,
                                                                                      Source.validity_stop > self.source.validity_start).all()
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
                            self._insert_event(list_events_to_be_created["events"], id, timestamp, event.stop,
                                               gauge_uuid, event.explicit_ref_uuid, True, source_id = event.source_uuid)
                            self._replicate_event_values(event_uuid, id, list_events_to_be_created["values"])
                            self._create_event_uuid_alias(event_uuid, id, list_event_uuids_aliases)
                            self._replicate_event_keys(event_uuid, id, list_events_to_be_created["keys"])
                            # Remove event
                            list_events_to_be_removed.append(event_uuid)
                        # end for
                        break
                    # end if

                    validity_start = timestamp
                    validity_stop = filtered_timeline_points[next_timestamp]
                    next_timestamp += 1
                    # Get the maximum generation time at this moment
                    max_generation_time = self.session.query(func.max(Source.generation_time)).join(DimSignature).filter(DimSignature.dim_signature_uuid == dim_signature.dim_signature_uuid,
                                                                                                                                Source.validity_start < validity_stop,
                                                                                                                                Source.validity_stop > validity_start)

                    # Get the related source
                    source_max_generation_time = self.session.query(Source).join(DimSignature).filter(Source.generation_time == max_generation_time,
                                                                                                             DimSignature.dim_signature_uuid == dim_signature.dim_signature_uuid,
                                                                                                             Source.validity_start < validity_stop,
                                                                                                             Source.validity_stop > validity_start).first()

                    # Events related to the DIM processing with the maximum generation time
                    events_max_generation_time = self.session.query(Event).filter(Event.source_uuid == source_max_generation_time.source_uuid,
                                                                                  Event.gauge_uuid == gauge_uuid,
                                                                                  Event.start < validity_stop,
                                                                                  Event.stop > validity_start).all()
                    
                    # Review events with higher generation time in the period
                    for event in events_max_generation_time:
                        if event.event_uuid in list_split_events:
                            if event.stop <= validity_stop:
                                id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                                self._insert_event(list_events_to_be_created["events"], id, validity_start, event.stop,
                                                   gauge_uuid, event.explicit_ref_uuid, True, source_id = event.source_uuid)
                                self._replicate_event_values(event.event_uuid, id, list_events_to_be_created["values"])
                                self._create_event_uuid_alias(event.event_uuid, id, list_event_uuids_aliases)
                                self._replicate_event_keys(event.event_uuid, id, list_events_to_be_created["keys"])
                                list_events_to_be_removed.append(event.event_uuid)
                            else:
                                list_events_to_be_created_not_ending_on_period[event.event_uuid] = validity_start
                            # end if
                            del list_split_events[event.event_uuid]
                        else:
                            event.visible = True
                            if event.event_uuid in list_events_to_be_created_not_ending_on_period:
                                event.start = list_events_to_be_created_not_ending_on_period[event.event_uuid]
                                del list_events_to_be_created_not_ending_on_period[event.event_uuid]
                            # end if
                        # end if
                    # end for

                    # Delete deprecated events fully contained into the validity period
                    event_uuids_to_be_removed = self.session.query(Event.event_uuid).filter(Event.source_uuid != source_max_generation_time.source_uuid,
                                                     Event.gauge_uuid == gauge_uuid,
                                                     Event.start >= validity_start,
                                                     Event.stop <= validity_stop)
                    self.session.query(EventLink).filter(EventLink.event_uuid_link.in_(event_uuids_to_be_removed)).delete(synchronize_session="fetch")
                    event_uuids_to_be_removed.delete(synchronize_session="fetch")

                    # Get the events ending on the current period to be removed
                    events_not_staying_ending_on_period = self.session.query(Event).join(Source).filter(Source.generation_time <= max_generation_time,
                                                                                                               Event.gauge_uuid == gauge_uuid,
                                                                                                               Event.start <= validity_start,
                                                                                                               Event.stop > validity_start,
                                                                                                               Event.stop <= validity_stop,
                                                                                                               Event.source_uuid != source_max_generation_time.source_uuid).all()

                    # Get the events ending on the current period to be removed
                    events_not_staying_not_ending_on_period = self.session.query(Event).join(Source).filter(Source.generation_time <= max_generation_time,
                                                                                                                   Event.gauge_uuid == gauge_uuid,
                                                                                                                   Event.start < validity_stop,
                                                                                                                   Event.stop > validity_stop,
                                                                                                                   Event.source_uuid != source_max_generation_time.source_uuid).all()

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
                                id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                                self._insert_event(list_events_to_be_created["events"], id, start, validity_start,
                                                   gauge_uuid, event.explicit_ref_uuid, True, source_id = event.source_uuid)
                                self._replicate_event_values(event.event_uuid, id, list_events_to_be_created["values"])
                                self._create_event_uuid_alias(event.event_uuid, id, list_event_uuids_aliases)
                                self._replicate_event_keys(event.event_uuid, id, list_events_to_be_created["keys"])
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
            # end def
            list_events_to_be_created_not_ending_on_period = {}
            list_split_events = {}
            _remove_deprecated_events_by_insert_and_erase_per_gauge(self, gauge_uuid, list_events_to_be_created, list_events_to_be_created_not_ending_on_period, list_split_events)
        # end for

        # Bulk insert events
        self.session.bulk_insert_mappings(Event, list_events_to_be_created["events"])
        # Bulk insert keys
        self.session.bulk_insert_mappings(EventKey, list_events_to_be_created["keys"])
        # Bulk insert links
        self._replicate_event_links(list_event_uuids_aliases, list_events_to_be_created["links"])
        self.session.bulk_insert_mappings(EventLink, list_events_to_be_created["links"])

        # Remove the events that were partially affected by the insert and erase operation
        self.session.query(Event).filter(Event.event_uuid.in_(list_events_to_be_removed)).delete(synchronize_session=False)

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
    def _replicate_event_values(self, from_event_uuid, to_event_uuid, list_values_to_be_created):
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
                                             "keys": [],
                                             "links": []}
                list_event_uuids_aliases = {}
                list_events_to_be_removed = []
                list_events_to_be_created_not_ending_on_period = {}
                list_split_events = {}
            
                # Make this method process and thread safe (lock_path -> where the lockfile will be stored)
                # /dev/shm is shared memory (RAM)
                lock = "insert_and_erase_per_event" + str(gauge_uuid)
                @self.synchronized(lock, external=True, lock_path="/dev/shm")
                def _remove_deprecated_events_by_insert_and_erase_per_event_per_gauge(self, gauge_uuid, list_events_to_be_created, list_events_to_be_created_not_ending_on_period, list_split_events):

                    segment_start = parser.parse(segment[0])
                    segment_stop = parser.parse(segment[1])
                    # Get the events intersecting the segment
                    events = self.session.query(Event).filter(Event.gauge_uuid == gauge_uuid,
                                                              Event.start < segment_stop,
                                                              Event.stop > segment_start).all()
                    # Get the timeline of validity periods intersecting
                    timeline_points = set(list(chain.from_iterable([[event.start,event.stop] for event in events])))

                    filtered_timeline_points = [timestamp for timestamp in timeline_points if timestamp >= segment_start and timestamp <= segment_stop]

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
                                self._insert_event(list_events_to_be_created["events"], id, timestamp, event.stop,
                                                   gauge_uuid, event.explicit_ref_uuid, True, source_id = event.source_uuid)
                                self._replicate_event_values(event_uuid, id, list_events_to_be_created["values"])
                                self._create_event_uuid_alias(event_uuid, id, list_event_uuids_aliases)
                                self._replicate_event_keys(event_uuid, id, list_events_to_be_created["keys"])
                                # Remove event
                                list_events_to_be_removed.append(event_uuid)
                            # end for
                            break
                        # end if

                        validity_start = timestamp
                        validity_stop = filtered_timeline_points[next_timestamp]
                        next_timestamp += 1
                        # Get the maximum generation time at this moment
                        max_generation_time = self.session.query(func.max(Source.generation_time)).join(Event).filter(Event.gauge_uuid == gauge_uuid,
                                                                                                                      Event.start < validity_stop,
                                                                                                                      Event.stop > validity_start)

                        # Get the related source
                        source_max_generation_time = self.session.query(Source).join(Event).filter(Source.generation_time == max_generation_time,
                                                                                                   Event.gauge_uuid == gauge_uuid,
                                                                                                   Event.start < validity_stop,
                                                                                                   Event.stop > validity_start).first()

                        # Events related to the DIM processing with the maximum generation time
                        events_max_generation_time = self.session.query(Event).filter(Event.source_uuid == source_max_generation_time.source_uuid,
                                                                                      Event.gauge_uuid == gauge_uuid,
                                                                                      Event.start < validity_stop,
                                                                                      Event.stop > validity_start).all()

                        # Review events with higher generation time in the period
                        for event in events_max_generation_time:
                            if event.event_uuid in list_split_events:
                                if event.stop <= validity_stop:
                                    id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                                    self._insert_event(list_events_to_be_created["events"], id, validity_start, event.stop,
                                                       gauge_uuid, event.explicit_ref_uuid, True, source_id = event.source_uuid)
                                    self._replicate_event_values(event.event_uuid, id, list_events_to_be_created["values"])
                                    self._create_event_uuid_alias(event.event_uuid, id, list_event_uuids_aliases)
                                    self._replicate_event_keys(event.event_uuid, id, list_events_to_be_created["keys"])
                                    list_events_to_be_removed.append(event.event_uuid)
                                else:
                                    list_events_to_be_created_not_ending_on_period[event.event_uuid] = validity_start
                                # end if
                                del list_split_events[event.event_uuid]
                            else:
                                event.visible = True
                                if event.event_uuid in list_events_to_be_created_not_ending_on_period:
                                    event.start = list_events_to_be_created_not_ending_on_period[event.event_uuid]
                                    del list_events_to_be_created_not_ending_on_period[event.event_uuid]
                                # end if

                            # end if
                        # end for

                        # Delete deprecated events fully contained into the validity period
                        event_uuids_to_be_removed = self.session.query(Event.event_uuid).filter(Event.source_uuid != source_max_generation_time.source_uuid,
                                                         Event.gauge_uuid == gauge_uuid,
                                                         Event.start >= validity_start,
                                                         Event.stop <= validity_stop)
                        self.session.query(EventLink).filter(EventLink.event_uuid_link.in_(event_uuids_to_be_removed)).delete(synchronize_session="fetch")
                        event_uuids_to_be_removed.delete(synchronize_session="fetch")

                        # Get the events ending on the current period to be removed
                        events_not_staying_ending_on_period = self.session.query(Event).join(Source).filter(Source.generation_time <= max_generation_time,
                                                                                                                   Event.gauge_uuid == gauge_uuid,
                                                                                                                   Event.start <= validity_start,
                                                                                                                   Event.stop > validity_start,
                                                                                                                   Event.stop <= validity_stop,
                                                                                                                   Event.source_uuid != source_max_generation_time.source_uuid).all()

                        # Get the events ending on the current period to be removed
                        events_not_staying_not_ending_on_period = self.session.query(Event).join(Source).filter(Source.generation_time <= max_generation_time,
                                                                                                                       Event.gauge_uuid == gauge_uuid,
                                                                                                                       Event.start < validity_stop,
                                                                                                                       Event.stop > validity_stop,
                                                                                                                       Event.source_uuid != source_max_generation_time.source_uuid).all()

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
                                    id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                                    self._insert_event(list_events_to_be_created["events"], id, start, validity_start,
                                                       gauge_uuid, event.explicit_ref_uuid, True, source_id = event.source_uuid)
                                    self._replicate_event_values(event.event_uuid, id, list_events_to_be_created["values"])
                                    self._create_event_uuid_alias(event.event_uuid, id, list_event_uuids_aliases)
                                    self._replicate_event_keys(event.event_uuid, id, list_events_to_be_created["keys"])
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
                    self.session.query(Event).filter(Event.event_uuid.in_(list_events_to_be_removed)).delete(synchronize_session=False)

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

                # end def
                _remove_deprecated_events_by_insert_and_erase_per_event_per_gauge(self, gauge_uuid, list_events_to_be_created, list_events_to_be_created_not_ending_on_period, list_split_events)
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

            max_generation_time = self.session.query(func.max(Source.generation_time)).join(Event).join(EventKey).filter(EventKey.event_key == key, Source.dim_signature_uuid == dim_signature_uuid)

            event_max_generation_time = self.session.query(Event).join(Source).join(EventKey).filter(Source.generation_time == max_generation_time,
                                                                                                            EventKey.event_key == key,
                                                                                                            Source.dim_signature_uuid == dim_signature_uuid).first()

            # Delete deprecated events
            events_uuids_to_delete = self.session.query(Event.event_uuid).join(Source).join(EventKey).filter(Event.source_uuid != event_max_generation_time.source_uuid,
                                                                                                                    Source.generation_time <= max_generation_time,
                                                                                                                    EventKey.event_key == key,
                                                                                                                    Source.dim_signature_uuid == dim_signature_uuid)
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
            max_generation_time = self.session.query(func.max(Source.generation_time)) \
                                              .join(Annotation) \
                                              .join(AnnotationCnf) \
                                              .join(ExplicitRef).filter(AnnotationCnf.annotation_cnf_uuid == annotation_cnf.annotation_cnf_uuid,
                                                                        ExplicitRef.explicit_ref_uuid == explicit_ref.explicit_ref_uuid)

            annotation_max_generation_time = self.session.query(Annotation) \
                                                         .join(Source) \
                                                         .join(AnnotationCnf) \
                                                         .join(ExplicitRef).filter(Source.generation_time == max_generation_time,
                                                                                   AnnotationCnf.annotation_cnf_uuid == annotation_cnf.annotation_cnf_uuid,
                                                                                   ExplicitRef.explicit_ref_uuid == explicit_ref.explicit_ref_uuid).first()

            # Delete deprecated annotations
            annotations_uuids_to_delete = annotations_uuids_to_delete + self.session.query(Annotation.annotation_uuid) \
                                                      .join(Source) \
                                                      .join(AnnotationCnf). \
                                                      join(ExplicitRef).filter(Annotation.source_uuid != annotation_max_generation_time.source_uuid,
                                                                               Source.generation_time <= max_generation_time,
                                                                               AnnotationCnf.annotation_cnf_uuid == annotation_cnf.annotation_cnf_uuid,
                                                                               ExplicitRef.explicit_ref_uuid == explicit_ref.explicit_ref_uuid).all()

            # Make annotations visible
            annotations_uuids_to_update = annotations_uuids_to_update + self.session.query(Annotation.annotation_uuid) \
                                                      .join(Source) \
                                                      .join(AnnotationCnf) \
                                                      .join(ExplicitRef).filter(Annotation.source_uuid == annotation_max_generation_time.source_uuid,
                                                                                AnnotationCnf.annotation_cnf_uuid == annotation_cnf.annotation_cnf_uuid,
                                                                                ExplicitRef.explicit_ref_uuid == explicit_ref.explicit_ref_uuid).all()
        # end for

        if len(annotations_uuids_to_delete) > 0:
            self.session.query(Annotation).filter(Annotation.annotation_uuid.in_(annotations_uuids_to_delete)).delete(synchronize_session=False)
        # end if

        if len(annotations_uuids_to_update) > 0:
            self.session.query(Annotation).filter(Annotation.annotation_uuid.in_(annotations_uuids_to_update)).update({"visible": True}, synchronize_session=False)
        # end if

        return

    def insert_event_values(self, event_uuid, values):
        """
        Method to insert values and associate them to the event related to the UUID received in the position specified

        IMPORTANT!!! This method does not perform the commit. Anyhow,
        the engine will block processes accesing this method at
        self.session.bulk_insert_mappings(EventObject,
        list_values["objects"]) so remember to commit or release the session

        :param event_uuid: event to assiciate the value to
        :type from_event_uuid: uuid
        :param values: list of values to be inserted
        :type values: dict

        """
        exit_status = {
            "error": False,
            "inserted": False,
        }

        # Validate the structure of the values received
        try:
            parsing.validate_values([values])
        except ErrorParsingDictionary as e:
            logger.error(str(e))
            exit_status = {
                "error": True,
                "inserted": False,
            }
            return exit_status
        # end try

        # The event had no values associated
        entity_uuid = {"name": "event_uuid",
                       "id": event_uuid
                   }

        values_name = values["name"]
        object = self.session.query(EventObject).filter(EventObject.parent_level == 0, EventObject.parent_position == 0, EventObject.name == values_name, EventObject.event_uuid == event_uuid).first()
        while not object:
            list_values = {}
            first_object = self.session.query(EventObject).filter(EventObject.parent_level == -1, EventObject.parent_position == 0, EventObject.event_uuid == event_uuid).first()
            if not first_object:
                new_structure_values = {
                    "name": "values",
                    "type": "object",
                    "values": [values]
                }
                self._insert_values(new_structure_values, entity_uuid, list_values)
            else:
                event_values = self.query.get_event_values(event_uuids = [event_uuid])
                event_values_first_level = [value for value in event_values if value.parent_level == 0 and value.parent_position == 0]
                self._insert_values(values, entity_uuid, list_values, level_position = len(event_values_first_level), parent_level = 0, parent_level_position = 0)
            # end if

            continue_with_values_ingestion = True
            self.session.begin_nested()
            # Bulk insert values
            if "objects" in list_values:
                try:
                    race_condition()
                    self.session.bulk_insert_mappings(EventObject, list_values["objects"])
                    exit_status = {
                        "error": False,
                        "inserted": True,
                    }
                except IntegrityError:
                    continue_with_values_ingestion = False
                    # The object has not been ingested because of two possible reasons:
                    # 1. The values with the same name have been inserted by another process
                    # 2. The level_position specified has been taken by another process
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
                        logger.error(exit_codes["WRONG_GEOMETRY"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version, e))
                        exit_status = {
                            "error": True,
                            "inserted": False,
                        }
                        return exit_status
                # end if
            except IntegrityError as e:
                self.session.rollback()
                raise DuplicatedValues(exit_codes["DUPLICATED_VALUES"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.source.processor, self.source.processor_version, e))
            # end try

            if continue_with_values_ingestion:
                self.session.commit()
            # end if

            object = self.session.query(EventObject).filter(EventObject.parent_level == 0, EventObject.parent_position == 0, EventObject.name == values_name, EventObject.event_uuid == event_uuid).first()
        # end for
        
        return exit_status

    def close_session (self):
        """
        Method to close the session
        """
        self.session.close()
        return

    def geometries_to_wkt(self, geometries):
        """
        Method to return the WKT values of the received geometries
        """
        returned_geometries = []
        for geometry in geometries:
            returned_geometries.append({
                "value": to_shape(geometry.value).to_wkt(),
                "name": geometry.name
            })
        # end for

        return returned_geometries
