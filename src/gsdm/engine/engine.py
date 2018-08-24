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
import json
import jsonschema

# Import SQLalchemy entities
from sqlalchemy.exc import IntegrityError, InternalError
from sqlalchemy.sql import func

# Import GEOalchemy entities
from geoalchemy2 import functions
from geoalchemy2.shape import to_shape

# Import exceptions
from .errors import LinksInconsistency, UndefinedEventLink, DuplicatedEventLinkRef, WrongPeriod, SourceAlreadyIngested, WrongValue, OddNumberOfCoordinates, GsdmResourcesPathNotAvailable, WrongGeometry, ErrorParsingDictionary

# Import datamodel
from gsdm.datamodel.base import Session, engine, Base
from gsdm.datamodel.dim_signatures import DimSignature
from gsdm.datamodel.events import Event, EventLink, EventKey, EventText, EventDouble, EventObject, EventGeometry, EventBoolean, EventTimestamp
from gsdm.datamodel.gauges import Gauge
from gsdm.datamodel.dim_processings import DimProcessing, DimProcessingStatus
from gsdm.datamodel.explicit_refs import ExplicitRef, ExplicitRefGrp, ExplicitRefLink
from gsdm.datamodel.annotations import Annotation, AnnotationCnf, AnnotationText, AnnotationDouble, AnnotationObject, AnnotationGeometry, AnnotationBoolean, AnnotationTimestamp

# Import query interface
from gsdm.engine.query import Query

# Import xml parser
from lxml import etree

# Import parsing module
import gsdm.engine.parsing as parsing

# Import logging
from gsdm.engine.logging import *

# Import debugging
from gsdm.engine.debugging import *

# Import auxiliary functions
from gsdm.engine.functions import *

config = read_configuration()
gsdm_resources_path = get_resources_path()

logging = Log()
logger = logging.logger

class Engine():
    """Class for communicating with the engine of the gsdm module

    Provides access to the logic for inserting, deleting and updating
    the information stored into the DDBB
    """
    # Set the synchronized module
    synchronized = lockutils.synchronized_with_prefix('gsdm-')

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
            "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} contains an event which defines the value {} that cannot be converted to the specified type {}"
        },
        "ODD_NUMBER_OF_COORDINATES": {
            "status": 8,
            "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} contains an event which defines the geometry value {} with an odd number of coordinates"
        },
        "FILE_NOT_VALID": {
            "status": 9,
            "message": "The source file with name {} does not pass the schema verification"
        },
        "WRONG_GEOMETRY": {
            "status": 10,
            "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} contains an event which defines a wrong geometry. The exception raised has been the following: {}"
        },
        "DUPLICATED_EVENT_LINK_REF": {
            "status": 11,
            "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} contains more than one event which defines the same link reference identifier {}"
        },
        "LINKS_INCONSISTENCY": {
            "status": 12,
            "message": "The source file with name {} associated to the DIM signature {} and DIM processing {} with version {} defines links between events which lead to clashing unique values into the DDBB. The exception raised has been the following: {}"
        }
    }

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
            error_message = self.exit_codes["FILE_NOT_VALID"]["message"].format(json_name)
            self._insert_source_without_dim_signature(json_name)
            self._insert_proc_status(self.exit_codes["FILE_NOT_VALID"]["status"], error_message = error_message)
            self.source.parse_error = "The json file cannot be loaded as it has a wrong structure"
            # Insert the content of the file into the DDBB
            with open(json_path) as input_file:
                self.source.content_text = input_file.read()
            self.session.commit()
            self.session.close()
            # Log the error
            logger.error(error_message)
            return self.exit_codes["FILE_NOT_VALID"]["status"]
        # end try

        if check_schema:
            try:
                parsing.validate_data_dictionary(data)
            except ErrorParsingDictionary as e:
                self._insert_source_without_dim_signature(json_name)
                self._insert_proc_status(self.exit_codes["FILE_NOT_VALID"]["status"], error_message = str(e))
                self.source.parse_error = str(e)
                # Insert the content of the file into the DDBB
                with open(json_path) as input_file:
                    self.source.content_text = input_file.read()
                self.session.commit()
                self.session.close()
                # Log the error
                logger.error(self.exit_codes["FILE_NOT_VALID"]["message"].format(json_name))
                return self.exit_codes["FILE_NOT_VALID"]["status"]
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
            self._insert_proc_status(self.exit_codes["FILE_NOT_VALID"]["status"], error_message = str(e))
            # Insert the parse error into the DDBB
            self.source.parse_error = str(e)
            # Insert the content of the file into the DDBB
            with open(xml_path,"r") as xml_file:
                self.source.content_text = xml_file.read()
            self.session.commit()
            self.session.close()
            # Log the error
            logger.error(self.exit_codes["FILE_NOT_VALID"]["message"].format(xml_name))
            return self.exit_codes["FILE_NOT_VALID"]["status"]
        # end try

        xpath_xml = etree.XPathEvaluator(parsed_xml)

        # Pass schema
        if check_schema:
            schema_path = gsdm_resources_path + "/" + config["RELATIVE_XML_SCHEMA_PATH"]
            parsed_schema = etree.parse(schema_path)
            schema = etree.XMLSchema(parsed_schema)
            valid = schema.validate(parsed_xml)
            if not valid:
                error_message = self.exit_codes["FILE_NOT_VALID"]["message"].format(xml_name)
                self._insert_source_without_dim_signature(xml_name)
                self._insert_proc_status(self.exit_codes["FILE_NOT_VALID"]["status"], error_message = error_message)
                # Insert the parse error into the DDBB
                self.source.parse_error = str(schema.error_log.last_error)
                # Insert the content of the file into the DDBB
                with open(xml_path,"r") as xml_file:
                    self.source.content_text = xml_file.read()
                self.session.commit()
                self.session.close()
                # Log the error
                logger.error(error_message)
                return self.exit_codes["FILE_NOT_VALID"]["status"]
            # end if
        # end if
        self.data["operations"] = []
        for operation in xpath_xml("/gsd/child::*"):
            if operation.tag == "insert":
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
                if event.get("link_ref"):
                    event_info["link_ref"] = event.get("link_ref")
                # end if
                event_info["gauge"] = {"name": event.xpath("gauge")[0].get("name"),
                                       "system": event.xpath("gauge")[0].get("system"),
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
    def validate_data(self, data, source):
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
            self._insert_source_without_dim_signature(source)
            self._insert_proc_status(self.exit_codes["FILE_NOT_VALID"]["status"], error_message = str(e))
            self.source.parse_error = str(e)
            self.session.commit()
            self.session.close()
            # Log the error
            logger.error(self.exit_codes["FILE_NOT_VALID"]["message"].format(source))
            return self.exit_codes["FILE_NOT_VALID"]["status"]
        # end try

        return

    def treat_data(self, data = None):
        """
        Method to treat the data stored in self.data
        """
        if data:
            self.data = data
        # end if

        for self.operation in self.data.get("operations") or []:
            if self.operation.get("mode") == "insert":
                returned_value = self._insert_data()
            # end if
            if returned_value != self.exit_codes["OK"]["status"]:
                return returned_value
            # end if
        # end for
        return self.exit_codes["OK"]["status"]

    def _initialize_context_insert_data(self):
        # Initialize context
        self.dim_signature = None
        self.source = None
        self.gauges = {}
        self.annotation_cnfs = {}
        self.expl_groups = {}
        self.explicit_refs = {}
        self.erase_and_replace_gauges = {}
        self.annotation_cnfs_explicit_refs = []
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
            self._insert_proc_status(self.exit_codes["INGESTION_STARTED"]["status"])
            # Log that the ingestion of the source file has been started
            logger.info(self.exit_codes["INGESTION_STARTED"]["message"].format(
                self.operation["source"]["name"],
                self.dim_signature.dim_signature,
                self.dim_signature.dim_exec_name, 
                self.operation["dim_signature"]["version"]))
        except SourceAlreadyIngested as e:
            self.session.rollback()
            self._insert_proc_status(self.exit_codes["SOURCE_ALREADY_INGESTED"]["status"], error_message = str(e))
            # Log that the source file has been already been processed
            logger.error(e)
            self.session.commit()
            self.session.close()
            return self.exit_codes["SOURCE_ALREADY_INGESTED"]["status"]
        except WrongPeriod as e:
            self.session.rollback()
            self._insert_proc_status(self.exit_codes["WRONG_SOURCE_PERIOD"]["status"], error_message = str(e))
            # Log that the source file has a wrong specified period as the stop is lower than the start
            logger.error(e)
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
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

        self.session.begin_nested()
        # Insert events
        try:
            self._insert_events()
        except DuplicatedEventLinkRef as e:
            self.session.rollback()
            self._insert_proc_status(self.exit_codes["DUPLICATED_EVENT_LINK_REF"]["status"], error_message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            self.session.close()
            # Log the error
            logger.error(e)
            return self.exit_codes["DUPLICATED_EVENT_LINK_REF"]["status"]
        except LinksInconsistency as e:
            self.session.rollback()
            self._insert_proc_status(self.exit_codes["LINKS_INCONSISTENCY"]["status"], error_message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            self.session.close()
            # Log the error
            logger.error(e)
            return self.exit_codes["LINKS_INCONSISTENCY"]["status"]
        except UndefinedEventLink as e:
            self.session.rollback()
            self._insert_proc_status(self.exit_codes["UNDEFINED_EVENT_LINK_REF"]["status"], error_message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            self.session.close()
            # Log the error
            logger.error(e)
            return self.exit_codes["UNDEFINED_EVENT_LINK_REF"]["status"]
        except WrongPeriod as e:
            self.session.rollback()
            self._insert_proc_status(self.exit_codes["WRONG_EVENT_PERIOD"]["status"], error_message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            self.session.close()
            # Log the error
            logger.error(e)
            return self.exit_codes["WRONG_EVENT_PERIOD"]["status"]
        except WrongValue as e:
            self.session.rollback()
            self._insert_proc_status(self.exit_codes["WRONG_VALUE"]["status"], error_message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            self.session.close()
            # Log the error
            logger.error(e)
            return self.exit_codes["WRONG_VALUE"]["status"]
        except OddNumberOfCoordinates as e:
            self.session.rollback()
            self._insert_proc_status(self.exit_codes["ODD_NUMBER_OF_COORDINATES"]["status"], error_message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            self.session.close()
            # Log the error
            logger.error(e)
            return self.exit_codes["ODD_NUMBER_OF_COORDINATES"]["status"]
        # end try

        # Insert annotations
        try:
            self._insert_annotations()
        except WrongValue as e:
            self.session.rollback()
            self._insert_proc_status(self.exit_codes["WRONG_VALUE"]["status"], error_message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            self.session.close()
            # Log the error
            logger.error(e)
            return self.exit_codes["WRONG_VALUE"]["status"]
        except OddNumberOfCoordinates as e:
            self.session.rollback()
            self._insert_proc_status(self.exit_codes["ODD_NUMBER_OF_COORDINATES"]["status"], error_message = str(e))
            # Insert content in the DDBB
            self.source.content_json = json.dumps(self.operation)
            self.session.commit()
            self.session.close()
            # Log the error
            logger.error(e)
            return self.exit_codes["ODD_NUMBER_OF_COORDINATES"]["status"]
        # end try

        # Review the inserted events (with modes EVENT_KEYS and
        # ERASE_and_REPLACE) and annotations for removing the
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
        self._insert_proc_status(self.exit_codes["OK"]["status"],True)
        logger.info(self.exit_codes["OK"]["message"].format(
            self.source.name,
            self.dim_signature.dim_signature,
            self.dim_signature.dim_exec_name, 
            self.source.dim_exec_version,
            n_events,
            n_annotations))
        self.source.ingestion_time = datetime.datetime.now()

        # Remove if the content was inserted due to errors processing the input
        self.source.content_json = None
        
        # Commit data and close connection
        self.session.commit()
        self.session.close()
        return self.exit_codes["OK"]["status"]
        
    @debug
    def _insert_dim_signature(self):
        """
        Method to insert the DIM signature
        """
        dim_signature = self.operation.get("dim_signature")
        dim_name = dim_signature.get("name")
        exec_name = dim_signature.get("exec")
        self.dim_signature = self.session.query(DimSignature).filter(DimSignature.dim_signature == dim_name, DimSignature.dim_exec_name == exec_name).first()
        if not self.dim_signature:
            id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
            self.dim_signature = DimSignature(id, dim_name, exec_name)
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
                self.dim_signature = self.session.query(DimSignature).filter(DimSignature.dim_signature == dim_name, DimSignature.dim_exec_name == exec_name).first()
                pass
            # end try
        # end if
        return

    @debug
    def _insert_source(self):
        """
        Method to insert the DIM processing
        """
        version = self.operation.get("dim_signature").get("version")
        source = self.operation.get("source")
        name = source.get("name")
        generation_time = source.get("generation_time")
        validity_start = source.get("validity_start")
        validity_stop = source.get("validity_stop")

        id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        if parser.parse(validity_stop) < parser.parse(validity_start):
            # The validity period is not correct (stop > start)
            # Create DimProcessing for registering the error in the DDBB
            self.source = DimProcessing(id, name, generation_time,
                                        version, self.dim_signature)
            self.session.add(self.source)
            try:
                race_condition()
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

        self.source = self.session.query(DimProcessing).filter(DimProcessing.name == name,
                                                               DimProcessing.dim_signature_id == self.dim_signature.dim_signature_id,
                                                               DimProcessing.dim_exec_version == version).first()
        if self.source and self.source.ingestion_duration:
            # The source has been already ingested
            raise SourceAlreadyIngested(self.exit_codes["SOURCE_ALREADY_INGESTED"]["message"].format(name,
                                                                                                     self.dim_signature.dim_signature,
                                                                                                     self.dim_signature.dim_exec_name, 
                                                                                                     version))
        elif self.source:
            # Upadte the information
            self.source.validity_start = validity_start
            self.source.validity_stop = validity_stop
            self.source.generation_time = generation_time
            return
        # end if

        self.source = DimProcessing(id, name,
                                    generation_time, version, self.dim_signature,
                                    validity_start, validity_stop)
        self.session.add(self.source)
        try:
            race_condition()
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

    def _insert_source_without_dim_signature(self, name):
        """
        Method to insert the DIM processing but without an associated dim_signature

        :param name: name of the DIM processing
        :type name: str
        """
        id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        self.source = self.session.query(DimProcessing).filter(DimProcessing.name == name).first()
        if not self.source:
            self.source = DimProcessing(id, name)
            self.session.add(self.source)
            # If there is a race condition here the gsdm will insert a
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
        gauges = [(event.get("gauge").get("name"), event.get("gauge").get("system"))  for event in self.operation.get("events") or []]
        unique_gauges = set(gauges)
        for gauge in unique_gauges:
            name = gauge[0]
            system = gauge[1]
            self.gauges[(name,system)] = self.session.query(Gauge).filter(Gauge.name == name, Gauge.system == system, Gauge.dim_signature_id == self.dim_signature.dim_signature_id).first()
            if not self.gauges[(name,system)]:
                self.session.begin_nested()
                id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                self.gauges[(name,system)] = Gauge(id, name, self.dim_signature, system)
                self.session.add(self.gauges[(name,system)])
                try:
                    race_condition()
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
            self.annotation_cnfs[(name,system)] = self.session.query(AnnotationCnf).filter(AnnotationCnf.name == name, AnnotationCnf.system == system, AnnotationCnf.dim_signature_id == self.dim_signature.dim_signature_id).first()
            if not self.annotation_cnfs[(name,system)]:
                self.session.begin_nested()
                id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                self.annotation_cnfs[(name,system)] = AnnotationCnf(id, name, self.dim_signature, system)
                self.session.add(self.annotation_cnfs[(name,system)])
                try:
                    race_condition()
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
                link_ddbb = self.session.query(ExplicitRefLink).filter(ExplicitRefLink.explicit_ref_id_link == explicit_ref1.explicit_ref_id, ExplicitRefLink.name == link.get("name"), ExplicitRefLink.explicit_ref_id == explicit_ref2.explicit_ref_id).first()
                if not link_ddbb:
                    self.session.begin_nested()
                    self.session.add(ExplicitRefLink(explicit_ref1.explicit_ref_id, link.get("name"), explicit_ref2))
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
                if bool(link.get("back_ref")):
                    link_ddbb = self.session.query(ExplicitRefLink).filter(ExplicitRefLink.explicit_ref_id_link == explicit_ref2.explicit_ref_id, ExplicitRefLink.name == link.get("name"), ExplicitRefLink.explicit_ref_id == explicit_ref1.explicit_ref_id).first()
                    if not link_ddbb:
                        self.session.begin_nested()
                        self.session.add(ExplicitRefLink(explicit_ref2.explicit_ref_id, link.get("name"), explicit_ref1))
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

    def _insert_event(self, list_events, id, start, stop, gauge_id, explicit_ref_id, visible, source = None, source_id = None):
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
        :param gauge_id: reference to the associated gauge
        :type gauge_id: int
        :param explicit_ref_id: identifier of the associated explicit reference
        :type explicit_ref_id: int
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
            raise WrongPeriod(self.exit_codes["WRONG_EVENT_PERIOD"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.dim_signature.dim_exec_name, self.source.dim_exec_version, stop, start))
        # end if
        
        if source == None:
            source = self.session.query(DimProcessing).filter(DimProcessing.processing_uuid == source_id).first()
        # end if

        source_start = source.validity_start
        source_stop = source.validity_stop
        if start < source_start or stop > source_stop:
            # The period of the event is not inside the validity period of the input
            self.session.rollback()
            raise WrongPeriod(self.exit_codes["EVENT_PERIOD_NOT_IN_SOURCE_PERIOD"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.dim_signature.dim_exec_name, self.source.dim_exec_version, start, stop, source_start, source_stop))
        # end if
            
        list_events.append(dict(event_uuid = id, start = start, stop = stop,
                                ingestion_time = datetime.datetime.now(),
                                gauge_id = gauge_id,
                                explicit_ref_id = explicit_ref_id,
                                processing_uuid = source.processing_uuid,
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
            elif gauge_info["insertion_type"] == "ERASE_and_REPLACE":
                self.erase_and_replace_gauges[gauge.gauge_id] = None
            elif gauge_info["insertion_type"] == "EVENT_KEYS":
                self.keys_events[key] = None
            # end if
            explicit_ref_id = None
            if explicit_ref != None:
                explicit_ref_id = explicit_ref.explicit_ref_id
            # end if
            # Insert the event into the list for bulk ingestion
            self._insert_event(list_events, id, start, stop, gauge.gauge_id, explicit_ref_id,
                                    visible, source = self.source)

            # Insert the key into the list for bulk ingestion
            if key != None:
                list_keys.append(dict(event_key = key, event_uuid = id,
                                      visible = visible,
                                      dim_signature_id = self.dim_signature.dim_signature_id))
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
                    if "back_ref" in link:
                        back_ref = True
                    # end if
                    
                    if link["link_mode"] == "by_ref":
                        if not link["link"] in list_event_links_by_ref:
                            list_event_links_by_ref[link["link"]] = []
                        # end if
                        list_event_links_by_ref[link["link"]].append({"name": link["name"],
                                                                      "event_uuid": id,
                                                                      "back_ref": back_ref})
                    else:
                        list_event_links_by_uuid_ddbb.append(dict(event_uuid_link = id,
                                                                  name = link["name"],
                                                                  event_uuid = link["link"]))
                        if back_ref:
                            list_event_links_by_uuid_ddbb.append(dict(event_uuid_link = link["link"],
                                                                      name = link["name"],
                                                                      event_uuid = id))
                        # end if
                    # end if
                # end for
            # end if
            if "link_ref" in event:
                if event["link_ref"] in list_event_link_refs:
                    # The same link identifier has been specified in more than one event
                    self.session.rollback()
                    raise DuplicatedEventLinkRef(self.exit_codes["DUPLICATED_EVENT_LINK_REF"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.dim_signature.dim_exec_name, self.source.dim_exec_version, event["link_ref"]))
                # end if
                list_event_link_refs[event["link_ref"]] = id
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
            try:
                self.session.bulk_insert_mappings(EventGeometry, list_values["geometries"])
            except InternalError as e:
                self.session.rollback()
                raise WrongGeometry(self.exit_codes["WRONG_GEOMETRY"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.dim_signature.dim_exec_name, self.source.dim_exec_version, e))
        # end if

        # Insert links by reference
        list_event_links_by_ref_ddbb = []
        for link_ref in list_event_links_by_ref:
            if not link_ref in list_event_link_refs:
                # There has not been defined this link reference in any event
                self.session.rollback()
                raise UndefinedEventLink(self.exit_codes["UNDEFINED_EVENT_LINK_REF"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.dim_signature.dim_exec_name, self.source.dim_exec_version, link_ref))
            # end if
            event_uuid = list_event_link_refs[link_ref]
            
            for link in list_event_links_by_ref[link_ref]:
                list_event_links_by_ref_ddbb.append(dict(event_uuid_link = link["event_uuid"],
                                                         name = link["name"],
                                                         event_uuid = event_uuid))
                if link["back_ref"]:
                    list_event_links_by_ref_ddbb.append(dict(event_uuid_link = event_uuid,
                                                             name = link["name"],
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
            raise LinksInconsistency(self.exit_codes["LINKS_INCONSISTENCY"]["message"].format(self.source.name, self.dim_signature.dim_signature, self.dim_signature.dim_exec_name, self.source.dim_exec_version, e))
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

    @debug
    def _insert_proc_status(self, status, final = False, error_message = None):
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
        status = DimProcessingStatus(datetime.datetime.now(),status,self.source)
        self.session.add(status)

        if error_message:
            # Insert the error message
            status.log = error_message
        # end if

        if final:
            # Insert processing duration
            self.source.ingestion_duration = datetime.datetime.now() - self.ingestion_start
        # end if

        self.session.commit()

        return

    @debug
    def _remove_deprecated_data(self):
        """
        Method to remove the events and annotations that were overwritten by other data
        """
        # Remove events due to ERASE_and_REPLACE insertion mode
        self._remove_deprecated_events_by_erase_and_replace()

        # Remove events due to EVENT_KEYS insertion mode
        self._remove_deprecated_events_event_keys()

        # Remove annotations due to the generation time
        self._remove_deprecated_annotations()

        return

    @debug
    def _remove_deprecated_events_by_erase_and_replace(self):
        """
        Method to remove events that were overwritten by other events due to ERASE and REPLACE insertion mode
        """
        list_events_to_be_created = {"events": [],
                                     "values": {},
                                     "keys": [],
                                     "links": []}
        list_event_uuids_aliases = {}
        list_events_to_be_removed = []
        for gauge_id in self.erase_and_replace_gauges:
            # Make this method process and thread safe (lock_path -> where the lockfile will be stored)
            # /dev/shm is shared memory (RAM)
            lock = "erase_and_replace" + str(gauge_id)
            @self.synchronized(lock, external=True, lock_path="/dev/shm")
            def _remove_deprecated_events_by_erase_and_replace_per_gauge(self, gauge_id, list_events_to_be_created, list_events_to_be_created_not_ending_on_period, list_split_events):
                # Get the sources of events intersecting the validity period
                sources = self.session.query(DimProcessing).join(Event).filter(Event.gauge_id == gauge_id,
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
                            self._insert_event(list_events_to_be_created["events"], id, timestamp, event.stop,
                                               gauge_id, event.explicit_ref_id, True, source_id = event.processing_uuid)
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
                    max_generation_time = self.session.query(func.max(DimProcessing.generation_time)).join(Event).filter(Event.gauge_id == gauge_id,
                                                                                                                         Event.start < validity_stop,
                                                                                                                         Event.stop > validity_start)
                
                    # The period does not contain events
                    if max_generation_time.first()[0] == None:
                        break
                    # end if
                    # Get the related source
                    source_max_generation_time = self.session.query(DimProcessing).join(Event).filter(DimProcessing.generation_time == max_generation_time,
                                                                                                     Event.gauge_id == gauge_id,
                                                                                                     Event.start < validity_stop,
                                                                                                     Event.stop > validity_start).first()

                    # Events related to the DIM processing with the maximum generation time
                    events_max_generation_time = self.session.query(Event).filter(Event.processing_uuid == source_max_generation_time.processing_uuid,
                                                                                  Event.gauge_id == gauge_id,
                                                                                  Event.start < validity_stop,
                                                                                  Event.stop > validity_start).all()
                    
                    # Review events with higher generation time in the period
                    for event in events_max_generation_time:
                        if event.event_uuid in list_split_events:
                            if event.stop <= validity_stop:
                                id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                                self._insert_event(list_events_to_be_created["events"], id, validity_start, event.stop,
                                                   gauge_id, event.explicit_ref_id, True, source_id = event.processing_uuid)
                                self._replicate_event_values(event.event_uuid, id, list_events_to_be_created["values"])
                                self._create_event_uuid_alias(event.event_uuid, id, list_event_uuids_aliases)
                                self._replicate_event_keys(event.event_uuid, id, list_events_to_be_created["keys"])
                                list_events_to_be_removed.append(event.event_uuid)
                            else:
                                list_events_to_be_created_not_ending_on_period[event.event_uuid] = validity_start
                            # end if
                            del list_split_events[event.event_uuid]
                        # end if
                        event.visible = True
                    # end for

                    # Delete deprecated events fully contained into the validity period
                    self.session.query(Event).filter(Event.processing_uuid != source_max_generation_time.processing_uuid,
                                                     Event.gauge_id == gauge_id,
                                                     Event.start >= validity_start,
                                                     Event.stop <= validity_stop).delete(synchronize_session="fetch")

                    # Get the events ending on the current period to be removed
                    events_not_staying_ending_on_period = self.session.query(Event).join(DimProcessing).filter(DimProcessing.generation_time <= max_generation_time,
                                                                                                               Event.gauge_id == gauge_id,
                                                                                                               Event.start <= validity_start,
                                                                                                               Event.stop > validity_start,
                                                                                                               Event.stop <= validity_stop,
                                                                                                               Event.processing_uuid != source_max_generation_time.processing_uuid).all()

                    # Get the events ending on the current period to be removed
                    events_not_staying_not_ending_on_period = self.session.query(Event).join(DimProcessing).filter(DimProcessing.generation_time <= max_generation_time,
                                                                                                                   Event.gauge_id == gauge_id,
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
                                id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
                                self._insert_event(list_events_to_be_created["events"], id, start, validity_start,
                                                   gauge_id, event.explicit_ref_id, True, source_id = event.processing_uuid)
                                self._replicate_event_values(event.event_uuid, id, list_events_to_be_created["values"])
                                self._create_event_uuid_alias(event.event_uuid, id, list_event_uuids_aliases)
                                self._replicate_event_keys(event.event_uuid, id, list_events_to_be_created["keys"])
                            # end if
                            if event.stop > validity_stop:
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
            _remove_deprecated_events_by_erase_and_replace_per_gauge(self, gauge_id, list_events_to_be_created, list_events_to_be_created_not_ending_on_period, list_split_events)
        # end for

        # Bulk insert events
        self.session.bulk_insert_mappings(Event, list_events_to_be_created["events"])
        # Bulk insert keys
        self.session.bulk_insert_mappings(EventKey, list_events_to_be_created["keys"])
        # Bulk insert links
        self._replicate_event_links(list_event_uuids_aliases, list_events_to_be_created["links"])
        self.session.bulk_insert_mappings(EventLink, list_events_to_be_created["links"])

        # Remove the events that were partially affected by the erase and replace operation
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
        values = self.query.get_event_values([from_event_uuid])
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
            links = self.query.get_event_links([event_uuid])
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
        keys = self.query.get_event_keys([from_event_uuid])
        for key in keys:
            list_keys_to_be_created.append(dict(event_key = key.event_key,
                                                event_uuid = to_event_uuid,
                                                visible = True,
                                                dim_signature_id = key.dim_signature_id))
        # end for
        
        return

    @debug
    def _remove_deprecated_events_event_keys(self):
        """
        Method to remove events that were overwritten by other events due to EVENT_KEYS insertion mode
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

    @debug
    def _remove_deprecated_annotations(self):
        """
        Method to remove annotations that were overwritten by other annotations
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

