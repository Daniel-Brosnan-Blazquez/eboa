"""
Ingestion module for the DEC_F_RECV files from DEC

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import os
import datetime

# Import xml parser
from lxml import etree

# Import ingestion_functions.helpers
import eboa.triggering.xpath_functions as xpath_functions
import s2boa.ingestions.functions as functions

# Import query
from eboa.engine.query import Query

# Import triggering conf
from eboa.triggering.eboa_triggering import get_triggering_conf

# Import logging
from eboa.logging import Log

# Import debugging
from eboa.debugging import race_condition

logging_module = Log(name = __name__)
logger = logging_module.logger

version = "1.0"

def get_sources_from_list(query, list_of_sources):
    sources = [source.__dict__ for source in query.get_sources(names = {"filter": list_of_sources, "op": "in"})]
    dim_signatures = query.get_dim_signatures(dim_signature_uuids = {"filter": list(set([source["dim_signature_uuid"] for source in sources])), "op": "in"})
    
    for source in sources:
        dim_signature = [dim_signature for dim_signature in dim_signatures if dim_signature.dim_signature_uuid == source["dim_signature_uuid"]]
        source["dim_signature_name"] = dim_signature[0].dim_signature
    # end for

    return sources

def process_file(file_path, engine, query, reception_time):
    """
    Function to process the file and insert its relevant information
    into the DDBB of the eboa

    :param file_path: path to the file to be processed
    :type file_path: str
    :param engine: Engine instance
    :type engine: Engine
    :param query: Query instance
    :type query: Query
    :param reception_time: time of the reception of the file by the triggering
    :type reception_time: str
    """
    file_name_dec_f_recv = os.path.basename(file_path)

    # Register needed xpath functions
    ns = etree.FunctionNamespace(None)
    ns["match"] = xpath_functions.match

    # Parse file
    parsed_xml = etree.parse(file_path)
    xpath_xml = etree.XPathEvaluator(parsed_xml)

    # Obtain the creation date
    creation_date = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Source/Creation_Date")[0].text.split("=")[1]
    # Obtain the validity start
    validity_start = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Start")[0].text.split("=")[1]
    # Obtain the validity stop
    validity_stop = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Stop")[0].text.split("=")[1]

    # Source for the main operation
    source= {
        "name": file_name_dec_f_recv,
        "reception_time": reception_time,
        "generation_time": creation_date,
        "validity_start": validity_start,
        "validity_stop": validity_stop
    }

    # Get the general source entry (processor = None, version = None, DIM signature = PENDING_SOURCES)
    # This is for registrering the ingestion progress
    query_general_source = Query()
    session_progress = query_general_source.session
    general_source_progress = query_general_source.get_sources(names = {"filter": file_name_dec_f_recv, "op": "=="},
                                                               dim_signatures = {"filter": "PENDING_SOURCES", "op": "=="},
                                                               processors = {"filter": "", "op": "=="},
                                                               processor_version_filters = [{"filter": "", "op": "=="}])

    if len(general_source_progress) > 0:
        general_source_progress = general_source_progress[0]
    # end if

    functions.insert_ingestion_progress(session_progress, general_source_progress, 10)    

    # Get triggering configuration
    triggering_xpath = get_triggering_conf()

    received_files_by_dec_to_be_queried = []    
    for file_name_node in xpath_xml("/Earth_Explorer_File/Data_Block/List_of_Files/Filename"):
        file_name = file_name_node.text
        matching_rules = triggering_xpath("/triggering_rules/rule[match(source_mask, $file_name)]", file_name = file_name)
        if len(matching_rules) > 0:
            rule = matching_rules[0]
            skip = rule.get("skip")
            if skip == "true":
                logger.info("The file {} has been received by DEC (reported in file {}) but the first rule matching in the triggering configuration indicates to skip tts processing".format(file_name, file_name_dec_f_recv))
            else:
                logger.info("The file {} has been received by DEC (reported in file {}) and should be processed by BOA. Its processing will be checked".format(file_name, file_name_dec_f_recv))
                received_files_by_dec_to_be_queried.append(file_name)
        else:
            logger.info("The file {} has been received by DEC (reported in file {}) but there are no matching rules in the triggering configuration".format(file_name, file_name_dec_f_recv))
        # end if
    # end for

    functions.insert_ingestion_progress(session_progress, general_source_progress, 20)
    
    # Get the sources already registered in DDBB
    sources_registered_in_ddbb = get_sources_from_list(query, received_files_by_dec_to_be_queried)
    sources_already_processed = [source["name"] for source in sources_registered_in_ddbb if source["dim_signature_name"] not in ["PENDING_RECEIVED_SOURCES_BY_DEC"]]

    functions.insert_ingestion_progress(session_progress, general_source_progress, 40)

    # Insert the corresponding alerts for notifying about pending processing of files
    list_of_operations = []
    received_files_by_dec_to_be_processed = []
    for file_name in received_files_by_dec_to_be_queried:
        if file_name not in sources_already_processed:
            logger.info("The file {} has not been processed yet by BOA. The corresponding alert is going to be inserted".format(file_name))
            
            received_files_by_dec_to_be_processed.append(file_name)
            
            # Insert an associated alert for checking pending ingestions
            list_of_operations.append({
                "mode": "insert",
                "dim_signature": {"name": "PENDING_RECEIVED_SOURCES_BY_DEC",
                                  "exec": "",
                                  "version": ""},
                "source": {"name": file_name,
                           "reception_time": reception_time,
                           "generation_time": reception_time,
                           "validity_start": validity_start,
                           "validity_stop": validity_stop,
                           "ingested": "false"},
                "alerts": [{
                    "message": "The input {} has been retrieved by DEC to be ingested".format(file_name),
                    "generator": os.path.basename(__file__),
                    "notification_time": (datetime.datetime.now() + datetime.timedelta(hours=2)).isoformat(),
                    "alert_cnf": {
                        "name": "PENDING_INGESTION_OF_RECEIVED_SOURCE_BY_DEC",
                        "severity": "fatal",
                        "description": "Alert refers to the pending ingestion of the relative input received by DEC",
                        "group": "INGESTION_CONTROL"
                    },
                    "entity": {
                        "reference_mode": "by_ref",
                        "reference": file_name,
                        "type": "source"
                    }
                }]
            })
        else:
            logger.info("The file {} has been or is being processed by BOA. No alert will be inserted".format(file_name))
        # end if
    # end for

    functions.insert_ingestion_progress(session_progress, general_source_progress, 60)
    
    if len(list_of_operations) > 0:
        data = {"operations": list_of_operations}
        engine.treat_data(data)
    # end if

    functions.insert_ingestion_progress(session_progress, general_source_progress, 80)
    
    race_condition()
    
    if len(received_files_by_dec_to_be_processed) > 0:
        # Query status of received sources in DDBB to remove the alert due to race conditions:
        # If this ingestion finishes after the ingestion of the related files, the alert could remain
        # because the ingestion could not remove it

        # Queries done to extract all information just in case sources are removed
        sources_registered_in_ddbb = get_sources_from_list(query, received_files_by_dec_to_be_processed)

        sources_to_remove = []
        for received_file_by_dec_to_be_processed in received_files_by_dec_to_be_processed:
            processing_source = [source for source in sources_registered_in_ddbb if source["name"] == received_file_by_dec_to_be_processed and source["dim_signature_name"] not in ["PENDING_RECEIVED_SOURCES_BY_DEC"]]
            pending_processing_source = [source for source in sources_registered_in_ddbb if source["name"] == received_file_by_dec_to_be_processed and source["dim_signature_name"] in ["PENDING_RECEIVED_SOURCES_BY_DEC"]]
            if len(processing_source) > 0 and len(pending_processing_source) > 0:
                # Remove the alert as it could not be remove by the ingestion chain (race condition)
                logger.info("The file {} has been or is being processed by BOA. The corresponding alert is going to be removed".format(file_name))
                sources_to_remove.append(received_file_by_dec_to_be_processed)
            else:
                logger.info("The file {} has not been processed yet by BOA. The corresponding alert is not removed".format(file_name))
            # end if
        # end for

        if len(sources_to_remove) > 0:
            # Remove the alert as it could not be remove by the ingestion chain (race condition)
            query.get_sources(names = {"filter": sources_to_remove, "op": "in"}, dim_signatures = {"filter": "PENDING_RECEIVED_SOURCES_BY_DEC", "op": "=="}, delete = True)
        # end if
    # end if

    functions.insert_ingestion_progress(session_progress, general_source_progress, 90)
    
    # Register ingestion of the file being processed
    data = {"operations": [{
        "mode": "insert",
        "dim_signature": {
            "name": "RECEIVED_FILES_BY_DEC",
            "exec": os.path.basename(__file__),
            "version": version
        },
        "source": source
    }]
    }

    return data
