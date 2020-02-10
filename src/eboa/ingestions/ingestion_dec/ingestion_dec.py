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
import eboa.triggering.xpath_functions as xpath_functions

# Import triggering conf
from eboa.triggering.eboa_triggering import get_triggering_conf

# Import logging
from eboa.logging import Log

logging_module = Log(name = __name__)
logger = logging_module.logger

version = "1.0"

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
    file_name = os.path.basename(file_path)

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
        "name": file_name,
        "reception_time": reception_time,
        "generation_time": creation_date,
        "validity_start": validity_start,
        "validity_stop": validity_stop
    }

    list_of_operations = []
    received_files_by_dec_to_be_processed = []

    # Check configuration
    triggering_xpath = get_triggering_conf()

    for file_name_node in xpath_xml("/Earth_Explorer_File/Data_Block/List_of_Files/Filename"):
        file_name = file_name_node.text
        matching_rules_without_skip = triggering_xpath("/triggering_rules/rule[match(source_mask, $file_name) and (not(boolean(@skip)) or @skip = 'false')]", file_name = file_name)
        if len(matching_rules_without_skip) > 0:
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
        # end if
    # end for

    data = {"operations": list_of_operations}
    engine.treat_data(data)
    
    # Query status of received sources in DDBB to remove the alert due to race conditions:
    # If this ingestion finishes after the ingestion of the related files, the alert could remain
    # because the ingestion could not remove it

    # Queries done to extract all information just in case sources are removed
    sources_information = [source.__dict__ for source in query.get_sources(names = {"filter": received_files_by_dec_to_be_processed, "op": "in"})]
    dim_signatures = query.get_dim_signatures(dim_signature_uuids = {"filter": list(set([source["dim_signature_uuid"] for source in sources_information])), "op": "in"})
    for source_information in sources_information:
        dim_signature = [dim_signature for dim_signature in dim_signatures if dim_signature.dim_signature_uuid == source_information["dim_signature_uuid"]]
        source_information["dim_signature_name"] = dim_signature[0].dim_signature
    # end for

    sources_to_remove = []
    for received_file_by_dec_to_be_processed in received_files_by_dec_to_be_processed:
        pending_source_information = [source for source in sources_information if source["name"] == received_file_by_dec_to_be_processed and source["dim_signature_name"] == "PENDING_SOURCES"]
        received_source_information = [source for source in sources_information if source["name"] == received_file_by_dec_to_be_processed and source["dim_signature_name"] == "PENDING_RECEIVED_SOURCES_BY_DEC"]
        processed_source_information = [source for source in sources_information if source["name"] == received_file_by_dec_to_be_processed and source["dim_signature_name"] not in ["PENDING_SOURCES", "PENDING_RECEIVED_SOURCES_BY_DEC"]]
        if len(pending_source_information) == 0 and len(received_source_information) > 0 and len(processed_source_information) > 0:
            # Remove the alert as it could not be remove by the ingestion chain (race condition)
            sources_to_remove.append(received_file_by_dec_to_be_processed)
        # end if
    # end for
    
    if len(sources_to_remove) > 0:
        # Remove the alert as it could not be remove by the ingestion chain (race condition)
        query.get_sources(names = {"filter": sources_to_remove, "op": "in"}, dim_signatures = {"filter": "PENDING_RECEIVED_SOURCES_BY_DEC", "op": "=="}, delete = True)
    # end if

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
