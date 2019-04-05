"""
Ingestion module for the REP_ARC files of Sentinel-2

Written by DEIMOS Space S.L. (femd)

module eboa
"""

# Import python utilities
import os
import argparse
from dateutil import parser
import datetime
import json
import sys
from tempfile import mkstemp

# Import xml parser
from lxml import etree

# Import engine
import eboa.engine.engine as eboa_engine
from eboa.engine.engine import Engine

# Import ingestion helpers
import eboa.engine.ingestion as ingestion
import ingestions.s2.functions as functions
import ingestions.functions.functions as ingestion_helpers
import ingestions.functions.date_functions as date_functions
import ingestions.s2.xpath_functions as xpath_functions

# Import query
from eboa.engine.query import Query

# Import debugging
from eboa.debugging import debug

# Import logging
from eboa.logging import Log

logging_module = Log()
logger = logging_module.logger

version = "1.0"

def process_file(file_path, engine, query):
    """
    Function to process the file and insert its relevant information
    into the DDBB of the eboa

    :param file_path: path to the file to be processed
    :type file_path: str
    :param engine: object to access the engine of the EBOA
    :type engine: Engine
    :param query: object to access the query interface of the EBOA
    :type query: Query
    """
    file_name = os.path.basename(file_path)

    # Remove namespaces
    (_, new_file_path) = new_file = mkstemp()
    ingestion.remove_namespaces(file_path, new_file_path)

    # Parse file
    parsed_xml = etree.parse(new_file_path)
    xpath_xml = etree.XPathEvaluator(parsed_xml)

    list_of_explicit_references = []
    list_of_annotations = []
    list_of_events = []
    list_of_timelines = []
    list_of_configuration_events = []
    list_of_configuration_explicit_references = []
    list_of_operations = []
    processed_datastrips = {}
    granule_timeline_per_detector = {}
    granule_timeline = []

    # Obtain the satellite
    satellite = file_name[0:3]
    # Obtain the station
    system = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Source/System")[0].text
    # Obtain the creation date
    creation_date = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Source/Creation_Date")[0].text.split("=")[1]
    # Obtain the validity start
    validity_start = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Start")[0].text.split("=")[1]
    # Obtain the validity stop
    validity_stop = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Stop")[0].text.split("=")[1]
    # Obtain the datastrip
    datastrip_info = xpath_xml("/Earth_Explorer_File/Data_Block/List_of_ItemMetadata/ItemMetadata[Catalogues/S2CatalogueReport/S2EarthObservation/Inventory_Metadata/File_Type[contains(text(),'_DS')]]")[0]
    # Obtain the datastrip ID
    datastrip_id = datastrip_info.xpath("Catalogues/S2CatalogueReport/S2EarthObservation/Inventory_Metadata/File_ID")[0].text
    # Obtain the datatake ID
    datatake_id = datastrip_info.xpath("CentralIndex/Datatake-id")[0].text
    # Obtain the baseline
    baseline = datastrip_id[58:]
    # Obtain the production level from the datastrip
    level = datastrip_id[13:16].replace("_","")
    # Obtain the sensing identifier
    sensing_id = datastrip_id[41:57]
    # Source for the main operation
    source = {
        "name": file_name,
        "generation_time": creation_date,
        "validity_start": validity_start,
        "validity_stop": validity_stop
    }

    processing_validity_db = query.get_events(explicit_refs = {"op": "like", "filter": datastrip_id},
                                              gauge_names = {"op": "like", "filter": "PROCESSING_VALIDITY"})

    processing_validity_exists = len(processing_validity_db) > 0
    # General file

    # Insert the datatake_annotation
    datatake_annotation = {
    "explicit_reference": datastrip_id,
    "annotation_cnf": {
        "name": "DATATAKE",
        "system": satellite
        },
    "values": [{
        "name": "details",
        "type": "object",
        "values": [
            {"name": "datatake_identifier",
             "type": "text",
             "value": datatake_id
             }]
        }]
    }
    list_of_annotations.append(datatake_annotation)

    # Insert the baseline_annotation
    if not processing_validity_exists:
        baseline_annotation = {
        "explicit_reference": datastrip_id,
        "annotation_cnf": {
            "name": "BASELINE",
            "system": system
            },
        "values": [{
            "name": "details",
            "type": "object",
            "values": [
                {"name": "baseline",
                 "type": "text",
                 "value": baseline
                 }]
            }]
        }
        list_of_annotations.append(baseline_annotation)
    # end if

    if not processing_validity_exists:
        datastrip_sensing_explicit_ref= {
            "group": level + "_DS",
            "links": [{
                "back_ref": "SENSING_ID",
                "link": "DATASTRIP",
                "name": sensing_id
                }
            ],
            "name": datastrip_id
        }
        list_of_explicit_references.append(datastrip_sensing_explicit_ref)
    # end if

    # Loop over all the ItemMetadata
    for item in xpath_xml("/Earth_Explorer_File/Data_Block/List_of_ItemMetadata/ItemMetadata"):

        item_id = item.xpath("Catalogues/S2CatalogueReport/S2EarthObservation/Inventory_Metadata/File_ID")[0].text
        data_size = item.xpath("Catalogues/S2CatalogueReport/S2EarthObservation/Inventory_Metadata/Data_Size")[0].text
        cloud_percentage = item.xpath("Catalogues/S2CatalogueReport/S2EarthObservation/Inventory_Metadata/CloudPercentage")[0].text
        physical_url = item.xpath("CentralIndex/PDIPhysicalUrl")[0].text
        # Obtain the footprint values
        footprint = item.xpath("Catalogues/S2CatalogueReport/S2EarthObservation/Product_Metadata/Footprint/EXT_POS_LIST")[0].text

        # Insert the footprint_annotation
        list_of_lat_long_coordinates = footprint.split(" ")
        if "" in list_of_lat_long_coordinates:
            list_of_lat_long_coordinates.remove("")
        # end if
        if "_DS_" in item_id:
            corrected_list_of_coordinates = list(reversed(ingestion_helpers.correct_list_of_coordinates_for_ds(list_of_lat_long_coordinates)))
        else:
            corrected_list_of_coordinates = list(reversed(ingestion_helpers.correct_list_of_coordinates_for_gr_tl(list_of_lat_long_coordinates)))
        # end if

        corrected_footprint = ingestion_helpers.list_of_coordinates_to_str_geometry(corrected_list_of_coordinates)
        if corrected_footprint != "":
            footprint_annotation = {
            "explicit_reference": item_id,
            "annotation_cnf": {
                "name": "FOOTPRINT",
                "system": satellite
                },
            "values": [{
                "name": "details",
                "type": "object",
                "values": [
                    {"name": "footprint",
                     "type": "geometry",
                     "value": corrected_footprint
                     }]
                }]
            }
            list_of_annotations.append(footprint_annotation)
        # end if

        # Insert the data_size_annotation per datastrip
        data_size_annotation = {
        "explicit_reference": item_id,
        "annotation_cnf": {
            "name": "SIZE",
            "system": satellite
            },
        "values": [{
            "name": "details",
            "type": "object",
            "values": [
                {"name": "size",
                 "type": "double",
                 "value": data_size
                 }]
            }]
        }
        list_of_annotations.append(data_size_annotation)

        # Insert the data_size_annotation per datastrip
        cloud_percentage_annotation = {
        "explicit_reference": item_id,
        "annotation_cnf": {
            "name": "CLOUD_PERCENTAGE",
            "system": satellite
            },
        "values": [{
            "name": "details",
            "type": "object",
            "values": [
                {"name": "cloud_percentage",
                 "type": "double",
                 "value": cloud_percentage
                 }]
            }]
        }
        list_of_annotations.append(cloud_percentage_annotation)
        # end if

        # Insert the data_size_annotation per datastrip
        physical_url_annotation = {
        "explicit_reference": item_id,
        "annotation_cnf": {
            "name": "PHYSICAL_URL",
            "system": satellite
            },
        "values": [{
            "name": "details",
            "type": "object",
            "values": [
                {"name": "physical_url",
                 "type": "text",
                 "value": physical_url
                 }]
            }]
        }
        list_of_annotations.append(physical_url_annotation)

        if '_GR' in item.xpath("CentralIndex/FileType")[0].text and not processing_validity_exists:
            if not processing_validity_exists:
                # Obtain the granule id
                granule_t = item_id
                level_gr = granule_t[13:16].replace("_","")
                granule_sensing_date = granule_t[42:57]
                detector = granule_t[59:61]

                # Create a segment for each granule with a margin calculated to get whole scenes
                start= parser.parse(granule_sensing_date)
                stop = start + datetime.timedelta(seconds=5)
                granule_segment = {
                    "start": start,
                    "stop": stop,
                    "id": granule_t
                }

                # Create a dictionary containing all the granules for each detector
                granule_timeline.append(granule_segment)
                if detector not in granule_timeline_per_detector:
                    granule_timeline_per_detector[detector] = []
                # end if
                granule_timeline_per_detector[detector].append(granule_segment)

            # Insert the granule explicit reference
            granule_explicit_reference = {
                "group": level + "_GR",
                "links": [{
                    "back_ref": "DATASTRIP",
                    "link": "GRANULE",
                    "name": datastrip_id
                    }
                ],
                "name": item_id
            }
            list_of_explicit_references.append(granule_explicit_reference)
        # end if

        if '_TL' in item.xpath("CentralIndex/FileType")[0].text and not processing_validity_exists:
        # Insert the tile explicit reference
            tile_explicit_reference = {
                "group": level + "_TL",
                "links": [{
                    "back_ref": "DATASTRIP",
                    "link": "TILE",
                    "name": datastrip_id
                    }
                ],
                "name": item_id
            }
            list_of_explicit_references.append(tile_explicit_reference)

            # #Modify the item_id to obtain a TC id
            # tc_id = item_id.replace('TL','TC')
            # #Insert the tc explicit reference if it doesn't already exist
            # tc_er = query.get_explicit_refs(groups = {"filter": level + "_TC", "op": "like"},
            #                                               explicit_refs = {"op": "like", "filter": tc_id})
            # if len(tc_er) is 0:
            #     tc_explicit_reference = {
            #         "group": level + "_TC",
            #         "links": [{
            #             "back_ref": "DATASTRIP",
            #             "link": "TCI",
            #             "name": datastrip_id
            #             }
            #         ],
            #         "name": item_id
            #     }
            #     list_of_explicit_references.append(tc_explicit_reference)
            # # end if
        # end if

    completeness_plan_db = query.get_events(gauge_names = {"filter": " 	PLANNED_IMAGING_PROCESSING_COMPLETENESS_" + level, "op": "like"})
    if len(completeness_plan_db) is 0:
        if level == "L0" or level == "L1A" or level == "L1B":
            functions.L0_L1A_L1B_processing(source, engine, query, granule_timeline,list_of_events,datastrip_id,granule_timeline_per_detector, list_of_operations, system, version, file_name)
        elif (level == "L1C" or level == "L2A"):
            upper_level_ers = query.get_explicit_refs(annotation_cnf_names = {"filter": "SENSING_IDENTIFIER", "op": "like"},
                                                      groups = {"filter": ["L0_DS", "L1B_DS"], "op": "in"},
                                                      annotation_value_filters = [{"name": {"str": "sensing_identifier", "op": "like"}, "type": "text", "value": {"op": "like", "value": sensing_id}}])
            upper_level_ers_same_satellite = [er.explicit_ref for er in upper_level_ers if er.explicit_ref[0:3] == satellite]
            upper_level_er = [er for er in upper_level_ers_same_satellite if er[13:16] == "L1B"]
            if len(upper_level_er) == 0:
                upper_level_er = [er for er in upper_level_ers_same_satellite if er[13:16] == "L0_"]
            # end if
            if len(upper_level_er) > 0:
                er = upper_level_er[0]

                processing_validity_events = query.get_linking_events(gauge_names = {"filter": ["PROCESSING_VALIDITY","PROCESSING_VALIDITY"], "op": "in"},
                                                                      explicit_refs = {"filter": er, "op": "like"},
                                                                      value_filters = [{"name": {"str": "level", "op": "like"}, "type": "text", "value": {"op": "in", "value": ["L0", "L1B"]}}],
                                                                      link_names = {"filter": ["PROCESSING_GAP", "PLANNED_IMAGING", "ISP_VALIDITY"], "op": "in"},
                                                                      return_prime_events = True)

                functions.L1C_L2A_processing(source, engine, query, list_of_events, processing_validity_events, datastrip_id, list_of_operations, system, version, file_name)
            # end if
        # end if

    # Adjust sources / operations
    source_indexing = source

    list_of_operations.append({
        "mode": "insert",
        "dim_signature": {
              "name": "INDEXING_" + satellite,
              "exec": os.path.basename(__file__),
              "version": version
        },
        "source": source_indexing,
        "annotations": list_of_annotations,
        "explicit_references": list_of_explicit_references
        })
    data = {"operations": list_of_operations}

    os.remove(new_file_path)

    return data

def insert_data_into_DDBB(data, filename, engine):
    # Treat data
    returned_value = engine.treat_data(data, filename)
    if returned_value == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]:
        logger.error("The file {} could not be validated".format(filename))
    # end if

    return returned_value

def command_process_file(file_path, output_path = None):
    engine = Engine()
    query = Query()

    # Process file
    data = process_file(file_path, engine, query)

    # Validate data
    filename = os.path.basename(file_path)

    returned_value = 0

    # Treat data
    if output_path == None:
        returned_value = insert_data_into_DDBB(data, filename, engine)
    else:
        with open(output_path, "w") as write_file:
            json.dump(data, write_file, indent=4)
    # end if

    return returned_value

if __name__ == "__main__":
    """
    This will be not here. After all the needs are identified, the ingestions a priori will have only the method command_process_file and this method will be called from outside
    """
    args_parser = argparse.ArgumentParser(description='Process MPL_SPs.')
    args_parser.add_argument('-f', dest='file_path', type=str, nargs=1,
                             help='path to the file to process', required=True)
    args_parser.add_argument('-o', dest='output_path', type=str, nargs=1,
                             help='path to the output file', required=False)
    args = args_parser.parse_args()
    file_path = args.file_path[0]
    output_path = None
    if args.output_path != None:
        output_path = args.output_path[0]
    # end if

    # Before calling to the processor there should be a validation of
    # the file following a schema. Schema not available for STNACQs

    returned_value = command_process_file(file_path, output_path)

    logger.info("The ingestion has been performed and the exit status is {}".format(returned_value))
