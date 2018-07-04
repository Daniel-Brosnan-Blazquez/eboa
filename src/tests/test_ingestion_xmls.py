"""
Test: ingest xml test

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
import os
import sys

# Import engine of the DDBB
from gsdm.engine.engine import Engine
from gsdm.datamodel.base import Session, engine, Base

# Import analysis module
from gsdm.engine.analysis import Analysis

# Import datamodel
from gsdm.datamodel.dim_signatures import DimSignature
from gsdm.datamodel.events import Event, EventLink, EventKey, EventText, EventDouble, EventObject, EventGeometry
from gsdm.datamodel.gauges import Gauge
from gsdm.datamodel.dim_processings import DimProcessing, DimProcessingStatus
from gsdm.datamodel.explicit_refs import ExplicitRef, ExplicitRefGrp, ExplicitRefLink
from gsdm.datamodel.annotations import Annotation, AnnotationCnf, AnnotationText, AnnotationDouble, AnnotationObject, AnnotationGeometry

# Import python utilities
from termcolor import colored
from inspect import currentframe, getframeinfo
import json
from dateutil.parser import parse

# Create session to connect to the database
session = Session()

# Clear all tables before executing the test
for table in reversed(Base.metadata.sorted_tables):
    engine.execute(table.delete())
# end for

# insert data from xml
engine_gsdm = Engine()
engine_gsdm.parse_data_from_xml(os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/test_simple_update.xml")
#print(json.dumps(engine_gsdm.data, indent=4))

### PENDING checks on the parser

engine_gsdm.treat_data()

# Checks
## DIM Signature ingestion
dim_signature = {"dim_signature":"test_dim_signature1", "dim_exec_name": "test_exec1"}
dim_signature_ddbb = session.query(DimSignature).filter(DimSignature.dim_signature == dim_signature["dim_signature"], DimSignature.dim_exec_name == dim_signature["dim_exec_name"]).first()
print(colored("Details", on_color="on_green") + "_{}:".format(getframeinfo(currentframe()).lineno) + str(dim_signature))
result = {"message":"OK","color":"green"}
if dim_signature_ddbb == None:
    result = {"message":"NOK","color":"red"}
# end if
print(colored("Check", on_color="on_blue") + "_{}: DIM signature has been inserted correcly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

## DIM Processing ingestion
dim_processing = {"name": "test_simple_update.xml",
                  "validity_start": "2018-06-05T02:07:03",
                  "validity_stop": "2018-06-05T02:07:36",
                  "generation_time": "2018-06-06T13:33:29",
                  "dim_exec_version": "1.0"}
dim_processing_ddbb = session.query(DimProcessing).filter(DimProcessing.name == dim_processing["name"], DimProcessing.validity_start == dim_processing["validity_start"], DimProcessing.validity_stop == dim_processing["validity_stop"], DimProcessing.generation_time == dim_processing["generation_time"], DimProcessing.dim_exec_version == dim_processing["dim_exec_version"]).first()
print(colored("Details", on_color="on_green") + "_{}:".format(getframeinfo(currentframe()).lineno) + str(dim_processing))
result = {"message":"OK","color":"green"}
if dim_processing_ddbb == None:
    result = {"message":"NOK","color":"red"}
# end if
print(colored("Check", on_color="on_blue") + "_{}: DIM processing has been inserted correcly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

result = {"message":"OK","color":"green"}
if dim_processing_ddbb.dim_signature_id != dim_signature_ddbb.dim_signature_id:
    result = {"message":"NOK","color":"red"}
# end if
print(colored("Check", on_color="on_blue") + "_{}: DIM processing has been associated to the DIM Signature correcly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))
dim_processing_status = session.query(DimProcessingStatus).filter(DimProcessingStatus.processing_uuid == dim_processing_ddbb.processing_uuid, DimProcessingStatus.proc_status == 0).first()
result = {"message":"OK","color":"green"}
if dim_processing_status == None:
    result = {"message":"NOK","color":"red"}
# end if
print(colored("Check", on_color="on_blue") + "_{}: The source with all the content has been ingested correctly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))
dim_processing_status = session.query(DimProcessingStatus).filter(DimProcessingStatus.processing_uuid == dim_processing_ddbb.processing_uuid, DimProcessingStatus.proc_status == 1).first()
result = {"message":"OK","color":"green"}
if dim_processing_status == None:
    result = {"message":"NOK","color":"red"}
# end if
print(colored("Check", on_color="on_blue") + "_{}: The start of the ingestion process for the source has been inserted correctly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))


## Gauges ingestion
list_gauges = {}
gauges = [{"name": "test_gauge_name1", "system": "test_gauge_system1"},
          {"name": "test_gauge_name2", "system": "test_gauge_system2"}]
for gauge in gauges:
    gauge_ddbb = session.query(Gauge).filter(Gauge.name == gauge["name"], Gauge.system == gauge["system"]).first()
    list_gauges[(gauge["name"],gauge["system"])] = gauge_ddbb
    print(colored("Details", on_color="on_green") + "_{}:".format(getframeinfo(currentframe()).lineno) + str(gauge))
    result = {"message":"OK","color":"green"}
    if gauge_ddbb == None:
        result = {"message":"NOK","color":"red"}
    # end if
    print(colored("Check", on_color="on_blue") + "_{}: Gauge has been inserted correcly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

    result = {"message":"OK","color":"green"}
    if gauge_ddbb.dim_signature_id != dim_signature_ddbb.dim_signature_id:
        result = {"message":"NOK","color":"red"}
    # end if
    print(colored("Check", on_color="on_blue") + "_{}: Gauge has been associated to the DIM signature correcly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))
# end for

## Annotation Configuration ingestion
list_annotation_cnfs = {}
annotation_cnfs = [{"name": "test_annotation_cnf_name1", "system": "test_annotation_cnf_system1"}]
for annotation_cnf in annotation_cnfs:
    annotation_cnf_ddbb = session.query(AnnotationCnf).filter(AnnotationCnf.name == annotation_cnf["name"], AnnotationCnf.system == annotation_cnf["system"]).first()
    list_annotation_cnfs[(annotation_cnf["name"],annotation_cnf["system"])] = annotation_cnf_ddbb
    print(colored("Details", on_color="on_green") + "_{}:".format(getframeinfo(currentframe()).lineno) + str(annotation_cnf))
    result = {"message":"OK","color":"green"}
    if annotation_cnf_ddbb == None:
        result = {"message":"NOK","color":"red"}
    # end if
    print(colored("Check", on_color="on_blue") + "_{}: Annotation configuration has been inserted correcly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

    result = {"message":"OK","color":"green"}
    if annotation_cnf_ddbb.dim_signature_id != dim_signature_ddbb.dim_signature_id:
        result = {"message":"NOK","color":"red"}
    # end if
    print(colored("Check", on_color="on_blue") + "_{}: Annotation configuration has been associated to the DIM signature correcly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))
# end for

## Explicit reference groups ingestion
expl_groups = [{"name": "test_explicit_ref_group1"}]
list_expl_groups = {}
for expl_group in expl_groups:
    expl_group_ddbb = session.query(ExplicitRefGrp).filter(ExplicitRefGrp.name == expl_group["name"]).first()
    list_expl_groups[expl_group["name"]] = expl_group_ddbb
    print(colored("Details", on_color="on_green") + "_{}:".format(getframeinfo(currentframe()).lineno) + str(expl_group))
    result = {"message":"OK","color":"green"}
    if expl_group_ddbb == None:
        result = {"message":"NOK","color":"red"}
    # end if
    print(colored("Check", on_color="on_blue") + "_{}: Explicit reference group has been inserted correcly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))
# end for

## Explicit reference ingestion
explicit_references = [{"explicit_ref": "test_explicit_ref1", "group": "test_explicit_ref_group1"},
                       {"explicit_ref": "test_explicit_ref2"}]
for explicit_reference in explicit_references:
    explicit_reference_ddbb = session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == explicit_reference["explicit_ref"]).first()
    print(colored("Details", on_color="on_green") + "_{}:".format(getframeinfo(currentframe()).lineno) + str(explicit_reference))
    result = {"message":"OK","color":"green"}
    if explicit_reference_ddbb == None:
        result = {"message":"NOK","color":"red"}
    # end if
    print(colored("Check", on_color="on_blue") + "_{}: Explicit reference has been inserted correcly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

    if "group" in explicit_reference:
        result = {"message":"OK","color":"green"}
        if explicit_reference_ddbb.expl_ref_cnf_id != list_expl_groups[explicit_reference["group"]].expl_ref_cnf_id:
            result = {"message":"NOK","color":"red"}
        # end if
        print(colored("Check", on_color="on_blue") + "_{}: Explicit reference has been associated to the group correcly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))
    # end if
# end for

## Events ingestion
list_events_ddbb = []
events = [{"start": "2018-06-05T02:07:03",
           "stop": "2018-06-05T02:07:36",
           "generation_time": "2018-06-06T13:33:29",
           "key": "test_key1",
           "explicit_reference": "test_explicit_ref1",
           "gauge": ("test_gauge_name1", "test_gauge_system1"),
           "link": "test_link_name1",
           "values":[
               {"name": "test_object_name1",
                "level_position": 0,
                "parent_level": -1,
                "parent_position": 0},
               {"name": "test_text_name1",
                "level_position": 0,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_timestamp_name1",
                "level_position": 1,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_boolean_name1",
                "level_position": 2,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_double_name1",
                "level_position": 3,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_object_name2",
                "level_position": 4,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_text_name2",
                "level_position": 0,
                "parent_level": 1,
                "parent_position": 4},
               {"name": "test_geometry1",
                "level_position": 1,
                "parent_level": 1,
                "parent_position": 4},
               {"name": "test_text_name10",
                "level_position": 5,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_boolean_name10",
                "level_position": 6,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_double_name10",
                "level_position": 7,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_object_name11",
                "level_position": 8,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_text_name11",
                "level_position": 2,
                "parent_level": 1,
                "parent_position": 8},
               {"name": "test_geometry10",
                "level_position": 3,
                "parent_level": 1,
                "parent_position": 8}
           ]},
          {"start": "2018-06-05T02:07:03",
           "stop": "2018-06-05T02:07:36",
           "generation_time": "2018-06-06T13:33:29",
           "key": "test_key2",
           "explicit_reference": "test_explicit_ref2",
           "gauge": ("test_gauge_name2", "test_gauge_system2"),
           "link": "test_link_name2",
           "values":[
               {"name": "test_object_name2",
                "level_position": 0,
                "parent_level": -1,
                "parent_position": 0},
               {"name": "test_text_name2",
                "level_position": 0,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_boolean_name2",
                "level_position": 1,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_double_name2",
                "level_position": 2,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_object_name3",
                "level_position": 3,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_text_name3",
                "level_position": 0,
                "parent_level": 1,
                "parent_position": 3},
               {"name": "test_geometry2",
                "level_position": 1,
                "parent_level": 1,
                "parent_position": 3}
           ]}
      ]
for event in events:
    event_ddbb = session.query(Event).join(ExplicitRef).join(DimProcessing).filter(Event.start == event["start"], Event.stop == event["stop"], DimProcessing.generation_time == event["generation_time"], ExplicitRef.explicit_ref == event["explicit_reference"]).first()
    list_events_ddbb.append(event_ddbb)
    print(colored("Details", on_color="on_green") + "_{}:".format(getframeinfo(currentframe()).lineno) + str(event))
    result = {"message":"OK","color":"green"}
    if event_ddbb == None:
        result = {"message":"NOK","color":"red"}
    # end if
    print(colored("Check", on_color="on_blue") + "_{}: Event has been inserted correcly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

    result = {"message":"OK","color":"green"}
    if event_ddbb.processing_uuid != dim_processing_ddbb.processing_uuid:
        result = {"message":"NOK","color":"red"}
    # end if
    print(colored("Check", on_color="on_blue") + "_{}: Event has been associated to the DIM processing correcly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

    result = {"message":"OK","color":"green"}
    if event_ddbb.gauge_id != list_gauges[event["gauge"]].gauge_id:
        result = {"message":"NOK","color":"red"}
    # end if
    print(colored("Check", on_color="on_blue") + "_{}: Event has been associated to the gauge correcly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

    link_ddbb = session.query(EventLink).filter(EventLink.name == event["link"], EventLink.event_uuid_link == event_ddbb.event_uuid).first()
    result = {"message":"OK","color":"green"}
    if link_ddbb == None:
        result = {"message":"NOK","color":"red"}
    # end if
    print(colored("Check", on_color="on_blue") + "_{}: Event link has been inserted correcly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

    event_key_ddbb = session.query(EventKey).filter(EventKey.event_uuid == event_ddbb.event_uuid, EventKey.event_key == event["key"]).first()
    result = {"message":"OK","color":"green"}
    if link_ddbb == None:
        result = {"message":"NOK","color":"red"}
    # end if
    print(colored("Check", on_color="on_blue") + "_{}: Event key has been inserted correcly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

    if "values" in event:
        values_ddbb = engine_gsdm.get_event_values([event_ddbb.event_uuid])
        for value in event["values"]:
            value_ddbb = [value_ddbb for value_ddbb in values_ddbb if value_ddbb.level_position == value["level_position"] and value_ddbb.parent_level == value["parent_level"] and value_ddbb.parent_position == value["parent_position"] and value_ddbb.name == value["name"]]
            result = {"message":"OK","color":"green"}
            if len(value_ddbb) != 1:
                result = {"message":"NOK","color":"red"}
            # end if
            print(colored("Check", on_color="on_blue") + "_{}: Event value with name {} has been inserted correcly --> ".format(getframeinfo(currentframe()).lineno, value["name"]) + colored(result["message"], result["color"], attrs=['bold']))

        # end for
    # end if

# end for

## Annotations ingestion
annotations = [{"generation_time": "2018-06-06T13:33:29",
                "explicit_reference": "test_explicit_ref1",
                "annotation_cnf": ("test_annotation_cnf_name1", "test_annotation_cnf_system1"),
                "values":[
                    {"name": "test_object_name1",
                     "level_position": 0,
                     "parent_level": -1,
                     "parent_position": 0},
                    {"name": "test_text_name1",
                     "level_position": 0,
                     "parent_level": 0,
                     "parent_position": 0},
                    {"name": "test_boolean_name1",
                     "level_position": 1,
                     "parent_level": 0,
                     "parent_position": 0},
                    {"name": "test_timestamp_name1",
                     "level_position": 2,
                     "parent_level": 0,
                     "parent_position": 0},
                    {"name": "test_double_name1",
                     "level_position": 3,
                     "parent_level": 0,
                     "parent_position": 0},
                    {"name": "test_object_name2",
                     "level_position": 4,
                     "parent_level": 0,
                     "parent_position": 0},
                    {"name": "test_text_name2",
                     "level_position": 0,
                     "parent_level": 1,
                     "parent_position": 4},
                    {"name": "test_geometry1",
                     "level_position": 1,
                     "parent_level": 1,
                     "parent_position": 4}
                ]
            }]
for annotation in annotations:
    annotation_ddbb = session.query(Annotation).join(DimProcessing).join(ExplicitRef).filter(DimProcessing.generation_time == annotation["generation_time"], ExplicitRef.explicit_ref == annotation["explicit_reference"]).first()
    print(colored("Details", on_color="on_green") + "_{}:".format(getframeinfo(currentframe()).lineno) + str(annotation))
    result = {"message":"OK","color":"green"}
    if annotation_ddbb == None:
        result = {"message":"NOK","color":"red"}
    # end if
    print(colored("Check", on_color="on_blue") + "_{}: Annotation has been inserted correcly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

    result = {"message":"OK","color":"green"}
    if annotation_ddbb.processing_uuid != dim_processing_ddbb.processing_uuid:
        result = {"message":"NOK","color":"red"}
    # end if
    print(colored("Check", on_color="on_blue") + "_{}: Annotation has been associated to the DIM processing correcly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

    result = {"message":"OK","color":"green"}
    if annotation_ddbb.annotation_cnf_id != list_annotation_cnfs[annotation["annotation_cnf"]].annotation_cnf_id:
        result = {"message":"NOK","color":"red"}
    # end if
    print(colored("Check", on_color="on_blue") + "_{}: Annotation has been associated to the annotation configuration correcly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

    if "values" in annotation:
        values_ddbb = engine_gsdm.get_annotation_values([annotation_ddbb.annotation_uuid])
        for value in annotation["values"]:
            value_ddbb = [value_ddbb for value_ddbb in values_ddbb if value_ddbb.level_position == value["level_position"] and value_ddbb.parent_level == value["parent_level"] and value_ddbb.parent_position == value["parent_position"] and value_ddbb.name == value["name"]]
            result = {"message":"OK","color":"green"}
            if len(value_ddbb) != 1:
                result = {"message":"NOK","color":"red"}
            # end if
            print(colored("Check", on_color="on_blue") + "_{}: Annotation value with name {} has been inserted correcly --> ".format(getframeinfo(currentframe()).lineno, value["name"]) + colored(result["message"], result["color"], attrs=['bold']))

        # end for
    # end if

# end for

## Links between explicit references ingestion
explicit_reference_links = [{"explicit_ref1": "test_explicit_ref1", 
                        "explicit_ref2": "test_explicit_ref2", 
                        "link": "test_link_name1"},
                       {"explicit_ref1": "test_explicit_ref2", 
                        "explicit_ref2": "test_explicit_ref1", 
                        "link": "test_link_name1"}]
for explicit_reference_link in explicit_reference_links:
    explicit_reference_ddbb = session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == explicit_reference["explicit_ref"]).first()
    explicit_reference_link_ddbb = session.query(ExplicitRefLink).filter(ExplicitRef.explicit_ref == explicit_reference_link["explicit_ref2"], ExplicitRefLink.name == explicit_reference_link["link"], ExplicitRefLink.explicit_ref_id_link == explicit_reference_ddbb.explicit_ref_id).first()
    print(colored("Details", on_color="on_green") + "_{}:".format(getframeinfo(currentframe()).lineno) + str(explicit_reference_link))
    result = {"message":"OK","color":"green"}
    if explicit_reference_link_ddbb == None:
        result = {"message":"NOK","color":"red"}
    # end if
    print(colored("Check", on_color="on_blue") + "_{}: Link between explicit references has been inserted correcly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

# end for

## Links between explicit references ingestion
explicit_reference_links = [{"explicit_ref1": "test_explicit_ref1", 
                        "explicit_ref2": "test_explicit_ref2", 
                        "link": "test_link_name1"},
                       {"explicit_ref1": "test_explicit_ref2", 
                        "explicit_ref2": "test_explicit_ref1", 
                        "link": "test_link_name1"}]
for explicit_reference_link in explicit_reference_links:
    explicit_reference_ddbb = session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == explicit_reference["explicit_ref"]).first()
    explicit_reference_link_ddbb = session.query(ExplicitRefLink).filter(ExplicitRef.explicit_ref == explicit_reference_link["explicit_ref2"], ExplicitRefLink.name == explicit_reference_link["link"], ExplicitRefLink.explicit_ref_id_link == explicit_reference_ddbb.explicit_ref_id).first()
    print(colored("Details", on_color="on_green") + "_{}:".format(getframeinfo(currentframe()).lineno) + str(explicit_reference_link))
    result = {"message":"OK","color":"green"}
    if explicit_reference_link_ddbb == None:
        result = {"message":"NOK","color":"red"}
    # end if
    print(colored("Check", on_color="on_blue") + "_{}: Link between explicit references has been inserted correcly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

# end for

## Links between events ingestion
list_event_links = [session.query(EventLink).filter(EventLink.event_uuid_link == x.event_uuid, EventLink.event_uuid == y.event_uuid).first() for x in list_events_ddbb for y in list_events_ddbb if x != y]
result = {"message":"OK","color":"green"}
if len (list_event_links) != 2:
    result = {"message":"NOK","color":"red"}
# end if
print(colored("Check", on_color="on_blue") + "_{}: Event links have been inserted correcly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

## Check that the same source is not ingested more than once
engine_gsdm.treat_data()
dim_processing_status = session.query(DimProcessingStatus).filter(DimProcessingStatus.processing_uuid == dim_processing_ddbb.processing_uuid, DimProcessingStatus.proc_status == engine_gsdm.exit_codes["SOURCE_ALREADY_INGESTED"]["status"]).first()
result = {"message":"OK","color":"green"}
if dim_processing_status == None:
    result = {"message":"NOK","color":"red"}
# end if
print(colored("Check", on_color="on_blue") + "_{}: Detected duplication on the ingestion of the same source --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

## Check that the validity period of the source has to be correctly specified (start lower than the stop)
engine_gsdm.parse_data_from_xml(os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/test_wrong_validity_period_source.xml")
engine_gsdm.treat_data()
dim_processing_status = session.query(DimProcessingStatus).join(DimProcessing).filter(DimProcessing.name == "test_wrong_validity_period_source.xml", DimProcessingStatus.proc_status == engine_gsdm.exit_codes["WRONG_SOURCE_PERIOD"]["status"]).first()
result = {"message":"OK","color":"green"}
if dim_processing_status == None:
    result = {"message":"NOK","color":"red"}
# end if
print(colored("Check", on_color="on_blue") + "_{}: Detected wrong period specified on the source --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

## Check that the validity period of the events has to be correctly specified (start lower than the stop)
engine_gsdm.parse_data_from_xml(os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/test_wrong_period_events.xml")
engine_gsdm.treat_data()
dim_processing_status = session.query(DimProcessingStatus).join(DimProcessing).filter(DimProcessing.name == "test_wrong_period_events.xml", DimProcessingStatus.proc_status == engine_gsdm.exit_codes["WRONG_EVENT_PERIOD"]["status"]).first()
result = {"message":"OK","color":"green"}
if dim_processing_status == None:
    result = {"message":"NOK","color":"red"}
# end if
print(colored("Check", on_color="on_blue") + "_{}: Detected wrong period specified on the events --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

## Check that the specified incomplete link between an event and another that does not exist is detected
engine_gsdm.parse_data_from_xml(os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/test_incomplete_event_links.xml")
engine_gsdm.treat_data()
dim_processing_status = session.query(DimProcessingStatus).join(DimProcessing).filter(DimProcessing.name == "test_incomplete_event_links.xml", DimProcessingStatus.proc_status == engine_gsdm.exit_codes["INCOMPLETE_EVENT_LINKS"]["status"]).first()
result = {"message":"OK","color":"green"}
if dim_processing_status == None:
    result = {"message":"NOK","color":"red"}
# end if
print(colored("Check", on_color="on_blue") + "_{}: Detected incomplete specified links between events --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

## Check that the boolean value for an event has to be correctly specified
engine_gsdm.parse_data_from_xml(os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/test_wrong_boolean_event.xml")
engine_gsdm.treat_data()
dim_processing_status = session.query(DimProcessingStatus).join(DimProcessing).filter(DimProcessing.name == "test_wrong_boolean_event.xml", DimProcessingStatus.proc_status == engine_gsdm.exit_codes["WRONG_VALUE"]["status"]).first()
result = {"message":"OK","color":"green"}
if dim_processing_status == None:
    result = {"message":"NOK","color":"red"}
# end if
print(colored("Check", on_color="on_blue") + "_{}: Detected wrong boolean value specified for an event --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

## Check that the double value for an event has to be correctly specified
engine_gsdm.parse_data_from_xml(os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/test_wrong_double_event.xml")
engine_gsdm.treat_data()
dim_processing_status = session.query(DimProcessingStatus).join(DimProcessing).filter(DimProcessing.name == "test_wrong_double_event.xml", DimProcessingStatus.proc_status == engine_gsdm.exit_codes["WRONG_VALUE"]["status"]).first()
result = {"message":"OK","color":"green"}
if dim_processing_status == None:
    result = {"message":"NOK","color":"red"}
# end if
print(colored("Check", on_color="on_blue") + "_{}: Detected wrong double value specified for an event --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

## Check that the timestamp value for an event has to be correctly specified
engine_gsdm.parse_data_from_xml(os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/test_wrong_timestamp_event.xml")
engine_gsdm.treat_data()
dim_processing_status = session.query(DimProcessingStatus).join(DimProcessing).filter(DimProcessing.name == "test_wrong_timestamp_event.xml", DimProcessingStatus.proc_status == engine_gsdm.exit_codes["WRONG_VALUE"]["status"]).first()
result = {"message":"OK","color":"green"}
if dim_processing_status == None:
    result = {"message":"NOK","color":"red"}
# end if
print(colored("Check", on_color="on_blue") + "_{}: Detected wrong timestamp value specified for an event --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

## Check that the geometry value for an event has to be correctly specified
engine_gsdm.parse_data_from_xml(os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/test_wrong_geometry_event.xml")
engine_gsdm.treat_data()
dim_processing_status = session.query(DimProcessingStatus).join(DimProcessing).filter(DimProcessing.name == "test_wrong_geometry_event.xml", DimProcessingStatus.proc_status == engine_gsdm.exit_codes["WRONG_VALUE"]["status"]).first()
result = {"message":"OK","color":"green"}
if dim_processing_status == None:
    result = {"message":"NOK","color":"red"}
# end if
print(colored("Check", on_color="on_blue") + "_{}: Detected wrong geometry value specified for an event --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

## Check that the geometry value for an event has a pair number of coordinates
engine_gsdm.parse_data_from_xml(os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/test_wrong_odd_coordinates_geometry_event.xml")
engine_gsdm.treat_data()
dim_processing_status = session.query(DimProcessingStatus).join(DimProcessing).filter(DimProcessing.name == "test_wrong_odd_coordinates_geometry_event.xml", DimProcessingStatus.proc_status == engine_gsdm.exit_codes["ODD_NUMBER_OF_COORDINATES"]["status"]).first()
result = {"message":"OK","color":"green"}
if dim_processing_status == None:
    result = {"message":"NOK","color":"red"}
# end if
print(colored("Check", on_color="on_blue") + "_{}: Detected odd number of coordinates in the geometry value specified for an event --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

## Check that the boolean value for an annotation has to be correctly specified
engine_gsdm.parse_data_from_xml(os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/test_wrong_boolean_annotation.xml")
engine_gsdm.treat_data()
dim_processing_status = session.query(DimProcessingStatus).join(DimProcessing).filter(DimProcessing.name == "test_wrong_boolean_annotation.xml", DimProcessingStatus.proc_status == engine_gsdm.exit_codes["WRONG_VALUE"]["status"]).first()
result = {"message":"OK","color":"green"}
if dim_processing_status == None:
    result = {"message":"NOK","color":"red"}
# end if
print(colored("Check", on_color="on_blue") + "_{}: Detected wrong boolean value specified for an annotation --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

## Check that the double value for an annotation has to be correctly specified
engine_gsdm.parse_data_from_xml(os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/test_wrong_double_annotation.xml")
engine_gsdm.treat_data()
dim_processing_status = session.query(DimProcessingStatus).join(DimProcessing).filter(DimProcessing.name == "test_wrong_double_annotation.xml", DimProcessingStatus.proc_status == engine_gsdm.exit_codes["WRONG_VALUE"]["status"]).first()
result = {"message":"OK","color":"green"}
if dim_processing_status == None:
    result = {"message":"NOK","color":"red"}
# end if
print(colored("Check", on_color="on_blue") + "_{}: Detected wrong double value specified for an annotation --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

## Check that the timestamp value for an annotation has to be correctly specified
engine_gsdm.parse_data_from_xml(os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/test_wrong_timestamp_annotation.xml")
engine_gsdm.treat_data()
dim_processing_status = session.query(DimProcessingStatus).join(DimProcessing).filter(DimProcessing.name == "test_wrong_timestamp_annotation.xml", DimProcessingStatus.proc_status == engine_gsdm.exit_codes["WRONG_VALUE"]["status"]).first()
result = {"message":"OK","color":"green"}
if dim_processing_status == None:
    result = {"message":"NOK","color":"red"}
# end if
print(colored("Check", on_color="on_blue") + "_{}: Detected wrong timestamp value specified for an annotation --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

## Check that the geometry value for an annotation has to be correctly specified
engine_gsdm.parse_data_from_xml(os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/test_wrong_geometry_annotation.xml")
engine_gsdm.treat_data()
dim_processing_status = session.query(DimProcessingStatus).join(DimProcessing).filter(DimProcessing.name == "test_wrong_geometry_annotation.xml", DimProcessingStatus.proc_status == engine_gsdm.exit_codes["WRONG_VALUE"]["status"]).first()
result = {"message":"OK","color":"green"}
if dim_processing_status == None:
    result = {"message":"NOK","color":"red"}
# end if
print(colored("Check", on_color="on_blue") + "_{}: Detected wrong geometry value specified for an annotation --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

## Check that the geometry value for an annotation has a pair number of coordinates
engine_gsdm.parse_data_from_xml(os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/test_wrong_odd_coordinates_geometry_annotation.xml")
engine_gsdm.treat_data()
dim_processing_status = session.query(DimProcessingStatus).join(DimProcessing).filter(DimProcessing.name == "test_wrong_odd_coordinates_geometry_annotation.xml", DimProcessingStatus.proc_status == engine_gsdm.exit_codes["ODD_NUMBER_OF_COORDINATES"]["status"]).first()
result = {"message":"OK","color":"green"}
if dim_processing_status == None:
    result = {"message":"NOK","color":"red"}
# end if
print(colored("Check", on_color="on_blue") + "_{}: Detected odd number of coordinates in the geometry value specified for an annotation --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

# Generate the excel file containing the inserted data into the DDBB
analysis = Analysis()
output_file = os.path.dirname(os.path.abspath(__file__)) + "/tmp/analysis_after_simple_update.xlsx"
analysis.generate_workbook_from_ddbb(output_file)

print("***Data present into DDBB exported into the excel file " + output_file)

# Generate the xml file containing the inserted data into the DDBB
output_xml_file = os.path.dirname(os.path.abspath(__file__)) + "/tmp/test_simple_update_query.xml"
engine_gsdm.get_source_xml("test_simple_update.xml", output_xml_file)

print("***Data present into DDBB exported into the xml file " + output_xml_file)

## Check multiple insertion operations
engine_gsdm.parse_data_from_xml(os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/test_multiple_operations.xml")
engine_gsdm.treat_data()

events = [{"start": "2018-06-05T03:07:03",
           "stop": "2018-06-05T03:07:36",
           "generation_time": "2018-06-06T13:33:29",
           "key": "test_key1",
           "explicit_reference": "test_explicit_ref1",
           "gauge": ("test_gauge_name1", "test_gauge_system1"),
           "values":[
               {"name": "test_object_name1",
                "level_position": 0,
                "parent_level": -1,
                "parent_position": 0},
               {"name": "test_text_name1",
                "level_position": 0,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_timestamp_name1",
                "level_position": 1,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_boolean_name1",
                "level_position": 2,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_double_name1",
                "level_position": 3,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_object_name2",
                "level_position": 4,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_text_name2",
                "level_position": 0,
                "parent_level": 1,
                "parent_position": 4},
               {"name": "test_geometry1",
                "level_position": 1,
                "parent_level": 1,
                "parent_position": 4},
               {"name": "test_text_name10",
                "level_position": 5,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_boolean_name10",
                "level_position": 6,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_double_name10",
                "level_position": 7,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_object_name11",
                "level_position": 8,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_text_name11",
                "level_position": 2,
                "parent_level": 1,
                "parent_position": 8},
               {"name": "test_geometry10",
                "level_position": 3,
                "parent_level": 1,
                "parent_position": 8}
           ]},
          {"start": "2018-06-05T04:07:03",
           "stop": "2018-06-05T04:07:36",
           "generation_time": "2020-06-06T13:33:29",
           "key": "test_key1",
           "explicit_reference": "test_explicit_ref1",
           "gauge": ("test_gauge_name1", "test_gauge_system1"),
           "values":[
               {"name": "test_object_name1",
                "level_position": 0,
                "parent_level": -1,
                "parent_position": 0},
               {"name": "test_text_name1",
                "level_position": 0,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_timestamp_name1",
                "level_position": 1,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_boolean_name1",
                "level_position": 2,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_double_name1",
                "level_position": 3,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_object_name2",
                "level_position": 4,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_text_name2",
                "level_position": 0,
                "parent_level": 1,
                "parent_position": 4},
               {"name": "test_geometry1",
                "level_position": 1,
                "parent_level": 1,
                "parent_position": 4},
               {"name": "test_text_name10",
                "level_position": 5,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_boolean_name10",
                "level_position": 6,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_double_name10",
                "level_position": 7,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_object_name11",
                "level_position": 8,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_text_name11",
                "level_position": 2,
                "parent_level": 1,
                "parent_position": 8},
               {"name": "test_geometry10",
                "level_position": 3,
                "parent_level": 1,
                "parent_position": 8}
           ]},
          {"start": "2018-06-05T05:07:03",
           "stop": "2018-06-05T05:07:36",
           "generation_time": "2018-06-06T13:33:29",
           "key": "test_key1",
           "explicit_reference": "test_explicit_ref1",
           "gauge": ("test_gauge_name1", "test_gauge_system1"),
           "values":[
               {"name": "test_object_name1",
                "level_position": 0,
                "parent_level": -1,
                "parent_position": 0},
               {"name": "test_text_name1",
                "level_position": 0,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_timestamp_name1",
                "level_position": 1,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_boolean_name1",
                "level_position": 2,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_double_name1",
                "level_position": 3,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_object_name2",
                "level_position": 4,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_text_name2",
                "level_position": 0,
                "parent_level": 1,
                "parent_position": 4},
               {"name": "test_geometry1",
                "level_position": 1,
                "parent_level": 1,
                "parent_position": 4},
               {"name": "test_text_name10",
                "level_position": 5,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_boolean_name10",
                "level_position": 6,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_double_name10",
                "level_position": 7,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_object_name11",
                "level_position": 8,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_text_name11",
                "level_position": 2,
                "parent_level": 1,
                "parent_position": 8},
               {"name": "test_geometry10",
                "level_position": 3,
                "parent_level": 1,
                "parent_position": 8}
           ]},
          {"start": "2018-06-05T06:07:03",
           "stop": "2018-06-05T06:07:36",
           "generation_time": "2020-06-06T13:33:29",
           "key": "test_key1",
           "explicit_reference": "test_explicit_ref1",
           "gauge": ("test_gauge_name1", "test_gauge_system1"),
           "values":[
               {"name": "test_object_name1",
                "level_position": 0,
                "parent_level": -1,
                "parent_position": 0},
               {"name": "test_text_name1",
                "level_position": 0,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_timestamp_name1",
                "level_position": 1,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_boolean_name1",
                "level_position": 2,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_double_name1",
                "level_position": 3,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_object_name2",
                "level_position": 4,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_text_name2",
                "level_position": 0,
                "parent_level": 1,
                "parent_position": 4},
               {"name": "test_geometry1",
                "level_position": 1,
                "parent_level": 1,
                "parent_position": 4},
               {"name": "test_text_name10",
                "level_position": 5,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_boolean_name10",
                "level_position": 6,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_double_name10",
                "level_position": 7,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_object_name11",
                "level_position": 8,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_text_name11",
                "level_position": 2,
                "parent_level": 1,
                "parent_position": 8},
               {"name": "test_geometry10",
                "level_position": 3,
                "parent_level": 1,
                "parent_position": 8}
           ]},
          {"start": "2018-06-05T01:07:03",
           "stop": "2018-06-05T08:07:36",
           "generation_time": "2016-06-01T13:33:29",
           "key": "test_key1",
           "explicit_reference": "test_explicit_ref1",
           "gauge": ("test_gauge_name1", "test_gauge_system1"),
           "values":[
               {"name": "test_object_name1",
                "level_position": 0,
                "parent_level": -1,
                "parent_position": 0},
               {"name": "test_text_name1",
                "level_position": 0,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_timestamp_name1",
                "level_position": 1,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_boolean_name1",
                "level_position": 2,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_double_name1",
                "level_position": 3,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_object_name2",
                "level_position": 4,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_text_name2",
                "level_position": 0,
                "parent_level": 1,
                "parent_position": 4},
               {"name": "test_geometry1",
                "level_position": 1,
                "parent_level": 1,
                "parent_position": 4},
               {"name": "test_text_name10",
                "level_position": 5,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_boolean_name10",
                "level_position": 6,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_double_name10",
                "level_position": 7,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_object_name11",
                "level_position": 8,
                "parent_level": 0,
                "parent_position": 0},
               {"name": "test_text_name11",
                "level_position": 2,
                "parent_level": 1,
                "parent_position": 8},
               {"name": "test_geometry10",
                "level_position": 3,
                "parent_level": 1,
                "parent_position": 8}
           ]}
      ]
for event in events:
    event_ddbb = session.query(Event).join(DimProcessing).join(ExplicitRef).filter(Event.start == event["start"], Event.stop == event["stop"], DimProcessing.generation_time == event["generation_time"], ExplicitRef.explicit_ref == event["explicit_reference"]).first()
    list_events_ddbb.append(event_ddbb)
    print(colored("Details", on_color="on_green") + "_{}:".format(getframeinfo(currentframe()).lineno) + str(event))
    result = {"message":"OK","color":"green"}
    if event_ddbb == None:
        result = {"message":"NOK","color":"red"}
    # end if
    print(colored("Check", on_color="on_blue") + "_{}: Event has been inserted correcly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

    result = {"message":"OK","color":"green"}
    if event_ddbb.gauge_id != list_gauges[event["gauge"]].gauge_id:
        result = {"message":"NOK","color":"red"}
    # end if
    print(colored("Check", on_color="on_blue") + "_{}: Event has been associated to the gauge correcly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

    event_key_ddbb = session.query(EventKey).filter(EventKey.event_uuid == event_ddbb.event_uuid, EventKey.event_key == event["key"]).first()
    result = {"message":"OK","color":"green"}
    if link_ddbb == None:
        result = {"message":"NOK","color":"red"}
    # end if
    print(colored("Check", on_color="on_blue") + "_{}: Event key has been inserted correcly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

    if "values" in event:
        values_ddbb = engine_gsdm.get_event_values([event_ddbb.event_uuid])
        for value in event["values"]:
            value_ddbb = [value_ddbb for value_ddbb in values_ddbb if value_ddbb.level_position == value["level_position"] and value_ddbb.parent_level == value["parent_level"] and value_ddbb.parent_position == value["parent_position"] and value_ddbb.name == value["name"]]
            result = {"message":"OK","color":"green"}
            if len(value_ddbb) != 1:
                result = {"message":"NOK","color":"red"}
            # end if
            print(colored("Check", on_color="on_blue") + "_{}: Event value with name {} has been inserted correcly --> ".format(getframeinfo(currentframe()).lineno, value["name"]) + colored(result["message"], result["color"], attrs=['bold']))

        # end for
    # end if

# end for

# Generate the excel file containing the inserted data into the DDBB
analysis = Analysis()
output_file = os.path.dirname(os.path.abspath(__file__)) + "/tmp/analysis_after_multiple_operations.xlsx"
analysis.generate_workbook_from_ddbb(output_file)

print("***Data present into DDBB exported into the excel file " + output_file)

## Check ERASE and REPLACE and EVENT KEYS insertion types
engine_gsdm.parse_data_from_xml(os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/test_erase_and_replace_and_keys.xml")
#print(json.dumps(engine_gsdm.data, indent=4))

### PENDING checks on the parser

engine_gsdm.treat_data()

events = [{"start": "2018-06-05T01:07:03",
           "stop": "2018-06-05T02:07:15",
           "generation_time": "2016-06-01T13:33:29"},
          {"start": "2018-06-05T02:07:03",
           "stop": "2018-06-05T02:07:15",
           "generation_time": "2018-06-06T13:33:29"},
          {"start": "2018-06-05T02:07:15",
           "stop": "2018-06-05T04:07:03",
           "generation_time": "2018-06-07T13:33:29"},
          {"start": "2018-06-05T02:07:43",
           "stop": "2018-06-05T02:07:56",
           "generation_time": "2018-06-07T13:33:29"},
          {"start": "2018-06-05T04:07:03",
           "stop": "2018-06-05T04:07:36",
           "generation_time": "2020-06-06T13:33:29"},
          {"start": "2018-06-05T04:07:36",
           "stop": "2018-06-05T06:07:03",
           "generation_time": "2018-06-07T13:33:29"},
          {"start": "2018-06-05T06:07:03",
           "stop": "2018-06-05T06:07:36",
           "generation_time": "2020-06-06T13:33:29"},
          {"start": "2018-06-05T06:07:15",
           "stop": "2018-06-05T08:07:36",
           "generation_time": "2016-06-01T13:33:29"}
]
events_ddbb = session.query(Event, DimProcessing.generation_time).join(DimProcessing).order_by(Event.start).all()
result = {"message":"OK","color":"green"}
if len(events_ddbb) != len(events):
    result = {"message":"NOK","color":"red"}
# end if
print(colored("Check", on_color="on_blue") + "_{}: The number of events inserted {} is the expected {} --> ".format(getframeinfo(currentframe()).lineno, len(events_ddbb), len(events)) + colored(result["message"], result["color"], attrs=['bold']))
for info in events_ddbb:
    generation_time = info[1]
    event_ddbb = info[0]
    print(colored("Details", on_color="on_green") + "_{}:".format(getframeinfo(currentframe()).lineno) + str(event_ddbb.__dict__) + "; 'generation_time': " + str(generation_time))
    event = [event for event in events if parse(event["start"]) == event_ddbb.start and 
             parse(event["stop"]) == event_ddbb.stop and 
             parse(event["generation_time"]) == generation_time]
    result = {"message":"OK","color":"green"}
    if len(event) == 0:
        result = {"message":"NOK","color":"red"}
    # end if
    print(colored("Check", on_color="on_blue") + "_{}: Event has been inserted correcly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))
# end for

# Generate the excel file containing the inserted data into the DDBB
analysis = Analysis()
output_file = os.path.dirname(os.path.abspath(__file__)) + "/tmp/analysis_after_erase_and_replace.xlsx"
analysis.generate_workbook_from_ddbb(output_file)

print("***Data present into DDBB exported into the excel file " + output_file)

sys.exit()
