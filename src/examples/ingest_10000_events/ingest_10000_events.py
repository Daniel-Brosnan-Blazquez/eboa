"""
Test: ingest xml test

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
import os
import sys

# Import engine of the DDBB
from eboa.engine.engine import Engine
from eboa.engine.query import Query
from eboa.datamodel.base import Session, engine, Base

# Import analysis module
from eboa.engine.analysis import Analysis

# Import datamodel
from eboa.datamodel.dim_signatures import DimSignature
from eboa.datamodel.events import Event, EventLink, EventKey, EventText, EventDouble, EventObject, EventGeometry
from eboa.datamodel.gauges import Gauge
from eboa.datamodel.dim_processings import DimProcessing, DimProcessingStatus
from eboa.datamodel.explicit_refs import ExplicitRef, ExplicitRefGrp, ExplicitRefLink
from eboa.datamodel.annotations import Annotation, AnnotationCnf, AnnotationText, AnnotationDouble, AnnotationObject, AnnotationGeometry

# Import python utilities
from termcolor import colored
from inspect import currentframe, getframeinfo
import json
from dateutil.parser import parse

# Import profiling module
import sqltap

# Create session to connect to the database
profiler = sqltap.start()
session = Session()

engine_eboa = Engine()
query_eboa = Query()

usage="Usage: " + sys.argv[0] + " input_file"

if len(sys.argv) != 2:
    print(usage)
    sys.exit(-1)
# end if

file_path = sys.argv[1]
file_name = os.path.basename(file_path)

if not os.path.isfile(file_path):
    print(usage)
    sys.exit(-1)
# end if

if file_path.split(".")[1] == "json":
    engine_eboa.parse_data_from_json(os.path.dirname(os.path.abspath(__file__)) + "/" + file_path, check_schema = False)
else:
    engine_eboa.parse_data_from_xml(os.path.dirname(os.path.abspath(__file__)) + "/" + file_path, check_schema = False)
# end if

engine_eboa.treat_data()

#engine_eboa.generate_json("test_ingestion_10000_events.json")

# Checks
## DIM Signature ingestion
dim_signature = {"dim_signature":"dim_signature", "dim_exec_name": "exec"}
dim_signature_ddbb = session.query(DimSignature).filter(DimSignature.dim_signature == dim_signature["dim_signature"], DimSignature.dim_exec_name == dim_signature["dim_exec_name"]).first()
print(colored("Details", on_color="on_green") + "_{}:".format(getframeinfo(currentframe()).lineno) + str(dim_signature))
result = {"message":"OK","color":"green"}
if dim_signature_ddbb == None:
    result = {"message":"NOK","color":"red"}
# end if
print(colored("Check", on_color="on_blue") + "_{}: DIM signature has been inserted correcly --> ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

## DIM Processing ingestion
dim_processing = {"name": file_name,
                  "validity_start": "2018-06-05T02:07:03",
                  "validity_stop": "2018-06-05T08:07:36",
                  "generation_time": "2018-07-05T02:07:03",
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
gauges = [{"name": "NAME", "system": "SYSTEM"}]
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

## Events ingestion
list_events_ddbb = []
events = [{"start": "2018-06-05T02:07:03",
           "stop": "2018-06-05T08:07:03",
           "generation_time": "2018-07-05T02:07:03",
           "gauge": ("NAME", "SYSTEM"),
           "values":[
               {"name": "VALUE_OBJECT",
                "level_position": 0,
                "parent_level": -1,
                "parent_position": 0},
               {"name": "VALUE_TEXT",
                "level_position": 0,
                "parent_level": 0,
                "parent_position": 0}
           ]}
      ]
for event in events:
    event_ddbb = session.query(Event).join(DimProcessing).filter(Event.start == event["start"], Event.stop == event["stop"], DimProcessing.generation_time == event["generation_time"]).first()
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

    if "values" in event:
        values_ddbb = query_eboa.get_event_values([event_ddbb.event_uuid])
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

statistics = profiler.collect()
sqltap.report(statistics, "report.html", report_format="html")

sys.exit()
