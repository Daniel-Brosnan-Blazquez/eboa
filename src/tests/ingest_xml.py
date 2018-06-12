"""
Test: ingest xml test

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
import os
import sys

# Adding path to the engine package
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine.engine import Engine
from datamodel.base import Session, engine, Base
from datamodel.dim_signatures import DimSignature
from datamodel.events import Event, EventLink, EventText, EventDouble, EventObject, EventGeometry
from datamodel.gauges import Gauge
from datamodel.dim_processings import DimProcessing, DimProcessingStatus
from datamodel.explicit_refs import ExplicitRef, ExplicitRefGrp, ExplicitRefLink
from datamodel.annotations import Annotation, AnnotationCnf, AnnotationText, AnnotationDouble, AnnotationObject, AnnotationGeometry
from termcolor import colored
from inspect import currentframe, getframeinfo
import json

# Create session to connect to the database
session = Session()

# Clear all tables before executing the test
for table in reversed(Base.metadata.sorted_tables):
    engine.execute(table.delete())
# end for

# insert data from xml
engine_gsdm = Engine()
engine_gsdm.parse_data_from_xml(os.path.dirname(os.path.abspath(__file__)) + "/test_input1.xml")
print(json.dumps(engine_gsdm.data, indent=4))
engine_gsdm.treat_data()

# Checks
## DIM Signature ingestion
dim_signature = {"dim_signature":"test_dim_signature1", "dim_exec_name": "test_exec1"}
dim_signature_ddbb = session.query(DimSignature).filter(DimSignature.dim_signature == dim_signature["dim_signature"], DimSignature.dim_exec_name == dim_signature["dim_exec_name"]).first()
print ("Details_{}:".format(getframeinfo(currentframe()).lineno) + str(dim_signature))
result = {"message":"OK","color":"green"}
if dim_signature_ddbb == None:
    result = {"message":"NOK","color":"red"}
# end if
print (colored("Check", on_color="on_blue") + "_{}: DIM signature has been inserted correcly? ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

## DIM Processing ingestion
dim_processing = {"filename": "test_input1.xml",
                  "validity_start": "2018-06-05T02:07:03",
                  "validity_stop": "2018-06-05T02:07:36",
                  "generation_time": "2018-06-06T13:33:29"}
dim_processing_ddbb = session.query(DimProcessing).filter(DimProcessing.filename == dim_processing["filename"], DimProcessing.validity_start == dim_processing["validity_start"], DimProcessing.validity_stop == dim_processing["validity_stop"], DimProcessing.generation_time == dim_processing["generation_time"]).first()
print ("Details_{}:".format(getframeinfo(currentframe()).lineno) + str(dim_processing))
result = {"message":"OK","color":"green"}
if dim_processing_ddbb == None:
    result = {"message":"NOK","color":"red"}
# end if
print (colored("Check", on_color="on_blue") + "_{}: DIM processing has been inserted correcly? ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

result = {"message":"OK","color":"green"}
if dim_processing_ddbb.dim_signature_id != dim_signature_ddbb.dim_signature_id:
    result = {"message":"NOK","color":"red"}
# end if
print (colored("Check", on_color="on_blue") + "_{}: DIM processing has been associated to the DIM Signature correcly? ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

## Gauges ingestion
gauges = [{"name": "test_gauge_name1", "system": "test_gauge_system1"},
          {"name": "test_gauge_name2", "system": "test_gauge_system2"}]
for gauge in gauges:
    gauge_ddbb = session.query(Gauge).filter(Gauge.name == gauge["name"], Gauge.system == gauge["system"]).first()
    print ("Details_{}:".format(getframeinfo(currentframe()).lineno) + str(gauge))
    result = {"message":"OK","color":"green"}
    if gauge_ddbb == None:
        result = {"message":"NOK","color":"red"}
    # end if
    print (colored("Check", on_color="on_blue") + "_{}: Gauge has been inserted correcly? ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

    result = {"message":"OK","color":"green"}
    if gauge_ddbb.dim_signature_id != dim_signature_ddbb.dim_signature_id:
        result = {"message":"NOK","color":"red"}
    # end if
    print (colored("Check", on_color="on_blue") + "_{}: Gauge has been associated to the DIM Signature correcly? ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))
# end for

## Annotation Configuration ingestion
annotation_cnfs = [{"name": "test_annotation_cnf_name1", "system": "test_annotation_cnf_system1"}]
for annotation_cnf in annotation_cnfs:
    annotation_cnf_ddbb = session.query(AnnotationCnf).filter(AnnotationCnf.name == annotation_cnf["name"], AnnotationCnf.system == annotation_cnf["system"]).first()
    print ("Details_{}:".format(getframeinfo(currentframe()).lineno) + str(gauge))
    result = {"message":"OK","color":"green"}
    if annotation_cnf_ddbb == None:
        result = {"message":"NOK","color":"red"}
    # end if
    print (colored("Check", on_color="on_blue") + "_{}: Annotation configuration has been inserted correcly? ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

    result = {"message":"OK","color":"green"}
    if annotation_cnf_ddbb.dim_signature_id != dim_signature_ddbb.dim_signature_id:
        result = {"message":"NOK","color":"red"}
    # end if
    print (colored("Check", on_color="on_blue") + "_{}: Annotation configuration has been associated to the DIM Signature correcly? ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))
# end for

## Explicit reference groups ingestion
expl_groups = [{"name": "test_explicit_ref_group1"}]
list_expl_groups = {}
for expl_group in expl_groups:
    expl_group_ddbb = session.query(ExplicitRefGrp).filter(ExplicitRefGrp.name == expl_group["name"]).first()
    list_expl_groups[expl_group["name"]] = expl_group_ddbb
    print ("Details_{}:".format(getframeinfo(currentframe()).lineno) + str(gauge))
    result = {"message":"OK","color":"green"}
    if expl_group_ddbb == None:
        result = {"message":"NOK","color":"red"}
    # end if
    print (colored("Check", on_color="on_blue") + "_{}: Explicit reference group has been inserted correcly? ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))
# end for

## Explicit reference ingestion
explicit_references = [{"explicit_ref": "test_explicit_ref1", "group": "test_explicit_ref_group1"},
                       {"explicit_ref": "test_explicit_ref2"}]
for explicit_reference in explicit_references:
    explicit_reference_ddbb = session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == explicit_reference["explicit_ref"]).first()
    print ("Details_{}:".format(getframeinfo(currentframe()).lineno) + str(gauge))
    result = {"message":"OK","color":"green"}
    if explicit_reference_ddbb == None:
        result = {"message":"NOK","color":"red"}
    # end if
    print (colored("Check", on_color="on_blue") + "_{}: Explicit reference has been inserted correcly? ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))

    if "group" in explicit_reference:
        result = {"message":"OK","color":"green"}
        if explicit_reference_ddbb.expl_ref_cnf_id != list_expl_groups[explicit_reference["group"]].expl_ref_cnf_id:
            result = {"message":"NOK","color":"red"}
        # end if
        print (colored("Check", on_color="on_blue") + "_{}: Explicit reference has been associated to the group correcly? ".format(getframeinfo(currentframe()).lineno) + colored(result["message"], result["color"], attrs=['bold']))
    # end if
# end for
