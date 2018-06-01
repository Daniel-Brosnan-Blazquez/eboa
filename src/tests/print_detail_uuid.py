"""
Test: initial test

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
import os
import sys

# Adding path to the datamodel package
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datamodel.base import Session, engine, Base
from datamodel.dim_signatures import DimSignature
from datamodel.events import Event, EventLink
from datamodel.gauges import Gauge
from datamodel.dim_processings import DimProcessing, DimProcessingStatus
from datamodel.explicit_refs import ExplicitRef, ExplicitRefGrp, ExplicitRefLink
from datamodel.annotations import Annotation, AnnotationCnf
import datetime
import uuid
import pprint

# Create session to connect to the database
session = Session()

for event in session.query(Event).all():
    uuid = event.event_uuid
    print ('bytes   :', repr(uuid.bytes))
    print ('hex     :', uuid.hex)
    print ('int     :', uuid.int)
    print ('urn     :', uuid.urn)
    print ('variant :', uuid.variant)
    print ('version :', uuid.version)
    print ('fields  :', uuid.fields)
    print ('\ttime_low            : ', uuid.time_low)
    print ('\ttime_mid            : ', uuid.time_mid)
    print ('\ttime_hi_version     : ', uuid.time_hi_version)
    print ('\tclock_seq_hi_variant: ', uuid.clock_seq_hi_variant)
    print ('\tclock_seq_low       : ', uuid.clock_seq_low)
    print ('\tnode                : ', uuid.node)
    print ('\ttime                : ', uuid.time)
    print ('\tclock_seq           : ', uuid.clock_seq)

