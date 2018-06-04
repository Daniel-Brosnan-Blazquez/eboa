"""
Test: ingest events test

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
import os
import sys

# Adding path to the datamodel package
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datamodel.base import Session, engine, Base
from datamodel.dim_signatures import DimSignature
from datamodel.events import Event
from datamodel.gauges import Gauge
from datamodel.explicit_refs import ExplicitRef
from datamodel.dim_processings import DimProcessing, DimProcessingStatus
import datetime
import uuid
import time
import random

nEvents = 10000
# Create session to connect to the database
session = Session()

def createEvents (nEvents, explicitRef, gauge):
    listEvents = []
    for i in range(nEvents):
        # Create event
        eventTime = datetime.datetime.now()
        eventUuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        event = dict (event_uuid = eventUuid, start = eventTime, stop = eventTime, time_stamp = eventTime, ingestion_time = eventTime, gauge_id = gauge.gauge_id, explicit_ref_id = explicitRef.explicit_ref_id)

        # Insert the event into the database
        listEvents.append (event)

    # Perform bulk ingestion
    session.bulk_insert_mappings(Event, listEvents)
    session.commit()    

if __name__ == '__main__':

    # Clear all tables before executing the test
    for table in reversed(Base.metadata.sorted_tables):
        engine.execute(table.delete())

    ################
    # DIM Signature
    ################
    # Create dim_signature
    dimSignature1 = DimSignature ('TEST', 'TEST')

    # Insert dim_signature into database
    session.add (dimSignature1)
    session.commit()
    
    if len (session.query(DimSignature).filter(DimSignature.dim_signature == 'TEST').all()) != 1:
        raise Exception("The DIM signature was not committed")

    ################
    # Explicit reference
    ################
    # Create explicit reference
    explicitRefTime = datetime.datetime.now()
    explicitRef1 = ExplicitRef (explicitRefTime, 'TEST')
    
    # Insert explicit reference into database
    session.add (explicitRef1)
    session.commit()
    
    explicitRefs = session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == 'TEST').all()
    if len (explicitRefs) != 1:
        raise Exception("The Explicit Reference was not committed")

    ################
    # Gauge
    ################

    # Create gauge
    gauge1 = Gauge ('TEST', dimSignature1, 'TEST', 'TEST')
    
    # Insert gauge into database
    session.add (gauge1)
    session.commit()

    gauges = session.query(Gauge).filter(Gauge.name == 'TEST').all()
    if len (gauges) != 1:
        raise Exception("The Gauge was not committed")

    ################
    # Events
    ################
    explicitRef1 = explicitRefs[0]
    gauge1 = gauges[0]
    startTime = time.time()
    createEvents (nEvents, explicitRef1, gauge1)
    stopTime = time.time()

    numberEvents = session.query(Event).count()
    # Check that the number of uuids inserted corresponds to the expected
    if numberEvents != nEvents:
        print ('ERROR: There has been an error creating events. Only created {} out of {}!!'.format(numberEvents, nEvents))
    else:
        print ('{} events inserted in {} seconds!!'.format(numberEvents, stopTime - startTime))

    # Returning connection to the pool
    session.close()

    # Closing the pool of connections
    engine.dispose()
