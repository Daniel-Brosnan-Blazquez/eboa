"""
Test: ingest events test

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
import os
import sys

from eboa.datamodel.base import Session, engine, Base
from eboa.datamodel.dim_signatures import DimSignature
from eboa.datamodel.events import Event
from eboa.datamodel.gauges import Gauge
from eboa.datamodel.explicit_refs import ExplicitRef
from eboa.datamodel.dim_processings import DimProcessing, DimProcessingStatus
import datetime
import uuid
import time
import random

nEvents = 10000
# Create session to connect to the database
session = Session()

def createEvents (nEvents, explicitRef, gauge, dimProcessing):
    for i in range(nEvents):
        # Create event
        eventTime = datetime.datetime.now()
        eventUuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        event = Event (eventUuid, eventTime, eventTime, eventTime,gauge, dimProcessing, explicit_ref = explicitRef)

        # Insert the event into the database
        session.add (event)

        if len (session.query(Event).filter(Event.event_uuid == eventUuid).all()) != 1:
            raise Exception("The Event was not committed")

    session.commit()    

if __name__ == '__main__':

    # Clear all tables before executing the test
    for table in reversed(Base.metadata.sorted_tables):
        engine.execute(table.delete())

    ################
    # DIM Signature
    ################
    # Create dim_signature
    dim_signature_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
    dimSignature1 = DimSignature (dim_signature_uuid, 'TEST', 'TEST')

    # Insert dim_signature into database
    session.add (dimSignature1)
    session.commit()
    
    if len (session.query(DimSignature).filter(DimSignature.dim_signature == 'TEST').all()) != 1:
        raise Exception("The DIM signature was not committed")

    ################
    # DIM Processing
    ################
    # Create dim_processing
    processingTime = datetime.datetime.now()
    processingUuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
    dimProcessing1 = DimProcessing (processingUuid, 'TEST', processingTime, "1.0", dimSignature1)

    # Insert dim_processing into database
    session.add (dimProcessing1)
    session.commit()
    
    if len (session.query(DimProcessing).filter(DimProcessing.name == 'TEST').all()) != 1:
        raise Exception("The DIM processing was not committed")

    ################
    # Explicit reference
    ################
    # Create explicit reference
    explicitRefTime = datetime.datetime.now()
    explicit_ref1_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
    explicitRef1 = ExplicitRef (explicit_ref1_uuid, explicitRefTime, 'TEST')
    
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
    gauge1_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
    gauge1 = Gauge (gauge1_uuid, 'TEST', dimSignature1, 'TEST')
    
    # Insert gauge into database
    session.add (gauge1)
    session.commit()

    gauges = session.query(Gauge).filter(Gauge.name == 'TEST').all()
    if len (gauges) != 1:
        raise Exception("The Gauge was not committed")

    ################
    # Events
    ################
    startTime = time.time()
    createEvents (nEvents, explicitRef1, gauge1, dimProcessing1)
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
