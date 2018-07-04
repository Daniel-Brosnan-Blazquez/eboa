"""
Test: ingest events using multiprocessing test

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
import os
import sys

from gsdm.datamodel.dim_signatures import DimSignature
from gsdm.datamodel.base import Session, engine, Base
from gsdm.datamodel.events import Event
from gsdm.datamodel.gauges import Gauge
from gsdm.datamodel.explicit_refs import ExplicitRef
from gsdm.datamodel.dim_processings import DimProcessing, DimProcessingStatus
import datetime
import uuid
import time
import random
from multiprocessing import Process

nProcesses = 10
nEvents = 1000
session = Session ()

def createEvents (nEvents, explicitRef, gauge, dimProcessing):
    session = Session ()
    for i in range(nEvents):
        # Create event
        eventTime = datetime.datetime.now()
        eventUuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        event = Event (eventUuid, eventTime, eventTime, eventTime,gauge, explicitRef, dimProcessing)

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
    dimSignature1 = DimSignature ('TEST', 'TEST')

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

    ################
    # Explicit reference
    ################
    # Create explicit reference
    explicitRefTime = datetime.datetime.now()
    explicitRef1 = ExplicitRef (explicitRefTime, 'TEST')
    
    # Insert explicit reference into database
    session.add (explicitRef1)
    session.commit()
    
    if len (session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == 'TEST').all()) != 1:
        raise Exception("The Explicit Reference was not committed")

    ################
    # Gauge
    ################

    # Create gauge
    gauge1 = Gauge ('TEST', dimSignature1, 'TEST')
    
    # Insert gauge into database
    session.add (gauge1)
    session.commit()

    if len (session.query(Gauge).filter(Gauge.name == 'TEST').all()) != 1:
        raise Exception("The Gauge was not committed")

    session.close()

    # Closing the pool of connections, so that it does not interfere with the connections opened by the forks
    engine.dispose()

    ################
    # Events
    ################
    startTime = time.time()
    print ('Running {} processes...'.format(nProcesses))

    workers = [Process(target=createEvents, args=(nEvents, explicitRef1, gauge1, dimProcessing1)) for x in range(nProcesses)]
    [x.start() for x in workers]
    [x.join() for x in workers]
    print ('Done!')
    stopTime = time.time()

    numberEvents = session.query(Event).count()
    # Check that the number of uuids inserted corresponds to the expected
    if numberEvents != nEvents*nProcesses:
        print ('ERROR: There has been an error creating events. Only created {} out of {}!!'.format(numberEvents, nEvents))
    else:
        print ('{} events inserted in {} seconds!!'.format(numberEvents, stopTime - startTime))

    # Returning connection to the pool
    session.close()

    # Closing the pool of connections
    engine.dispose()
