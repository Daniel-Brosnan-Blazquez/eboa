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
from datamodel.events import Event
from datamodel.dim_signatures import DimSignature
from datamodel.gauges import Gauge
import datetime
import uuid
import pprint

# Create session to connect to the database
session = Session()

# Clear all tables before executing the test
for table in reversed(Base.metadata.sorted_tables):
    engine.execute(table.delete())

input ('Database cleared. Press any key to continue:')

# Create dim_signature
dimSignature1 = DimSignature ('TEST', 'TEST')

# Insert dim_signature into database
session.add (dimSignature1)
session.commit()

if len (session.query(DimSignature).filter(DimSignature.dim_signature == 'TEST').all()) != 1:
    raise Exception("The DIM signature was not committed")

# Create gauge
gauge1 = Gauge ('TEST', dimSignature1)

# Insert gauge into database
session.add (gauge1)
session.commit()

if len (session.query(Gauge).filter(Gauge.name == 'TEST').all()) != 1:
    raise Exception("The Gauge was not committed")

# Create event
eventTime = datetime.datetime.now()
event1 = Event (uuid.uuid1(), eventTime, eventTime, eventTime, eventTime,gauge1)

# Add the event to be committed
session.add (event1)
session.commit()

if len (session.query(Event).filter(Event.start == eventTime).all()) != 1:
    raise Exception("The Event was not committed")

print ('Inserted DIM Signatures ({}):'.format(len (session.query(DimSignature).all())))
for idx, dimSignature in enumerate(session.query(DimSignature).all()):
    print ('DIM signature {}:'.format(idx))
    pprint.pprint(dimSignature.__dict__)
    print ()
print ('\nInserted gauges ({}):'.format(len(session.query(Gauge).all())))
for idx, gauge in enumerate(session.query(Gauge).all()):
    print ('Gauge {}:'.format(idx))
    pprint.pprint(gauge.__dict__)
    print ()
print ('\nInserted events ({}):'.format(len(session.query(Event).all())))
for idx, event in enumerate(session.query(Event).all()):
    print ('Event {}:'.format(idx))
    pprint.pprint(event.__dict__)
    print ()

print ('Searching events by gauge name TEST\n>>> session.query(Event).join(Gauge).filter(Gauge.name == \'TEST\').all()')
eventsGaugeTest = session.query(Event).join(Gauge).filter(Gauge.name == 'TEST').all()
for idx, event in enumerate(eventsGaugeTest):
    print ('Event {}:'.format(idx))
    pprint.pprint(event.__dict__)
    print ()

# Returning connection to the pool
session.close()

# Closing the pool of connections
engine.dispose()
