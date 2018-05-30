"""
Test: concurrent creation of uuids and insertion into DDBB

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""

# Adding path to the datamodel package
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from datamodel.base import Session, engine, Base
from uuids import UuidTest
import time

import threading, uuid

NUMBER_OF_THREADS=8
NUMBER_OF_INSERTIONS=100000

def test_uuid(test_func):
  """
  Function to execute the function received X times

  Args:
  test_func (function): function to be executed

  Returns: Nothing
  """
  session = Session ()
  for i in range(NUMBER_OF_INSERTIONS):
    # Create the uuid instance
    uuidInstance = UuidTest (test_func())
    # Commit to database
    session.add (uuidInstance)
  session.commit()
  session.close()

def test(test_func, threads):
  """
  Function to execute threads running the uuid function

  Args:
  test_func (function): function to be executed by the threads
  test_func (threads): number of threads to be executed

  Returns: Nothing
  """
  print ('Running {} with {} threads...'.format(test_func.__name__, threads))

  workers = [threading.Thread(target=test_uuid, args=(test_func,)) for x in range(threads)]
  [x.start() for x in workers]
  [x.join() for x in workers]
  print ('Done!')

if __name__ == '__main__':
  # Remove content of the table
  engine.execute(UuidTest.__table__.delete())

  startTime = time.time()
  test(uuid.uuid1, NUMBER_OF_THREADS)
  stopTime = time.time()

  session = Session ()
  numberUuids = session.query(UuidTest).count()
  # Check that the number of uuids inserted corresponds to the expected
  if numberUuids != NUMBER_OF_THREADS*NUMBER_OF_INSERTIONS:
    print ('ERROR: There has been a colission!!')
  else:
    print ('{} uuids inserted without collision in {} seconds!!'.format(numberUuids, stopTime - startTime))

  # Returning connection to the pool
  session.close()

  # Closing the pool of connections
  engine.dispose()
