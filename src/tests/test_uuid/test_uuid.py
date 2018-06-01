"""
Test: concurrent creation of uuids and insertion into DDBB

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""

# Adding path to the datamodel package
from uuids import UuidTest
import time, uuid, random
from multiprocessing import Process

NUMBER_OF_PROCESSES=3000
NUMBER_OF_INSERTIONS=1000

def test_uuid():
  """
  Function to execute the function received X times

  Args:
  None

  Returns: Nothing
  """
  import sys
  import os
  sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
  
  from datamodel.base import Session, engine, Base

  session = Session ()
  for i in range(NUMBER_OF_INSERTIONS):
    # Create the uuid instance
    uuidInstance = UuidTest (uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14)))
    # Commit to database
    session.add (uuidInstance)
  session.commit()
  session.close()

def test(processes):
  """
  Function to execute processes running the uuid function

  Args:
  processes (integer): number of processes to be executed

  Returns: Nothing
  """
  print ('Running {} processes...'.format(processes))

  workers = [Process(target=test_uuid) for x in range(processes)]
  [x.start() for x in workers]
  [x.join() for x in workers]
  print ('Done!')

if __name__ == '__main__':
  # Remove content of the table
  import sys
  import os
  sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

  from datamodel.base import Session, engine, Base
  engine.execute(UuidTest.__table__.delete())

  # Closing the pool of connections
  engine.dispose()

  startTime = time.time()
  test(NUMBER_OF_PROCESSES)
  stopTime = time.time()

  session = Session ()
  numberUuids = session.query(UuidTest).count()
  # Check that the number of uuids inserted corresponds to the expected
  if numberUuids != NUMBER_OF_PROCESSES*NUMBER_OF_INSERTIONS:
    print ('ERROR: There has been a colission!!')
  else:
    print ('{} uuids inserted without collision in {} seconds!!'.format(numberUuids, stopTime - startTime))

  # Returning connection to the pool
  session.close()

  # Closing the pool of connections
  engine.dispose()
