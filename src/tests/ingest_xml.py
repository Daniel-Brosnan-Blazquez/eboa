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

# Create session to connect to the database
session = Session()

# Clear all tables before executing the test
for table in reversed(Base.metadata.sorted_tables):
    engine.execute(table.delete())

# insert data from xml
engine_gsdm = Engine()
engine_gsdm.insert_data (os.path.dirname(os.path.abspath(__file__)) + "/test_input1.xml")
