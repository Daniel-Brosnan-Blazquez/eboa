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

# insert data from xml
engine = Engine()
engine.insertData (os.path.dirname(os.path.abspath(__file__)) + "/test_input1.xml")
