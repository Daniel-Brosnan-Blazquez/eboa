"""
Test: ingest xml test

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
import os
import sys

# Adding path to the engine package
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine.analysis import Analysis

analysis = Analysis()

output_file = os.path.dirname(os.path.abspath(__file__)) + "/tmp/analysis.xlsx"
analysis.generate_workbook_from_ddbb(output_file)

print("Data present into DDBB exported into the file " + output_file)
