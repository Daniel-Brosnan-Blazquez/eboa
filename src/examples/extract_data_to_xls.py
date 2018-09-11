"""
Test: ingest xml test

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
import os
import sys

from eboa.engine.analysis import Analysis

def extract_data_to_xls():
    analysis = Analysis()
    
    output_file = os.path.dirname(os.path.abspath(__file__)) + "/tmp/analysis.xlsx"
    analysis.generate_workbook_from_ddbb(output_file)
    
    print("Data present into DDBB exported into the file " + output_file)

if __name__ == "__main__":
    extract_data_to_xls()
