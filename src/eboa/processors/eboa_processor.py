"""
Basic processor module

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
import json

def process_file(file_path, engine, query, reception_time):
    """Function to process the file and insert its relevant information
    into the DDBB of the eboa
    
    :param file_path: path to the file to be processed
    :type file_path: str
    :param engine: Engine instance
    :type engine: Engine
    :param query: Query instance
    :type query: Query
    :param reception_time: time of the reception of the file by the triggering
    :type reception_time: str
    """

    with open(file_path) as input_file:
        data = json.load(input_file)

    return data
