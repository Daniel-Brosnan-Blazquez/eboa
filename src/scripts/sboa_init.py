#!/usr/bin/env python3
"""
Script for initializing SBOA environment (scheduler)

Written by DEIMOS Space S.L. (dibb)

module sboa
"""
# Import python utilities
import argparse
import re
from eboa.common.commands import execute_command
import os
import shutil

# Import auxiliary functions
from sboa.datamodel.functions import read_configuration

# Import engine functions
from eboa.engine.functions import get_resources_path

config = read_configuration()

db_configuration = config["DDBB_CONFIGURATION"]

def init(datamodel_path = None):

    # Path to the datamodel
    if datamodel_path != None:
        # Check if file exists
        if not os.path.isfile(datamodel_path):
            print("The specified path to the datamodel file {} does not exist".format(datamodel_path))
            exit(-1)
        # end if
    else:
        # Default path for the docker environment
        datamodel_path = "/datamodel/sboa_data_model.sql"
    # end if

    datatabse_address = db_configuration["host"]
    datatabse_port = db_configuration["port"]

    command = "sboa_init_ddbb.sh -h {} -p {} -f {}".format(datatabse_address, datatabse_port, datamodel_path)
    print("The SBOA database is going to be initialize using the datamodel SQL file {}...".format(datamodel_path))
    execute_command(command, "The SBOA database has been initialized successfully :-)")
    
def main():

    args_parser = argparse.ArgumentParser(description="Initialize SBOA environment (Scheduler).")
    args_parser.add_argument("-f", dest="datamodel_path", type=str, nargs=1,
                             help="path to the datamodel", required=False)
    args_parser.add_argument("-y", "--accept_everything",
                             help="Accept by default every request (Be careful when using this because it will drop all the data without requesting any confirmation)", action="store_true")
    
    args = args_parser.parse_args()

    if not args.accept_everything:
        continue_flag = input("\n" +
                              "Welcome to the SBOA initializer (Scheduler) :-)\n" +
                              "You are about to initialize the DDBB of the scheduler.\n" +
                              "This operation will erase all the information stored related to the scheduler, would you still want to continue? [Ny]")

        if continue_flag != "y":
            print("No worries! The initialization is going to be aborted :-)")
            exit(0)
        # end if
    # end if

    datamodel_path = None
    if args.datamodel_path != None:
        datamodel_path = args.datamodel_path[0]
    # end if

    # Initialize DDBB
    init(datamodel_path)
    
    exit(0)
    
if __name__ == "__main__":

    main()

