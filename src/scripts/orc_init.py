#!/usr/bin/env python3
"""
Script for initializing ORC environment (orchestrator)

Written by DEIMOS Space S.L. (dibb)

module orc
"""
# Import python utilities
import argparse
import re
from eboa.common.commands import execute_command
import os
import shutil

# Import auxiliary functions
from eboa.datamodel.functions import read_configuration

# Import engine functions
from eboa.engine.functions import get_resources_path

config = read_configuration()

db_configuration = config["DDBB_CONFIGURATION"]

def init():

    datatabse_address = db_configuration["host"]
    datatabse_port = db_configuration["port"]

    print("The MINARC and ORC database is going to be deleted...")
    command = 'psql -p {} -h {} -t -U postgres -c "DROP DATABASE minarc_orc_db;"'.format(datatabse_port, datatabse_address)
    execute_command(command, "The MINARC and ORC database has been successfully deleted :-)", check_error = False)

    print("The MINARC and ORC database is going to be created...")
    command = 'psql -p {} -h {} -t -U postgres -c "CREATE DATABASE minarc_orc_db;"'.format(datatabse_port, datatabse_address)
    execute_command(command, "The MINARC and ORC database has been successfully created :-)")

    print("The minarc_orc role is going to be deleted...")
    command = 'psql -p {} -h {} -t -U postgres -c "DROP ROLE minarc_orc;"'.format(datatabse_port, datatabse_address)
    execute_command(command, "The minarc_orc role has been successfully created :-)", check_error = False)

    print("The minarc_orc role is going to be created...")
    command = 'psql -p {} -h {} -t -U postgres -c "CREATE ROLE minarc_orc WITH INHERIT LOGIN;"'.format(datatabse_port, datatabse_address)
    execute_command(command, "The minarc_orc role has been successfully created :-)")

    command = "minArcDB --create-tables"
    execute_command(command, "The MINARC and ORC database has been initialized successfully :-)")

    print("The MINARC and ORC archive is going to be initialize...")
    command = "minArcPurge -Y"
    execute_command(command, "The MINARC and ORC archive has been initialized successfully :-)")

    command = "orcManageDB --create-tables"
    execute_command(command, "The MINARC and ORC database has been initialized successfully :-)")

    return
    
def main():

    args_parser = argparse.ArgumentParser(description="Initialize EBOA environment (Scheduler).")
    args_parser.add_argument('-f', dest='datamodel_path', type=str, nargs=1,
                             help='path to the datamodel', required=False)
    args_parser.add_argument("-y", "--accept_everything",
                             help="Accept by default every request (Be careful when using this because it will drop all the data without requesting any confirmation)", action="store_true")

    args = args_parser.parse_args()

    if not args.accept_everything:
        continue_flag = input("\n" +
                              "Welcome to the ORC initializer (Orchestrator) :-)\n" +
                              "You are about to initialize the DDBB of the orchestrator.\n" +
                              "This operation will erase all the information stored related to the orchestrator, would you still want to continue? [Ny]")

        if continue_flag != "y":
            print("No worries! The initialization is going to be aborted :-)")
            exit(0)
        # end if
    # end if
    
    # Initialize DDBB
    init()
    
    exit(0)
    
if __name__ == "__main__":

    main()

