#!/usr/bin/env python3
"""
Script for initializing UBOA environment (user management)

Written by DEIMOS Space S.L. (dibb)

module uboa
"""
# Import python utilities
import argparse
import re
from eboa.common.commands import execute_command
import os
import shutil

# Import auxiliary functions
from uboa.datamodel.functions import read_configuration

# Import UBOA engine functions
import uboa.engine.engine as uboa_engine
from uboa.engine.engine import Engine

config = read_configuration()

db_configuration = config["DDBB_CONFIGURATION"]

def init(datamodel_path = None, not_insert_users = False, users_configuration = None):

    # Path to the datamodel
    if datamodel_path != None:
        # Check if file exists
        if not os.path.isfile(datamodel_path):
            print("The specified path to the datamodel file {} does not exist".format(datamodel_path))
            exit(-1)
        # end if
    else:
        # Default path for the docker environment
        datamodel_path = "/datamodel/uboa_data_model.sql"
    # end if

    datatabse_address = db_configuration["host"]
    datatabse_port = db_configuration["port"]

    command = "uboa_init_ddbb.sh -h {} -p {} -f {}".format(datatabse_address, datatabse_port, datamodel_path)
    print("The UBOA database is going to be initialize using the datamodel SQL file {}...".format(datamodel_path))
    execute_command(command, "The UBOA database has been initialized successfully :-)")

    if not not_insert_users:

        engine_uboa = Engine()
        exit_status = engine_uboa.insert_configuration(users_configuration)

        failed_operations = [item for item in exit_status if item["status"] not in (uboa_engine.exit_codes["OK"]["status"], uboa_engine.exit_codes["FILE_VALID"]["status"])]
        if len(failed_operations) != 0:
            print("There have been the following failed operations when treating the users configuration {}".format(users_configuration))
            for failed_operation in failed_operations:
                print("Status: {}, message: {}".format(failed_operation["status"], failed_operation["message"]))
            # end if
            exit(-1)
        # end if
        
        print("The users configuration has been inserted successfully :-)")
    # end if


def main():

    args_parser = argparse.ArgumentParser(description="Initialize UBOA environment (User management).")
    args_parser.add_argument("-f", dest="datamodel_path", type=str, nargs=1,
                             help="Path to the datamodel", required=False)
    args_parser.add_argument("-u", dest="users_configuration", type=str, nargs=1,
                             help="Path to the users configuration", required=False)
    args_parser.add_argument("-n", "--not_insert_users",
                             help="Do not insert the users provided by the configuration", action="store_true")
    args_parser.add_argument("-y", "--accept_everything",
                             help="Accept by default every request (Be careful when using this because it will drop all the data without requesting any confirmation)", action="store_true")

    args = args_parser.parse_args()

    if not args.accept_everything:
        continue_flag = input("\n" +
                              "Welcome to the UBOA initializer (User management) :-)\n" +
                              "You are about to initialize the DDBB of the user management.\n" +
                              "This operation will erase all the information stored related to the user management, would you still want to continue? [Ny]")

        if continue_flag != "y":
            print("No worries! The initialization is going to be aborted :-)")
            exit(0)
        # end if
    # end if
    
    datamodel_path = None
    if args.datamodel_path != None:
        datamodel_path = args.datamodel_path[0]
    # end if

    users_configuration = None
    if args.users_configuration != None:
        users_configuration = args.users_configuration[0]
    # end if

    # Initialize DDBB
    init(datamodel_path, args.not_insert_users, users_configuration)
    
    exit(0)
    
if __name__ == "__main__":

    main()

