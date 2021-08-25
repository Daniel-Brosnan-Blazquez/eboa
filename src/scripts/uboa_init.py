#!/usr/bin/env python3
"""
Script for initializing UBOA environment (scheduler)

Written by DEIMOS Space S.L. (dibb)

module uboa
"""
# Import python utilities
import argparse
import re
import shlex
from subprocess import Popen, PIPE
import os
import shutil

# Import auxiliary functions
from uboa.datamodel.functions import read_configuration

# Import engine functions
from eboa.engine.functions import get_resources_path

config = read_configuration()

db_configuration = config["DDBB_CONFIGURATION"]

def execute_command(command, success_message):
    command_split = shlex.split(command)
    program = Popen(command_split, stdin=PIPE, stdout=PIPE, stderr=PIPE)    
    output, error = program.communicate()
    return_code = program.returncode
    if return_code != 0:
        print("The execution of the command {} has ended unexpectedly with the following error: {} and the following output: {}".format(command, str(error), str(output)))
        exit(-1)
    else:
        print(success_message)
    # end try


def main():

    args_parser = argparse.ArgumentParser(description="Initialize UBOA environment (Scheduler).")
    args_parser.add_argument('-f', dest='datamodel_path', type=str, nargs=1,
                             help='path to the datamodel', required=False)

    continue_flag = input("\n" +
                          "Welcome to the UBOA initializer (Scheduler) :-)\n" +
                          "You are about to initialize the DDBB of the scheduler.\n" +
                          "This operation will erase all the information stored related to the scheduler, would you still want to continue? [Ny]")
    
    if continue_flag != "y":
        print("No worries! The initialization is going to be aborted :-)")
        exit(0)
    # end if
    
    args = args_parser.parse_args()

    # Default path for the docker environment
    datamodel_path = "/datamodel/uboa_data_model.sql"
    if args.datamodel_path != None:
        datamodel_path = args.datamodel_path[0]
        # Check if file exists
        if not os.path.isfile(datamodel_path):
            print("The specified path to the datamodel file {} does not exist".format(datamodel_path))
            exit(-1)
        # end if
    # end if

    datatabse_address = db_configuration["host"]
    datatabse_port = db_configuration["port"]

    command = "uboa_init_ddbb.sh -h {} -p {} -f {}".format(datatabse_address, datatabse_port, datamodel_path)
    print("The UBOA database is going to be initialize using the datamodel SQL file {}...".format(datamodel_path))
    command_split = shlex.split(command)
    program = Popen(command_split, stdin=PIPE, stdout=PIPE, stderr=PIPE)    
    output, error = program.communicate()
    return_code = program.returncode
    if return_code != 0:
        print("The execution of the command {} has ended unexpectedly with the following error: {} and the following output: {}".format(command, str(error), str(output)))
        exit(-1)
    else:
        print("The BOA database has been initialized successfully :-)")
    # end try
    
    exit(0)
    
if __name__ == "__main__":

    main()

