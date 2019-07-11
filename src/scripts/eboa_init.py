#!/usr/bin/env python3
"""
Script for initializing BOA environment

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import argparse
import re
import shlex
from subprocess import Popen, PIPE
import os

# Import auxiliary functions
from eboa.datamodel.functions import read_configuration

config = read_configuration()

db_configuration = config["DDBB_CONFIGURATION"]

def main():

    args_parser = argparse.ArgumentParser(description="Initialize BOA environment.")
    args_parser.add_argument("-o", "--initialize_orc",
                             help="Initialize also ORC environment", action="store_true")

    continue_flag = input("\n" +
                          "Welcome to the BOA initializer :-)\n" +
                          "You are about to initialize the system.\n" +
                          "This operation will erase all the information, would you still want to continue? [Ny]")
    
    if continue_flag != "y":
        print("No worries! The initialization is going to be aborted :-)")
        exit(0)
    # end if
    
    args = args_parser.parse_args()

    datatabse_address = db_configuration["host"]
    datatabse_port = db_configuration["port"]

    command = "init_ddbb.sh -h {} -p {} -f /datamodel/eboa_data_model.sql".format(datatabse_address, datatabse_port)
    print("The BOA database is going to be initialize...")
    command_split = shlex.split(command)
    program = Popen(command_split, stdin=PIPE, stdout=PIPE, stderr=PIPE)    
    output, error = program.communicate()
    return_code = program.returncode
    if return_code != 0:
        print("The execution of the command {} has ended unexpectedly with the following error: {}".format(command, str(error)))
        exit(-1)
    else:
        print("The BOA database has been initialized successfully :-)")
    # end try

    if args.initialize_orc:

        print("The MINARC archive is going to be initialize...")
        command = "minArcPurge -Y"
        command_split = shlex.split(command)
        program = Popen(command_split, stdin=PIPE, stdout=PIPE, stderr=PIPE)    
        output, error = program.communicate()
        return_code = program.returncode
        if return_code != 0:
            print("The execution of the command {} has ended unexpectedly with the following error: {}".format(command, str(error)))
            exit(-1)
        else:
            print("The MINARC archive has been initialized successfully :-)")
        # end try

        print("The MINARC database is going to be initialize...")
        command = "minArcDB --drop-tables ; minArcDB --create-tables"
        command_split = shlex.split(command)
        program = Popen(command_split, stdin=PIPE, stdout=PIPE, stderr=PIPE)    
        output, error = program.communicate()
        return_code = program.returncode
        if return_code != 0:
            print("The execution of the command {} has ended unexpectedly with the following error: {}".format(command, str(error)))
            exit(-1)
        else:
            print("The MINARC database has been initialized successfully :-)")
        # end try

        print("The ORC database is going to be initialize...")
        command = "orcManageDB --drop-tables ; orcManageDB --create-tables"
        command_split = shlex.split(command)
        program = Popen(command_split, stdin=PIPE, stdout=PIPE, stderr=PIPE)    
        output, error = program.communicate()
        return_code = program.returncode
        if return_code != 0:
            print("The execution of the command {} has ended unexpectedly with the following error: {}".format(command, str(error)))
            exit(-1)
        else:
            print("The ORC database has been initialized successfully :-)")
        # end try
        
    # end if
    
    exit(0)
    
if __name__ == "__main__":

    main()

