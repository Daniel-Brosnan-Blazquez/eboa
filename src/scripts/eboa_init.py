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
import shutil

# Import auxiliary functions
from eboa.datamodel.functions import read_configuration

# Import engine functions
from eboa.engine.functions import get_resources_path

config = read_configuration()

db_configuration = config["DDBB_CONFIGURATION"]

def execute_command(command, success_message, check_error = True):
    command_split = shlex.split(command)
    program = Popen(command_split, stdin=PIPE, stdout=PIPE, stderr=PIPE)    
    output, error = program.communicate()
    return_code = program.returncode
    if check_error and return_code != 0:
        print("The execution of the command {} has ended unexpectedly with the following error: {} and the following output: {}".format(command, str(error), str(output)))
        exit(-1)
    else:
        print(success_message)
    # end try


def main():

    args_parser = argparse.ArgumentParser(description="Initialize BOA environment.")
    args_parser.add_argument('-f', dest='datamodel_path', type=str, nargs=1,
                             help='path to the datamodel', required=False)
    args_parser.add_argument("-o", "--initialize_orc",
                             help="Initialize also ORC environment", action="store_true")
    args_parser.add_argument("-s", "--initialize_sboa",
                             help="Initialize also SBOA environment", action="store_true")
    args_parser.add_argument("-u", "--initialize_uboa",
                             help="Initialize also UBOA environment", action="store_true")
    args_parser.add_argument("-y", "--accept_everything",
                             help="Accept by default every request (Be careful when using this because it will drop all the data without requesting any confirmation)", action="store_true")

    args = args_parser.parse_args()

    if not args.accept_everything:
        continue_flag = input("\n" +
                              "Welcome to the BOA initializer :-)\n" +
                              "You are about to initialize the system.\n" +
                              "This operation will erase all the information, would you still want to continue? [Ny]")

        if continue_flag != "y":
            print("No worries! The initialization is going to be aborted :-)")
            exit(0)
        # end if

        if args.initialize_sboa:
            continue_sboa_initialization_flag = input("\n" +
                                                     "You selected also to initialize SBOA environment\n" +
                                                     "This operation will erase all the information in sboa DDBB, would you still want to continue? [Ny]")

            if continue_sboa_initialization_flag != "y":
                print("No worries! The initialization is going to be aborted :-)")
                exit(0)
            # end if
        # end if

        if args.initialize_uboa:
            continue_uboa_initialization_flag = input("\n" +
                                                     "You selected also to initialize UBOA environment\n" +
                                                     "This operation will erase all the information in uboa DDBB, would you still want to continue? [Ny]")

            if continue_uboa_initialization_flag != "y":
                print("No worries! The initialization is going to be aborted :-)")
                exit(0)
            # end if
        # end if

        if args.initialize_orc:
            continue_orc_initialization_flag = input("\n" +
                                                     "You selected also to initialize ORC environment\n" +
                                                     "This operation will erase all the information in ORC DDBB, would you still want to continue? [Ny]")

            if continue_orc_initialization_flag != "y":
                print("No worries! The initialization is going to be aborted :-)")
                exit(0)
            # end if
        # end if
    else:
        message = "\n" + \
            "Welcome to the BOA initializer :-)\n" + \
            "You are about to initialize the system.\n"
        if args.initialize_orc:
            message = message + "You selected also to initialize ORC environment.\n"
        # end if
        if args.initialize_sboa:
            message = message + "You selected also to initialize SBOA environment.\n"
        # end if
        if args.initialize_uboa:
            message = message + "You selected also to initialize UBOA environment.\n"
        # end if
        print(message)
    # end if

    # Default path for the docker environment
    datamodel_path = "/datamodel/eboa_data_model.sql"
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

    command = "init_ddbb.sh -h {} -p {} -f {}".format(datatabse_address, datatabse_port, datamodel_path)
    print("The EBOA database is going to be initialized using the datamodel SQL file {}...".format(datamodel_path))
    command_split = shlex.split(command)
    program = Popen(command_split, stdin=PIPE, stdout=PIPE, stderr=PIPE)    
    output, error = program.communicate()
    return_code = program.returncode
    if return_code != 0:
        print("The execution of the command {} has ended unexpectedly with the following error: {} and the following output: {}".format(command, str(error), str(output)))
        exit(-1)
    else:
        print("The EBOA database has been initialized successfully :-)")
    # end try

    # Remove the on_going_ingestions directory
    on_going_ingestions_folder = get_resources_path() + "/on_going_ingestions/"
    try:
        shutil.rmtree(on_going_ingestions_folder)
    except FileNotFoundError:
        pass
    # end try
    
    # Initialize SBOA
    if args.initialize_sboa:

        # Default path for the docker environment
        datamodel_path = "/datamodel/sboa_data_model.sql"
        if args.datamodel_path != None:
            datamodel_path = args.datamodel_path[0]
            # Check if file exists
            if not os.path.isfile(datamodel_path):
                print("The specified path to the datamodel file {} does not exist".format(datamodel_path))
                exit(-1)
            # end if
        # end if

        command = "sboa_init_ddbb.sh -h {} -p {} -f {}".format(datatabse_address, datatabse_port, datamodel_path)
        print("The SBOA database is going to be initialized using the datamodel SQL file {}...".format(datamodel_path))
        command_split = shlex.split(command)
        program = Popen(command_split, stdin=PIPE, stdout=PIPE, stderr=PIPE)    
        output, error = program.communicate()
        return_code = program.returncode
        if return_code != 0:
            print("The execution of the command {} has ended unexpectedly with the following error: {} and the following output: {}".format(command, str(error), str(output)))
            exit(-1)
        else:
            print("The SBOA database has been initialized successfully :-)")
        # end try
    # end if

    # Initialize UBOA
    if args.initialize_uboa:

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

        command = "uboa_init_ddbb.sh -h {} -p {} -f {}".format(datatabse_address, datatabse_port, datamodel_path)
        print("The UBOA database is going to be initialized using the datamodel SQL file {}...".format(datamodel_path))
        command_split = shlex.split(command)
        program = Popen(command_split, stdin=PIPE, stdout=PIPE, stderr=PIPE)    
        output, error = program.communicate()
        return_code = program.returncode
        if return_code != 0:
            print("The execution of the command {} has ended unexpectedly with the following error: {} and the following output: {}".format(command, str(error), str(output)))
            exit(-1)
        else:
            print("The UBOA database has been initialized successfully :-)")
        # end try
    # end if

    # Initialize ORC and minArc
    if args.initialize_orc:

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
        
    # end if
    
    exit(0)
    
if __name__ == "__main__":

    main()

