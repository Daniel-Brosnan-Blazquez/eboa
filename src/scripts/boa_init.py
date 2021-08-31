#!/usr/bin/env python3
"""
Script for initializing BOA environment

Written by DEIMOS Space S.L. (dibb)

module boa
"""
# Import python utilities
import argparse
import re
from eboa.common.commands import execute_command
import os
import shutil

def main():

    args_parser = argparse.ArgumentParser(description="Initialize BOA environment.")
    args_parser.add_argument("-e", "--initialize_eboa",
                             help="Initialize also EBOA environment", action="store_true")
    args_parser.add_argument("-s", "--initialize_sboa",
                             help="Initialize also SBOA environment", action="store_true")
    args_parser.add_argument("-u", "--initialize_uboa",
                             help="Initialize also UBOA environment", action="store_true")
    args_parser.add_argument("-o", "--initialize_orc",
                             help="Initialize also ORC environment", action="store_true")
    args_parser.add_argument("-y", "--accept_everything",
                             help="Accept by default every request (Be careful when using this because it will drop all the data without requesting any confirmation)", action="store_true")

    args = args_parser.parse_args()

    initialize_any_component = False
    if not args.accept_everything:
        continue_flag = input("\n" +
                              "Welcome to the BOA initializer :-)\n" +
                              "You are about to initialize the system.\n" +
                              "This operation will erase all the information, would you still want to continue? [Ny]")

        if continue_flag != "y":
            print("No worries! The initialization is going to be aborted :-)")
            exit(0)
        # end if

        if args.initialize_eboa:
            initialize_any_component = True
            continue_eboa_initialization_flag = input("\n" +
                                                     "You selected also to initialize EBOA environment\n" +
                                                     "This operation will erase all the information in eboa DDBB, would you still want to continue? [Ny]")

            if continue_eboa_initialization_flag != "y":
                print("No worries! The initialization is going to be aborted :-)")
                exit(0)
            # end if
        # end if

        if args.initialize_sboa:
            initialize_any_component = True
            continue_sboa_initialization_flag = input("\n" +
                                                     "You selected also to initialize SBOA environment\n" +
                                                     "This operation will erase all the information in sboa DDBB, would you still want to continue? [Ny]")

            if continue_sboa_initialization_flag != "y":
                print("No worries! The initialization is going to be aborted :-)")
                exit(0)
            # end if
        # end if

        if args.initialize_uboa:
            initialize_any_component = True
            continue_uboa_initialization_flag = input("\n" +
                                                     "You selected also to initialize UBOA environment\n" +
                                                     "This operation will erase all the information in uboa DDBB, would you still want to continue? [Ny]")

            if continue_uboa_initialization_flag != "y":
                print("No worries! The initialization is going to be aborted :-)")
                exit(0)
            # end if
        # end if

        if args.initialize_orc:
            initialize_any_component = True
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
        if args.initialize_eboa:
            initialize_any_component = True
            message = message + "You selected also to initialize EBOA environment.\n"
        # end if
        if args.initialize_sboa:
            initialize_any_component = True
            message = message + "You selected also to initialize SBOA environment.\n"
        # end if
        if args.initialize_uboa:
            initialize_any_component = True
            message = message + "You selected also to initialize UBOA environment.\n"
        # end if
        if args.initialize_orc:
            initialize_any_component = True
            message = message + "You selected also to initialize ORC environment.\n"
        # end if
        print(message)
    # end if

    if not initialize_any_component:
        print("You have not selected any component to initialize. Check usage for more information:")
        args_parser.print_help()
        exit(0)
    # end if
    
    # Initialize EBOA
    if args.initialize_eboa:

        execute_command("eboa_init.py -y", "The EBOA database has been initialized successfully :-)")
        
    # end if
    
    # Initialize SBOA
    if args.initialize_sboa:

        execute_command("sboa_init.py -y", "The SBOA database has been initialized successfully :-)")
        
    # end if

    # Initialize UBOA
    if args.initialize_uboa:

        execute_command("uboa_init.py -y", "The UBOA database has been initialized successfully :-)")
        
    # end if

    # Initialize ORC and minArc
    if args.initialize_orc:

        execute_command("orc_init.py -y", "The ORC and minArc databases have been initialized successfully :-)")
        
    # end if
    
    exit(0)
    
if __name__ == "__main__":

    main()

