#!/usr/bin/env python3
"""
Script for initializing UBOA environment (scheduler)

Written by DEIMOS Space S.L. (dibb)

module uboa
"""
# Import python utilities
import shlex
from subprocess import Popen, PIPE

def execute_command(command, success_message, check_error = True):
    command_split = shlex.split(command)
    program = Popen(command_split, stdin=PIPE, stdout=PIPE, stderr=PIPE)    
    output, error = program.communicate()
    return_code = program.returncode
    if check_error and return_code != 0:
        print("The execution of the command {} has ended unexpectedly with the following output: {} but the following error: {}".format(command, str(output.decode()), str(error.decode())))
        exit(-1)
    else:
        print(success_message)
    # end try
