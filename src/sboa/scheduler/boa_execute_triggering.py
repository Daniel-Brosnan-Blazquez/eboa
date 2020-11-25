#!/usr/bin/env python3
"""
BOA execute triggering

Written by DEIMOS Space S.L. (dibb)

module sboa
"""
# Import python utilities
import os
import datetime
import sys
import argparse
import psutil
import shlex
from subprocess import Popen, PIPE
from daemon import pidfile
import lockfile
import traceback

# Get eboa auxiliary functions
from eboa.engine.functions import get_resources_path

# Import logging
from sboa.logging import Log

logging = Log(name = __name__)
logger = logging.logger

# Import sboa utils
from sboa.engine.query import Query
from sboa.engine.engine import Engine

# Import engine
import sboa.engine.engine as sboa_engine

# Get boa scheduler functions
import sboa.scheduler.boa_scheduler_functions as functions

pid_files_folder = functions.pid_files_folder

# Function to execute the command
def execute_triggering(triggering_uuid, start, stop):

    engine = Engine()
    query = Query()

    triggerings = query.get_triggerings(triggering_uuids = {"filter": str(triggering_uuid), "op": "=="})

    if len(triggerings) > 0:
        triggering = triggerings[0]
        task = triggering.task
        command = task.command

        # Notify start triggering
        pid = os.getpid()
        pid_file = pid_files_folder + "/boa_scheduler_" + str(pid) + ".pid"

        # Acquire the pid lock file for stopping processes if needed
        try:
            pidfile.TimeoutPIDLockFile(pid_file).acquire()
        except lockfile.LockFailed as e:
            logger.error("The lock of the file {} failed unexpectedly with the following error {}".format(pid_file, str(e)))
            logger.error(traceback.format_exc())
            traceback.print_exc(file=sys.stdout)
            return
        # end try

        if task.add_window_arguments:
            command = command + " -b '" + start + "' -e '" + stop + "'"
        # end if

        logger.info("The command '{}' is going to be executed with triggering time {} and window arguments (if applied) -b {} -e {}".format(command, task.triggering_time, start, stop))

        command_split = shlex.split(command)
        program = Popen(command_split, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        output, error = program.communicate()        
        return_code = program.returncode

        logger.info("Returned code of command '{}' is: {}".format(command, return_code))

        # Notify end triggering
        engine.triggering_done(triggering_uuid)

        # Release the pid lock file
        pidfile.TimeoutPIDLockFile(pid_file).release()
    # end if

    engine.close_session()
    query.close_session()
        
    return
    
# end def
    
if __name__ == "__main__":

    args_parser = argparse.ArgumentParser(description="BOA execute triggering providing triggering UUID.")
    args_parser.add_argument("-u", dest="triggering_uuid", type=str, nargs=1,
                             help="Triggering UUID", required=True)
    args_parser.add_argument("-b", dest="begin", type=str, nargs=1,
                             help="start date of the reporting period", required=True)
    args_parser.add_argument("-e", dest="end", type=str, nargs=1,
                             help="stop date of the reporting period", required=True)
    
    args = args_parser.parse_args()
    triggering_uuid = args.triggering_uuid[0]
    start = args.begin[0]
    stop = args.end[0]

    newpid = os.fork()
    if newpid == 0:
        execute_triggering(triggering_uuid, start, stop)
    # end if

    exit(0)    
