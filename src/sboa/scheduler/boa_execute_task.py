#!/usr/bin/env python3
"""
Scheduler daemon for BOA

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
def execute_task(task_uuid):

    engine = Engine()
    query = Query()

    tasks = query.get_tasks(task_uuids = {"filter": str(task_uuid), "op": "=="})

    if len(tasks) > 0:
        task = tasks[0]
        command = task.command
        date = datetime.datetime.now()

        # Notify start triggering
        pid = os.getpid()
        pid_file = pid_files_folder + "/boa_scheduler_" + str(pid) + ".pid"

        pidfile.TimeoutPIDLockFile(pid_file).acquire()
        triggering_info = engine.insert_triggering(date, task_uuid)

        if triggering_info["status"] == sboa_engine.exit_codes["OK_TRIGGERING"]["status"]:
            stop = task.triggering_time - datetime.timedelta(days=float(task.rule.window_delay))
            start = stop - datetime.timedelta(days=float(task.rule.window_size))            

            new_triggering_time = task.triggering_time + datetime.timedelta(days=float(task.rule.periodicity))
            engine.set_triggering_time(task_uuid, new_triggering_time)

            command = command + " -b '" + start.isoformat() + "' -e '" + stop.isoformat() + "'"

            logger.info("The command '{}' is going to be executed".format(command))
            
            command_split = shlex.split(command)
            program = Popen(command_split, stdin=PIPE, stdout=PIPE, stderr=PIPE)
            output, error = program.communicate()        
            return_code = program.returncode

            logger.info("Returned code of command '{}' is: {}".format(command, return_code))

            # Notify end triggering
            engine.triggering_done(triggering_info["triggering_uuid"])
        # end if
        pidfile.TimeoutPIDLockFile(pid_file).release()
    # end if

    engine.close_session()
    query.close_session()
        
    return
    
# end def
    
if __name__ == "__main__":

    args_parser = argparse.ArgumentParser(description="BOA execute task providing task UUID.")
    args_parser.add_argument("-u", dest="task_uuid", type=str, nargs=1,
                             help="Task UUID", required=True)
    
    args = args_parser.parse_args()
    task_uuid = args.task_uuid[0]

    newpid = os.fork()
    result = 0
    if newpid == 0:
        execute_task(task_uuid)
    # end if

    exit(0)    
