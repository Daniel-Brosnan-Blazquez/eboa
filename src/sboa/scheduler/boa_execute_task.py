#!/usr/bin/env python3
"""
Scheduler daemon for BOA

Written by DEIMOS Space S.L. (dibb)

module sboa
"""
# Import python utilities
import datetime
import sys
import argparse
import psutil
import shlex
from subprocess import Popen, PIPE

# Get eboa auxiliary functions
from eboa.engine.functions import get_resources_path

# Import logging
from sboa.logging import Log

# Import sboa utils
from sboa.engine.query import Query
from sboa.engine.engine import Engine

# Import engine
import sboa.engine.engine as sboa_engine

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
        triggering_info = engine.insert_triggering(date, task_uuid)

        if triggering_info["status"] == sboa_engine.exit_codes["OK_TRIGGERING"]["status"]:
            stop = task.triggering_time - datetime.timedelta(days=float(task.rule.window_delay))
            start = stop - datetime.timedelta(days=float(task.rule.window_size))            

            command = command + " -b '" + start.isoformat() + "' -e '" + stop.isoformat() + "'"
            command_split = shlex.split(command)
            program = Popen(command_split, stdin=PIPE, stdout=PIPE, stderr=PIPE)
            output, error = program.communicate()        
            return_code = program.returncode

            # Notify end triggering
            engine.triggering_done(triggering_info["triggering_uuid"])

            new_triggering_time = task.triggering_time + datetime.timedelta(days=float(task.rule.periodicity))
            engine.set_triggering_time(task_uuid, new_triggering_time)
        # end if
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

    execute_task(task_uuid)
