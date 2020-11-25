#!/usr/bin/env python3
"""
Scheduler daemon for BOA

Written by DEIMOS Space S.L. (dibb)

module sboa
"""
# Import python utilities
import time
import daemon
from daemon import pidfile
import lockfile
import datetime
import signal
import sys
import argparse
import psutil
import shlex
from subprocess import Popen, PIPE
from multiprocessing import Pool

# Get boa scheduler functions
import sboa.scheduler.boa_scheduler_functions as functions

# Import logging
from sboa.logging import Log

# Import sboa utils
from sboa.engine.query import Query
from sboa.engine.engine import Engine

# Import engine
import sboa.engine.engine as sboa_engine

pid_file = functions.pid_file

# Import auxiliary functions
from sboa.datamodel.functions import read_configuration

config = read_configuration()
maximum_parallel_tasks = config["MAXIMUM_PARALLEL_TASKS"]

# Function to execute the command
def execute_triggering(parameters):

    command = "boa_execute_triggering.py -u " + parameters["triggering_uuid"] + " -b '" + parameters["start"] + "' -e '" + parameters["stop"] + "'"
    command_split = shlex.split(command)
    program = Popen(command_split, stdin=PIPE, stdout=PIPE, stderr=PIPE)

    return
    
# end def

def query_and_execute_tasks(logger = None):

    if not logger:
        logging = Log(name = __name__)
        logger = logging.logger
    # end if
    
    query = Query()
    engine = Engine()

    stop = datetime.datetime.now()
    start = stop + datetime.timedelta(minutes=-30)
    on_going_triggerings = query.get_triggerings(triggered = False,
                                                 date_filters=[{"date": start.isoformat(), "op": ">"},
                                                               {"date": stop.isoformat(), "op": "<="}])
    
    if len(on_going_triggerings) >= maximum_parallel_tasks:
        logger.error("The system has reached the maximum number of parallel tasks set as {}".format(maximum_parallel_tasks))
        return
    # end if

    available_slots = maximum_parallel_tasks - len(on_going_triggerings)

    time = datetime.datetime.now().isoformat()
    tasks = query.get_tasks(triggering_time_filters=[{"date": time, "op": "<="}])
    tasks_to_trigger = len(tasks)

    if tasks_to_trigger > 0:

        logger.info("There are {} tasks to trigger and {} available slots".format(tasks_to_trigger, available_slots))
        
        if tasks_to_trigger > available_slots:
            tasks_to_trigger = available_slots
        # end if
        parameters = []
        for task in tasks[:tasks_to_trigger]:
            date = datetime.datetime.now()
            triggering_info = engine.insert_triggering(date, task.task_uuid)
            if triggering_info["status"] == sboa_engine.exit_codes["OK_TRIGGERING"]["status"]:
                stop = task.triggering_time - datetime.timedelta(days=float(task.rule.window_delay))
                start = stop - datetime.timedelta(days=float(task.rule.window_size))            

                new_triggering_time = task.triggering_time + datetime.timedelta(days=float(task.rule.periodicity))
                engine.set_triggering_time(task.task_uuid, new_triggering_time)

                parameters.append({
                    "triggering_uuid": str(triggering_info["triggering_uuid"]),
                    "start": start.isoformat(),
                    "stop": stop.isoformat()
                })
            # end if
        # end for

        logger.info("{} tasks are going to be triggered".format(len(parameters)))
        
        # Trigger parallel generation of reports
        pool = Pool(tasks_to_trigger)
        try:
            pool.map(execute_triggering, parameters)
        finally:
            pool.close()
            pool.join()
        # end try
    # end if

    query.close_session()
    engine.close_session()    
    
    return

def start_scheduler():

    print("BOA scheduler started...")
    logging = Log(name = __name__)
    logger = logging.logger
    logger.info("BOA scheduler started...")
    while True:
        time.sleep(1)
        logger.info("BOA scheduler is going to review tasks for triggerig")
        query_and_execute_tasks(logger)    
    # end while

def stop_scheduler():

    functions.stop_scheduler()
    
def status_scheduler():

    return functions.status_scheduler()
    
if __name__ == "__main__":

    args_parser = argparse.ArgumentParser(description="BOA scheduler.")
    args_parser.add_argument("-c", dest="command", type=str, nargs=1,
                             help="command to execute (start, stop or status)", required=True)
    args_parser.add_argument("-o", "--no_output",
                             help="execute daemon without redirecting stdout and stderr to sys.stdout", action="store_true")
    
    args = args_parser.parse_args()
    command = args.command[0]

    if command == "start":
        if not pidfile.TimeoutPIDLockFile(pid_file).is_locked() and not args.no_output:
            print("BOA scheduler is going to be started...")
            with daemon.DaemonContext(
                    working_directory="/tmp",
                    pidfile=pidfile.TimeoutPIDLockFile(pid_file),
                    stdout=sys.stdout, stderr=sys.stdout) as context:
                start_scheduler()
            # end with
        elif not pidfile.TimeoutPIDLockFile(pid_file).is_locked():
            print("BOA scheduler is going to be started...")
            with daemon.DaemonContext(
                    working_directory="/tmp",
                    pidfile=pidfile.TimeoutPIDLockFile(pid_file)) as context:
                start_scheduler()
            # end with
        else:
            print("BOA scheduler is already running...")
            exit(-1)
        # end if
    elif command == "stop":
        stop_scheduler()
    elif command == "status":
        status_scheduler()
    else:
        print("Command {} is not a valid argument".format(command))
    # end if
