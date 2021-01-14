#!/usr/bin/env python3
"""
Scheduler daemon for BOA

Written by DEIMOS Space S.L. (dibb)

module sboa
"""
# Import python utilities
import psutil
import shlex
from subprocess import Popen, PIPE
from daemon import pidfile
import os
import errno

# Get eboa auxiliary functions
from eboa.engine.functions import get_resources_path

# Import logging
from sboa.logging import Log

# Import sboa utils
from sboa.engine.query import Query

pid_file = get_resources_path() + "/boa_scheduler.pid"
pid_files_folder = get_resources_path() + "/on_going_triggerings/"

def stop_scheduler():

    query = Query()
    status = status_scheduler()
    if status["status"] == "on":
        print("BOA scheduler is going to be stopped...")
        logging = Log(name = __name__)
        logger = logging.logger
        logger.info("BOA scheduler is going to be stopped...")
        pid = pidfile.TimeoutPIDLockFile(pid_file).read_pid()
        try:
            p = psutil.Process(pid)
            p.terminate()
            logger.info("BOA scheduler stopped")
            print("BOA scheduler stopped")
        except psutil.NoSuchProcess:
            logger.info("BOA scheduler was already stopped but PID file {} was still there".format(pid_file))
            print("BOA scheduler was already stopped but PID file {} was still there".format(pid_file))
        # end try

        # Remove the PID file
        try:
            os.remove(pid_file)
        except FileNotFoundError:
            pass
        # end for

        i = 0
        on_going_triggerings = os.listdir(pid_files_folder)
        if len(on_going_triggerings) > 0:
            for file in on_going_triggerings:
                pid = pidfile.TimeoutPIDLockFile(pid_files_folder + "/" + file).read_pid()
                try:
                    p = psutil.Process(pid)
                    p.terminate()
                except psutil.NoSuchProcess:
                    # Process terminated
                    pass
                # end try
                try:
                    os.remove(pid_files_folder + "/" + file)
                except FileNotFoundError:
                    pass
                # end try
        # end if
    else:
        print("BOA scheduler was not running...")
    # end if
    query.close_session()
        
def status_scheduler():

    if pidfile.TimeoutPIDLockFile(pid_file).is_locked():
        message = "BOA scheduler is running..."
        print(message)
        return {"status": "on", "message": message}
    else:
        message = "BOA scheduler is not running..."
        print(message)
        return {"status": "off", "message": message}
    # end if

def command_start_scheduler():
    
    command = "boa_scheduler.py -c start -o"

    command_split = shlex.split(command)
    try:
        program = Popen(command_split, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        output, error = program.communicate()
        return_code = program.returncode
        command_status = {"command": command,
                          "output": output.decode("utf-8"),
                          "error": error.decode("utf-8"),
                          "return_code": return_code}
    except FileNotFoundError:
        output = ""
        error = "Command {} not found".format(command)
        return_code = -1
        command_status = {"command": command,
                          "output": output,
                          "error": error,
                          "return_code": return_code}
    # end try

    return command_status
