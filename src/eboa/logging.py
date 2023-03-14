"""
Logging definition

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import logging
from logging.handlers import RotatingFileHandler
import os
import json

# Import auxiliary functions
from eboa.engine.functions import get_log_path, read_configuration

config = read_configuration()
log_path = get_log_path()

class RotatingFileHandlerAllUsers(RotatingFileHandler):

    def doRollover(self):
        """
        Override base class method to change the permissions to 666 of the new log file.
        """
        # Rotate the file first.
        RotatingFileHandler.doRollover(self)

        # Change permission of the log file to 666
        os.chmod(self.baseFilename, 0o0666)

class Log():

    def __init__(self, name = None, log_name = "eboa_engine.log"):
        self.log_name = log_name
        self._add_new_level("DEBUGI", 15)
        self.define_logging_configuration(name)

    def define_logging_configuration(self, name = None):
        """
        Method to define the logging configuration for a module
        
        :param name: name of the module
        :type name: string
        """
        
        # Define logging configuration
        if name == None:
            name = __name__
        # end if
        self.logger = logging.getLogger(name)

        if "EBOA_LOG_LEVEL" in os.environ:
            self.logger.setLevel(eval("logging." + os.environ["EBOA_LOG_LEVEL"]))
        else:
            self.logger.setLevel(eval("logging." + config["LOG"]["LEVEL"]))
        # end if
        # format for logs
        formatter = logging.Formatter("%(levelname)s\t; (%(asctime)s.%(msecs)03d) ; %(name)s(%(lineno)d) [%(process)d] -> %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")

        stream_handlers = [handler for handler in self.logger.handlers if type(handler) == logging.StreamHandler]
        if "EBOA_STREAM_LOG" in os.environ and len(stream_handlers) < 1:
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            self.logger.addHandler(stream_handler)
        elif not "EBOA_STREAM_LOG" in os.environ and len(stream_handlers) > 0:
            self.logger.info("Stream Log handler is going to be removed")
            self.logger.removeHandler(stream_handlers[0])
        # end if

        # Define maxBytes
        if "EBOA_LOG_MAX_BYTES" in os.environ:
            max_bytes = int(os.environ["EBOA_LOG_MAX_BYTES"])
        else:
            max_bytes = config["LOG"]["MAX_BYTES"]
        # end if

        # Define backupCount
        if "EBOA_LOG_MAX_BACKUP" in os.environ:
            max_backup = int(os.environ["EBOA_LOG_MAX_BACKUP"])
        else:
            max_backup = config["LOG"]["MAX_BACKUP"]
        # end if
        
        file_handlers = [handler for handler in self.logger.handlers if type(handler) == RotatingFileHandlerAllUsers]
        if len(file_handlers) < 1:
            # Set the path to the log file
            file_handler = RotatingFileHandlerAllUsers(log_path + "/" + self.log_name, maxBytes=max_bytes, backupCount=max_backup)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
        # end if

        return

    def _add_new_level(self, name, level):
        """
        Method to add a new log level
        This method was inspired by this thread in Stack Overflow:
        https://stackoverflow.com/questions/2183233/how-to-add-a-custom-loglevel-to-pythons-logging-facility/35804945#35804945
        
        :param name: name of the level
        :type name: string
        :param level: numeric representation of level
        :type level: int
        """

        # Add new level name
        logging.addLevelName(level, name)

        # These instructions were inspired by the answers to Stack Overflow post
        # http://stackoverflow.com/q/2183233/2988730, especially
        # http://stackoverflow.com/a/13638084/2988730
        def log_for_level(self, message, *args, **kwargs):
            if self.isEnabledFor(level):
                self._log(level, message, args, **kwargs)
        
        # Add new method associated to the new level name
        setattr(logging, name, level)
        setattr(logging.getLoggerClass(), name.lower(), log_for_level)

        
