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

class Log():

    def __init__(self, name = None):
        self.define_logging_configuration(name)

    def define_logging_configuration(self, name = None):
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

        file_handlers = [handler for handler in self.logger.handlers if type(handler) == logging.handlers.RotatingFileHandler]
        if len(file_handlers) < 1:
            # Set the path to the log file
            file_handler = RotatingFileHandler(log_path + "/sboa_engine.log", maxBytes=config["LOG"]["MAX_BYTES"], backupCount=config["LOG"]["MAX_BACKUP"])
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
        # end if

        return
