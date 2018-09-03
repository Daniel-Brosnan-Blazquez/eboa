"""
Logging definition

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
# Import python utilities
import logging
from logging.handlers import RotatingFileHandler
import os
import json

# Import auxiliary functions
from gsdm.engine.functions import get_resources_path, read_configuration

config = read_configuration()
gsdm_resources_path = get_resources_path()

class Log():

    def __init__(self):
        self.define_logging_configuration()

    def define_logging_configuration(self):
        # Define logging configuration
        self.logger = logging.getLogger(__name__)
        if "GSDM_LOG_LEVEL" in os.environ:
            self.logger.setLevel(eval("logging." + os.environ["GSDM_LOG_LEVEL"]))
        else:
            self.logger.setLevel(eval("logging." + config["LOG"]["LEVEL"]))
        # end if
        # format for logs
        formatter = logging.Formatter("%(levelname)s\t; (%(asctime)s.%(msecs)03d) ; %(name)s(%(lineno)d) [%(process)d] -> %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")

        stream_handlers = [handler for handler in self.logger.handlers if type(handler) == logging.StreamHandler]
        if "GSDM_STREAM_LOG" in os.environ and len(stream_handlers) < 1:
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            self.logger.addHandler(stream_handler)
            self.logger.info("Stream Log handler created")
        elif not "GSDM_STREAM_LOG" in os.environ and len(stream_handlers) > 0:
            self.logger.info("Stream Log handler is going to be removed")
            self.logger.removeHandler(stream_handlers[0])
        # end if

        file_handlers = [handler for handler in self.logger.handlers if type(handler) == logging.FileHandler]
        if len(file_handlers) < 1:
            # Set the path to the log file
            file_handler = RotatingFileHandler(gsdm_resources_path + "/" + config["LOG"]["PATH"], maxBytes=config["LOG"]["MAX_BYTES"], backupCount=config["LOG"]["MAX_BACKUP"])
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            self.logger.info("File log handler created using file stored in path {}".format(gsdm_resources_path + "/" + config["LOG"]["PATH"]))
        # end if

        return