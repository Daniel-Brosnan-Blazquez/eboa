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
from gsdm.engine.functions import *

config = read_configuration()
gsdm_resources_path = get_resources_path()

class Log():

    def __init__(self):
        self.stream_handler = None
        self.define_logging_configuration()

    def define_logging_configuration(self):
        # Define logging configuration
        self.logger = logging.getLogger(__name__)
        if "GSDM_LOG_LEVEL" in os.environ:
            self.logging_level = eval("logging." + os.environ["GSDM_LOG_LEVEL"])
        else:
            self.logging_level = eval("logging." + config["LOG"]["LEVEL"])
        # end if

        if not self.logger.hasHandlers():
            # Set logging level
            self.logger.setLevel(self.logging_level)
            # Set the path to the log file
            file_handler = RotatingFileHandler(gsdm_resources_path + "/" + config["LOG"]["PATH"], maxBytes=config["LOG"]["MAX_BYTES"], backupCount=config["LOG"]["MAX_BACKUP"])
            # Add format to the logs
            formatter = logging.Formatter("%(levelname)s\t; (%(asctime)s.%(msecs)03d) ; %(name)s(%(lineno)d) [%(process)d] -> %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

            if "GSDM_STREAM_LOG" in os.environ:
                self.stream_handler = logging.StreamHandler()
                self.stream_handler.setFormatter(formatter)
                self.logger.addHandler(self.stream_handler)
            elif self.stream_handler:
                self.logger.removeHandler(self.stream_handler)
            # end if

        # end if

        return
