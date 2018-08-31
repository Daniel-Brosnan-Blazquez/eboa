"""
Automated tests for the logging submodule

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
# Import python utilities
import os
import unittest
import logging

# Import logging
from gsdm.logging import Log

# Import auxiliary functions
from gsdm.engine.functions import read_configuration

config = read_configuration()

class TestLogging(unittest.TestCase):
    def test_change_logging_level(self):
        previous_logging_level = None
        if "GSDM_LOG_LEVEL" in os.environ:
            previous_logging_level = os.environ["GSDM_LOG_LEVEL"]
        # end if

        previous_stream_log = None
        if "GSDM_STREAM_LOG" in os.environ:
            previous_stream_log = os.environ["GSDM_STREAM_LOG"]
        # end if

        os.environ["GSDM_LOG_LEVEL"] = "DEBUG"
        os.environ["GSDM_STREAM_LOG"] = "YES"

        logging_module = Log()
        logger = logging_module.logger

        assert logger.level == logging.DEBUG

        # Deleting the environment variables for setting the logging
        # level of the configuration and remove the streaming of logs
        del os.environ["GSDM_LOG_LEVEL"]
        del os.environ["GSDM_STREAM_LOG"]

        logging_module.define_logging_configuration()

        config_log_level = eval("logging." + config["LOG"]["LEVEL"])

        assert logger.level == config_log_level

        if previous_logging_level != None:
            os.environ["GSDM_LOG_LEVEL"] = previous_logging_level
        # end if
        if previous_stream_log != None:
            os.environ["GSDM_STREAM_LOG"] = previous_stream_log
        # end if
        logging_module.define_logging_configuration()
        
