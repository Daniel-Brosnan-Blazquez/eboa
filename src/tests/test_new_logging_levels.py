"""
Automated tests for checking new logging levels

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import os
import unittest
import logging

# Import logging
from eboa.logging import Log
from sboa.logging import Log as SboaLog

# Import auxiliary functions
from eboa.engine.functions import read_configuration

config = read_configuration()

class TestLogging(unittest.TestCase):

    def test_debugi_level(self):
        """
        Test to verify the DEBUGI log level
        """
        
        previous_logging_level = None
        if "EBOA_LOG_LEVEL" in os.environ:
            previous_logging_level = os.environ["EBOA_LOG_LEVEL"]
        # end if

        os.environ["EBOA_LOG_LEVEL"] = "DEBUGI"

        logging_module = Log(name = "eboa" + __name__)
        logger = logging_module.logger

        logger.debugi("TESTING DEBUGI LOG LEVEL")

        with open("/log/eboa_engine.log", "r") as f:
            last_line = f.readlines()[-1]
        # end with

        assert "TESTING DEBUGI LOG LEVEL" in last_line

        # Deleting the environment variables for setting the logging
        # level of the configuration and remove the streaming of logs
        del os.environ["EBOA_LOG_LEVEL"]

        logging_module.define_logging_configuration(name = "eboa" + __name__)

        config_log_level = eval("logging." + config["LOG"]["LEVEL"])

        assert logger.level == config_log_level

        if previous_logging_level != None:
            os.environ["EBOA_LOG_LEVEL"] = previous_logging_level
        # end if
        logging_module.define_logging_configuration()
