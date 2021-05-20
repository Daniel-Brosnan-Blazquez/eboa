"""
Automated tests for the logging submodule

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

    def test_change_eboa_logging_level(self):
        previous_logging_level = None
        if "EBOA_LOG_LEVEL" in os.environ:
            previous_logging_level = os.environ["EBOA_LOG_LEVEL"]
        # end if

        previous_stream_log = None
        if "EBOA_STREAM_LOG" in os.environ:
            previous_stream_log = os.environ["EBOA_STREAM_LOG"]
        # end if

        os.environ["EBOA_LOG_LEVEL"] = "DEBUG"
        os.environ["EBOA_STREAM_LOG"] = "YES"

        logging_module = Log(name = "eboa" + __name__)
        logger = logging_module.logger

        assert logger.level == logging.DEBUG

        # Deleting the environment variables for setting the logging
        # level of the configuration and remove the streaming of logs
        del os.environ["EBOA_LOG_LEVEL"]
        del os.environ["EBOA_STREAM_LOG"]

        logging_module.define_logging_configuration(name = "eboa" + __name__)

        config_log_level = eval("logging." + config["LOG"]["LEVEL"])

        assert logger.level == config_log_level

        if previous_logging_level != None:
            os.environ["EBOA_LOG_LEVEL"] = previous_logging_level
        # end if
        if previous_stream_log != None:
            os.environ["EBOA_STREAM_LOG"] = previous_stream_log
        # end if
        logging_module.define_logging_configuration()
        
    def test_rotate_eboa_log_configure_by_env(self):

        previous_logging_level = None
        if "EBOA_LOG_LEVEL" in os.environ:
            previous_logging_level = os.environ["EBOA_LOG_LEVEL"]
        # end if

        previous_eboa_log_max_bytes = None
        if "EBOA_LOG_MAX_BYTES" in os.environ:
            previous_eboa_log_max_bytes = os.environ["EBOA_LOG_MAX_BYTES"]
        # end if

        previous_eboa_log_max_backup = None
        if "EBOA_LOG_MAX_BACKUP" in os.environ:
            previous_eboa_log_max_backup = os.environ["EBOA_LOG_MAX_BACKUP"]
        # end if

        os.environ["EBOA_LOG_LEVEL"] = "DEBUG"
        os.environ["EBOA_LOG_MAX_BYTES"] = "2000"
        os.environ["EBOA_LOG_MAX_BACKUP"] = "10"
        
        logging_module = Log(name = "eboa" + __name__)
        logger = logging_module.logger
        
        for i in range(10000):
            logger.debug("TESTING LOG ROTATION USING ENVIRONMENT VARIABLES {}".format(i))
        # end if

        # Deleting the environment variables for setting the logging
        # as specified by the configuration
        del os.environ["EBOA_LOG_LEVEL"]
        del os.environ["EBOA_LOG_MAX_BYTES"]
        del os.environ["EBOA_LOG_MAX_BACKUP"]

        if previous_eboa_log_max_bytes != None:
            os.environ["EBOA_LOG_MAX_BYTES"] = previous_eboa_log_max_bytes
        # end if
        if previous_eboa_log_max_backup != None:
            os.environ["EBOA_LOG_MAX_BACKUP"] = previous_eboa_log_max_backup
        # end if
        if previous_logging_level != None:
            os.environ["EBOA_LOG_LEVEL"] = previous_logging_level
        # end if

        logging_module.define_logging_configuration()

    def test_rotate_eboa_log_configure_by_json(self):
        
        logging_module = Log(name = "eboa" + __name__)
        logger = logging_module.logger
        
        for i in range(10000):
            logger.info("TESTING LOG ROTATION USING CONFIGURATION {}".format(i))
        # end if

    def test_rotate_sboa_log_configure_by_env(self):

        previous_logging_level = None
        if "EBOA_LOG_LEVEL" in os.environ:
            previous_logging_level = os.environ["EBOA_LOG_LEVEL"]
        # end if

        previous_eboa_log_max_bytes = None
        if "EBOA_LOG_MAX_BYTES" in os.environ:
            previous_eboa_log_max_bytes = os.environ["EBOA_LOG_MAX_BYTES"]
        # end if

        previous_eboa_log_max_backup = None
        if "EBOA_LOG_MAX_BACKUP" in os.environ:
            previous_eboa_log_max_backup = os.environ["EBOA_LOG_MAX_BACKUP"]
        # end if

        os.environ["EBOA_LOG_LEVEL"] = "DEBUG"
        os.environ["EBOA_LOG_MAX_BYTES"] = "2000"
        os.environ["EBOA_LOG_MAX_BACKUP"] = "10"
        
        logging_module = SboaLog(name = "sboa" + __name__)
        logger = logging_module.logger
        
        for i in range(10000):
            logger.debug("TESTING LOG ROTATION USING ENVIRONMENT VARIABLES {}".format(i))
        # end if

        # Deleting the environment variables for setting the logging
        # as specified by the configuration
        del os.environ["EBOA_LOG_LEVEL"]
        del os.environ["EBOA_LOG_MAX_BYTES"]
        del os.environ["EBOA_LOG_MAX_BACKUP"]

        if previous_eboa_log_max_bytes != None:
            os.environ["EBOA_LOG_MAX_BYTES"] = previous_eboa_log_max_bytes
        # end if
        if previous_eboa_log_max_backup != None:
            os.environ["EBOA_LOG_MAX_BACKUP"] = previous_eboa_log_max_backup
        # end if
        if previous_logging_level != None:
            os.environ["EBOA_LOG_LEVEL"] = previous_logging_level
        # end if

        logging_module.define_logging_configuration()

    def test_rotate_sboa_log_configure_by_json(self):
        
        logging_module = SboaLog(name = "sboa" + __name__)
        logger = logging_module.logger
        
        for i in range(10000):
            logger.info("TESTING LOG ROTATION USING CONFIGURATION {}".format(i))
        # end if
