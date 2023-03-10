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

# Import engine of the DDBB
import eboa.engine.engine as eboa_engine
from eboa.engine.engine import Engine
from eboa.engine.query import Query

config = read_configuration()

class TestLogging(unittest.TestCase):

    def setUp(self):
        # Create the engine to manage the data
        self.engine_eboa = Engine()
        self.query_eboa = Query()

        # Clear all tables before executing the test
        self.query_eboa.clear_db()

    def tearDown(self):
        # Close connections to the DDBB
        self.engine_eboa.close_session()
        self.query_eboa.close_session()

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

    def test_final_message_successfull_ingestion_all_entities(self):
        
        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 30,
                       "alerts": [{
                           "message": "Alert message",
                           "generator": "test",
                           "notification_time": "2018-06-05T08:07:36",
                           "alert_cnf": {
                               "name": "alert_name1",
                               "severity": "critical",
                               "description": "Alert description",
                               "group": "alert_group"
                           }}]
                       },
            "explicit_references": [{
                "name": "EXPLICIT_REFERENCE",
                "alerts": [{
                    "message": "Alert message",
                    "generator": "test",
                    "notification_time": "2018-06-05T08:07:36",
                    "alert_cnf": {
                        "name": "alert_name2",
                        "severity": "critical",
                        "description": "Alert description",
                        "group": "alert_group"
                    }}]
            }],
            "events": [{
                "link_ref": "EVENT_REF",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36",
                "alerts": [{
                    "message": "Alert message",
                    "generator": "test",
                    "notification_time": "2018-06-05T08:07:36",
                    "alert_cnf": {
                        "name": "alert_name3",
                        "severity": "critical",
                        "description": "Alert description",
                        "group": "alert_group"
                    }}]
            }],
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {
                    "name": "ANNOTATION_CNF",
                    "system": "SYSTEM"
                },
                "alerts": [{
                    "message": "Alert message",
                    "generator": "test",
                    "notification_time": "2018-06-05T08:07:36",
                    "alert_cnf": {
                        "name": "alert_name4",
                        "severity": "critical",
                        "description": "Alert description",
                        "group": "alert_group"
                    }}]
            }],
            "alerts": [{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name5",
                    "severity": "critical",
                    "description": "Alert description",
                    "group": "alert_group"
                },
                "entity": {
                    "reference_mode": "by_ref",
                    "reference": "EVENT_REF",
                    "type": "event"
                }
            },{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name6",
                    "severity": "warning",
                    "description": "Alert description",
                    "group": "alert_group"
                },
                "entity": {
                    "reference_mode": "by_ref",
                    "reference": "EXPLICIT_REFERENCE",
                    "type": "explicit_ref"
                }
            },{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name7",
                    "severity": "critical",
                    "description": "Alert description",
                    "group": "alert_group"
                },
                "entity": {
                    "reference_mode": "by_ref",
                    "reference": "source.json",
                    "type": "source"
                }
            }]
        }]
                }

        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        with open("/log/eboa_engine.log", "r") as f:
            last_line = f.readlines()[-1]
        # end with

        assert "The source file source.json associated to the DIM signature dim_signature and DIM processing exec with version 1.0 has ingested correctly 1 event/s, 1 annotation/s and 7 alert/s" in last_line

    def test_final_message_successfull_ingestion_no_alerts(self):
        
        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 30
                       },
            "explicit_references": [{
                "name": "EXPLICIT_REFERENCE",
            }],
            "events": [{
                "link_ref": "EVENT_REF",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {
                    "name": "ANNOTATION_CNF",
                    "system": "SYSTEM"
                }
            }],
        }]
                }

        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        with open("/log/eboa_engine.log", "r") as f:
            last_line = f.readlines()[-1]
        # end with

        assert "The source file source.json associated to the DIM signature dim_signature and DIM processing exec with version 1.0 has ingested correctly 1 event/s, 1 annotation/s and 0 alert/s" in last_line

    def test_final_message_successfull_ingestion_no_entities(self):
        
        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 30
                       }
        }]
                }

        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        with open("/log/eboa_engine.log", "r") as f:
            last_line = f.readlines()[-1]
        # end with

        assert "The source file source.json associated to the DIM signature dim_signature and DIM processing exec with version 1.0 has ingested correctly 0 event/s, 0 annotation/s and 0 alert/s" in last_line
