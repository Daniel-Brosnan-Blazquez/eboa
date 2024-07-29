"""
Automated tests for the functions used by engine submodule

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import os
import unittest

# Import auxiliary functions
from eboa.engine.functions import read_configuration
from eboa.engine import functions as eboa_engine_functions

# Import exceptions
from eboa.engine.errors import EboaResourcesPathNotAvailable

# Import engine of the DDBB
import eboa.engine.engine as eboa_engine
from eboa.engine.engine import Engine
from eboa.engine.query import Query
from eboa.datamodel.base import Session, engine, Base
from eboa.engine.errors import UndefinedEventLink, DuplicatedEventLinkRef, WrongPeriod, SourceAlreadyIngested, WrongValue, OddNumberOfCoordinates, EboaResourcesPathNotAvailable, WrongGeometry
from eboa.engine.errors import LinksInconsistency

# Import datamodel
from eboa.datamodel.dim_signatures import DimSignature
from eboa.datamodel.alerts import Alert, AlertGroup, EventAlert, AnnotationAlert, SourceAlert, ExplicitRefAlert
from eboa.datamodel.events import Event, EventLink, EventKey, EventText, EventDouble, EventObject, EventGeometry, EventBoolean, EventTimestamp
from eboa.datamodel.gauges import Gauge
from eboa.datamodel.sources import Source, SourceStatus
from eboa.datamodel.explicit_refs import ExplicitRef, ExplicitRefGrp, ExplicitRefLink
from eboa.datamodel.annotations import Annotation, AnnotationCnf, AnnotationText, AnnotationDouble, AnnotationObject, AnnotationGeometry, AnnotationBoolean, AnnotationTimestamp

class TestEngineFunctions(unittest.TestCase):
    def setUp(self):
        # Create the engine to manage the data
        self.engine_eboa = Engine()
        self.engine_eboa_race_conditions = Engine()
        self.query_eboa = Query()

        # Create session to connect to the database
        self.session = Session()

        # Clear all tables before executing the test
        self.query_eboa.clear_db()

    def tearDown(self):
        # Close connections to the DDBB
        self.engine_eboa.close_session()
        self.engine_eboa_race_conditions.close_session()
        self.query_eboa.close_session()
        self.session.close()

    def test_no_eboa_resources_path(self):

        if "EBOA_RESOURCES_PATH" in os.environ.keys():
            eboa_resources_path = os.environ["EBOA_RESOURCES_PATH"]
            del os.environ["EBOA_RESOURCES_PATH"]
        # end if

        try:
            read_configuration()
        except EboaResourcesPathNotAvailable:
            assert True == True
        except:
            assert False == True

        os.environ["EBOA_RESOURCES_PATH"] = eboa_resources_path

    def test_extract_events_with_alerts_to_be_notified(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"
                       },
            "events": [{
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
                        "name": "alert_name1",
                        "severity": "critical",
                        "description": "Alert description",
                        "group": "alert_group"
                    }}]
            },{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36",
                "alerts": [{
                    "message": "Alert message",
                    "generator": "test",
                    "notification_time": "9999-06-05T08:07:36",
                    "alert_cnf": {
                        "name": "alert_name1",
                        "severity": "critical",
                        "description": "Alert description",
                        "group": "alert_group"
                    }}]
            },{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"}]
        }]
                }

        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        sources = self.session.query(Source).all()

        assert len(sources) == 1

        events = self.session.query(Event).all()

        assert len(events) == 3

        alert_events = self.session.query(EventAlert) \
                                    .join(Alert) \
                                    .join(AlertGroup) \
                                    .filter(EventAlert.message == "Alert message",
                                            EventAlert.generator == "test",
                                            EventAlert.notification_time > "1900-01-01",
                                            EventAlert.notification_time < "2100-01-01",
                                            EventAlert.message == "Alert message",
                                            Alert.name == "alert_name1",
                                            Alert.severity == 4,
                                            Alert.description == "Alert description",
                                            AlertGroup.name == "alert_group").all()

        assert len(alert_events) == 1

        events_with_alerts_to_be_notified = eboa_engine_functions.extract_events_with_alerts_to_be_notified(events)

        assert len(events_with_alerts_to_be_notified) == 1


        alerts_to_be_notified = eboa_engine_functions.extract_alerts_to_be_notified([alert for event in events for alert in event.alerts])

        assert len(alerts_to_be_notified) == 1
