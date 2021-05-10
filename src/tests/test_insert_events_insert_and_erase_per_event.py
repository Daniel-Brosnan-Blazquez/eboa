"""
Automated tests for the engine submodule (event insertion type INSERT_and_ERASE_per_EVENT)

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import os
import sys
import unittest
import datetime
import uuid
import random
import before_after

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

# Import GEOalchemy entities
from geoalchemy2 import functions
from geoalchemy2.shape import to_shape

# Import SQLalchemy entities
from sqlalchemy import func

# Import logging
from eboa.logging import Log

class TestInsertEventsInsertAndErasePerEvent(unittest.TestCase):
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
        
    def test_insert_event_insert_and_erase_per_event(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        },{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2020-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        }]
        }
        self.engine_eboa.treat_data(data)

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 1

    def test_insert_event_insert_and_erase_per_event_event_not_ending_on_period(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2020-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T06:07:36"
            }]
        },{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        },{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T00:07:03",
                       "validity_stop": "2018-06-06T04:07:03"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT"},
                "start": "2018-06-05T03:07:03",
                "stop": "2018-06-05T08:07:03"
            }]
        }]
        }
        self.engine_eboa.treat_data(data)

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T06:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T06:07:36",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 2
        
    def test_insert_event_insert_and_erase_per_event_event_duration_0_in_the_middle_to_remove(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        },{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T04:07:03"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03"
            }]
        }]
        }
        self.engine_eboa.treat_data(data)

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 1

    def test_insert_event_insert_and_erase_per_event_event_duration_0_in_the_middle_to_stay(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        },{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-08-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T04:07:03"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03"
            }]
        }]
        }
        self.engine_eboa.treat_data(data)

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T04:07:03",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:03",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:03",
                                                                           Event.stop == "2018-06-05T04:07:03",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 3
        
        events = self.session.query(Event).filter(Event.visible == True).all()

        assert len(events) == 3
        
    def test_insert_event_insert_and_erase_per_event_event_split(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36",
                "values": [{"name": "VALUES",
                            "type": "object",
                            "values": [
                                {"type": "text",
                                 "name": "TEXT",
                                 "value": "TEXT"},
                                {"type": "boolean",
                                 "name": "BOOLEAN",
                                 "value": "true"},
                                {"type": "boolean",
                                 "name": "BOOLEAN2",
                                 "value": "false"},
                                {"type": "double",
                                 "name": "DOUBLE",
                                 "value": "0.9"},
                                {"type": "timestamp",
                                 "name": "TIMESTAMP",
                                 "value": "20180712T00:00:00"},
                                {"type": "object",
                                 "name": "VALUES2",
                                 "values": [
                                     {"type": "geometry",
                                      "name": "GEOMETRY",
                                      "value": "29.012974905944 -118.33483458667 28.8650301641571 -118.372028380632 28.7171766138274 -118.409121707686 28.5693112139334 -118.44612300623 28.4213994944367 -118.483058731035 28.2734085660472 -118.519970531113 28.1253606038163 -118.556849863134 27.9772541759126 -118.593690316835 27.8291247153939 -118.630472520505 27.7544158362332 -118.64900551674 27.7282373644786 -118.48032600682 27.7015162098732 -118.314168792268 27.6742039940042 -118.150246300849 27.6462511775992 -117.98827485961 27.6176070520608 -117.827974178264 27.5882197156561 -117.669066835177 27.5580360448116 -117.511277813618 27.5270016492436 -117.354334035359 27.4950608291016 -117.197963963877 27.4621565093409 -117.041897175848 27.4282301711374 -116.885863967864 27.3932217651372 -116.729594956238 27.3570696128269 -116.572820673713 27.3197103000253 -116.415271199941 27.2810785491022 -116.256675748617 27.241107085821 -116.09676229722 27.1997272484913 -115.935260563566 27.1524952198729 -115.755436839005 27.2270348347386 -115.734960009089 27.3748346522356 -115.694299254844 27.5226008861849 -115.653563616829 27.6702779354428 -115.612760542177 27.8178690071708 -115.571901649363 27.9653506439026 -115.531000691074 28.1127600020619 -115.490011752733 28.2601469756437 -115.44890306179 28.4076546372628 -115.407649898021 28.455192866856 -115.589486631416 28.4968374106496 -115.752807970928 28.5370603096399 -115.91452902381 28.575931293904 -116.074924211465 28.6135193777855 -116.234273707691 28.6498895688451 -116.392847129762 28.6851057860975 -116.550917254638 28.7192301322012 -116.708756374584 28.752323018501 -116.866636764481 28.7844432583843 -117.024831047231 28.8156481533955 -117.183612605748 28.8459935678779 -117.343255995623 28.8755339855462 -117.504037348554 28.9043225601122 -117.666234808407 28.9324111491586 -117.830128960026 28.9598503481156 -117.996003330616 28.9866878706574 -118.164136222141 29.012974905944 -118.33483458667"}]
            }]
                        }]
        }]
        }]
        }
        self.engine_eboa.treat_data(data1)

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T06:07:36"
            }]
        }]
        }
        self.engine_eboa.treat_data(data2)

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:03",
                                                                           Event.stop == "2018-06-05T06:07:36",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T04:07:03",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T06:07:36",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 3

    def test_insert_event_insert_and_erase_per_event_event_in_ddbb_split(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-07T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T06:07:36"
            }]
        }]
        }
        self.engine_eboa.treat_data(data1)

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        }]
        }
        self.engine_eboa.treat_data(data2)

        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        }]
        }
        self.engine_eboa.treat_data(data3)

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:03",
                                                                           Event.stop == "2018-06-05T06:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T04:07:03",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T06:07:36",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 3

    def test_insert_event_insert_and_erase_per_event_event_in_ddbb_cut(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-07T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        }]
        }
        self.engine_eboa.treat_data(data1)

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T06:07:36"
            }]
        }]
        }
        self.engine_eboa.treat_data(data2)

        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        }]
        }
        self.engine_eboa.treat_data(data3)

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:03",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T04:07:03",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 2

    def test_insert_event_insert_and_erase_per_event_event_in_ddbb_cut_by_previous_event(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-07T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T03:07:03",
                "stop": "2018-06-05T04:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data4 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source4.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T06:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data4)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data3)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T03:07:03",
                                                                           Event.stop == "2018-06-05T04:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T03:07:03",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:36",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 3

    def test_insert_event_insert_and_erase_per_event_event_in_ddbb_cut_by_previous_and_later_event(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-07T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T03:07:03",
                "stop": "2018-06-05T04:07:36"
            },
                       {
                           "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                           "gauge": {"name": "GAUGE_NAME",
                                     "system": "GAUGE_SYSTEM",
                                     "insertion_type": "SIMPLE_UPDATE"},
                           "start": "2018-06-05T06:07:03",
                           "stop": "2018-06-05T08:07:36"
                       }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data4 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source4.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T06:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data4)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",                       
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data3)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T03:07:03",
                                                                           Event.stop == "2018-06-05T04:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T06:07:03",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T03:07:03",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:36",
                                                                           Event.stop == "2018-06-05T06:07:03",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 4

    def test_insert_event_insert_and_erase_per_event_event_inserted_in_the_middle_by_other_ingestion(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        }]
        }

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2020-07-05T02:07:03",
                       "validity_start": "2018-06-04T02:07:03",
                       "validity_stop": "2018-06-05T04:07:36"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-04T02:07:03",
                "stop": "2018-06-05T04:07:36"
            }]
        }]
        }

        def insert_data1():
            self.engine_eboa.treat_data(data1)
        # end def

        def insert_data2():
            self.engine_eboa_race_conditions.data = data2
            self.engine_eboa_race_conditions.operation = data2["operations"][0]
            self.engine_eboa_race_conditions._initialize_context_insert_data()
            self.engine_eboa_race_conditions._insert_dim_signature()
            self.engine_eboa_race_conditions._insert_source()
            self.engine_eboa_race_conditions._insert_gauges()
            self.engine_eboa_race_conditions._insert_events()
            self.engine_eboa_race_conditions.session.commit()
        # end def

        with before_after.before("eboa.engine.engine.race_condition3", insert_data2):
            insert_data1()
        # end with

        events = self.query_eboa.get_events()

        assert len(events) == 2

        events = self.query_eboa.get_events(start_filters = [{"date": "2018-06-04T02:07:03", "op": "=="}],
                                            stop_filters = [{"date": "2018-06-05T04:07:36", "op": "=="}])

        assert len(events) == 1

        events = self.query_eboa.get_events(start_filters = [{"date": "2018-06-05T02:07:03", "op": "=="}],
                                            stop_filters = [{"date": "2018-06-05T08:07:36", "op": "=="}])

        assert len(events) == 1
