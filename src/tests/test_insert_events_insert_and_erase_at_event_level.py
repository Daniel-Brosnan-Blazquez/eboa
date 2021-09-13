"""
Automated tests for the engine submodule (event insertion type INSERT_and_ERASE)

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

class TestInsertEventsInsertAndEraseAtEvent(unittest.TestCase):
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
        
    def test_insert_event_insert_and_erase(self):

        self.engine_eboa._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "INSERT_and_ERASE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36"
                }]
            }
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data["source"]["name"], Source.validity_start == data["source"]["validity_start"], Source.validity_stop == data["source"]["validity_stop"], Source.generation_time == data["source"]["generation_time"], Source.processor_version == data["dim_signature"]["version"], Source.processor == data["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["events"][0]["explicit_reference"]).first()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()
        event_ddbb = self.session.query(Event).filter(Event.start == data["events"][0]["start"],
                                                      Event.stop == data["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == False).all()

        assert len(event_ddbb) == 1

    def test_insert_event_insert_and_erase_remove_events(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE"},
                "start": "2018-06-05T05:07:03",
                "stop": "2018-06-05T05:07:36",
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
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE"},
                "start": "2018-06-05T05:07:04",
                "stop": "2018-06-05T05:07:35"
            }]
        }]
        }
        self.engine_eboa.treat_data(data2)

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T05:07:03",
                                                                           Event.stop == "2018-06-05T05:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 1

    def test_insert_event_insert_and_erase_source_duration_0_in_the_middle_to_remove(self):

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
                          "insertion_type": "INSERT_and_ERASE"},
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
                          "insertion_type": "INSERT_and_ERASE"},
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

    def test_insert_event_insert_and_erase_source_duration_0_in_the_middle_to_stay(self):

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
                          "insertion_type": "INSERT_and_ERASE"},
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
                          "insertion_type": "INSERT_and_ERASE"},
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
        
    def test_insert_event_insert_and_erase_partial_remove_with_links(self):

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
        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 1

        event_uuid1 = events[0].event_uuid

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-08-01T02:07:03",
                       "validity_start": "2018-08-01T02:07:03",
                       "validity_stop": "2018-08-05T06:07:36"},
            "events": [{
                "links": [{
                    "link": str(event_uuid1),
                    "link_mode": "by_uuid",
                    "name": "EVENT_LINK_NAME"
                }],
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-08-01T02:07:03",
                "stop": "2018-08-01T012:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 2

        events = self.session.query(Event).join(Source).filter(Source.name == "source2.json").all()

        assert len(events) == 1

        event_uuid2 = events[0].event_uuid

        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-09-06T02:07:03",
                       "validity_start": "2018-08-02T04:07:03",
                       "validity_stop": "2018-08-06T10:07:36"},
            "events": [{
                "links": [{
                    "link": str(event_uuid1),
                    "link_mode": "by_uuid",
                    "name": "EVENT_LINK_NAME"
                }],
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE"},
                "start": "2018-08-05T04:07:03",
                "stop": "2018-08-05T10:07:36"
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data3)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).join(Source).filter(Source.name == "source3.json").all()

        assert len(events) == 1

        event_uuid2 = events[0].event_uuid

        data4 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source4.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-10-03T02:07:03",
                       "validity_start": "2018-08-03T02:07:03",
                       "validity_stop": "2018-08-07T12:07:36"},
            "events": [{
                "links": [{
                    "link": str(event_uuid1),
                    "link_mode": "by_uuid",
                    "name": "EVENT_LINK_NAME"
                }],
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE"},
                "start": "2018-08-05T02:07:03",
                "stop": "2018-08-05T012:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data4)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0
        
        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:03",
                                                                           Event.stop == "2018-06-05T06:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-08-01T02:07:03",
                                                                           Event.stop == "2018-08-01T12:07:36",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        event_uuid2 = events[0].event_uuid

        events = self.session.query(Event).join(Gauge).join(Source).filter(Source.name == "source3.json").all()

        assert len(events) == 0

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME",
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-08-05T02:07:03",
                                                                           Event.stop == "2018-08-05T12:07:36",
                                                                           Source.name == "source4.json").all()
        assert len(events) == 1

        event_uuid3 = events[0].event_uuid

        events = self.session.query(Event).all()

        assert len(events) == 3

        event_links = self.session.query(EventLink).all()

        assert len(event_links) == 2

        event_links = self.session.query(EventLink).filter(EventLink.name == "EVENT_LINK_NAME",
                                                           EventLink.event_uuid == event_uuid1,
                                                           EventLink.event_uuid_link == event_uuid2).all()

        assert len(event_links) == 1

        event_links = self.session.query(EventLink).filter(EventLink.name == "EVENT_LINK_NAME",
                                                           EventLink.event_uuid == event_uuid1,
                                                           EventLink.event_uuid_link == event_uuid3).all()

        assert len(event_links) == 1
        
    def test_remove_deprecated_event_insert_and_erase_same_period(self):

        self.engine_eboa._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
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
                                 }]}]
                }]
            }
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()

        self.engine_eboa._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "INSERT_and_ERASE"},
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
                                 }]}]
                }]
            }
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()
        self.engine_eboa._remove_deprecated_events_by_insert_and_erase_at_event_level()
        self.engine_eboa.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data["source"]["name"], Source.validity_start == data["source"]["validity_start"], Source.validity_stop == data["source"]["validity_stop"], Source.generation_time == data["source"]["generation_time"], Source.processor_version == data["dim_signature"]["version"], Source.processor == data["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data["events"][0]["start"],
                                                      Event.stop == data["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 1

        values_ddbb = self.query_eboa.get_event_values()

        assert len(values_ddbb) == 8

    def test_remove_deprecated_event_insert_and_erase_one_starts_before_links(self):

        self.engine_eboa._initialize_context_insert_data()
        data1 = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-04T02:07:00",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "link_ref": "EVENT_LINK1",
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-04T02:07:00",
                    "stop": "2018-06-05T08:07:36",
                    "links": [{
                        "link": "EVENT_LINK2",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK_NAME1",
                        "back_ref": "LINK_BACK_REF_NAME"
                    }],
                    "key": "EVENT_KEY",
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
                                 }]}]
                },
                           {
                               "link_ref": "EVENT_LINK2",
                               "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                               "gauge": {"name": "GAUGE_NAME",
                                         "system": "GAUGE_SYSTEM",
                                         "insertion_type": "SIMPLE_UPDATE"},
                               "start": "2018-06-04T02:07:02",
                               "stop": "2018-06-05T08:07:36",
                               "links": [{
                                   "link": "EVENT_LINK1",
                                   "link_mode": "by_ref",
                                   "name": "EVENT_LINK_NAME2",
                                   "back_ref": "LINK_BACK_REF_NAME"
                               }]},
                           {
                               "explicit_reference": "EXPLICIT_REFERENCE_EVENT3",
                               "gauge": {"name": "GAUGE_NAME",
                                         "system": "GAUGE_SYSTEM",
                                         "insertion_type": "SIMPLE_UPDATE"},
                               "start": "2018-06-04T02:07:03",
                               "stop": "2018-06-04T04:07:36",
                               "links": [{
                                   "link": "EVENT_LINK1",
                                   "link_mode": "by_ref",
                                   "name": "EVENT_LINK_NAME3",
                                   "back_ref": "LINK_BACK_REF_NAME"
                               }]}]
            }
        self.engine_eboa.operation = data1
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()

        self.engine_eboa._initialize_context_insert_data()
        data2 = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT4",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "INSERT_and_ERASE"},
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
                                 }]}]
                }]
            }
        self.engine_eboa.operation = data2
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()
        self.engine_eboa._remove_deprecated_events_by_insert_and_erase_at_event_level()
        self.engine_eboa.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data2["events"][0]["gauge"]["name"], Gauge.system == data2["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data2["source"]["name"], Source.validity_start == data2["source"]["validity_start"], Source.validity_stop == data2["source"]["validity_stop"], Source.generation_time == data2["source"]["generation_time"], Source.processor_version == data2["dim_signature"]["version"], Source.processor == data2["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data2["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data2["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data2["events"][0]["start"],
                                                      Event.stop == data2["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data1["events"][0]["gauge"]["name"], Gauge.system == data1["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data1["source"]["name"], Source.validity_start == data1["source"]["validity_start"], Source.validity_stop == data1["source"]["validity_stop"], Source.generation_time == data1["source"]["generation_time"], Source.processor_version == data1["dim_signature"]["version"], Source.processor == data1["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data1["dim_signature"]["name"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data1["events"][0]["start"],
                                                      Event.stop == data2["source"]["validity_start"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data1["events"][1]["start"],
                                                      Event.stop == data2["source"]["validity_start"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data1["events"][2]["start"],
                                                      Event.stop == data1["events"][2]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 4

        values_ddbb = self.query_eboa.get_event_values()

        assert len(values_ddbb) == 16

        links_ddbb = self.query_eboa.get_event_links()

        assert len(links_ddbb) == 6

        events_with_links = self.session.query(Event).join(Source).filter(Source.name == "source.xml").order_by(Event.start).all()
        
        rest_of_event_uuids = [events_with_links[1].event_uuid, events_with_links[2].event_uuid]
        
        event_links_ddbb = self.query_eboa.get_event_links(event_uuids = {"filter": rest_of_event_uuids, "op": "in"}, event_uuid_links = {"filter": [events_with_links[0].event_uuid], "op": "in"})

        assert len(event_links_ddbb) == 3

        rest_of_event_uuids = [events_with_links[0].event_uuid, events_with_links[2].event_uuid]
        
        event_links_ddbb = self.query_eboa.get_event_links(event_uuids = {"filter": rest_of_event_uuids, "op": "in"}, event_uuid_links = {"filter": [events_with_links[1].event_uuid], "op": "in"})

        assert len(event_links_ddbb) == 2

        rest_of_event_uuids = [events_with_links[0].event_uuid, events_with_links[1].event_uuid]
        
        event_links_ddbb = self.query_eboa.get_event_links(event_uuids = {"filter": rest_of_event_uuids, "op": "in"}, event_uuid_links = {"filter": [events_with_links[2].event_uuid], "op": "in"})

        assert len(event_links_ddbb) == 1

    def test_remove_deprecated_event_insert_and_erase_only_remains_last_inserted_source(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-04T02:07:04",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                               "explicit_reference": "EXPLICIT_REFERENCE_EVENT3",
                               "gauge": {"name": "GAUGE_NAME",
                                         "system": "GAUGE_SYSTEM",
                                         "insertion_type": "SIMPLE_UPDATE"},
                               "start": "2018-06-04T02:07:04",
                               "stop": "2018-06-04T04:07:36",
                               "links": [{
                                   "link": "EVENT_LINK1",
                                   "link_mode": "by_ref",
                                   "name": "EVENT_LINK_NAME4",
                                   "back_ref": "LINK_BACK_REF_NAME"
                               }]},
                           {
                    "link_ref": "EVENT_LINK1",
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T02:07:04",
                    "stop": "2018-06-05T08:07:36",
                    "links": [{
                        "link": "EVENT_LINK2",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK_NAME1",
                        "back_ref": "LINK_BACK_REF_NAME"
                    }],
                    "key": "EVENT_KEY",
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
                                 }]}]
                },
                           {
                               "link_ref": "EVENT_LINK2",
                               "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                               "gauge": {"name": "GAUGE_NAME",
                                         "system": "GAUGE_SYSTEM",
                                         "insertion_type": "SIMPLE_UPDATE"},
                               "start": "2018-06-05T02:07:04",
                               "stop": "2018-06-05T08:07:36",
                               "links": [{
                                   "link": "EVENT_LINK1",
                                   "link_mode": "by_ref",
                                   "name": "EVENT_LINK_NAME2",
                                   "back_ref": "LINK_BACK_REF_NAME"
                               }]},
                           {
                               "explicit_reference": "EXPLICIT_REFERENCE_EVENT3",
                               "gauge": {"name": "GAUGE_NAME",
                                         "system": "GAUGE_SYSTEM",
                                         "insertion_type": "SIMPLE_UPDATE"},
                               "start": "2018-06-05T02:07:04",
                               "stop": "2018-06-05T04:07:36",
                               "links": [{
                                   "link": "EVENT_LINK1",
                                   "link_mode": "by_ref",
                                   "name": "EVENT_LINK_NAME3",
                                   "back_ref": "LINK_BACK_REF_NAME"
                               }]}]
            }]}

        self.engine_eboa.treat_data(data1)

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT4",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "INSERT_and_ERASE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36"
                }]
            }]}
        self.engine_eboa.treat_data(data2)
        
        events = self.query_eboa.get_events()

        assert len(events) == 2

        event_links = self.session.query(EventLink).all()

        assert len(event_links) == 0

        event_values = self.query_eboa.get_event_values()

        assert len(event_values) == 0

    def test_remove_deprecated_event_insert_and_erase_long_event_cut(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.json",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-06T02:07:03",
                           "validity_start": "2018-06-05T03:07:03",
                           "validity_stop": "2018-06-05T04:07:36"},
                "events": [{
                               "explicit_reference": "EXPLICIT_REFERENCE_EVENT3",
                               "gauge": {"name": "GAUGE_NAME",
                                         "system": "GAUGE_SYSTEM",
                                         "insertion_type": "SIMPLE_UPDATE"},
                               "start": "2018-06-05T03:07:03",
                               "stop": "2018-06-05T04:07:36"
                }]
            }]}

        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source3.json",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-04T02:07:03",
                           "validity_start": "2018-06-05T06:07:03",
                           "validity_stop": "2018-06-05T07:07:36"},
                "events": [{
                               "explicit_reference": "EXPLICIT_REFERENCE_EVENT3",
                               "gauge": {"name": "GAUGE_NAME",
                                         "system": "GAUGE_SYSTEM",
                                         "insertion_type": "SIMPLE_UPDATE"},
                               "start": "2018-06-05T06:07:03",
                               "stop": "2018-06-05T07:07:36"
                }]
            }]}

        exit_status = self.engine_eboa.treat_data(data3)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.json",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT4",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "INSERT_and_ERASE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36"
                }]
            }]}

        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0
        
        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T3:07:03",
                                                                           Event.stop == "2018-06-05T04:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T2:07:03",
                                                                           Event.stop == "2018-06-05T03:07:03",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T4:07:36",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 3

    def test_remove_deprecated_event_insert_and_erase_one_starts_after(self):

        self.engine_eboa._initialize_context_insert_data()
        data1 = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:15",
                           "validity_stop": "2018-06-06T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T02:07:15",
                    "stop": "2018-06-06T08:07:36",
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
                                 }]}]
                }]
            }
        self.engine_eboa.operation = data1
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()

        self.engine_eboa._initialize_context_insert_data()
        data2 = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "INSERT_and_ERASE"},
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
                                 }]}]
                }]
            }
        self.engine_eboa.operation = data2
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()
        self.engine_eboa._remove_deprecated_events_by_insert_and_erase_at_event_level()
        self.engine_eboa.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data2["events"][0]["gauge"]["name"], Gauge.system == data2["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data2["source"]["name"], Source.validity_start == data2["source"]["validity_start"], Source.validity_stop == data2["source"]["validity_stop"], Source.generation_time == data2["source"]["generation_time"], Source.processor_version == data2["dim_signature"]["version"], Source.processor == data2["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data2["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data2["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data2["events"][0]["start"],
                                                      Event.stop == data2["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data1["events"][0]["gauge"]["name"], Gauge.system == data1["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data1["source"]["name"], Source.validity_start == data1["source"]["validity_start"], Source.validity_stop == data1["source"]["validity_stop"], Source.generation_time == data1["source"]["generation_time"], Source.processor_version == data1["dim_signature"]["version"], Source.processor == data1["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data1["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data1["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data2["events"][0]["stop"],
                                                      Event.stop == data1["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 2

        values_ddbb = self.query_eboa.get_event_values()

        assert len(values_ddbb) == 16

    def test_remove_deprecated_event_insert_and_erase_events_not_overlapping(self):

        self.engine_eboa._initialize_context_insert_data()
        data1 = {"dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source2.xml",
                            "reception_time": "2018-06-06T13:33:29",
                            "generation_time": "2018-07-05T02:07:03",
                            "validity_start": "2018-06-05T02:07:03",
                            "validity_stop": "2018-06-05T08:07:36"},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "SIMPLE_UPDATE"},
                     "start": "2018-06-05T02:07:03",
                     "stop": "2018-06-05T04:07:36",
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
                                  }]}]
                }]
            }
        self.engine_eboa.operation = data1
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()

        data2 = {"dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source.xml",
                            "reception_time": "2018-06-06T13:33:29",
                            "generation_time": "2018-07-05T02:07:04",
                            "validity_start": "2018-06-04T02:07:03",
                            "validity_stop": "2018-06-06T08:07:36"},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "INSERT_and_ERASE"},
                     "start": "2018-06-04T05:07:03",
                     "stop": "2018-06-05T07:07:36",
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
                                  }]}]
                }]
            }
        self.engine_eboa._initialize_context_insert_data()
        self.engine_eboa.operation = data2
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()
        self.engine_eboa._remove_deprecated_events_by_insert_and_erase_at_event_level()
        self.engine_eboa.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data2["events"][0]["gauge"]["name"], Gauge.system == data2["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data2["source"]["name"], Source.validity_start == data2["source"]["validity_start"], Source.validity_stop == data2["source"]["validity_stop"], Source.generation_time == data2["source"]["generation_time"], Source.processor_version == data2["dim_signature"]["version"], Source.processor == data2["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data2["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data2["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data2["events"][0]["start"],
                                                      Event.stop == data2["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 1

        values_ddbb = self.query_eboa.get_event_values()

        assert len(values_ddbb) == 8

    def test_remove_deprecated_event_insert_and_erase_split_events(self):

        self.engine_eboa._initialize_context_insert_data()
        data1 = {"dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source1.xml",
                            "reception_time": "2018-06-06T13:33:29",
                            "generation_time": "2018-07-10T02:07:03",
                            "validity_start": "2018-06-05T02:07:03",
                            "validity_stop": "2018-06-05T08:07:36"},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "SIMPLE_UPDATE"},
                     "start": "2018-06-05T02:07:03",
                     "stop": "2018-06-05T04:07:36"
                }]
            }
        self.engine_eboa.operation = data1
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()

        self.engine_eboa._initialize_context_insert_data()
        data2 = {"dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source2.xml",
                            "reception_time": "2018-06-06T13:33:29",
                            "generation_time": "2018-07-10T02:07:03",
                            "validity_start": "2018-06-05T11:07:03",
                            "validity_stop": "2018-06-05T17:07:36"},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "SIMPLE_UPDATE"},
                     "start": "2018-06-05T12:07:03",
                     "stop": "2018-06-05T15:07:36"
                }]
            }
        self.engine_eboa.operation = data2
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()

        data3 = {"dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source3.xml",
                            "reception_time": "2018-06-06T13:33:29",
                            "generation_time": "2018-07-05T02:07:04",
                            "validity_start": "2018-06-04T02:07:03",
                            "validity_stop": "2018-06-06T08:07:36"},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "INSERT_and_ERASE"},
                     "start": "2018-06-04T05:07:03",
                     "stop": "2018-06-06T07:07:36"
                }]
            }
        self.engine_eboa._initialize_context_insert_data()
        self.engine_eboa.operation = data3
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()
        self.engine_eboa._remove_deprecated_events_by_insert_and_erase_at_event_level()
        self.engine_eboa.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data1["events"][0]["gauge"]["name"], Gauge.system == data1["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data1["source"]["name"], Source.validity_start == data1["source"]["validity_start"], Source.validity_stop == data1["source"]["validity_stop"], Source.generation_time == data1["source"]["generation_time"], Source.processor_version == data1["dim_signature"]["version"], Source.processor == data1["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data1["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data1["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data1["events"][0]["start"],
                                                      Event.stop == data1["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data2["events"][0]["gauge"]["name"], Gauge.system == data2["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data2["source"]["name"], Source.validity_start == data2["source"]["validity_start"], Source.validity_stop == data2["source"]["validity_stop"], Source.generation_time == data2["source"]["generation_time"], Source.processor_version == data2["dim_signature"]["version"], Source.processor == data2["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data2["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data2["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data2["events"][0]["start"],
                                                      Event.stop == data2["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data3["events"][0]["gauge"]["name"], Gauge.system == data3["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data3["source"]["name"], Source.validity_start == data3["source"]["validity_start"], Source.validity_stop == data3["source"]["validity_stop"], Source.generation_time == data3["source"]["generation_time"], Source.processor_version == data3["dim_signature"]["version"], Source.processor == data3["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data3["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data3["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data3["events"][0]["start"],
                                                      Event.stop == data1["source"]["validity_start"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data3["events"][0]["gauge"]["name"], Gauge.system == data3["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data3["source"]["name"], Source.validity_start == data3["source"]["validity_start"], Source.validity_stop == data3["source"]["validity_stop"], Source.generation_time == data3["source"]["generation_time"], Source.processor_version == data3["dim_signature"]["version"], Source.processor == data3["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data3["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data3["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data1["source"]["validity_stop"],
                                                      Event.stop == data2["source"]["validity_start"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data2["source"]["validity_stop"],
                                                      Event.stop == data3["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 5

    def test_remove_deprecated_event_insert_and_erase_split_events_not_created(self):

        self.engine_eboa._initialize_context_insert_data()
        data1 = {"dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source1.xml",
                            "reception_time": "2018-06-06T13:33:29",
                            "generation_time": "2018-07-10T02:07:03",
                            "validity_start": "2018-06-05T02:07:03",
                            "validity_stop": "2018-06-05T08:07:36"},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "SIMPLE_UPDATE"},
                     "start": "2018-06-05T02:07:03",
                     "stop": "2018-06-05T04:07:36"
                }]
            }
        self.engine_eboa.operation = data1
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()

        self.engine_eboa._initialize_context_insert_data()
        data2 = {"dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source2.xml",
                            "reception_time": "2018-06-06T13:33:29",
                            "generation_time": "2018-07-10T02:07:03",
                            "validity_start": "2018-06-05T08:07:36",
                            "validity_stop": "2018-06-05T17:07:36"},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "SIMPLE_UPDATE"},
                     "start": "2018-06-05T09:07:03",
                     "stop": "2018-06-05T15:07:36"
                }]
            }
        self.engine_eboa.operation = data2
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()

        data3 = {"dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source3.xml",
                            "reception_time": "2018-06-06T13:33:29",
                            "generation_time": "2018-07-05T02:07:04",
                            "validity_start": "2018-06-04T02:07:03",
                            "validity_stop": "2018-06-06T08:07:36"},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "INSERT_and_ERASE"},
                     "start": "2018-06-04T05:07:03",
                     "stop": "2018-06-05T09:07:36"
                }]
            }
        self.engine_eboa._initialize_context_insert_data()
        self.engine_eboa.operation = data3
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()
        self.engine_eboa._remove_deprecated_events_by_insert_and_erase_at_event_level()
        self.engine_eboa.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data1["events"][0]["gauge"]["name"], Gauge.system == data1["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data1["source"]["name"], Source.validity_start == data1["source"]["validity_start"], Source.validity_stop == data1["source"]["validity_stop"], Source.generation_time == data1["source"]["generation_time"], Source.processor_version == data1["dim_signature"]["version"], Source.processor == data1["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data1["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data1["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data1["events"][0]["start"],
                                                      Event.stop == data1["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data2["events"][0]["gauge"]["name"], Gauge.system == data2["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data2["source"]["name"], Source.validity_start == data2["source"]["validity_start"], Source.validity_stop == data2["source"]["validity_stop"], Source.generation_time == data2["source"]["generation_time"], Source.processor_version == data2["dim_signature"]["version"], Source.processor == data2["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data2["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data2["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data2["events"][0]["start"],
                                                      Event.stop == data2["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data3["events"][0]["gauge"]["name"], Gauge.system == data3["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data3["source"]["name"], Source.validity_start == data3["source"]["validity_start"], Source.validity_stop == data3["source"]["validity_stop"], Source.generation_time == data3["source"]["generation_time"], Source.processor_version == data3["dim_signature"]["version"], Source.processor == data3["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data3["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data3["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data3["events"][0]["start"],
                                                      Event.stop == data1["source"]["validity_start"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 3
    def test_insert_and_erase_at_event_level_with_split_event_crossing_validity(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source1.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-07T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T06:07:36"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T06:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source2.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T05:07:03",
                       "validity_stop": "2018-06-05T09:07:03"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T05:07:03",
                "stop": "2018-06-05T09:07:03",
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
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source3.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        events = self.session.query(Event).all()

        assert len(events) == 2

        events = self.session.query(Event).join(Source).filter(Source.name == "source1.xml",
                                                               Event.start == "2018-06-05T02:07:03",
                                                               Event.stop == "2018-06-05T06:07:36").all()


        assert len(events) == 1

        events = self.session.query(Event).join(Source).filter(Source.name == "source2.xml",
                                                               Event.start == "2018-06-05T06:07:36",
                                                               Event.stop == "2018-06-05T09:07:03").all()


        assert len(events) == 1

        alert_events = self.session.query(EventAlert) \
                                    .join(Alert) \
                                    .join(AlertGroup) \
                                    .filter(EventAlert.message == "Alert message",
                                            EventAlert.generator == "test",
                                            EventAlert.notification_time > "1900-01-01",
                                            EventAlert.message == "Alert message",
                                            Alert.name == "alert_name1",
                                            Alert.severity == 4,
                                            Alert.description == "Alert description",
                                            AlertGroup.name == "alert_group",
                                            EventAlert.event_uuid == events[0].event_uuid).all()

        assert len(alert_events) == 1


    def test_remove_deprecated_event_insert_and_erase_at_event_level_remove_event_after_deletion(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source1.xml",
                            "reception_time": "2018-06-06T13:33:29",
                            "generation_time": "2020-07-10T02:07:03",
                            "validity_start": "2018-06-05T03:07:03",
                            "validity_stop": "2018-06-05T03:07:36"},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "SIMPLE_UPDATE"},
                     "start": "2018-06-05T03:07:03",
                     "stop": "2018-06-05T03:07:36"
                }]
        }]}

        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source2.xml",
                            "reception_time": "2018-06-06T13:33:29",
                            "generation_time": "2016-07-10T02:07:03",
                            "validity_start": "2018-06-05T04:07:36",
                            "validity_stop": "2018-06-05T08:07:36"},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "SIMPLE_UPDATE"},
                     "start": "2018-06-05T05:07:03",
                     "stop": "2018-06-05T05:07:36"
                }]
        }]}

        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0
        
        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source3.xml",
                            "reception_time": "2018-06-06T13:33:29",
                            "generation_time": "2019-07-05T02:07:04",
                            "validity_start": "2018-06-04T02:07:03",
                            "validity_stop": "2018-06-06T08:07:36"},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "INSERT_and_ERASE"},
                     "start": "2018-06-05T03:07:04",
                     "stop": "2018-06-05T03:07:35"
                 }]
        }]}

        exit_status = self.engine_eboa.treat_data(data3)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0
 
        filtered_events_ddbb = self.session.query(Event).join(Source).filter(Event.start == "2018-06-05T03:07:03",
                                                                             Event.stop == "2018-06-05T03:07:36",
                                                                             Source.name == "source1.xml",
                                                                             Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 1

    def test_remove_deprecated_event_insert_and_erase_more_gauges_lower_generation_time(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                    "gauge": {"name": "GAUGE_NAME1",
                              "system": "GAUGE_SYSTEM1",
                              "insertion_type": "SIMPLE_UPDATE"},
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
                                     "value": "20180712T00:00:00"}]
                }]
        }]}]}
        self.engine_eboa.data = data
        self.engine_eboa.treat_data()

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 1

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-04T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                    "gauge": {"name": "GAUGE_NAME1",
                              "system": "GAUGE_SYSTEM1",
                              "insertion_type": "INSERT_and_ERASE"},
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
                                     "value": "20180712T00:00:00"}]
                                }]
                },
                           {
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                    "gauge": {"name": "GAUGE_NAME2",
                              "system": "GAUGE_SYSTEM2",
                              "insertion_type": "INSERT_and_ERASE"},
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
                                     "value": "20180712T00:00:00"}]
                                }]
                },
                           {
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT3",
                    "gauge": {"name": "GAUGE_NAME3",
                              "system": "GAUGE_SYSTEM3",
                              "insertion_type": "INSERT_and_ERASE"},
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
                                     "value": "20180712T00:00:00"}]
                                }]
                           },
                           {
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT4",
                    "gauge": {"name": "GAUGE_NAME4",
                              "system": "GAUGE_SYSTEM4",
                              "insertion_type": "INSERT_and_ERASE"},
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
                                     "value": "20180712T00:00:00"}]
                                }]
                           }]}]}
        self.engine_eboa.data = data
        self.engine_eboa.treat_data()

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 4

    def test_linking_to_a_removed_event(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2020-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"
                       },
            "events": [{
                "link_ref": "EVENT_LINK",
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
                        }]
        }]
        }

        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 1
        
        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"
                       },
            "events": [{
                "link_ref": "EVENT_LINK1",
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
            },{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"
                       },
                "events": [{
                           "link_ref": "EVENT_LINK2",
                           "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                           "gauge": {"name": "GAUGE_NAME2",
                                     "system": "GAUGE_SYSTEM2",
                                     "insertion_type": "SIMPLE_UPDATE"},
                           "start": "2018-06-05T02:07:03",
                           "stop": "2018-06-05T08:07:36",
                           "links": [{
                               "link": str(events[0].event_uuid),
                               "link_mode": "by_uuid",
                               "name": "EVENT_LINK_NAME2"
                           },{
                               "link": "EVENT_LINK1",
                               "link_mode": "by_ref",
                               "name": "EVENT_LINK_NAME"
                           }]
                       }]
        }]
        }

        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

    def test_insert_event_insert_and_erase_at_event_event_inserted_in_the_middle_by_other_ingestion(self):

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
                          "insertion_type": "INSERT_and_ERASE"},
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

    def test_remove_deprecated_event_insert_and_erase_event_outside_period(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source1.xml",
                            "reception_time": "2018-06-06T13:33:29",
                            "generation_time": "2020-07-10T02:07:03",
                            "validity_start": "2018-06-02T03:07:03",
                            "validity_stop": "2018-06-08T03:07:36"},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "SIMPLE_UPDATE"},
                     "start": "2018-06-02T03:07:03",
                     "stop": "2018-06-02T03:07:36"
                }]
        }]}

        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0
        
        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source3.xml",
                            "reception_time": "2018-06-06T13:33:29",
                            "generation_time": "2018-07-05T02:07:04",
                            "validity_start": "2018-06-04T02:07:03",
                            "validity_stop": "2018-06-06T08:07:36"},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "INSERT_and_ERASE"},
                     "start": "2018-06-05T03:07:04",
                     "stop": "2018-06-05T03:07:35"
                 }]
        }]}

        exit_status = self.engine_eboa.treat_data(data3)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0
 
        filtered_events_ddbb = self.session.query(Event).join(Source).filter(Event.start == "2018-06-02T03:07:03",
                                                                             Event.stop == "2018-06-02T03:07:36",
                                                                             Source.name == "source1.xml",
                                                                             Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 1
