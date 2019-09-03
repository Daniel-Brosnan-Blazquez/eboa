"""
Automated tests for the query submodule

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import os
import sys
import unittest
import datetime

# Import engine of the DDBB
from eboa.engine.engine import Engine
from eboa.engine.query import Query
from eboa.datamodel.base import Session, engine, Base

# Import datamodel
from eboa.datamodel.base import Session, engine, Base
from eboa.datamodel.dim_signatures import DimSignature
from eboa.datamodel.events import Event, EventLink, EventKey, EventText, EventDouble, EventObject, EventGeometry, EventBoolean, EventTimestamp
from eboa.datamodel.gauges import Gauge
from eboa.datamodel.sources import Source, SourceStatus
from eboa.datamodel.explicit_refs import ExplicitRef, ExplicitRefGrp, ExplicitRefLink
from eboa.datamodel.annotations import Annotation, AnnotationCnf, AnnotationText, AnnotationDouble, AnnotationObject, AnnotationGeometry, AnnotationBoolean, AnnotationTimestamp
from sqlalchemy.dialects import postgresql

# Import exceptions
from eboa.engine.errors import InputError

class TestQuery(unittest.TestCase):
    def setUp(self):
        # Instantiate the query component
        self.query = Query()

        # Create the engine to manage the data
        self.engine_eboa = Engine()

        # Clear all tables before executing the test
        self.query.clear_db()

    def tearDown(self):
        # Close connections to the DDBB
        self.engine_eboa.close_session()
        self.query.close_session()

    def test_query_dim_signature(self):
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"}
                                  }
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()

        dim_signature1 = self.query.get_dim_signatures()

        assert len(dim_signature1) == 1

        dim_signature2 = self.query.get_dim_signatures(dim_signature_uuids = {"filter": [dim_signature1[0].dim_signature_uuid], "op": "in"})

        assert len(dim_signature2) == 1

        dim_signature3 = self.query.get_dim_signatures(dim_signatures = {"filter": [data["dim_signature"]["name"]], "op": "in"})

        assert len(dim_signature3) == 1

        name = data["dim_signature"]["name"][0:7] + "%"
        dim_signature4 = self.query.get_dim_signatures(dim_signatures = {"filter": name, "op": "like"})

        assert len(dim_signature4) == 1

        dim_signature5 = self.query.get_dim_signatures(dim_signature_uuids = {"filter": [dim_signature1[0].dim_signature_uuid], "op": "in"},
                                                       dim_signatures = {"filter": [data["dim_signature"]["name"]], "op": "in"})

        assert len(dim_signature5) == 1

    def test_wrong_inputs_query_dim_signature(self):

        result = False
        try:
            self.query.get_dim_signatures(dim_signature_uuids = "not_a_dict")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_dim_signatures(dim_signature_uuids = {"filter": ["e6f03f0c-aced-11e8-9fef-000000001643"]})
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_dim_signatures(dim_signature_uuids = {"op": "in"})
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_dim_signatures(dim_signature_uuids = {"filter": ["e6f03f0c-aced-11e8-9fef-000000001643"], "op": "in", "not_a_valid_key": 0})
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_dim_signatures(dim_signature_uuids = {"filter": ["e6f03f0c-aced-11e8-9fef-000000001643"], "op": "not_a_valid_operator"})
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_dim_signatures(dim_signature_uuids = {"filter": "not_a_list", "op": "in"})
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_dim_signatures(dim_signature_uuids = {"filter": [["not_a_string"]], "op": "in"})
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_dim_signatures(dim_signatures = "not_a_dict")
        except InputError:
            result = True
        # end try

        assert result == True
        result = False
        try:
            self.query.get_dim_signatures(dim_signature_uuids = {"filter": ["not_a_string"], "op": "like"})
        except InputError:
            result = True
        # end try

        assert result == True

    def test_query_source(self):
        data = {"operations": [{
                "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"}
        },
{
                "mode": "insert",
            "dim_signature": {"name": "dim_signature2",
                                  "exec": "exec2",
                                  "version": "2.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2017-07-05T02:07:03",
                           "validity_start": "2017-06-05T02:07:03",
                           "validity_stop": "2017-06-05T08:07:36"}
        }]}
        self.engine_eboa.treat_data(data)

        dim_signature1 = self.query.get_dim_signatures(dim_signatures = {"filter": "dim_signature", "op": "like"})

        source1 = self.query.get_sources(dim_signature_uuids = {"filter": [dim_signature1[0].dim_signature_uuid], "op": "in"})

        assert len(source1) == 1

        sources = self.query.get_sources(ingestion_error = {"filter": True, "op": "!="})

        assert len(sources) == 2

        source = self.query.get_sources(source_uuids = {"filter": [source1[0].source_uuid], "op": "in"})

        assert len(source) == 1

        source = self.query.get_sources(names = {"filter": [data["operations"][0]["source"]["name"]], "op": "in"})

        assert len(source) == 1

        name = data["operations"][0]["source"]["name"]
        source = self.query.get_sources(names = {"filter": name, "op": "like"})

        assert len(source) == 1

        source = self.query.get_sources(validity_stop_filters = [{"date": "2018-06-05T08:07:36", "op": "=="}])

        assert len(source) == 1

        source = self.query.get_sources(generation_time_filters = [{"date": "2018-07-05T02:07:03", "op": "=="}])

        assert len(source) == 1

        sources = self.query.get_sources(ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}])

        assert len(sources) == 2

        sources = self.query.get_sources(ingestion_duration_filters = [{"float": 10, "op": "<"}])

        assert len(sources) == 2

        source = self.query.get_sources(processor_version_filters = [{"filter": "2.0", "op": "<"}])

        assert len(source) == 1

        source = self.query.get_sources(processors = {"filter": [data["operations"][0]["dim_signature"]["exec"]], "op": "in"})

        assert len(source) == 1

        sources = self.query.get_sources(statuses = [{"float": 0, "op": "=="}])

        assert len(sources) == 2

        dim_sig_name = data["operations"][0]["dim_signature"]["name"]
        source = self.query.get_sources(dim_signatures = {"filter": dim_sig_name, "op": "like"})

        assert len(source) == 1

        source = self.query.get_sources(dim_signature_uuids = {"filter": [dim_signature1[0].dim_signature_uuid], "op": "in"},
                                        source_uuids = {"filter": [source1[0].source_uuid], "op": "in"},
                                        names = {"filter": name, "op": "like"},
                                        validity_start_filters = [{"date": "2018-06-05T02:07:03", "op": "=="}],
                                        validity_stop_filters = [{"date": "2018-06-05T08:07:36", "op": "=="}],
                                        generation_time_filters = [{"date": "2018-07-05T02:07:03", "op": "=="}],
                                        ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}],
                                        ingestion_duration_filters = [{"float": 10, "op": "<"}],
                                        processor_version_filters = [{"filter": "0.0", "op": ">"}],
                                        dim_signatures = {"filter": dim_sig_name, "op": "like"},
        statuses = [{"float": 0, "op": "=="}])

        assert len(source) == 1

    def test_query_source_by_validity_all_operators(self):

        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"}
                                  }
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()

        source = self.query.get_sources(validity_start_filters = [{"date": "2018-06-05T02:07:03", "op": "=="}])

        assert len(source) == 1

        source = self.query.get_sources(validity_start_filters = [{"date": "2018-06-05T02:07:00", "op": ">"}])

        assert len(source) == 1

        source = self.query.get_sources(validity_start_filters = [{"date": "2018-06-05T02:07:10", "op": "<"}])

        assert len(source) == 1

        source = self.query.get_sources(validity_start_filters = [{"date": "2018-06-05T02:07:10", "op": "!="}])

        assert len(source) == 1

        source = self.query.get_sources(validity_start_filters = [{"date": "2018-06-05T02:07:00", "op": ">="}])

        assert len(source) == 1

        source = self.query.get_sources(validity_start_filters = [{"date": "2018-06-05T02:07:10", "op": "<="}])

        assert len(source) == 1

        source = self.query.get_sources(validity_start_filters = [{"date": "2018-06-05T02:07:03", "op": "!="}])

        assert len(source) == 0

        source = self.query.get_sources(validity_start_filters = [{"date": "2018-06-05T02:07:00", "op": "<"}])

        assert len(source) == 0

        source = self.query.get_sources(validity_start_filters = [{"date": "2018-06-05T02:07:10", "op": ">"}])

        assert len(source) == 0

        source = self.query.get_sources(validity_start_filters = [{"date": "2018-06-05T02:07:10", "op": "=="}])

        assert len(source) == 0

        source = self.query.get_sources(validity_start_filters = [{"date": "2018-06-05T02:07:00", "op": "<="}])

        assert len(source) == 0

        source = self.query.get_sources(validity_start_filters = [{"date": "2018-06-05T02:07:10", "op": ">="}])

        assert len(source) == 0

    def test_wrong_inputs_query_source(self):

        result = False
        try:
            self.query.get_sources(dim_signature_uuids = "not_a_dict")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(source_uuids = "not_a_dict")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(names = "not_a_dict")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(names = "not_a_dict")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(validity_start_filters = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(validity_start_filters = ["not_a_dict"])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(validity_start_filters = [{"date": "2018-06-05T02:07:10"}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(validity_start_filters = [{"op": ">="}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(validity_start_filters = [{"date": "2018-06-05T02:07:10", "op": ">=", "not_a_valid_key": "not_a_valid_value"}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(validity_start_filters = [{"date": "2018-06-05T02:07:10", "op": "not_a_valid_operator"}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(validity_start_filters = [{"date": "not_a_valid_date", "op": "=="}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(ingestion_duration_filters = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(ingestion_duration_filters = ["not_a_dict"])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(ingestion_duration_filters = [{"float": 10}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(ingestion_duration_filters = [{"op": ">="}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(ingestion_duration_filters = [{"float": 10, "op": "<", "not_a_valid_key": "not_a_valid_value"}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(ingestion_duration_filters = [{"float": 10, "op": "not_a_valid_operator"}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(ingestion_duration_filters = [{"float": "not_a_valid_float", "op": "=="}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(processor_version_filters = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(processor_version_filters = ["not_a_dict"])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(processor_version_filters = [{"filter": "0.0"}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(processor_version_filters = [{"op": ">"}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(processor_version_filters = [{"filter": "0.0", "op": ">", "not_a_valid_key": "not_a_valid_value"}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(processor_version_filters = [{"filter": "0.0", "op": "not_a_valid_operator"}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(processor_version_filters = [{"filter": ["not_a_valid_string"], "op": "=="}])
        except InputError:
            result = True
        # end try

        assert result == True

    def test_query_gauge(self):
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
                    "gauge": {
                        "name": "GAUGE",
                        "system": "SYSTEM",
                        "insertion_type": "SIMPLE_UPDATE"
                    },
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36"

                }]
            },
{
                "mode": "insert",
            "dim_signature": {"name": "dim_signature2",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "gauge": {
                        "name": "GAUGE2",
                        "system": "SYSTEM2",
                        "insertion_type": "SIMPLE_UPDATE"
                    },
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36"

                }]
            }]}
        self.engine_eboa.treat_data(data)

        dim_signature1 = self.query.get_dim_signatures()

        gauge1 = self.query.get_gauges(dim_signature_uuids = {"filter": [dim_signature1[0].dim_signature_uuid], "op": "in"})

        assert len(gauge1) == 1

        gauges = self.query.get_gauges()

        assert len(gauges) == 2

        gauge = self.query.get_gauges(gauge_uuids = {"filter": [gauge1[0].gauge_uuid], "op": "in"})

        assert len(gauge) == 1

        gauge = self.query.get_gauges(names = {"filter": [data["operations"][0]["events"][0]["gauge"]["name"]], "op": "in"})

        assert len(gauge) == 1

        gauge_name = data["operations"][0]["events"][0]["gauge"]["name"]
        gauge = self.query.get_gauges(names = {"filter": gauge_name, "op": "like"})

        assert len(gauge) == 1

        gauge = self.query.get_gauges(systems = {"filter": [data["operations"][0]["events"][0]["gauge"]["system"]], "op": "in"})

        assert len(gauge) == 1

        gauge_system = data["operations"][0]["events"][0]["gauge"]["system"]
        gauge = self.query.get_gauges(systems = {"filter": gauge_system, "op": "like"})

        assert len(gauge) == 1

        dim_sig_name = data["operations"][0]["dim_signature"]["name"]
        gauge = self.query.get_gauges(dim_signatures = {"filter": dim_sig_name, "op": "like"})

        assert len(gauge) == 1

        gauge = self.query.get_gauges(dim_signature_uuids = {"filter": [dim_signature1[0].dim_signature_uuid], "op": "in"},
                                        gauge_uuids = {"filter": [gauge1[0].gauge_uuid], "op": "in"},
                                        names = {"filter": [data["operations"][0]["events"][0]["gauge"]["name"]], "op": "in"},
                                        systems = {"filter": gauge_system, "op": "like"},
        dim_signatures = {"filter": dim_sig_name, "op": "like"})

        assert len(gauge) == 1

    def test_wrong_inputs_query_gauge(self):

        result = False
        try:
            self.query.get_gauges(dim_signature_uuids = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_gauges(gauge_uuids = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_gauges(names = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_gauges(names = ["not_a_valid_string"])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_gauges(systems = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_gauges(systems = ["not_a_valid_string"])
        except InputError:
            result = True
        # end try

        assert result == True

    def test_query_event(self):
        data = {"operations": [{
                "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-06T08:07:36"},
                "events": [{
                    "key": "EVENT_KEY",
                    "explicit_reference": "EXPLICIT_REFERENCE",
                    "gauge": {
                        "name": "GAUGE",
                        "system": "SYSTEM",
                        "insertion_type": "SIMPLE_UPDATE"
                    },
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "values": [{"name": "VALUES",
                                "type": "object",
                                "values": [
                                    {"type": "text",
                                     "name": "TEXT",
                                     "value": "TEXT"},
                                    {"type": "text",
                                     "name": "TEXT2",
                                     "value": "TEXT2"},
                                    {"type": "boolean",
                                     "name": "BOOLEAN",
                                     "value": "true"}]}]
                },{
                    "explicit_reference": "EXPLICIT_REFERENCE2",
                    "gauge": {
                        "name": "GAUGE2",
                        "system": "SYSTEM2",
                        "insertion_type": "SIMPLE_UPDATE"
                    },
                    "start": "2018-06-06T02:07:03",
                    "stop": "2018-06-06T08:07:36"
                    }]
            }]}
        self.engine_eboa.treat_data(data)

        source1 = self.query.get_sources()

        event = self.query.get_events(source_uuids = {"filter": [source1[0].source_uuid], "op": "in"})

        assert len(event) == 2

        explicit_ref1 = self.query.get_explicit_refs(explicit_refs = {"filter": "EXPLICIT_REFERENCE", "op": "like"})

        event = self.query.get_events(explicit_ref_uuids = {"filter": [explicit_ref1[0].explicit_ref_uuid], "op": "in"})

        assert len(event) == 1

        gauge1 = self.query.get_gauges(names = {"filter": "GAUGE", "op": "like"})

        event1 = self.query.get_events(gauge_uuids = {"filter": [gauge1[0].gauge_uuid], "op": "in"})

        assert len(event1) == 1

        events = self.query.get_events()

        assert len(events) == 2

        event = self.query.get_events(event_uuids = {"filter": [event1[0].event_uuid], "op": "in"})

        assert len(event) == 1

        event = self.query.get_events(start_filters = [{"date": "2018-06-05T02:07:03", "op": "=="}])

        assert len(event) == 1

        event = self.query.get_events(stop_filters = [{"date": "2018-06-05T08:07:36", "op": "=="}])

        assert len(event) == 1

        event = self.query.get_events(ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}])

        assert len(event) == 2

        event = self.query.get_events(sources = {"filter": [data["operations"][0]["source"]["name"]], "op": "in"})

        assert len(event) == 2

        explicit_ref_name = data["operations"][0]["events"][0]["explicit_reference"][0:4] + "%"
        event = self.query.get_events(explicit_refs = {"filter": explicit_ref_name, "op": "like"})

        assert len(event) == 2

        gauge_name = data["operations"][0]["events"][0]["gauge"]["name"]
        event = self.query.get_events(gauge_names = {"filter": [gauge_name], "op": "in"})

        assert len(event) == 1

        gauge_system = data["operations"][0]["events"][0]["gauge"]["system"][0:4] + "%"
        event = self.query.get_events(gauge_systems = {"filter": gauge_system, "op": "like"})

        assert len(event) == 2

        event = self.query.get_events(keys = {"filter": "EVENT_KEY", "op": "like"})

        assert len(event) == 1

        event = self.query.get_events(value_filters = [{"name": {
            "op": "like",
            "filter": "TEXT"},
                                                        "type": "text",
                                                        "value": {
                                                            "op": "in",
                                                            "filter": ["TEXT", "TEXT2"]}
                                                    },
                                                       {"name": {
                                                           "op": "like",
                                                           "filter": "BOOLEAN"},
                                                        "type": "boolean",
                                                        "value": {
                                                            "op": "==",
                                                            "filter": "true"}
                                                       }])

        assert len(event) == 1

        event = self.query.get_events(source_uuids = {"filter": [source1[0].source_uuid], "op": "in"},
                                      explicit_ref_uuids = {"filter": [explicit_ref1[0].explicit_ref_uuid], "op": "in"},
                                      gauge_uuids = {"filter": [gauge1[0].gauge_uuid], "op": "in"},
                                      event_uuids = {"filter": [event1[0].event_uuid], "op": "in"},
                                      start_filters = [{"date": "2018-06-05T02:07:03", "op": "=="}],
                                      stop_filters = [{"date": "2018-06-05T08:07:36", "op": "=="}],
                                      ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}],
                                      sources = {"filter": [data["operations"][0]["source"]["name"]], "op": "in"},
                                      explicit_refs = {"filter": explicit_ref_name, "op": "like"},
                                      gauge_names = {"filter": gauge_name, "op": "like"},
                                      gauge_systems = {"filter": gauge_system, "op": "like"},
                                      keys = {"filter": "EVENT_KEY", "op": "like"},
                                      value_filters = [{"name": {
                                          "op": "like",
                                          "filter": "TEXT"},
                                                        "type": "text",
                                                        "value": {
                                                            "op": "in",
                                                            "filter": ["TEXT", "TEXT2"]}
                                                    },
                                                       {"name": {
                                                           "op": "like",
                                                           "filter": "BOOLEAN"},
                                                        "type": "boolean",
                                                        "value": {
                                                            "op": "==",
                                                            "filter": "true"}
                                                       }])

        assert len(event) == 1

    def test_wrong_inputs_query_event(self):

        result = False
        try:
            self.query.get_events(source_uuids = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events(event_uuids = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events(explicit_ref_uuids = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events(gauge_uuids = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events(start_filters = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events(stop_filters = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events(ingestion_time_filters = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events(sources = "not_a_dict")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events(sources = {"op": "not_a_valid_op"})
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events(explicit_refs = "not_a_dict")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events(gauge_names = "not_a_dict")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events(gauge_systems = "not_a_dict")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events(value_filters = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events(value_filters = ["not_a_dict"])
        except InputError:
            result = True
        # end try

        assert result == True

    def test_query_event_key(self):
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
                    "key": "EVENT_KEY",
                    "gauge": {
                        "name": "GAUGE",
                        "system": "SYSTEM",
                        "insertion_type": "SIMPLE_UPDATE"
                    },
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36"

                }]
            }]}
        self.engine_eboa.treat_data(data)

        dim_signature1 = self.query.get_dim_signatures()

        event_key = self.query.get_event_keys(dim_signature_uuids = {"filter": [dim_signature1[0].dim_signature_uuid], "op": "in"})

        assert len(event_key) == 1

        event1 = self.query.get_events()

        assert len(event1) == 1

        event_key = self.query.get_events(event_uuids = {"filter": [event1[0].event_uuid], "op": "in"})

        assert len(event_key) == 1

        event_key = self.query.get_event_keys(keys = {"filter": [data["operations"][0]["events"][0]["key"]], "op": "in"})

        assert len(event_key) == 1

        event_key_name = data["operations"][0]["events"][0]["key"][0:4] + "%"
        event_key = self.query.get_event_keys(keys = {"filter": event_key_name, "op": "like"})

        assert len(event_key) == 1

        event_key = self.query.get_event_keys(dim_signature_uuids = {"filter": [dim_signature1[0].dim_signature_uuid], "op": "in"},
                                              event_uuids = {"filter": [event1[0].event_uuid], "op": "in"},
                                              keys = {"filter": [data["operations"][0]["events"][0]["key"]], "op": "in"})

        assert len(event_key) == 1

    def test_wrong_inputs_query_event_key(self):

        result = False
        try:
            self.query.get_event_keys(dim_signature_uuids = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_event_keys(event_uuids = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_event_keys(keys = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_event_keys(keys = ["not_a_valid_string"])
        except InputError:
            result = True
        # end try

        assert result == True

    def test_query_event_link(self):
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
                    "link_ref": "EVENT_LINK1",
                    "links": [{
                        "link": "EVENT_LINK2",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK1"
                    }],
                    "gauge": {
                        "name": "GAUGE_NAME",
                        "system": "GAUGE_SYSTEM",
                        "insertion_type": "SIMPLE_UPDATE"
                    },
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36"
                },
                {
                    "link_ref": "EVENT_LINK2",
                    "links": [{
                        "link": "EVENT_LINK1",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK2"
                    }],
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T04:07:03",
                    "stop": "2018-06-05T08:07:36"
                }]
            }]}
        self.engine_eboa.treat_data(data)

        event1 = self.query.get_events(start_filters = [{"date": "2018-06-05T02:07:03", "op": "=="}])

        event2 = self.query.get_events(start_filters = [{"date": "2018-06-05T04:07:03", "op": "=="}])

        event_links = self.query.get_event_links(link_names = {"filter": "EVENT_LINK%", "op": "like"})

        assert len(event_links) == 2

        event_link = self.query.get_event_links(event_uuid_links = {"filter": [event1[0].event_uuid], "op": "in"})

        assert len(event_link) == 1

        event_link = self.query.get_event_links(event_uuids = {"filter": [event1[0].event_uuid], "op": "in"})

        assert len(event_link) == 1

        event_link = self.query.get_event_links(event_uuid_links = {"filter": [event2[0].event_uuid], "op": "in"})

        assert len(event_link) == 1

        event_link = self.query.get_event_links(event_uuids = {"filter": [event2[0].event_uuid], "op": "in"})

        assert len(event_link) == 1

        event_links = self.query.get_event_links()

        assert len(event_links) == 2

        event_link = self.query.get_event_links(event_uuids = {"filter": [event1[0].event_uuid], "op": "in"},
                                                event_uuid_links = {"filter": [event2[0].event_uuid], "op": "in"},
                                                link_names = {"filter": "EVENT_LINK%", "op": "like"})

        assert len(event_link) == 1

        event_link = self.query.get_event_links(event_uuids = {"filter": [event2[0].event_uuid], "op": "in"},
                                                event_uuid_links = {"filter": [event1[0].event_uuid], "op": "in"},
                                                link_names = {"filter": ["EVENT_LINK1"], "op": "in"})

        assert len(event_link) == 1

    def test_wrong_inputs_query_event_link(self):

        result = False
        try:
            self.query.get_event_links(event_uuids = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_event_links(event_uuid_links = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_event_links(link_names = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_event_links(link_names = ["not_a_valid_string"])
        except InputError:
            result = True
        # end try

        assert result == True

    def test_query_linked_events(self):
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
                    "explicit_reference": "EXPLICIT_REFERENCE",
                    "link_ref": "EVENT_LINK1",
                    "links": [{
                        "link": "EVENT_LINK2",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK1"
                    }],
                    "gauge": {
                        "name": "GAUGE_NAME",
                        "system": "GAUGE_SYSTEM",
                        "insertion_type": "SIMPLE_UPDATE"
                    },
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36"
                },
                {
                    "explicit_reference": "EXPLICIT_REFERENCE",
                    "link_ref": "EVENT_LINK2",
                    "links": [{
                        "link": "EVENT_LINK1",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK2"
                    }],
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T04:07:03",
                    "stop": "2018-06-05T08:07:36"
                }]
            }]}
        self.engine_eboa.treat_data(data)

        events = self.query.get_linked_events()

        assert len(events["linked_events"]) == 2
        assert len(events["prime_events"]) == 2

        source1 = self.query.get_sources()
        explicit_ref1 = self.query.get_explicit_refs()
        gauge1 = self.query.get_gauges()
        event1 = self.query.get_events(start_filters = [{"date": "2018-06-05T02:07:03", "op": "=="}])
        event2 = self.query.get_events(start_filters = [{"date": "2018-06-05T04:07:03", "op": "=="}])


        events = self.query.get_linked_events(source_uuids = {"filter": [source1[0].source_uuid], "op": "in"},
                                                   explicit_ref_uuids = {"filter": [explicit_ref1[0].explicit_ref_uuid], "op": "in"},
                                                   gauge_uuids = {"filter": [gauge1[0].gauge_uuid], "op": "in"},
                                                   event_uuids = {"filter": [event1[0].event_uuid], "op": "in"},
                                                   start_filters = [{"date": "2018-06-05T02:07:03", "op": "=="}],
                                                   stop_filters = [{"date": "2018-06-05T08:07:36", "op": "=="}],
                                                   link_names = {"filter": ["EVENT_LINK1"], "op": "in"})

        assert len(events["linked_events"]) == 1
        assert len(events["prime_events"]) == 1

        events = self.query.get_linked_events(source_uuids = {"filter": [source1[0].source_uuid], "op": "in"},
                                                   explicit_ref_uuids = {"filter": [explicit_ref1[0].explicit_ref_uuid], "op": "in"},
                                                   gauge_uuids = {"filter": [gauge1[0].gauge_uuid], "op": "in"},
                                                   event_uuids = {"filter": [event2[0].event_uuid], "op": "in"},
                                                   start_filters = [{"date": "2018-06-05T04:07:03", "op": "=="}],
                                                   stop_filters = [{"date": "2018-06-05T08:07:36", "op": "=="}],
                                                   link_names = {"filter": "EVENT_LINK%", "op": "like"})

        assert len(events["linked_events"]) == 1
        assert len(events["prime_events"]) == 1

        events = self.query.get_linked_events(event_uuids = {"filter": [event2[0].event_uuid], "op": "in"})

        assert len(events["linked_events"]) == 1
        assert len(events["prime_events"]) == 1

        events = self.query.get_linked_events(back_ref = True)

        assert len(events["linked_events"]) == 2
        assert len(events["prime_events"]) == 2
        assert len(events["events_linking"]) == 2

    def test_wrong_inputs_query_linked_events(self):

        result = False
        try:
            self.query.get_linked_events(event_uuids = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

    def test_query_linked_event_join_to_other_tables(self):
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
                    "explicit_reference": "EXPLICIT_REFERENCE",
                    "link_ref": "EVENT_LINK1",
                    "links": [{
                        "link": "EVENT_LINK2",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK1"
                    }],
                    "gauge": {
                        "name": "GAUGE_NAME",
                        "system": "GAUGE_SYSTEM",
                        "insertion_type": "SIMPLE_UPDATE"
                    },
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36"
                },
                {
                    "explicit_reference": "EXPLICIT_REFERENCE",
                    "link_ref": "EVENT_LINK2",
                    "links": [{
                        "link": "EVENT_LINK1",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK2"
                    }],
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T04:07:03",
                    "stop": "2018-06-05T08:07:36"
                }]
            }]}
        self.engine_eboa.treat_data(data)

        events = self.query.get_linked_events()

        assert len(events["linked_events"]) == 2
        assert len(events["prime_events"]) == 2

        source1 = self.query.get_sources()
        explicit_ref1 = self.query.get_explicit_refs()
        gauge1 = self.query.get_gauges()
        event1 = self.query.get_events(start_filters = [{"date": "2018-06-05T02:07:03", "op": "=="}])
        event2 = self.query.get_events(start_filters = [{"date": "2018-06-05T04:07:03", "op": "=="}])

        events = self.query.get_linked_events(sources = {"filter": [data["operations"][0]["source"]["name"]], "op": "in"},
                                                     explicit_refs = {"filter": [data["operations"][0]["events"][0]["explicit_reference"]], "op": "in"},
                                                     gauge_names = {"filter": "%", "op": "like"},
                                                     gauge_systems = {"filter": [data["operations"][0]["events"][0]["gauge"]["system"]], "op": "in"},
                                                     start_filters = [{"date": "2018-06-05T02:07:03", "op": "=="}],
                                                     stop_filters = [{"date": "2018-06-05T08:07:36", "op": "=="}],
                                                     link_names = {"filter": ["EVENT_LINK1"], "op": "in"})
        assert len(events["linked_events"]) == 1
        assert len(events["prime_events"]) == 1

        events = self.query.get_linked_events(sources = {"filter": [data["operations"][0]["source"]["name"]], "op": "in"},
                                                     explicit_refs = {"filter": [data["operations"][0]["events"][1]["explicit_reference"]], "op": "in"},
                                                     gauge_names = {"filter": "%", "op": "like"},
                                                     gauge_systems = {"filter": [data["operations"][0]["events"][1]["gauge"]["system"]], "op": "in"},
                                                     start_filters = [{"date": "2018-06-05T04:07:03", "op": "=="}],
                                                     stop_filters = [{"date": "2018-06-05T08:07:36", "op": "=="}],
                                                     link_names = {"filter": "EVENT_LINK%", "op": "like"})

        assert len(events["linked_events"]) == 1
        assert len(events["prime_events"]) == 1

        events = self.query.get_linked_events(back_ref = True)

        assert len(events["linked_events"]) == 2
        assert len(events["prime_events"]) == 2
        assert len(events["events_linking"]) == 2

    def test_query_linked_event_details(self):
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
                    "explicit_reference": "EXPLICIT_REFERENCE1",
                    "link_ref": "EVENT_LINK1",
                    "links": [{
                        "link": "EVENT_LINK2",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK1"
                    }],
                    "gauge": {
                        "name": "GAUGE_NAME",
                        "system": "GAUGE_SYSTEM",
                        "insertion_type": "SIMPLE_UPDATE"
                    },
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36"
                },
                {
                    "explicit_reference": "EXPLICIT_REFERENCE2",
                    "link_ref": "EVENT_LINK2",
                    "links": [{
                        "link": "EVENT_LINK1",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK2"
                    }],
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T04:07:03",
                    "stop": "2018-06-05T08:07:36"
                }]
            }]}
        self.engine_eboa.treat_data(data)

        event1 = self.query.get_events(explicit_refs = {"filter": ["EXPLICIT_REFERENCE1"], "op": "in"})

        events = self.query.get_linked_events_details(event_uuid = event1[0].event_uuid, back_ref = True)

        assert len(events["linked_events"]) == 1

        assert events["linked_events"][0]["link_name"] == "EVENT_LINK1"

        assert len(events["prime_events"]) == 1

        assert len(events["events_linking"]) == 1
        assert events["events_linking"][0]["link_name"] == "EVENT_LINK2"

    def test_wrong_inputs_query_linked_event_details(self):

        result = False
        try:
            self.query.get_linked_events_details(event_uuid = "not_a_UUID")
        except InputError:
            result = True
        # end try

        assert result == True


    def test_query_linking_events(self):
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
                    "explicit_reference": "EXPLICIT_REFERENCE",
                    "link_ref": "EVENT_LINK1",
                    "links": [{
                        "link": "EVENT_LINK2",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK1"
                    }],
                    "gauge": {
                        "name": "GAUGE_NAME",
                        "system": "GAUGE_SYSTEM",
                        "insertion_type": "SIMPLE_UPDATE"
                    },
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36"
                },
                {
                    "explicit_reference": "EXPLICIT_REFERENCE",
                    "link_ref": "EVENT_LINK2",
                    "links": [{
                        "link": "EVENT_LINK1",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK2"
                    }],
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T04:07:03",
                    "stop": "2018-06-05T08:07:36"
                }]
            }]}
        self.engine_eboa.treat_data(data)

        events = self.query.get_linking_events()

        assert len(events["linking_events"]) == 2
        assert len(events["prime_events"]) == 2

        source1 = self.query.get_sources()
        explicit_ref1 = self.query.get_explicit_refs()
        gauge1 = self.query.get_gauges()
        event1 = self.query.get_events(start_filters = [{"date": "2018-06-05T02:07:03", "op": "=="}])
        event2 = self.query.get_events(start_filters = [{"date": "2018-06-05T04:07:03", "op": "=="}])


        events = self.query.get_linking_events(source_uuids = {"filter": [source1[0].source_uuid], "op": "in"},
                                                   explicit_ref_uuids = {"filter": [explicit_ref1[0].explicit_ref_uuid], "op": "in"},
                                                   gauge_uuids = {"filter": [gauge1[0].gauge_uuid], "op": "in"},
                                                   event_uuids = {"filter": [event1[0].event_uuid], "op": "in"},
                                                   start_filters = [{"date": "2018-06-05T02:07:03", "op": "=="}],
                                                   stop_filters = [{"date": "2018-06-05T08:07:36", "op": "=="}],
                                                   link_names = {"filter": ["EVENT_LINK2"], "op": "in"})

        assert len(events["linking_events"]) == 1
        assert events["linking_events"][0].event_uuid == event2[0].event_uuid
        assert len(events["prime_events"]) == 1

        events = self.query.get_linking_events(source_uuids = {"filter": [source1[0].source_uuid], "op": "in"},
                                                   explicit_ref_uuids = {"filter": [explicit_ref1[0].explicit_ref_uuid], "op": "in"},
                                                   gauge_uuids = {"filter": [gauge1[0].gauge_uuid], "op": "in"},
                                                   event_uuids = {"filter": [event2[0].event_uuid], "op": "in"},
                                                   start_filters = [{"date": "2018-06-05T04:07:03", "op": "=="}],
                                                   stop_filters = [{"date": "2018-06-05T08:07:36", "op": "=="}],
                                                   link_names = {"filter": "EVENT_LINK%", "op": "like"})

        assert len(events["linking_events"]) == 1
        assert events["linking_events"][0].event_uuid == event1[0].event_uuid
        assert len(events["prime_events"]) == 1

        events = self.query.get_linking_events(event_uuids = {"filter": [event2[0].event_uuid], "op": "in"},
                                              link_names = {"filter": ["EVENT_LINK1"], "op": "in"})

        assert len(events["linking_events"]) == 1
        assert events["linking_events"][0].event_uuid == event1[0].event_uuid
        assert len(events["prime_events"]) == 1

        events = self.query.get_linking_events(back_ref = True)

        assert len(events["linked_events"]) == 2
        assert len(events["prime_events"]) == 2
        assert len(events["linking_events"]) == 2

    def test_wrong_inputs_query_linking_events(self):

        result = False
        try:
            self.query.get_linking_events(event_uuids = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

    def test_query_annotation_cnf(self):
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
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {"name": "ANNOTATION_CNF_NAME",
                                   "system": "ANNOTATION_CNF_SYSTEM"},
                }]
        },
{
                "mode": "insert",
            "dim_signature": {"name": "dim_signature2",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE2",
                "annotation_cnf": {"name": "ANNOTATION_CNF_NAME2",
                                   "system": "ANNOTATION_CNF_SYSTEM2"},
                }]
            }]}
        self.engine_eboa.treat_data(data)

        dim_signature1 = self.query.get_dim_signatures()

        annotation_cnf = self.query.get_annotation_cnfs(dim_signature_uuids = {"filter": [dim_signature1[0].dim_signature_uuid], "op": "in"})

        assert len(annotation_cnf) == 1

        annotation_cnf = self.query.get_annotation_cnfs(dim_signatures = {"filter": [data["operations"][0]["dim_signature"]["name"]], "op": "in"})

        assert len(annotation_cnf) == 1

        annotation_cnf1 = self.query.get_annotation_cnfs()

        assert len(annotation_cnf1) == 2

        annotation_cnf = self.query.get_annotation_cnfs(annotation_cnf_uuids = {"filter": [annotation_cnf1[0].annotation_cnf_uuid], "op": "in"})

        assert len(annotation_cnf) == 1

        annotation_cnf = self.query.get_annotation_cnfs(names = {"filter": [data["operations"][0]["annotations"][0]["annotation_cnf"]["name"]], "op": "in"})

        assert len(annotation_cnf) == 1

        annotation_cnf_name = data["operations"][0]["annotations"][0]["annotation_cnf"]["name"][0:4] + "%"

        annotation_cnf = self.query.get_annotation_cnfs(systems = {"filter": [data["operations"][0]["annotations"][0]["annotation_cnf"]["system"]], "op": "in"})

        assert len(annotation_cnf) == 1

        annotation_cnf_system = data["operations"][0]["annotations"][0]["annotation_cnf"]["system"][0:4] + "%"

        annotation_cnf = self.query.get_annotation_cnfs(dim_signature_uuids = {"filter": [dim_signature1[0].dim_signature_uuid], "op": "in"},
                                        annotation_cnf_uuids = {"filter": [annotation_cnf1[0].annotation_cnf_uuid], "op": "in"},
                                        names = {"filter": annotation_cnf_name, "op": "like"},
                                                        systems = {"filter": [data["operations"][0]["annotations"][0]["annotation_cnf"]["system"]], "op": "in"},
        dim_signatures = {"filter": [data["operations"][0]["dim_signature"]["name"]], "op": "in"})

        assert len(annotation_cnf) == 1

    def test_query_annotation(self):
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
                "annotations": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_ANNOTATION",
                    "annotation_cnf": {"name": "NAME",
                                       "system": "SYSTEM"},
                            "values": [{"name": "VALUES",
                                       "type": "object",
                                       "values": [
                                           {"type": "text",
                                            "name": "TEXT",
                                            "value": "TEXT"},
                                           {"type": "boolean",
                                            "name": "BOOLEAN",
                                            "value": "true"}]
                                    }]
                }]
        }]}
        self.engine_eboa.treat_data(data)

        data2 = {"operations": [{
                "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "annotations": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_ANNOTATION2",
                    "annotation_cnf": {"name": "NAME2",
                                       "system": "SYSTEM2"},
                            "values": [{"name": "VALUES2",
                                       "type": "object",
                                       "values": [
                                           {"type": "text",
                                            "name": "TEXT2",
                                            "value": "TEXT"},
                                           {"type": "boolean",
                                            "name": "BOOLEAN2",
                                            "value": "true"}]
                                    }]
                }]
        }]}
        self.engine_eboa.treat_data(data2)

        source1 = self.query.get_sources(names = {"filter": "source.xml", "op": "like"})

        annotation = self.query.get_annotations(source_uuids = {"filter": [source1[0].source_uuid], "op": "in"})

        assert len(annotation) == 1

        source_name = "source.%"
        annotation = self.query.get_annotations(sources = {"filter": source_name, "op": "like"})

        assert len(annotation) == 1

        annotation = self.query.get_annotations(explicit_refs = {"filter": [data["operations"][0]["annotations"][0]["explicit_reference"]], "op": "in"})

        assert len(annotation) == 1

        explicit_ref1 = self.query.get_explicit_refs(explicit_refs = {"filter": "EXPLICIT_REFERENCE_ANNOTATION", "op": "like"})

        annotation = self.query.get_annotations(explicit_ref_uuids = {"filter": [explicit_ref1[0].explicit_ref_uuid], "op": "in"})

        assert len(annotation) == 1

        annotation_cnf1 = self.query.get_annotation_cnfs(names = {"filter": "NAME", "op": "like"})

        annotation = self.query.get_annotations(annotation_cnf_uuids = {"filter": [annotation_cnf1[0].annotation_cnf_uuid], "op": "in"})

        assert len(annotation) == 1

        annotation1 = self.query.get_annotations(explicit_refs = {"filter": "EXPLICIT_REFERENCE_ANNOTATION", "op": "like"})

        assert len(annotation1) == 1

        annotation = self.query.get_annotations(annotation_uuids = {"filter": [annotation1[0].annotation_uuid], "op": "in"})

        assert len(annotation) == 1

        annotation = self.query.get_annotations(ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}])

        assert len(annotation) == 2

        annotation = self.query.get_annotations(value_filters = [{"name": {
            "op": "like",
            "filter": "TEXT"},
                                                        "type": "text",
                                                        "value": {
                                                            "op": "in",
                                                            "filter": ["TEXT", "TEXT2"]}
                                                    },
                                                       {"name": {
                                                           "op": "like",
                                                           "filter": "BOOLEAN"},
                                                        "type": "boolean",
                                                        "value": {
                                                            "op": "==",
                                                            "filter": "true"}
                                                       }])

        assert len(annotation) == 1

        annotation = self.query.get_annotations(source_uuids = {"filter": [source1[0].source_uuid], "op": "in"},
                                                sources = {"filter": source_name, "op": "like"},
                                                explicit_refs = {"filter": [data["operations"][0]["annotations"][0]["explicit_reference"]], "op": "in"},
                                      explicit_ref_uuids = {"filter": [explicit_ref1[0].explicit_ref_uuid], "op": "in"},
                                      annotation_cnf_uuids = {"filter": [annotation_cnf1[0].annotation_cnf_uuid], "op": "in"},
                                      annotation_uuids = {"filter": [annotation1[0].annotation_uuid], "op": "in"},
                                      ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}],
                                      value_filters = [{"name": {
                                          "op": "like",
                                          "filter": "TEXT"},
                                                        "type": "text",
                                                        "value": {
                                                            "op": "in",
                                                            "filter": ["TEXT", "TEXT2"]}
                                                    },
                                                       {"name": {
                                                           "op": "like",
                                                           "filter": "BOOLEAN"},
                                                        "type": "boolean",
                                                        "value": {
                                                            "op": "==",
                                                            "filter": "true"}
                                                       }])

        assert len(annotation) == 1

    def test_query_explicit_ref(self):
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
            "explicit_references": [{
                "name": "EXPLICIT_REFERENCE",
                "group": "EXPL_GROUP"
            }],
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE",
                    "link_ref": "EVENT_LINK1",
                    "gauge": {
                        "name": "GAUGE_NAME",
                        "system": "GAUGE_SYSTEM",
                        "insertion_type": "SIMPLE_UPDATE"
                    },
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
                                            "value": "true"}]
                                    }]
                }],
                "annotations": [{
                    "explicit_reference": "EXPLICIT_REFERENCE",
                    "annotation_cnf": {"name": "NAME",
                                       "system": "SYSTEM"},
                            "values": [{"name": "VALUES",
                                       "type": "object",
                                       "values": [
                                           {"type": "text",
                                            "name": "TEXT",
                                            "value": "TEXT"},
                                           {"type": "boolean",
                                            "name": "BOOLEAN",
                                            "value": "true"}]
                                    }]
                }]
            }]}
        self.engine_eboa.treat_data(data)

        self.engine_eboa.treat_data(data)

        expl_group1 = self.query.get_explicit_refs_groups()

        explicit_ref1 = self.query.get_explicit_refs(group_ids = {"filter": [expl_group1[0].expl_ref_cnf_uuid], "op": "in"})

        assert len(explicit_ref1) == 1

        explicit_refs = self.query.get_explicit_refs()

        assert len(explicit_refs) == 1

        explicit_reference = self.query.get_explicit_refs(explicit_ref_uuids = {"filter": [explicit_ref1[0].explicit_ref_uuid], "op": "in"})

        assert len(explicit_reference) == 1

        explicit_reference = self.query.get_explicit_refs(explicit_refs = {"filter": ["EXPLICIT_REFERENCE"], "op": "in"})

        assert len(explicit_reference) == 1

        explicit_reference = self.query.get_explicit_refs(explicit_refs = {"filter": "EXPLICIT_REFERENCE_L%", "op": "notlike"})

        assert len(explicit_reference) == 1


        explicit_reference = self.query.get_explicit_refs(explicit_ref_ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}])

        assert len(explicit_reference) == 1

        explicit_reference = self.query.get_explicit_refs(group_ids = {"filter": [expl_group1[0].expl_ref_cnf_uuid], "op": "in"},
                                      explicit_ref_uuids = {"filter": [explicit_ref1[0].explicit_ref_uuid], "op": "in"},
                                      explicit_refs = {"filter": "EXPLICIT%", "op": "like"},
                                      explicit_ref_ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}])

        assert len(explicit_reference) == 1

        explicit_references = self.query.get_explicit_refs(explicit_refs = {"filter": [data["operations"][0]["annotations"][0]["explicit_reference"]], "op": "in"},
                                                                      gauge_names = {"filter": "GAUGE%", "op": "like"},
                                                                      gauge_systems = {"filter": [data["operations"][0]["events"][0]["gauge"]["system"]], "op": "in"},
                                                                      start_filters = [{"date": "2018-06-05T02:07:03", "op": "=="}],
                                                                      stop_filters = [{"date": "2018-06-05T08:07:36", "op": "=="}],
                                                                      annotation_cnf_names = {"filter": [data["operations"][0]["annotations"][0]["annotation_cnf"]["name"]], "op": "in"},
                                                                      annotation_cnf_systems = {"filter": "SYS%", "op": "like"},
                                                                      explicit_ref_ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}],
                                                                      event_value_filters = [{"name": {
                                                                          "op": "like",
                                                                          "filter": "TEXT"},
                                                                                              "type": "text",
                                                                                              "value": {
                                                                                                  "op": "in",
                                                                                                  "filter": ["TEXT", "TEXT2"]}
                                                                                          },
                                                                                             {"name": {
                                                                                                 "op": "like",
                                                                                                 "filter": "BOOLEAN"},
                                                                                              "type": "boolean",
                                                                                              "value": {
                                                                                                  "op": "==",
                                                                                                  "filter": "true"}
                                                                                          }],
                                                                      annotation_value_filters = [{"name": {
                                                                          "op": "like",
                                                                          "filter": "TEXT"},
                                                                                                   "type": "text",
                                                                                                   "value": {
                                                                                                       "op": "in",
                                                                                                       "filter": ["TEXT", "TEXT2"]}
                                                                                               },
                                                                                                  {"name": {
                                                                                                      "op": "like",
                                                                                                      "filter": "BOOLEAN"},
                                                                                                   "type": "boolean",
                                                                                                   "value": {
                                                                                                       "op": "==",
                                                                                                       "filter": "true"}
                                                                                               }],
                                                                      groups = {"filter": "EXPL_%", "op": "like"})

        assert len(explicit_references) == 1

    def test_query_explicit_ref_link(self):
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
            "explicit_references": [{
                "name": "EXPLICIT_REFERENCE",
                "links": [{"name": "LINK_NAME",
                           "link": "EXPLICIT_REFERENCE_EVENT",
                           "back_ref": "LINK_NAME"}]
            }],
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                    "gauge": {
                        "name": "GAUGE_NAME",
                        "system": "GAUGE_SYSTEM",
                        "insertion_type": "SIMPLE_UPDATE"
                    },
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36"
                }]
            }]}
        self.engine_eboa.treat_data(data)

        explicit_ref1 = self.query.get_explicit_refs(explicit_refs = {"filter": ["EXPLICIT_REFERENCE"], "op": "in"})

        explicit_ref2 = self.query.get_explicit_refs(explicit_refs = {"filter": ["EXPLICIT_REFERENCE_EVENT"], "op": "in"})

        explicit_ref_links = self.query.get_explicit_ref_links(link_names = {"filter": "LINK_N%", "op": "like"})

        assert len(explicit_ref_links) == 2

        explicit_ref_link = self.query.get_explicit_ref_links(explicit_ref_uuids = {"filter": [explicit_ref1[0].explicit_ref_uuid], "op": "in"},
                                                explicit_ref_uuid_links = {"filter": [explicit_ref2[0].explicit_ref_uuid], "op": "in"},
                                                link_names = {"filter": "LINK_N%", "op": "like"})

        assert len(explicit_ref_link) == 1

        explicit_ref_link = self.query.get_explicit_ref_links(explicit_ref_uuids = {"filter": [explicit_ref2[0].explicit_ref_uuid], "op": "in"},
                                                explicit_ref_uuid_links = {"filter": [explicit_ref1[0].explicit_ref_uuid], "op": "in"},
                                                link_names = {"filter": "LINK_N%", "op": "like"})

        assert len(explicit_ref_link) == 1

    def test_query_linked_explicit_ref(self):
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
            "explicit_references": [{
                "group": "EXPL_GROUP",
                "name": "EXPLICIT_REFERENCE",
                "links": [{"name": "LINK_NAME",
                           "link": "EXPLICIT_REFERENCE_EVENT",
                           "back_ref": "LINK_BACK_REF_NAME"}]
            }],
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                    "gauge": {
                        "name": "GAUGE_NAME",
                        "system": "GAUGE_SYSTEM",
                        "insertion_type": "SIMPLE_UPDATE"
                    },
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36"
                }],
                "annotations": [{
                    "explicit_reference": "EXPLICIT_REFERENCE",
                    "annotation_cnf": {"name": "NAME",
                                       "system": "SYSTEM"},
                            "values": [{"name": "VALUES",
                                       "type": "object",
                                       "values": [
                                           {"type": "text",
                                            "name": "TEXT",
                                            "value": "TEXT"},
                                           {"type": "boolean",
                                            "name": "BOOLEAN",
                                            "value": "true"}]
                                    }]
                }]
            }]}
        self.engine_eboa.treat_data(data)

        linked_explicit_refs = self.query.get_linked_explicit_refs(explicit_refs = {"filter": [data["operations"][0]["annotations"][0]["explicit_reference"]], "op": "in"},
                                                                   ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}],
                                                                   groups = {"filter": ["EXPL_GROUP"], "op": "in"}, back_ref = True)

        assert len(linked_explicit_refs["linked_explicit_refs"]) == 1
        assert len(linked_explicit_refs["prime_explicit_refs"]) == 1
        assert len(linked_explicit_refs["explicit_refs_linking"]) == 1

    def test_query_linked_explicit_ref_details(self):
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
            "explicit_references": [{
                "group": "EXPL_GROUP",
                "name": "EXPLICIT_REFERENCE",
                "links": [{"name": "LINK_NAME",
                           "link": "EXPLICIT_REFERENCE_EVENT",
                           "back_ref": "LINK_BACK_REF_NAME"}]
            }],
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                    "gauge": {
                        "name": "GAUGE_NAME",
                        "system": "GAUGE_SYSTEM",
                        "insertion_type": "SIMPLE_UPDATE"
                    },
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36"
                }],
                "annotations": [{
                    "explicit_reference": "EXPLICIT_REFERENCE",
                    "annotation_cnf": {"name": "NAME",
                                       "system": "SYSTEM"},
                            "values": [{"name": "VALUES",
                                       "type": "object",
                                       "values": [
                                           {"type": "text",
                                            "name": "TEXT",
                                            "value": "TEXT"},
                                           {"type": "boolean",
                                            "name": "BOOLEAN",
                                            "value": "true"}]
                                    }]
                }]
            }]}
        self.engine_eboa.treat_data(data)

        explicit_ref1 = self.query.get_explicit_refs(explicit_refs = {"filter": ["EXPLICIT_REFERENCE"], "op": "in"})

        explicit_refs = self.query.get_linked_explicit_refs_details(explicit_ref_uuid = explicit_ref1[0].explicit_ref_uuid, back_ref = True)

        assert len(explicit_refs["linked_explicit_refs"]) == 1

        assert explicit_refs["linked_explicit_refs"][0]["link_name"] == "LINK_NAME"

        assert len(explicit_refs["prime_explicit_refs"]) == 1

        assert len(explicit_refs["explicit_refs_linking"]) == 1
        assert explicit_refs["explicit_refs_linking"][0]["link_name"] == "LINK_BACK_REF_NAME"

    def test_wrong_inputs_query_linked_explicit_ref_details(self):

        result = False
        try:
            self.query.get_linked_explicit_refs_details(explicit_ref_uuid = "not_a_UUID")
        except InputError:
            result = True
        # end try

        assert result == True


    def test_query_explicit_ref_group(self):
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
            "explicit_references": [{
                "name": "EXPLICIT_REFERENCE",
                "group": "EXPL_GROUP",
            }]
        }]}
        self.engine_eboa.treat_data(data)

        group1 = self.query.get_explicit_refs_groups()

        assert len(group1) == 1


        group = self.query.get_explicit_refs_groups(group_ids = {"filter": [group1[0].expl_ref_cnf_uuid],
                                                                 "op": "in"},
                                                    names = {"filter": "EXPL_G%", "op": "like"})

        assert len(group) == 1

    def test_query_event_values(self):
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
                    "gauge": {
                        "name": "GAUGE_NAME",
                        "system": "GAUGE_SYSTEM",
                        "insertion_type": "SIMPLE_UPDATE"
                    },
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
                                     "value": "true"}]}]
                }]
            }]}
        self.engine_eboa.treat_data(data)

        event = self.query.get_events()

        values = self.query.get_event_values()

        assert len(values) == 3

        values = self.query.get_event_values(event_uuids = [event[0].event_uuid])

        assert len(values) == 3

    def test_query_event_values_interface(self):
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
                    "gauge": {
                        "name": "GAUGE_NAME",
                        "system": "GAUGE_SYSTEM",
                        "insertion_type": "SIMPLE_UPDATE"
                    },
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
                                     "value": "true"}]}]
                },{
                    "gauge": {
                        "name": "GAUGE_NAME",
                        "system": "GAUGE_SYSTEM",
                        "insertion_type": "SIMPLE_UPDATE"
                    },
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
                                     "value": "true"}]}]
                }]
            }]}
        self.engine_eboa.treat_data(data)

        events = self.query.get_events()

        values = self.query.get_event_values_interface(value_type = "text", event_uuids = {"filter": [event.event_uuid for event in events], "op": "in"}, value_filters = [{"name": {"op": "like", "filter": "TEXT"}, "type": "text", "value": {"op": "like", "filter": "TEXT"}}])

        assert len(values) == 2

        assert values[0].name == "TEXT"

    def test_wrong_query_event_values(self):

        result = False
        try:
            self.query.get_event_values(event_uuids = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_event_values_type(EventText, event_uuids = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

    def test_query_annotation_values(self):
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
                "annotations": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_ANNOTATION",
                    "annotation_cnf": {"name": "NAME",
                                       "system": "SYSTEM"},
                            "values": [{"name": "VALUES",
                                       "type": "object",
                                       "values": [
                                           {"type": "text",
                                            "name": "TEXT",
                                            "value": "TEXT"},
                                           {"type": "boolean",
                                            "name": "BOOLEAN",
                                            "value": "true"}]
                                    }]
                }]
            }]}
        self.engine_eboa.treat_data(data)

        annotation = self.query.get_annotations()

        values = self.query.get_annotation_values()

        assert len(values) == 3

        values = self.query.get_annotation_values(annotation_uuids = [annotation[0].annotation_uuid])

        assert len(values) == 3

    def test_wrong_query_annotation_values(self):

        result = False
        try:
            self.query.get_annotation_values(annotation_uuids = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_annotation_values_type(AnnotationText, annotation_uuids = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

    def test_query_annotation_values_interface(self):
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
                "annotations": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_ANNOTATION",
                    "annotation_cnf": {"name": "NAME",
                                       "system": "SYSTEM"},
                            "values": [{"name": "VALUES",
                                       "type": "object",
                                       "values": [
                                           {"type": "text",
                                            "name": "TEXT",
                                            "value": "TEXT"},
                                           {"type": "boolean",
                                            "name": "BOOLEAN",
                                            "value": "true"}]
                                    }]
                },{
                    "explicit_reference": "EXPLICIT_REFERENCE_ANNOTATION_2",
                    "annotation_cnf": {"name": "NAME",
                                       "system": "SYSTEM"},
                            "values": [{"name": "VALUES",
                                       "type": "object",
                                       "values": [
                                           {"type": "text",
                                            "name": "TEXT",
                                            "value": "TEXT"},
                                           {"type": "boolean",
                                            "name": "BOOLEAN",
                                            "value": "true"}]
                                    }]
                }]
            }]}
        self.engine_eboa.treat_data(data)

        annotations = self.query.get_annotations()

        values = self.query.get_annotation_values_interface(value_type = "text", annotation_uuids = {"filter": [annotation.annotation_uuid for annotation in annotations], "op": "in"}, value_filters = [{"name": {"op": "like", "filter": "TEXT"}, "type": "text", "value": {"op": "like", "filter": "TEXT"}}])

        assert len(values) == 2

        assert values[0].name == "TEXT"

    def test_use_query_with_existing_session(self):

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
                    "gauge": {
                        "name": "GAUGE",
                        "system": "SYSTEM",
                        "insertion_type": "SIMPLE_UPDATE"
                    },
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36"

                }]
            }]}
        self.engine_eboa.treat_data(data)

        query = Query(session = self.engine_eboa.session)

        events = query.get_events()

        assert len(events) == 1

        query.close_session()

        result = False
        try:
            gauge = events[0].gauge
        except:
            result = True
        # end try

        assert result == True
