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
from eboa.datamodel.dim_processings import DimProcessing, DimProcessingStatus
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
        for table in reversed(Base.metadata.sorted_tables):
            engine.execute(table.delete())

    def test_query_dim_signature(self):
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"}
                                  }
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()

        dim_signature1 = self.query.get_dim_signatures()

        assert len(dim_signature1) == 1

        dim_signature2 = self.query.get_dim_signatures(dim_signature_ids = {"list": [dim_signature1[0].dim_signature_id], "op": "in"})

        assert len(dim_signature2) == 1

        dim_signature3 = self.query.get_dim_signatures(dim_signatures = {"list": [data["dim_signature"]["name"]], "op": "in"})

        assert len(dim_signature3) == 1

        name = data["dim_signature"]["name"][0:7] + "%"
        dim_signature4 = self.query.get_dim_signatures(dim_signature_like = {"str": name, "op": "like"})

        assert len(dim_signature4) == 1

        dim_signature5 = self.query.get_dim_signatures(dim_exec_names = {"list": [data["dim_signature"]["exec"]], "op": "in"})

        assert len(dim_signature5) == 1

        exec_name = data["dim_signature"]["exec"][0:1] + "%"
        dim_signature6 = self.query.get_dim_signatures(dim_exec_name_like = {"str": exec_name, "op": "like"})

        assert len(dim_signature6) == 1

        dim_signature7 = self.query.get_dim_signatures(dim_signature_ids = {"list": [dim_signature1[0].dim_signature_id], "op": "in"},
                                                       dim_signatures = {"list": [data["dim_signature"]["name"]], "op": "in"},
                                                       dim_signature_like = {"str": name, "op": "like"},
                                                       dim_exec_names = {"list": [data["dim_signature"]["exec"]], "op": "in"},
                                                       dim_exec_name_like = {"str": exec_name, "op": "like"})

        assert len(dim_signature7) == 1

    def test_wrong_inputs_query_dim_signature(self):
        
        result = False
        try:
            self.query.get_dim_signatures(dim_signature_ids = "not_a_dict")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_dim_signatures(dim_signature_ids = {"list": ["e6f03f0c-aced-11e8-9fef-000000001643"]})
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_dim_signatures(dim_signature_ids = {"op": "in"})
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_dim_signatures(dim_signature_ids = {"list": ["e6f03f0c-aced-11e8-9fef-000000001643"], "op": "in", "not_a_valid_key": 0})
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_dim_signatures(dim_signature_ids = {"list": ["e6f03f0c-aced-11e8-9fef-000000001643"], "op": "not_a_valid_operator"})
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_dim_signatures(dim_signature_ids = {"list": "not_a_list", "op": "in"})
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_dim_signatures(dim_signature_ids = {"list": [["not_a_string"]], "op": "in"})
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
            dim_signature = self.query.get_dim_signatures(dim_signature_like = "not_a_dict")
        except InputError:
            result = True
        # end try

        assert result == True


        result = False
        try:
            self.query.get_dim_signatures(dim_signature_like = {"str": "%"})
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_dim_signatures(dim_signature_like = {"op": "like"})
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_dim_signatures(dim_signature_like = {"str": "%", "op": "like", "not_a_valid_key": 0})
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_dim_signatures(dim_signature_like = {"str": "%", "op": "not_a_valid_operator"})
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_dim_signatures(dim_signature_like = {"str": [["not_a_string"]], "op": "like"})
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_dim_signatures(dim_exec_names = "not_a_dict")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_dim_signatures(dim_exec_name_like = "not_a_dict")
        except InputError:
            result = True
        # end try

        assert result == True

    def test_query_source(self):
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"}
                                  }
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa.ingestion_start = datetime.datetime.now()
        self.engine_eboa._insert_proc_status(0, final = True)

        dim_signature1 = self.query.get_dim_signatures()

        source = self.query.get_sources(dim_signature_ids = {"list": [dim_signature1[0].dim_signature_id], "op": "in"})

        assert len(source) == 1

        source1 = self.query.get_sources()

        assert len(source1) == 1

        source = self.query.get_sources(processing_uuids = {"list": [source1[0].processing_uuid], "op": "in"})

        assert len(source) == 1

        source = self.query.get_sources(names = {"list": [data["source"]["name"]], "op": "in"})

        assert len(source) == 1

        name = data["source"]["name"][0:4] + "%"
        source = self.query.get_sources(name_like = {"str": name, "op": "like"})

        assert len(source) == 1

        source = self.query.get_sources(validity_stop_filters = [{"date": "2018-06-05T08:07:36", "op": "=="}])

        assert len(source) == 1

        source = self.query.get_sources(generation_time_filters = [{"date": "2018-07-05T02:07:03", "op": "=="}])

        assert len(source) == 1

        source = self.query.get_sources(ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}])

        assert len(source) == 1

        source = self.query.get_sources(ingestion_duration_filters = [{"float": 10, "op": "<"}])

        assert len(source) == 1

        source = self.query.get_sources(dim_exec_version_filters = [{"str": "0.0", "op": ">"}])

        assert len(source) == 1

        source = self.query.get_sources(dim_signature_ids = {"list": [dim_signature1[0].dim_signature_id], "op": "in"},
                                        processing_uuids = {"list": [source1[0].processing_uuid], "op": "in"},
                                        names = {"list": [data["source"]["name"]], "op": "in"},
                                        name_like = {"str": name, "op": "like"},
                                        validity_start_filters = [{"date": "2018-06-05T02:07:03", "op": "=="}],
                                        validity_stop_filters = [{"date": "2018-06-05T08:07:36", "op": "=="}],
                                        generation_time_filters = [{"date": "2018-07-05T02:07:03", "op": "=="}],
                                        ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}],
                                        ingestion_duration_filters = [{"float": 10, "op": "<"}],
                                        dim_exec_version_filters = [{"str": "0.0", "op": ">"}])

        assert len(source) == 1


    def test_query_source_by_validity_all_operators(self):

        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
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
            self.query.get_sources(dim_signature_ids = "not_a_dict")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(processing_uuids = "not_a_dict")
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
            self.query.get_sources(name_like = "not_a_dict")
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
            self.query.get_sources(dim_exec_version_filters = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(dim_exec_version_filters = ["not_a_dict"])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(dim_exec_version_filters = [{"str": "0.0"}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(dim_exec_version_filters = [{"op": ">"}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(dim_exec_version_filters = [{"str": "0.0", "op": ">", "not_a_valid_key": "not_a_valid_value"}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(dim_exec_version_filters = [{"str": "0.0", "op": "not_a_valid_operator"}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources(dim_exec_version_filters = [{"str": ["not_a_valid_string"], "op": "=="}])
        except InputError:
            result = True
        # end try

        assert result == True

    def test_query_source_join(self):
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"}
                                  }
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa.ingestion_start = datetime.datetime.now()
        self.engine_eboa._insert_proc_status(0, final = True)

        source = self.query.get_sources_join(dim_signatures = {"list": [data["dim_signature"]["name"]], "op": "in"})

        assert len(source) == 1

        source = self.query.get_sources_join()

        assert len(source) == 1

        dim_sig_name = data["dim_signature"]["name"][0:7] + "%"
        source = self.query.get_sources_join(dim_signature_like = {"str": dim_sig_name, "op": "like"})

        assert len(source) == 1

        source = self.query.get_sources_join(dim_exec_names = {"list": [data["dim_signature"]["exec"]], "op": "in"})

        assert len(source) == 1

        exec_name = data["dim_signature"]["exec"][0:2] + "%"
        source = self.query.get_sources_join(dim_exec_name_like = {"str": exec_name, "op": "like"})

        assert len(source) == 1

        source = self.query.get_sources_join(status_filters = [{"float": 0, "op": "=="}])

        assert len(source) == 1

        source = self.query.get_sources_join(names = {"list": [data["source"]["name"]], "op": "in"})

        assert len(source) == 1

        source_name = data["source"]["name"][0:4] + "%"
        source = self.query.get_sources_join(name_like = {"str": source_name, "op": "like"})

        assert len(source) == 1

        source = self.query.get_sources_join(validity_start_filters = [{"date": "2018-06-05T02:07:03", "op": "=="}])

        assert len(source) == 1

        source = self.query.get_sources_join(validity_stop_filters = [{"date": "2018-06-05T08:07:36", "op": "=="}])

        assert len(source) == 1

        source = self.query.get_sources_join(generation_time_filters = [{"date": "2018-07-05T02:07:03", "op": "=="}])

        assert len(source) == 1

        source = self.query.get_sources_join(ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}])

        assert len(source) == 1

        source = self.query.get_sources_join(ingestion_duration_filters = [{"float": 10, "op": "<"}])

        assert len(source) == 1

        source = self.query.get_sources_join(dim_exec_version_filters = [{"str": "0.0", "op": ">"}])

        assert len(source) == 1




        source = self.query.get_sources_join(dim_signatures = {"list": [data["dim_signature"]["name"]], "op": "in"},
                                             dim_signature_like = {"str": dim_sig_name, "op": "like"},
                                             dim_exec_names = {"list": [data["dim_signature"]["exec"]], "op": "in"},
                                             dim_exec_name_like = {"str": exec_name, "op": "like"},
                                             status_filters = [{"float": 0, "op": "=="}],
                                             names = {"list": [data["source"]["name"]], "op": "in"},
                                             name_like = {"str": source_name, "op": "like"},
                                             validity_start_filters = [{"date": "2018-06-05T02:07:03", "op": "=="}],
                                             validity_stop_filters = [{"date": "2018-06-05T08:07:36", "op": "=="}],
                                             generation_time_filters = [{"date": "2018-07-05T02:07:03", "op": "=="}],
                                             ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}],
                                             ingestion_duration_filters = [{"float": 10, "op": "<"}],
                                             dim_exec_version_filters = [{"str": "0.0", "op": ">"}])

        assert len(source) == 1

    def test_wrong_inputs_query_source_join(self):
        
        result = False
        try:
            self.query.get_sources_join(dim_signatures = "not_a_dict")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources_join(dim_exec_names = "not_a_dict")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources_join(names = "not_a_dict")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources_join(name_like = "not_a_dict")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources_join(dim_signature_like = "not_a_dict")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_sources_join(dim_exec_name_like = "not_a_dict")
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

        dim_signature1 = self.query.get_dim_signatures()

        gauge = self.query.get_gauges(dim_signature_ids = {"list": [dim_signature1[0].dim_signature_id], "op": "in"})

        assert len(gauge) == 1

        gauge1 = self.query.get_gauges()

        assert len(gauge1) == 1

        gauge = self.query.get_gauges(gauge_ids = {"list": [gauge1[0].gauge_id], "op": "in"})

        assert len(gauge) == 1

        gauge = self.query.get_gauges(names = {"list": [data["operations"][0]["events"][0]["gauge"]["name"]], "op": "in"})

        assert len(gauge) == 1

        gauge_name = data["operations"][0]["events"][0]["gauge"]["name"][0:4] + "%"
        gauge = self.query.get_gauges(name_like = {"str": gauge_name, "op": "like"})

        assert len(gauge) == 1

        gauge = self.query.get_gauges(systems = {"list": [data["operations"][0]["events"][0]["gauge"]["system"]], "op": "in"})

        assert len(gauge) == 1

        gauge_system = data["operations"][0]["events"][0]["gauge"]["system"][0:4] + "%"
        gauge = self.query.get_gauges(system_like = {"str": gauge_system, "op": "like"})

        assert len(gauge) == 1

        gauge = self.query.get_gauges(dim_signature_ids = {"list": [dim_signature1[0].dim_signature_id], "op": "in"},
                                        gauge_ids = {"list": [gauge1[0].gauge_id], "op": "in"},
                                        names = {"list": [data["operations"][0]["events"][0]["gauge"]["name"]], "op": "in"},
                                        name_like = {"str": gauge_name, "op": "like"},
                                        systems = {"list": [data["operations"][0]["events"][0]["gauge"]["system"]], "op": "in"},
                                        system_like = {"str": gauge_system, "op": "like"})

        assert len(gauge) == 1

    def test_wrong_inputs_query_gauge(self):
        
        result = False
        try:
            self.query.get_gauges(dim_signature_ids = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_gauges(gauge_ids = "not_a_list")
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
            self.query.get_gauges(name_like = ["not_a_valid_string"])
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
            self.query.get_gauges(system_like = ["not_a_valid_string"])
        except InputError:
            result = True
        # end try

        assert result == True

    def test_query_gauge_join(self):
        data = {"operations": [{
                "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
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

        dim_signature1 = self.query.get_dim_signatures()

        gauge = self.query.get_gauges_join(dim_signatures = {"list": [data["operations"][0]["dim_signature"]["name"]], "op": "in"})

        assert len(gauge) == 1

        dim_sig_name = data["operations"][0]["dim_signature"]["name"][0:7] + "%"
        gauge = self.query.get_gauges_join(dim_signature_like = {"str": dim_sig_name, "op": "like"})

        assert len(gauge) == 1

        gauge = self.query.get_gauges_join(dim_exec_names = {"list": [data["operations"][0]["dim_signature"]["exec"]], "op": "in"})

        assert len(gauge) == 1

        exec_name = data["operations"][0]["dim_signature"]["exec"][0:2] + "%"
        gauge = self.query.get_gauges_join(dim_exec_name_like = {"str": exec_name, "op": "like"})

        assert len(gauge) == 1

        gauge1 = self.query.get_gauges_join()

        assert len(gauge1) == 1

        gauge = self.query.get_gauges_join(names = {"list": [data["operations"][0]["events"][0]["gauge"]["name"]], "op": "in"})

        assert len(gauge) == 1

        gauge_name = data["operations"][0]["events"][0]["gauge"]["name"][0:4] + "%"
        gauge = self.query.get_gauges_join(name_like = {"str": gauge_name, "op": "like"})

        assert len(gauge) == 1

        gauge = self.query.get_gauges_join(systems = {"list": [data["operations"][0]["events"][0]["gauge"]["system"]], "op": "in"})

        assert len(gauge) == 1

        gauge_system = data["operations"][0]["events"][0]["gauge"]["system"][0:4] + "%"
        gauge = self.query.get_gauges_join(system_like = {"str": gauge_system, "op": "like"})

        assert len(gauge) == 1

        gauge = self.query.get_gauges_join(dim_signatures = {"list": [data["operations"][0]["dim_signature"]["name"]], "op": "in"},
                                           dim_signature_like = {"str": dim_sig_name, "op": "like"},
                                           dim_exec_names = {"list": [data["operations"][0]["dim_signature"]["exec"]], "op": "in"},
                                           dim_exec_name_like = {"str": exec_name, "op": "like"},
                                           names = {"list": [data["operations"][0]["events"][0]["gauge"]["name"]], "op": "in"},
                                           name_like = {"str": gauge_name, "op": "like"},
                                           systems = {"list": [data["operations"][0]["events"][0]["gauge"]["system"]], "op": "in"},
                                           system_like = {"str": gauge_system, "op": "like"})

        assert len(gauge) == 1

    def test_wrong_inputs_query_gauge_join(self):
        
        result = False
        try:
            self.query.get_gauges_join(dim_signatures = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_gauges_join(dim_exec_names = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_gauges_join(dim_signature_like = ["not_a_valid_string"])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_gauges_join(dim_exec_name_like = ["not_a_valid_string"])
        except InputError:
            result = True
        # end try

        result = False
        try:
            self.query.get_gauges_join(names = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_gauges_join(name_like = ["not_a_valid_string"])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_gauges_join(systems = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_gauges_join(system_like = ["not_a_valid_string"])
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
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE",
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

        source1 = self.query.get_sources()

        event = self.query.get_events(processing_uuids = {"list": [source1[0].processing_uuid], "op": "in"})

        assert len(event) == 1

        explicit_ref1 = self.query.get_explicit_refs()

        event = self.query.get_events(explicit_ref_ids = {"list": [explicit_ref1[0].explicit_ref_id], "op": "in"})

        assert len(event) == 1

        gauge1 = self.query.get_gauges()

        event = self.query.get_events(gauge_ids = {"list": [gauge1[0].gauge_id], "op": "in"})

        assert len(event) == 1

        event1 = self.query.get_events()

        assert len(event1) == 1

        event = self.query.get_events(event_uuids = {"list": [event1[0].event_uuid], "op": "in"})

        assert len(event) == 1

        event = self.query.get_events(start_filters = [{"date": "2018-06-05T02:07:03", "op": "=="}])

        assert len(event) == 1

        event = self.query.get_events(stop_filters = [{"date": "2018-06-05T08:07:36", "op": "=="}])

        assert len(event) == 1

        event = self.query.get_events(ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}])

        assert len(event) == 1

        event = self.query.get_events(processing_uuids = {"list": [source1[0].processing_uuid], "op": "in"},
                                      explicit_ref_ids = {"list": [explicit_ref1[0].explicit_ref_id], "op": "in"},
                                      gauge_ids = {"list": [gauge1[0].gauge_id], "op": "in"},
                                      event_uuids = {"list": [event1[0].event_uuid], "op": "in"},
                                      start_filters = [{"date": "2018-06-05T02:07:03", "op": "=="}],
                                      stop_filters = [{"date": "2018-06-05T08:07:36", "op": "=="}],
                                      ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}])

        assert len(event) == 1

    def test_wrong_inputs_query_event(self):
        
        result = False
        try:
            self.query.get_events(processing_uuids = "not_a_list")
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
            self.query.get_events(explicit_ref_ids = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events(gauge_ids = "not_a_list")
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

    def test_query_event_join(self):
        data = {"operations": [{
                "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
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
                                    {"type": "boolean",
                                     "name": "BOOLEAN",
                                     "value": "true"}]}]
                    
                }]
            }]}
        self.engine_eboa.treat_data(data)

        event = self.query.get_events_join(sources = {"list": [data["operations"][0]["source"]["name"]], "op": "in"})

        assert len(event) == 1

        source_name = data["operations"][0]["source"]["name"][0:4] + "%"
        event = self.query.get_events_join(source_like = {"str": source_name, "op": "like"})

        assert len(event) == 1

        event = self.query.get_events_join(explicit_refs = {"list": [data["operations"][0]["events"][0]["explicit_reference"]], "op": "in"})

        assert len(event) == 1

        explicit_ref_name = data["operations"][0]["events"][0]["explicit_reference"][0:4] + "%"
        event = self.query.get_events_join(explicit_ref_like = {"str": explicit_ref_name, "op": "like"})

        assert len(event) == 1

        event = self.query.get_events_join(gauge_names = {"list": [data["operations"][0]["events"][0]["gauge"]["name"]], "op": "in"})

        assert len(event) == 1

        gauge_name_name = data["operations"][0]["events"][0]["gauge"]["name"][0:4] + "%"
        event = self.query.get_events_join(gauge_name_like = {"str": gauge_name_name, "op": "like"})

        assert len(event) == 1

        event = self.query.get_events_join(gauge_systems = {"list": [data["operations"][0]["events"][0]["gauge"]["system"]], "op": "in"})

        assert len(event) == 1

        gauge_system_name = data["operations"][0]["events"][0]["gauge"]["system"][0:4] + "%"
        event = self.query.get_events_join(gauge_system_like = {"str": gauge_system_name, "op": "like"})

        assert len(event) == 1

        event1 = self.query.get_events_join()

        assert len(event1) == 1

        event = self.query.get_events_join(start_filters = [{"date": "2018-06-05T02:07:03", "op": "=="}])

        assert len(event) == 1

        event = self.query.get_events_join(stop_filters = [{"date": "2018-06-05T08:07:36", "op": "=="}])

        assert len(event) == 1

        event = self.query.get_events_join(ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}])

        assert len(event) == 1

        event = self.query.get_events_join(ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}])

        assert len(event) == 1

        event = self.query.get_events_join(value_filters = [{"value": "TEXT", "type": "text", "op": "=="}, {"value": "true", "type": "boolean", "op": "=="}])

        assert len(event) == 1

        event = self.query.get_events_join(values_names_type = [{"names": ["TEXT"], "type": "text"}, {"names": ["BOOLEAN"], "type": "boolean"}])

        assert len(event) == 1

        event = self.query.get_events_join(values_name_type_like = [{"name_like": "TEX%", "type": "text"}, {"name_like": "BOOL%", "type": "boolean"}])

        assert len(event) == 1

        event = self.query.get_events_join(sources = {"list": [data["operations"][0]["source"]["name"]], "op": "in"},
                                           source_like = {"str": source_name, "op": "like"},
                                           explicit_refs = {"list": [data["operations"][0]["events"][0]["explicit_reference"]], "op": "in"},
                                           explicit_ref_like = {"str": explicit_ref_name, "op": "like"},
                                           gauge_names = {"list": [data["operations"][0]["events"][0]["gauge"]["name"]], "op": "in"},
                                           gauge_name_like = {"str": gauge_name_name, "op": "like"},
                                           gauge_systems = {"list": [data["operations"][0]["events"][0]["gauge"]["system"]], "op": "in"},
                                           gauge_system_like = {"str": gauge_system_name, "op": "like"},
                                      
                                           start_filters = [{"date": "2018-06-05T02:07:03", "op": "=="}],
                                           stop_filters = [{"date": "2018-06-05T08:07:36", "op": "=="}],
                                           ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}],
                                           value_filters = [{"value": "TEXT", "type": "text", "op": "=="}, {"value": "true", "type": "boolean", "op": "=="}],
                                           values_names_type = [{"names": ["TEXT"], "type": "text"}, {"names": ["BOOLEAN"], "type": "boolean"}],
                                           values_name_type_like = [{"name_like": "TEX%", "type": "text"}, {"name_like": "BOOL%", "type": "boolean"}])

        assert len(event) == 1

    def test_wrong_inputs_query_event_join(self):
        
        result = False
        try:
            self.query.get_events_join(sources = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(source_like = ["not_a_valid_string"])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(explicit_refs = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(explicit_ref_like = ["not_a_valid_string"])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(gauge_names = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(gauge_name_like = ["not_a_valid_string"])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(gauge_systems = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(gauge_system_like = ["not_a_valid_string"])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(value_filters = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(value_filters = ["not_a_dict"])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(value_filters = [{"value": "TEXT", "type": "text"}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(value_filters = [{"value": "TEXT", "op": "=="}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(value_filters = [{"type": "text", "op": "=="}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(value_filters = [{"value": "TEXT"}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(value_filters = [{"type": "text"}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(value_filters = [{"op": "=="}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(value_filters = [{"value": "TEXT", "type": "text", "op": "==", "not_a_valid_key": "not_a_valid_value"}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(value_filters = [{"value": "TEXT", "type": "text", "op": "not_a_valid_operator"}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(value_filters = [{"value": ["not_a_valid_string"], "type": "text", "op": "=="}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(value_filters = [{"value": "TEXT", "type": "not_a_valid_type", "op": "=="}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(values_names_type = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(values_names_type = ["not_a_dict"])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(values_names_type = [{"names": ["TEXT"]}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(values_names_type = [{"type": "text"}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(values_names_type = [{"names": ["TEXT"], "type": "text", "not_a_valid_key": "not_a_valid_value"}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(values_names_type = [{"names": "not_a_list", "type": "text"}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(values_names_type = [{"names": [["not_a_string"]], "type": "text"}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(values_names_type = [{"names": ["TEXT"], "type": "not_a_valid_type"}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(values_name_type_like = "not_a_list")
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(values_name_type_like = ["not_a_dict"])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(values_name_type_like = [{"name_like": ["TEXT%"]}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(values_name_type_like = [{"type": "text"}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(values_name_type_like = [{"name_like": "TEX%", "type": "text", "not_a_valid_key": "not_a_valid_value"}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(values_name_type_like = [{"name_like": ["not_a_string"], "type": "text"}])
        except InputError:
            result = True
        # end try

        assert result == True

        result = False
        try:
            self.query.get_events_join(values_name_type_like = [{"name_like": "TEX%", "type": "not_a_valid_type"}])
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

        event_key = self.query.get_event_keys(dim_signature_ids = {"list": [dim_signature1[0].dim_signature_id], "op": "in"})

        assert len(event_key) == 1

        event1 = self.query.get_events()

        assert len(event1) == 1

        event_key = self.query.get_events(event_uuids = {"list": [event1[0].event_uuid], "op": "in"})

        assert len(event_key) == 1

        event_key = self.query.get_event_keys(keys = {"list": [data["operations"][0]["events"][0]["key"]], "op": "in"})

        assert len(event_key) == 1

        event_key_name = data["operations"][0]["events"][0]["key"][0:4] + "%"
        event_key = self.query.get_event_keys(key_like = {"str": event_key_name, "op": "like"})

        assert len(event_key) == 1

        event_key = self.query.get_event_keys(dim_signature_ids = {"list": [dim_signature1[0].dim_signature_id], "op": "in"},
                                              event_uuids = {"list": [event1[0].event_uuid], "op": "in"},
                                              keys = {"list": [data["operations"][0]["events"][0]["key"]], "op": "in"},
                                              key_like = {"str": event_key_name, "op": "like"})

        assert len(event_key) == 1

    def test_wrong_inputs_query_event_key(self):
        
        result = False
        try:
            self.query.get_event_keys(dim_signature_ids = "not_a_list")
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
            self.query.get_event_keys(key_like = ["not_a_valid_string"])
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

        event_links = self.query.get_event_links(link_name_like = {"str": "EVENT_LINK%", "op": "like"})

        assert len(event_links) == 2

        event_link = self.query.get_event_links(event_uuid_links = {"list": [event1[0].event_uuid], "op": "in"})

        assert len(event_link) == 1

        event_link = self.query.get_event_links(event_uuids = {"list": [event1[0].event_uuid], "op": "in"})

        assert len(event_link) == 1

        event_link = self.query.get_event_links(event_uuid_links = {"list": [event2[0].event_uuid], "op": "in"})

        assert len(event_link) == 1

        event_link = self.query.get_event_links(event_uuids = {"list": [event2[0].event_uuid], "op": "in"})

        assert len(event_link) == 1

        event_links = self.query.get_event_links()

        assert len(event_links) == 2

        event_link = self.query.get_event_links(event_uuids = {"list": [event1[0].event_uuid], "op": "in"},
                                                event_uuid_links = {"list": [event2[0].event_uuid], "op": "in"},
                                                link_names = {"list": ["EVENT_LINK2"], "op": "in"},
                                                link_name_like = {"str": "EVENT_LINK%", "op": "like"})

        assert len(event_link) == 1

        event_link = self.query.get_event_links(event_uuids = {"list": [event2[0].event_uuid], "op": "in"},
                                                event_uuid_links = {"list": [event1[0].event_uuid], "op": "in"},
                                                link_names = {"list": ["EVENT_LINK1"], "op": "in"},
                                                link_name_like = {"str": "EVENT_LINK%", "op": "like"})

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
            self.query.get_event_links(link_name_like = ["not_a_valid_string"])
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


        events = self.query.get_linked_events(processing_uuids = {"list": [source1[0].processing_uuid], "op": "in"},
                                                   explicit_ref_ids = {"list": [explicit_ref1[0].explicit_ref_id], "op": "in"},
                                                   gauge_ids = {"list": [gauge1[0].gauge_id], "op": "in"},
                                                   event_uuids = [event1[0].event_uuid],
                                                   start_filters = [{"date": "2018-06-05T02:07:03", "op": "=="}],
                                                   stop_filters = [{"date": "2018-06-05T08:07:36", "op": "=="}],
                                                   link_names = {"list": ["EVENT_LINK1"], "op": "in"},
                                                   link_name_like = {"str": "EVENT_LINK%", "op": "like"})

        assert len(events["linked_events"]) == 1
        assert len(events["prime_events"]) == 1

        events = self.query.get_linked_events(processing_uuids = {"list": [source1[0].processing_uuid], "op": "in"},
                                                   explicit_ref_ids = {"list": [explicit_ref1[0].explicit_ref_id], "op": "in"},
                                                   gauge_ids = {"list": [gauge1[0].gauge_id], "op": "in"},
                                                   event_uuids = [event2[0].event_uuid],
                                                   start_filters = [{"date": "2018-06-05T04:07:03", "op": "=="}],
                                                   stop_filters = [{"date": "2018-06-05T08:07:36", "op": "=="}],
                                                   link_names = {"list": ["EVENT_LINK2"], "op": "in"},
                                                   link_name_like = {"str": "EVENT_LINK%", "op": "like"})

        assert len(events["linked_events"]) == 1
        assert len(events["prime_events"]) == 1

        events = self.query.get_linked_events(event_uuids = [event2[0].event_uuid])

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

    def test_query_linked_event_join(self):
        data = {"operations": [{
                "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
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

        events = self.query.get_linked_events_join()

        assert len(events["linked_events"]) == 2
        assert len(events["prime_events"]) == 2

        source1 = self.query.get_sources()
        explicit_ref1 = self.query.get_explicit_refs()
        gauge1 = self.query.get_gauges()
        event1 = self.query.get_events(start_filters = [{"date": "2018-06-05T02:07:03", "op": "=="}])
        event2 = self.query.get_events(start_filters = [{"date": "2018-06-05T04:07:03", "op": "=="}])

        events = self.query.get_linked_events_join(sources = {"list": [data["operations"][0]["source"]["name"]], "op": "in"},
                                                     source_like = {"str": "%", "op": "like"},
                                                     explicit_refs = {"list": [data["operations"][0]["events"][0]["explicit_reference"]], "op": "in"},
                                                     gauge_names = {"list": [data["operations"][0]["events"][0]["gauge"]["name"]], "op": "in"},
                                                     gauge_name_like = {"str": "%", "op": "like"},
                                                     gauge_systems = {"list": [data["operations"][0]["events"][0]["gauge"]["system"]], "op": "in"},
                                                     gauge_system_like = {"str": "%", "op": "like"},
                                                     start_filters = [{"date": "2018-06-05T02:07:03", "op": "=="}],
                                                     stop_filters = [{"date": "2018-06-05T08:07:36", "op": "=="}],
                                                     link_names = {"list": ["EVENT_LINK1"], "op": "in"},
                                                     link_name_like = {"str": "EVENT_LINK%", "op": "like"})
        assert len(events["linked_events"]) == 1
        assert len(events["prime_events"]) == 1

        events = self.query.get_linked_events_join(sources = {"list": [data["operations"][0]["source"]["name"]], "op": "in"},
                                                     source_like = {"str": "%", "op": "like"},
                                                     explicit_refs = {"list": [data["operations"][0]["events"][1]["explicit_reference"]], "op": "in"},
                                                     gauge_names = {"list": [data["operations"][0]["events"][1]["gauge"]["name"]], "op": "in"},
                                                     gauge_name_like = {"str": "%", "op": "like"},
                                                     gauge_systems = {"list": [data["operations"][0]["events"][1]["gauge"]["system"]], "op": "in"},
                                                     gauge_system_like = {"str": "%", "op": "like"},
                                                     start_filters = [{"date": "2018-06-05T04:07:03", "op": "=="}],
                                                     stop_filters = [{"date": "2018-06-05T08:07:36", "op": "=="}],
                                                     link_names = {"list": ["EVENT_LINK2"], "op": "in"},
                                                     link_name_like = {"str": "EVENT_LINK%", "op": "like"})

        assert len(events["linked_events"]) == 1
        assert len(events["prime_events"]) == 1

        events = self.query.get_linked_events(back_ref = True)

        assert len(events["linked_events"]) == 2
        assert len(events["prime_events"]) == 2
        assert len(events["events_linking"]) == 2

    def test_query_annotation_cnf(self):
        data = {"operations": [{
                "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {"name": "ANNOTATION_CNF_NAME",
                                   "system": "ANNOTATION_CNF_SYSTEM"},
                }]
            }]}
        self.engine_eboa.treat_data(data)

        dim_signature1 = self.query.get_dim_signatures()

        annotation_cnf = self.query.get_annotation_cnfs(dim_signature_ids = {"list": [dim_signature1[0].dim_signature_id], "op": "in"})

        assert len(annotation_cnf) == 1

        annotation_cnf1 = self.query.get_annotation_cnfs()

        assert len(annotation_cnf1) == 1

        annotation_cnf = self.query.get_annotation_cnfs(annotation_cnf_ids = {"list": [annotation_cnf1[0].annotation_cnf_id], "op": "in"})

        assert len(annotation_cnf) == 1

        annotation_cnf = self.query.get_annotation_cnfs(names = {"list": [data["operations"][0]["annotations"][0]["annotation_cnf"]["name"]], "op": "in"})

        assert len(annotation_cnf) == 1

        annotation_cnf_name = data["operations"][0]["annotations"][0]["annotation_cnf"]["name"][0:4] + "%"
        annotation_cnf = self.query.get_annotation_cnfs(name_like = {"str": annotation_cnf_name, "op": "like"})

        assert len(annotation_cnf) == 1

        annotation_cnf = self.query.get_annotation_cnfs(systems = {"list": [data["operations"][0]["annotations"][0]["annotation_cnf"]["system"]], "op": "in"})

        assert len(annotation_cnf) == 1

        annotation_cnf_system = data["operations"][0]["annotations"][0]["annotation_cnf"]["system"][0:4] + "%"
        annotation_cnf = self.query.get_annotation_cnfs(system_like = {"str": annotation_cnf_system, "op": "like"})

        assert len(annotation_cnf) == 1

        annotation_cnf = self.query.get_annotation_cnfs(dim_signature_ids = {"list": [dim_signature1[0].dim_signature_id], "op": "in"},
                                        annotation_cnf_ids = {"list": [annotation_cnf1[0].annotation_cnf_id], "op": "in"},
                                        names = {"list": [data["operations"][0]["annotations"][0]["annotation_cnf"]["name"]], "op": "in"},
                                        name_like = {"str": annotation_cnf_name, "op": "like"},
                                        systems = {"list": [data["operations"][0]["annotations"][0]["annotation_cnf"]["system"]], "op": "in"},
                                        system_like = {"str": annotation_cnf_system, "op": "like"})

        assert len(annotation_cnf) == 1

    def test_query_annotation_cnf_join(self):
        data = {"operations": [{
                "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {"name": "ANNOTATION_CNF_NAME",
                                   "system": "ANNOTATION_CNF_SYSTEM"},
                }]
            }]}
        self.engine_eboa.treat_data(data)

        dim_signature1 = self.query.get_dim_signatures()

        annotation_cnf = self.query.get_annotation_cnfs_join(dim_signatures = {"list": [data["operations"][0]["dim_signature"]["name"]], "op": "in"})

        assert len(annotation_cnf) == 1

        dim_sig_name = data["operations"][0]["dim_signature"]["name"][0:7] + "%"
        annotation_cnf = self.query.get_annotation_cnfs_join(dim_signature_like = {"str": dim_sig_name, "op": "like"})

        assert len(annotation_cnf) == 1

        annotation_cnf = self.query.get_annotation_cnfs_join(dim_exec_names = {"list": [data["operations"][0]["dim_signature"]["exec"]], "op": "in"})

        assert len(annotation_cnf) == 1

        exec_name = data["operations"][0]["dim_signature"]["exec"][0:2] + "%"
        annotation_cnf = self.query.get_annotation_cnfs_join(dim_exec_name_like = {"str": exec_name, "op": "like"})

        assert len(annotation_cnf) == 1

        annotation_cnf1 = self.query.get_annotation_cnfs_join()

        assert len(annotation_cnf1) == 1

        annotation_cnf = self.query.get_annotation_cnfs_join(names = {"list": [data["operations"][0]["annotations"][0]["annotation_cnf"]["name"]], "op": "in"})

        assert len(annotation_cnf) == 1

        annotation_cnf_name = data["operations"][0]["annotations"][0]["annotation_cnf"]["name"][0:4] + "%"
        annotation_cnf = self.query.get_annotation_cnfs_join(name_like = {"str": annotation_cnf_name, "op": "like"})

        assert len(annotation_cnf) == 1

        annotation_cnf = self.query.get_annotation_cnfs_join(systems = {"list": [data["operations"][0]["annotations"][0]["annotation_cnf"]["system"]], "op": "in"})

        assert len(annotation_cnf) == 1

        annotation_cnf_system = data["operations"][0]["annotations"][0]["annotation_cnf"]["system"][0:4] + "%"
        annotation_cnf = self.query.get_annotation_cnfs_join(system_like = {"str": annotation_cnf_system, "op": "like"})

        assert len(annotation_cnf) == 1

        annotation_cnf = self.query.get_annotation_cnfs_join(dim_signatures = {"list": [data["operations"][0]["dim_signature"]["name"]], "op": "in"},
                                           dim_signature_like = {"str": dim_sig_name, "op": "like"},
                                           dim_exec_names = {"list": [data["operations"][0]["dim_signature"]["exec"]], "op": "in"},
                                           dim_exec_name_like = {"str": exec_name, "op": "like"},
                                           names = {"list": [data["operations"][0]["annotations"][0]["annotation_cnf"]["name"]], "op": "in"},
                                           name_like = {"str": annotation_cnf_name, "op": "like"},
                                           systems = {"list": [data["operations"][0]["annotations"][0]["annotation_cnf"]["system"]], "op": "in"},
                                           system_like = {"str": annotation_cnf_system, "op": "like"})

        assert len(annotation_cnf) == 1

    def test_query_annotation(self):
        data = {"operations": [{
                "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
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

        source1 = self.query.get_sources()

        annotation = self.query.get_annotations(processing_uuids = {"list": [source1[0].processing_uuid], "op": "in"})

        assert len(annotation) == 1

        explicit_ref1 = self.query.get_explicit_refs()

        annotation = self.query.get_annotations(explicit_ref_ids = {"list": [explicit_ref1[0].explicit_ref_id], "op": "in"})

        assert len(annotation) == 1

        annotation_cnf1 = self.query.get_annotation_cnfs()

        annotation = self.query.get_annotations(annotation_cnf_ids = {"list": [annotation_cnf1[0].annotation_cnf_id], "op": "in"})

        assert len(annotation) == 1

        annotation1 = self.query.get_annotations()

        assert len(annotation1) == 1

        annotation = self.query.get_annotations(annotation_uuids = {"list": [annotation1[0].annotation_uuid], "op": "in"})

        assert len(annotation) == 1

        annotation = self.query.get_annotations(ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}])

        assert len(annotation) == 1

        annotation = self.query.get_annotations(processing_uuids = {"list": [source1[0].processing_uuid], "op": "in"},
                                      explicit_ref_ids = {"list": [explicit_ref1[0].explicit_ref_id], "op": "in"},
                                      annotation_cnf_ids = {"list": [annotation_cnf1[0].annotation_cnf_id], "op": "in"},
                                      annotation_uuids = {"list": [annotation1[0].annotation_uuid], "op": "in"},
                                      ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}])

        assert len(annotation) == 1

    def test_query_annotation_join(self):
        data = {"operations": [{
                "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
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

        annotation = self.query.get_annotations_join(sources = {"list": [data["operations"][0]["source"]["name"]], "op": "in"})

        assert len(annotation) == 1

        source_name = data["operations"][0]["source"]["name"][0:4] + "%"
        annotation = self.query.get_annotations_join(source_like = {"str": source_name, "op": "like"})

        assert len(annotation) == 1

        annotation = self.query.get_annotations_join(explicit_refs = {"list": [data["operations"][0]["annotations"][0]["explicit_reference"]], "op": "in"})

        assert len(annotation) == 1

        explicit_ref_name = data["operations"][0]["annotations"][0]["explicit_reference"][0:4] + "%"
        annotation = self.query.get_annotations_join(explicit_ref_like = {"str": explicit_ref_name, "op": "like"})

        assert len(annotation) == 1

        annotation = self.query.get_annotations_join(annotation_cnf_names = {"list": [data["operations"][0]["annotations"][0]["annotation_cnf"]["name"]], "op": "in"})

        assert len(annotation) == 1

        annotation_cnf_name_name = data["operations"][0]["annotations"][0]["annotation_cnf"]["name"][0:4] + "%"
        annotation = self.query.get_annotations_join(annotation_cnf_name_like = {"str": annotation_cnf_name_name, "op": "like"})

        assert len(annotation) == 1

        annotation = self.query.get_annotations_join(annotation_cnf_systems = {"list": [data["operations"][0]["annotations"][0]["annotation_cnf"]["system"]], "op": "in"})

        assert len(annotation) == 1

        annotation_cnf_system_name = data["operations"][0]["annotations"][0]["annotation_cnf"]["system"][0:4] + "%"
        annotation = self.query.get_annotations_join(annotation_cnf_system_like = {"str": annotation_cnf_system_name, "op": "like"})

        assert len(annotation) == 1

        annotation1 = self.query.get_annotations_join()

        assert len(annotation1) == 1

        annotation = self.query.get_annotations_join(ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}])

        assert len(annotation) == 1

        annotation = self.query.get_annotations_join(ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}])

        assert len(annotation) == 1

        annotation = self.query.get_annotations_join(value_filters = [{"value": "TEXT", "type": "text", "op": "=="}, {"value": "true", "type": "boolean", "op": "=="}])

        assert len(annotation) == 1

        annotation = self.query.get_annotations_join(values_names_type = [{"names": ["TEXT"], "type": "text"}, {"names": ["BOOLEAN"], "type": "boolean"}])

        assert len(annotation) == 1

        annotation = self.query.get_annotations_join(values_name_type_like = [{"name_like": "TEX%", "type": "text"}, {"name_like": "BOOL%", "type": "boolean"}])

        assert len(annotation) == 1

        annotation = self.query.get_annotations_join(sources = {"list": [data["operations"][0]["source"]["name"]], "op": "in"},
                                           source_like = {"str": source_name, "op": "like"},
                                           explicit_refs = {"list": [data["operations"][0]["annotations"][0]["explicit_reference"]], "op": "in"},
                                           explicit_ref_like = {"str": explicit_ref_name, "op": "like"},
                                           annotation_cnf_names = {"list": [data["operations"][0]["annotations"][0]["annotation_cnf"]["name"]], "op": "in"},
                                           annotation_cnf_name_like = {"str": annotation_cnf_name_name, "op": "like"},
                                           annotation_cnf_systems = {"list": [data["operations"][0]["annotations"][0]["annotation_cnf"]["system"]], "op": "in"},
                                           annotation_cnf_system_like = {"str": annotation_cnf_system_name, "op": "like"},
                                           ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}],
                                           value_filters = [{"value": "TEXT", "type": "text", "op": "=="}, {"value": "true", "type": "boolean", "op": "=="}],
                                           values_names_type = [{"names": ["TEXT"], "type": "text"}, {"names": ["BOOLEAN"], "type": "boolean"}],
                                           values_name_type_like = [{"name_like": "TEX%", "type": "text"}, {"name_like": "BOOL%", "type": "boolean"}])

        assert len(annotation) == 1

    def test_query_explicit_ref(self):
        data = {"operations": [{
                "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
            "explicit_references": [{
                "name": "EXPLICIT_REFERENCE",
                "group": "EXPL_GROUP",
                "links": [{"name": "LINK_NAME",
                    "link": "EXPLICIT_REFERENCE_LINK"}]
            }],
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE",
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

        expl_group1 = self.query.get_explicit_refs_groups()

        explicit_ref1 = self.query.get_explicit_refs(group_ids = {"list": [expl_group1[0].expl_ref_cnf_id], "op": "in"})

        assert len(explicit_ref1) == 1

        explicit_refs = self.query.get_explicit_refs()

        assert len(explicit_refs) == 2

        explicit_reference = self.query.get_explicit_refs(explicit_ref_ids = {"list": [explicit_ref1[0].explicit_ref_id], "op": "in"})

        assert len(explicit_reference) == 1

        explicit_reference = self.query.get_explicit_refs(explicit_refs = {"list": ["EXPLICIT_REFERENCE"], "op": "in"})

        assert len(explicit_reference) == 1

        explicit_reference = self.query.get_explicit_refs(explicit_ref_like = {"str": "EXPLICIT_REFERENCE_L%", "op": "notlike"})

        assert len(explicit_reference) == 1


        explicit_reference = self.query.get_explicit_refs(ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}])

        assert len(explicit_reference) == 2

        explicit_reference = self.query.get_explicit_refs(group_ids = {"list": [expl_group1[0].expl_ref_cnf_id], "op": "in"},
                                      explicit_ref_ids = {"list": [explicit_ref1[0].explicit_ref_id], "op": "in"},
                                      explicit_refs = {"list": ["EXPLICIT_REFERENCE"], "op": "in"},
                                      explicit_ref_like = {"str": "EXPLICIT%", "op": "like"},
                                      ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}])

        assert len(explicit_reference) == 1

    def test_query_explicit_ref_join(self):
        data = {"operations": [{
                "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
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

        explicit_references = self.query.get_explicit_refs_join(explicit_refs = {"list": [data["operations"][0]["annotations"][0]["explicit_reference"]], "op": "in"},
                                                                      explicit_ref_like = {"str": "EXPL%", "op": "like"},
                                                                      gauge_names = {"list": [data["operations"][0]["events"][0]["gauge"]["name"]], "op": "in"},
                                                                      gauge_name_like = {"str": "GAUGE%", "op": "like"},
                                                                      gauge_systems = {"list": [data["operations"][0]["events"][0]["gauge"]["system"]], "op": "in"},
                                                                      gauge_system_like = {"str": "GAUGE%", "op": "like"},
                                                                      start_filters = [{"date": "2018-06-05T02:07:03", "op": "=="}],
                                                                      stop_filters = [{"date": "2018-06-05T08:07:36", "op": "=="}],
                                                                      annotation_cnf_names = {"list": [data["operations"][0]["annotations"][0]["annotation_cnf"]["name"]], "op": "in"},
                                                                      annotation_cnf_name_like = {"str": "NAM%", "op": "like"},
                                                                      annotation_cnf_systems = {"list": [data["operations"][0]["annotations"][0]["annotation_cnf"]["system"]], "op": "in"},
                                                                      annotation_cnf_system_like = {"str": "SYS%", "op": "like"},
                                                                      explicit_ref_ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}],
                                                                      event_value_filters = [{"value": "TEXT", "type": "text", "op": "=="}, {"value": "true", "type": "boolean", "op": "=="}],
                                                                      event_values_names_type = [{"names": ["TEXT"], "type": "text"}, {"names": ["BOOLEAN"], "type": "boolean"}],
                                                                      event_values_name_type_like = [{"name_like": "TEX%", "type": "text"}, {"name_like": "BOOL%", "type": "boolean"}],
                                                                      annotation_value_filters = [{"value": "TEXT", "type": "text", "op": "=="}, {"value": "true", "type": "boolean", "op": "=="}],
                                                                      annotation_values_names_type = [{"names": ["TEXT"], "type": "text"}, {"names": ["BOOLEAN"], "type": "boolean"}],
                                                                      annotation_values_name_type_like = [{"name_like": "TEX%", "type": "text"}, {"name_like": "BOOL%", "type": "boolean"}],
                                                                      expl_groups = {"list": ["EXPL_GROUP"], "op": "in"},
                                                                      expl_group_like = {"str": "EXPL_%", "op": "like"})

        assert len(explicit_references) == 1

    def test_query_explicit_ref_link(self):
        data = {"operations": [{
                "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
            "explicit_references": [{
                "name": "EXPLICIT_REFERENCE",
                "links": [{"name": "LINK_NAME",
                           "link": "EXPLICIT_REFERENCE_EVENT",
                           "back_ref": "true"}]
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

        explicit_ref1 = self.query.get_explicit_refs(explicit_refs = {"list": ["EXPLICIT_REFERENCE"], "op": "in"})

        explicit_ref2 = self.query.get_explicit_refs(explicit_refs = {"list": ["EXPLICIT_REFERENCE_EVENT"], "op": "in"})

        explicit_ref_links = self.query.get_explicit_ref_links(link_name_like = {"str": "LINK_N%", "op": "like"})

        assert len(explicit_ref_links) == 2

        explicit_ref_link = self.query.get_explicit_ref_links(explicit_ref_ids = {"list": [explicit_ref1[0].explicit_ref_id], "op": "in"},
                                                explicit_ref_id_links = {"list": [explicit_ref2[0].explicit_ref_id], "op": "in"},
                                                link_names = {"list": ["LINK_NAME"], "op": "in"},
                                                link_name_like = {"str": "LINK_N%", "op": "like"})

        assert len(explicit_ref_link) == 1

        explicit_ref_link = self.query.get_explicit_ref_links(explicit_ref_ids = {"list": [explicit_ref2[0].explicit_ref_id], "op": "in"},
                                                explicit_ref_id_links = {"list": [explicit_ref1[0].explicit_ref_id], "op": "in"},
                                                link_names = {"list": ["LINK_NAME"], "op": "in"},
                                                link_name_like = {"str": "LINK_N%", "op": "like"})

        assert len(explicit_ref_link) == 1

    def test_query_linked_explicit_ref(self):
        data = {"operations": [{
                "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
            "explicit_references": [{
                "group": "EXPL_GROUP",
                "name": "EXPLICIT_REFERENCE",
                "links": [{"name": "LINK_NAME",
                           "link": "EXPLICIT_REFERENCE_EVENT",
                           "back_ref": "true"}]
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

        linked_explicit_refs = self.query.get_linked_explicit_refs()

        assert len(linked_explicit_refs) == 4

        expl_group1 = self.query.get_explicit_refs_groups()

        explicit_ref1 = self.query.get_explicit_refs(explicit_ref_like = {"str": "EXPLICIT_REFERENCE", "op": "like"})
        explicit_ref2 = self.query.get_explicit_refs(explicit_ref_like = {"str": "EXPLICIT_REFERENCE", "op": "notlike"})

        linked_explicit_refs = self.query.get_linked_explicit_refs(group_ids = {"list": [expl_group1[0].expl_ref_cnf_id], "op": "in"},
                                                            explicit_ref_ids = {"list": [explicit_ref1[0].explicit_ref_id], "op": "in"},
                                                            explicit_refs = {"list": ["EXPLICIT_REFERENCE"], "op": "in"},
                                                            explicit_ref_like = {"str": "EXPLICIT_REF%", "op": "like"},
                                                            ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}],
                                                            link_names = {"list": ["LINK_NAME"], "op": "in"},
                                                            link_name_like = {"str": "LINK_NAM%", "op": "like"})

        assert len(linked_explicit_refs) == 2 

        linked_explicit_refs = self.query.get_linked_explicit_refs(explicit_ref_ids = {"list": [explicit_ref2[0].explicit_ref_id], "op": "in"},
                                                            explicit_refs = {"list": ["EXPLICIT_REFERENCE_EVENT"], "op": "in"},
                                                            explicit_ref_like = {"str": "EXPLICIT_REF%", "op": "like"},
                                                            ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}],
                                                            link_names = {"list": ["LINK_NAME"], "op": "in"},
                                                            link_name_like = {"str": "LINK_NAM%", "op": "like"})

        assert len(linked_explicit_refs) == 2

    def test_query_linked_explicit_ref_join(self):
        data = {"operations": [{
                "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
            "explicit_references": [{
                "group": "EXPL_GROUP",
                "name": "EXPLICIT_REFERENCE",
                "links": [{"name": "LINK_NAME",
                           "link": "EXPLICIT_REFERENCE_EVENT",
                           "back_ref": "true"}]
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

        linked_explicit_refs = self.query.get_linked_explicit_refs_join(explicit_refs = {"list": [data["operations"][0]["annotations"][0]["explicit_reference"]], "op": "in"},
                                                                      explicit_ref_like = {"str": "EXPL%", "op": "like"},
                                                                      annotation_cnf_names = {"list": [data["operations"][0]["annotations"][0]["annotation_cnf"]["name"]], "op": "in"},
                                                                      annotation_cnf_name_like = {"str": "NAM%", "op": "like"},
                                                                      annotation_cnf_systems = {"list": [data["operations"][0]["annotations"][0]["annotation_cnf"]["system"]], "op": "in"},
                                                                      annotation_cnf_system_like = {"str": "SYS%", "op": "like"},
                                                                      explicit_ref_ingestion_time_filters = [{"date": "1960-07-05T02:07:03", "op": ">"}],
                                                                      annotation_value_filters = [{"value": "TEXT", "type": "text", "op": "=="}, {"value": "true", "type": "boolean", "op": "=="}],
                                                                      annotation_values_names_type = [{"names": ["TEXT"], "type": "text"}, {"names": ["BOOLEAN"], "type": "boolean"}],
                                                                      annotation_values_name_type_like = [{"name_like": "TEX%", "type": "text"}, {"name_like": "BOOL%", "type": "boolean"}],
                                                                      expl_groups = {"list": ["EXPL_GROUP"], "op": "in"},
                                                                      expl_group_like = {"str": "EXPL_%", "op": "like"})

        assert len(linked_explicit_refs) == 2

    def test_query_explicit_ref_group(self):
        data = {"operations": [{
                "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
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


        group = self.query.get_explicit_refs_groups(group_ids = {"list": [group1[0].expl_ref_cnf_id],
                                                                 "op": "in"},
                                                    names = {"list": ["EXPL_GROUP"], "op": "in"},
                                                    name_like = {"str": "EXPL_G%", "op": "like"})

        assert len(group) == 1

    def test_query_event_values(self):
        data = {"operations": [{
                "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
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
