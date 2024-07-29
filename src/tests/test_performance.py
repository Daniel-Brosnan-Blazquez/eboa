"""
Tests for checking performances

Written by DEIMOS Space S.L. (dibb)

module eboa
"""# Import python utilities
import unittest
import datetime

# Import engine of the DDBB
import eboa.engine.engine as eboa_engine
from eboa.engine.engine import Engine
from eboa.engine.query import Query
from eboa.datamodel.base import Session, engine, Base

class TestEngine(unittest.TestCase):
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

    def test_performance_insertion_explicit_references(self):
        """
        Method to test the race condition that could be produced if the
        several processes try to insert the values even with different name in the same position
        """
        print()
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
                "explicit_references": [],
            }]
        }
        for i in range(0,5000):
            data["operations"][0]["explicit_references"].append({
                "group": "explicit_ref_group",
                "name": "explicit_ref" + str(i)
            })
        # end for
        start = datetime.datetime.now()
        returned_value = self.engine_eboa.treat_data(data)[0]["status"]
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]
        
        stop = datetime.datetime.now()
        print("The insertion lasted {} seconds.".format((stop - start).total_seconds()))

        assert len(self.query_eboa.get_explicit_refs()) == 5000

        data["operations"][0]["source"]["name"] = "source2.xml"
        start = datetime.datetime.now()
        returned_value = self.engine_eboa.treat_data(data)[0]["status"]
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]
        stop = datetime.datetime.now()
        print("The insertion lasted {} seconds.".format((stop - start).total_seconds()))

        assert len(self.query_eboa.get_explicit_refs()) == 5000
        
    def test_performance_insertion_annotations(self):
        """
        Method to test the performance of inserting the same annotations with SIMPLE_UPDATE mode
        """
        print()
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
                "annotations": [],
            }]
        }
        for i in range(0,5000):
            data["operations"][0]["annotations"].append({
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {"name": "ANNOTATION_NAME_" + str(i),
                                   "system": "ANNOTATION_SYSTEM_" + str(i)}
            })
        # end for
        start = datetime.datetime.now()
        returned_value = self.engine_eboa.treat_data(data)[0]["status"]
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]
        
        stop = datetime.datetime.now()
        print("The insertion lasted {} seconds.".format((stop - start).total_seconds()))

        assert len(self.query_eboa.get_annotations()) == 5000

        data["operations"][0]["source"]["name"] = "source2.xml"
        start = datetime.datetime.now()
        returned_value = self.engine_eboa.treat_data(data)[0]["status"]
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]
        stop = datetime.datetime.now()
        print("The insertion lasted {} seconds.".format((stop - start).total_seconds()))

        assert len(self.query_eboa.get_annotations()) == 10000

    def test_performance_insertion_annotations_with_insert_and_erase(self):
        """
        Method to test the performance of inserting the same annotations with INSERT_and_ERASE mode
        """
        print()
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
                "annotations": [],
            }]
        }
        for i in range(0,5000):
            data["operations"][0]["annotations"].append({
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {"name": "ANNOTATION_NAME_" + str(i),
                                   "system": "ANNOTATION_SYSTEM_" + str(i),
                                   "insertion_type": "INSERT_and_ERASE"}
            })
        # end for
        start = datetime.datetime.now()
        returned_value = self.engine_eboa.treat_data(data)[0]["status"]
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]
        
        stop = datetime.datetime.now()
        print("The insertion lasted {} seconds.".format((stop - start).total_seconds()))

        assert len(self.query_eboa.get_annotations()) == 5000

        data["operations"][0]["source"]["name"] = "source2.xml"
        start = datetime.datetime.now()
        returned_value = self.engine_eboa.treat_data(data)[0]["status"]
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]
        stop = datetime.datetime.now()
        print("The insertion lasted {} seconds.".format((stop - start).total_seconds()))

        assert len(self.query_eboa.get_annotations()) == 5000
