"""
Automated tests for the ingestion of the REP_OPLTAS files

Written by DEIMOS Space S.L. (femd)

module ingestions
"""
# Import python utilities
import os
import sys
import unittest
import datetime
from dateutil import parser

# Import engine of the DDBB
import eboa.engine.engine as eboa_engine
from eboa.engine.engine import Engine
from eboa.engine.query import Query
from eboa.datamodel.base import Session, engine, Base
from eboa.engine.errors import LinksInconsistency, UndefinedEventLink, DuplicatedEventLinkRef, WrongPeriod, SourceAlreadyIngested, WrongValue, OddNumberOfCoordinates, EboaResourcesPathNotAvailable, WrongGeometry, ErrorParsingDictionary
from eboa.engine.query import Query

# Import datamodel
from eboa.datamodel.dim_signatures import DimSignature
from eboa.datamodel.events import Event, EventLink, EventKey, EventText, EventDouble, EventObject, EventGeometry, EventBoolean, EventTimestamp
from eboa.datamodel.gauges import Gauge
from eboa.datamodel.sources import Source, SourceStatus
from eboa.datamodel.explicit_refs import ExplicitRef, ExplicitRefGrp, ExplicitRefLink
from eboa.datamodel.annotations import Annotation, AnnotationCnf, AnnotationText, AnnotationDouble, AnnotationObject, AnnotationGeometry, AnnotationBoolean, AnnotationTimestamp

# Import ingestion
import ingestions.s2.ingestion_ltas.ingestion_ltas as ingestion_ltas

class TestEngine(unittest.TestCase):
    def setUp(self):
        # Create the engine to manage the data
        self.engine_eboa = Engine()
        self.query_eboa = Query()

        # Create session to connectx to the database
        self.session = Session()

        # Clear all tables before executing the test
        self.query_eboa.clear_db()

    def test_rep_ltas(self):

        filename = "S2__OPER_REP_OPLTAS_UPA__20180722T060002_RIPPED.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_ltas.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        sources = self.query_eboa.get_sources()

        assert len(sources) == 1

        sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T03:00:02", "op": "=="}],
                                              validity_stop_filters = [{"date": "2018-07-22T06:00:02", "op": "=="}],
                                              generation_time_filters = [{"date": "2018-07-22T06:00:02", "op": "=="}],
                                              processors = {"filter": "ingestion_ltas.py", "op": "like"},
                                              names = {"filter": "S2__OPER_REP_OPLTAS_UPA__20180722T060002_RIPPED.EOF", "op": "like"})

        assert len(sources) == 1

        #Check definite archiving_time
        definite_archiving_time = self.query_eboa.get_annotations(annotation_cnf_names = {"op": "like", "filter": "LONG_TERM_ARCHIVING_TIME"},
                                                     explicit_refs = {"op": "like", "filter": "S2B_OPER_MSI_L1C_LT_MPS__20180720T232515_S20180720T200443_N02.06"})

        assert definite_archiving_time[0].get_structured_values() == [{
            'type': 'object',
            'name': 'details',
            'values': [{'type': 'timestamp',
                'name': 'long_term_archiving_time',
                'value': '2018-07-21 03:02:03'
                }]
        }]

        #Check archiving_times
        archiving_times = self.query_eboa.get_annotations(annotation_cnf_names = {"op": "like", "filter": "LONG_TERM_ARCHIVING_TIME"})

        assert len(archiving_times) == 3