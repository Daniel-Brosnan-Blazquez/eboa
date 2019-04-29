"""
Automated tests for the ingestion of the REP_OPDHUS files

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
import ingestions.s2.ingestion_dhus.ingestion_dhus as ingestion_dhus

class TestEngine(unittest.TestCase):
    def setUp(self):
        # Create the engine to manage the data
        self.engine_eboa = Engine()
        self.query_eboa = Query()

        # Create session to connectx to the database
        self.session = Session()

        # Clear all tables before executing the test
        self.query_eboa.clear_db()

    def test_rep_dhus(self):

        filename = "S2__OPER_REP_OPDHUS_DHUS_20180721T085448_RIPPED.xml"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_dhus.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        sources = self.query_eboa.get_sources()

        assert len(sources) == 1

        sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:54:48", "op": "=="}],
                                              validity_stop_filters = [{"date": "2018-07-21T08:54:48", "op": "=="}],
                                              generation_time_filters = [{"date": "2018-07-21T08:54:48", "op": "=="}],
                                              processors = {"filter": "ingestion_dhus.py", "op": "like"},
                                              names = {"filter": "S2__OPER_REP_OPDHUS_DHUS_20180721T085448_RIPPED.xml", "op": "like"})

        assert len(sources) == 1

        #Check definite dissemination_time
        definite_dissemination_time = self.query_eboa.get_annotations(annotation_cnf_names = {"op": "like", "filter": "DHUS_DISSEMINATION_TIME"},
                                                     explicit_refs = {"op": "like", "filter": "S2B_OPER_MSI_L1C_DS_SGS__20180719T124224_S20180719T085240_N02.06"})

        assert definite_dissemination_time[0].get_structured_values() == [{
            'type': 'object',
            'name': 'details',
            'values': [{'type': 'timestamp',
                'name': 'dhus_dissemination_time',
                'value': '2018-07-21 08:54:48'
                }]
        }]

        #Check definite user_products
        definite_dissemination_time = self.query_eboa.get_annotations(annotation_cnf_names = {"op": "like", "filter": "USER_PRODUCT"},
                                                     explicit_refs = {"op": "like", "filter": "S2B_OPER_MSI_L1C_TL_SGS__20180719T124224_A007140_T36VXH_N02.06"})

        assert definite_dissemination_time[0].get_structured_values() == [{
            'type': 'object',
            'name': 'details',
            'values': [{'type': 'text',
                'name': 'product_name',
                'value': 'S2B_MSIL1C_20180719T084559_N0206_R107_T36VXH_20180719T124224.SAFE'
                }]
        }]

        #Check dissemination_times
        dissemination_times = self.query_eboa.get_annotations(annotation_cnf_names = {"op": "like", "filter": "DHUS_DISSEMINATION_TIME"})

        assert len(dissemination_times) == 4

        #Check user_products
        dissemination_times = self.query_eboa.get_annotations(annotation_cnf_names = {"op": "like", "filter": "USER_PRODUCT"})

        assert len(dissemination_times) == 3
