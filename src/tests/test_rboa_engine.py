"""
Automated tests for the engine submodule

Written by DEIMOS Space S.L. (dibb)

module rboa
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
import rboa.engine.engine as rboa_engine
from rboa.engine.engine import Engine

# Import query interface
from eboa.engine.query import Query

# Import datamodel
from eboa.datamodel.base import Session

# Import datamodel
from eboa.datamodel.base import Session
from rboa.datamodel.reports import Report, ReportGroup, ReportStatus, ReportText, ReportDouble, ReportObject, ReportGeometry, ReportBoolean, ReportTimestamp
from rboa.datamodel.alerts import ReportAlert
from eboa.datamodel.alerts import Alert, AlertGroup

class TestEngine(unittest.TestCase):
    def setUp(self):
        # Create the engine to manage the data
        self.engine_rboa = Engine()
        self.query_eboa = Query()

        # Create session to connect to the database
        self.session = Session()

        # Clear all tables before executing the test
        self.query_eboa.clear_db()

    def tearDown(self):
        # Close connections to the DDBB
        self.engine_rboa.close_session()
        self.query_eboa.close_session()
        self.session.close()

    def test_insert_report(self):

        filename = "report.html"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/html_inputs/" + filename
        
        data = {"operations": [{
            "mode": "insert",
            "report": {"name": filename,
                       "group": "report_group",
                       "group_description": "Group of reports for testing",
                       "path": file_path,
                       "compress": "true",
                       "generation_mode": "MANUAL",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "triggering_time": "2018-07-05T02:07:03",
                       "generation_start": "2018-07-05T02:07:10",
                       "generation_stop": "2018-07-05T02:15:10",
                       "generator": "report_generator",
                       "generator_version": "1.0",
                       "values": [{"name": "VALUES",
                                   "type": "object",
                                   "values": [
                                       {"type": "text",
                                        "name": "TEXT",
                                        "value": "TEXT"},
                                       {"type": "boolean",
                                        "name": "BOOLEAN",
                                        "value": "true"},
                                       {"type": "double",
                                        "name": "DOUBLE",
                                        "value": "0.9"},
                                       {"type": "timestamp",
                                        "name": "TIMESTAMP",
                                        "value": "20180712T00:00:00"},
                                       {"type": "geometry",
                                        "name": "GEOMETRY",
                                        "value": "29.012974905944 -118.33483458667 28.8650301641571 -118.372028380632 28.7171766138274 -118.409121707686 28.5693112139334 -118.44612300623 28.4213994944367 -118.483058731035 28.2734085660472 -118.519970531113 28.1253606038163 -118.556849863134 27.9772541759126 -118.593690316835 27.8291247153939 -118.630472520505 27.7544158362332 -118.64900551674 27.7282373644786 -118.48032600682 27.7015162098732 -118.314168792268 27.6742039940042 -118.150246300849 27.6462511775992 -117.98827485961 27.6176070520608 -117.827974178264 27.5882197156561 -117.669066835177 27.5580360448116 -117.511277813618 27.5270016492436 -117.354334035359 27.4950608291016 -117.197963963877 27.4621565093409 -117.041897175848 27.4282301711374 -116.885863967864 27.3932217651372 -116.729594956238 27.3570696128269 -116.572820673713 27.3197103000253 -116.415271199941 27.2810785491022 -116.256675748617 27.241107085821 -116.09676229722 27.1997272484913 -115.935260563566 27.1524952198729 -115.755436839005 27.2270348347386 -115.734960009089 27.3748346522356 -115.694299254844 27.5226008861849 -115.653563616829 27.6702779354428 -115.612760542177 27.8178690071708 -115.571901649363 27.9653506439026 -115.531000691074 28.1127600020619 -115.490011752733 28.2601469756437 -115.44890306179 28.4076546372628 -115.407649898021 28.455192866856 -115.589486631416 28.4968374106496 -115.752807970928 28.5370603096399 -115.91452902381 28.575931293904 -116.074924211465 28.6135193777855 -116.234273707691 28.6498895688451 -116.392847129762 28.6851057860975 -116.550917254638 28.7192301322012 -116.708756374584 28.752323018501 -116.866636764481 28.7844432583843 -117.024831047231 28.8156481533955 -117.183612605748 28.8459935678779 -117.343255995623 28.8755339855462 -117.504037348554 28.9043225601122 -117.666234808407 28.9324111491586 -117.830128960026 28.9598503481156 -117.996003330616 28.9866878706574 -118.164136222141 29.012974905944 -118.33483458667"}
                                   ]
                       }]
            },
            "alerts": [{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name1",
                    "severity": "critical",
                    "description": "Alert description",
                    "group": "alert_group"
                }
            },{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name2",
                    "severity": "warning",
                    "description": "Alert description",
                    "group": "alert_group"
                }
            },{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name3",
                    "severity": "critical",
                    "description": "Alert description",
                    "group": "alert_group"
                }
            }]
        }]
        }
        exit_status = self.engine_rboa.treat_data(data, filename)

        assert exit_status[0]["status"] == rboa_engine.exit_codes["OK"]["status"]

        report_groups = self.session.query(ReportGroup).all()

        assert len(report_groups) == 1

        assert report_groups[0].name == "report_group"
        assert report_groups[0].description == "Group of reports for testing"        
        
        reports = self.session.query(Report).all()

        assert len(reports) == 1

        assert reports[0].name == "report.html"
        assert reports[0].generation_mode == "MANUAL"
        assert reports[0].relative_path == "2018/07/05/report.tgz"
        assert reports[0].validity_start.isoformat() == "2018-06-05T02:07:03"
        assert reports[0].validity_stop.isoformat() == "2018-06-05T08:07:36"
        assert reports[0].triggering_time.isoformat() == "2018-07-05T02:07:03"
        assert reports[0].generation_start.isoformat() == "2018-07-05T02:07:10"
        assert reports[0].generation_stop.isoformat() == "2018-07-05T02:15:10"
        assert reports[0].generated == True
        assert reports[0].compressed == True
        assert reports[0].generation_progress == 100
        assert reports[0].metadata_ingestion_progress == 100
        assert reports[0].generator == "report_generator"
        assert reports[0].generator_version == "1.0"
        assert reports[0].generation_error == False
        assert reports[0].report_group_uuid == report_groups[0].report_group_uuid

        report_statuses = self.session.query(ReportStatus).all()

        assert len(report_statuses) == 3

        report_statuses.sort(key=lambda x:x.time_stamp)

        assert report_statuses[0].status == rboa_engine.exit_codes["METADATA_INGESTION_STARTED"]["status"]
        assert report_statuses[0].log == "The metadata of the report report.html associated to the generator report_generator with version 1.0 is going to be ingested"
        assert report_statuses[0].report_uuid == reports[0].report_uuid
        
        alert_groups = self.session.query(AlertGroup).all()

        assert len(alert_groups) == 1

        assert alert_groups[0].name == "alert_group"

        alerts = self.session.query(Alert).all()

        assert len(alerts) == 3

        alerts.sort(key=lambda x:x.name)
        assert alerts[0].name == "alert_name1"

        report_alerts = self.session.query(ReportAlert).all()

        assert len(report_alerts) == 3

        report_alerts.sort(key=lambda x:x.alertDefinition.name)
        assert report_alerts[0].message == "Alert message"
        assert report_alerts[0].generator == "test"
        assert report_alerts[0].notification_time.isoformat() == "2018-06-05T08:07:36"
        assert report_alerts[0].report_uuid == reports[0].report_uuid
        assert report_alerts[0].alert_uuid == alerts[0].alert_uuid        

    def test_wrong_metadata(self):

        filename = "report.html"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/html_inputs/" + filename

        data = {"operations": [{
            "mode": "insert",
            "report": {"name": filename,
                       "group": "report_group",
                       "group_description": "Group of reports for testing",
                       "path": file_path,
                       "compress": "true",
                       "generation_mode": "MANUAL",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "triggering_time": "2018-07-05T02:07:03",
                       "generation_start": "2018-07-05T02:07:10",
                       "generation_stop": "2018-07-05T02:15:10",
                       "generator": "report_generator",
                       "generator_version": "1.0",
                       "values": [{"name": "VALUES",
                                   "type": "object",
                                   "values": [
                                       {"type": "text",
                                        "name": "TEXT",
                                        "value": "TEXT"},
                                       {"type": "boolean",
                                        "name": "BOOLEAN",
                                        "value": "true"},
                                       {"type": "double",
                                        "name": "DOUBLE",
                                        "value": "0.9"},
                                       {"type": "timestamp",
                                        "name": "TIMESTAMP",
                                        "value": "20180712T00:00:00"},
                                       {"type": "geometry",
                                        "name": "GEOMETRY",
                                        "value": "29.012974905944 -118.33483458667 28.8650301641571 -118.372028380632 28.7171766138274 -118.409121707686 28.5693112139334 -118.44612300623 28.4213994944367 -118.483058731035 28.2734085660472 -118.519970531113 28.1253606038163 -118.556849863134 27.9772541759126 -118.593690316835 27.8291247153939 -118.630472520505 27.7544158362332 -118.64900551674 27.7282373644786 -118.48032600682 27.7015162098732 -118.314168792268 27.6742039940042 -118.150246300849 27.6462511775992 -117.98827485961 27.6176070520608 -117.827974178264 27.5882197156561 -117.669066835177 27.5580360448116 -117.511277813618 27.5270016492436 -117.354334035359 27.4950608291016 -117.197963963877 27.4621565093409 -117.041897175848 27.4282301711374 -116.885863967864 27.3932217651372 -116.729594956238 27.3570696128269 -116.572820673713 27.3197103000253 -116.415271199941 27.2810785491022 -116.256675748617 27.241107085821 -116.09676229722 27.1997272484913 -115.935260563566 27.1524952198729 -115.755436839005 27.2270348347386 -115.734960009089 27.3748346522356 -115.694299254844 27.5226008861849 -115.653563616829 27.6702779354428 -115.612760542177 27.8178690071708 -115.571901649363 27.9653506439026 -115.531000691074 28.1127600020619 -115.490011752733 28.2601469756437 -115.44890306179 28.4076546372628 -115.407649898021 28.455192866856 -115.589486631416 28.4968374106496 -115.752807970928 28.5370603096399 -115.91452902381 28.575931293904 -116.074924211465 28.6135193777855 -116.234273707691 28.6498895688451 -116.392847129762 28.6851057860975 -116.550917254638 28.7192301322012 -116.708756374584 28.752323018501 -116.866636764481 28.7844432583843 -117.024831047231 28.8156481533955 -117.183612605748 28.8459935678779 -117.343255995623 28.8755339855462 -117.504037348554 28.9043225601122 -117.666234808407 28.9324111491586 -117.830128960026 28.9598503481156 -117.996003330616 28.9866878706574 -118.164136222141 29.012974905944 -118.33483458667"}
                                   ]
                       }]
            },
            "alerts": [{
                "messag": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name1",
                    "severity": "critical",
                    "description": "Alert description",
                    "group": "alert_group"
                }
            },{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name2",
                    "severity": "warning",
                    "description": "Alert description",
                    "group": "alert_group"
                }
            },{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name3",
                    "severity": "critical",
                    "description": "Alert description",
                    "group": "alert_group"
                }
            }]
        }]
        }
        exit_status = self.engine_rboa.treat_data(data, "wrong_metadata.html")

        assert exit_status[0]["status"] == rboa_engine.exit_codes["FILE_NOT_VALID"]["status"]

        report_groups = self.session.query(ReportGroup).all()

        assert len(report_groups) == 1

        assert report_groups[0].name == "PENDING_GENERATION"
        assert report_groups[0].description == None
        
        reports = self.session.query(Report).all()

        assert len(reports) == 1
        assert reports[0].name == "wrong_metadata.html"
        assert reports[0].generation_error == True
        assert reports[0].report_group_uuid == report_groups[0].report_group_uuid

        report_statuses = self.session.query(ReportStatus).all()

        assert len(report_statuses) == 1

        assert report_statuses[0].status == rboa_engine.exit_codes["FILE_NOT_VALID"]["status"]
        assert report_statuses[0].log == "The allowed tags inside alerts structure are: message, generator, notification_time and alert_cnf"
        assert report_statuses[0].report_uuid == reports[0].report_uuid
        
        alert_groups = self.session.query(AlertGroup).all()

        assert len(alert_groups) == 0

        alerts = self.session.query(Alert).all()

        assert len(alerts) == 0

        report_alerts = self.session.query(ReportAlert).all()

        assert len(report_alerts) == 0

    def test_wrong_validity_period(self):

        filename = "report.html"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/html_inputs/" + filename

        data = {"operations": [{
            "mode": "insert",
            "report": {"name": filename,
                       "group": "report_group",
                       "group_description": "Group of reports for testing",
                       "path": file_path,
                       "compress": "true",
                       "generation_mode": "MANUAL",
                       "validity_start": "2018-07-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "triggering_time": "2018-07-05T02:07:03",
                       "generation_start": "2018-07-05T02:07:10",
                       "generation_stop": "2018-07-05T02:15:10",
                       "generator": "report_generator",
                       "generator_version": "1.0",
                       "values": [{"name": "VALUES",
                                   "type": "object",
                                   "values": [
                                       {"type": "text",
                                        "name": "TEXT",
                                        "value": "TEXT"},
                                       {"type": "boolean",
                                        "name": "BOOLEAN",
                                        "value": "true"},
                                       {"type": "double",
                                        "name": "DOUBLE",
                                        "value": "0.9"},
                                       {"type": "timestamp",
                                        "name": "TIMESTAMP",
                                        "value": "20180712T00:00:00"},
                                       {"type": "geometry",
                                        "name": "GEOMETRY",
                                        "value": "29.012974905944 -118.33483458667 28.8650301641571 -118.372028380632 28.7171766138274 -118.409121707686 28.5693112139334 -118.44612300623 28.4213994944367 -118.483058731035 28.2734085660472 -118.519970531113 28.1253606038163 -118.556849863134 27.9772541759126 -118.593690316835 27.8291247153939 -118.630472520505 27.7544158362332 -118.64900551674 27.7282373644786 -118.48032600682 27.7015162098732 -118.314168792268 27.6742039940042 -118.150246300849 27.6462511775992 -117.98827485961 27.6176070520608 -117.827974178264 27.5882197156561 -117.669066835177 27.5580360448116 -117.511277813618 27.5270016492436 -117.354334035359 27.4950608291016 -117.197963963877 27.4621565093409 -117.041897175848 27.4282301711374 -116.885863967864 27.3932217651372 -116.729594956238 27.3570696128269 -116.572820673713 27.3197103000253 -116.415271199941 27.2810785491022 -116.256675748617 27.241107085821 -116.09676229722 27.1997272484913 -115.935260563566 27.1524952198729 -115.755436839005 27.2270348347386 -115.734960009089 27.3748346522356 -115.694299254844 27.5226008861849 -115.653563616829 27.6702779354428 -115.612760542177 27.8178690071708 -115.571901649363 27.9653506439026 -115.531000691074 28.1127600020619 -115.490011752733 28.2601469756437 -115.44890306179 28.4076546372628 -115.407649898021 28.455192866856 -115.589486631416 28.4968374106496 -115.752807970928 28.5370603096399 -115.91452902381 28.575931293904 -116.074924211465 28.6135193777855 -116.234273707691 28.6498895688451 -116.392847129762 28.6851057860975 -116.550917254638 28.7192301322012 -116.708756374584 28.752323018501 -116.866636764481 28.7844432583843 -117.024831047231 28.8156481533955 -117.183612605748 28.8459935678779 -117.343255995623 28.8755339855462 -117.504037348554 28.9043225601122 -117.666234808407 28.9324111491586 -117.830128960026 28.9598503481156 -117.996003330616 28.9866878706574 -118.164136222141 29.012974905944 -118.33483458667"}
                                   ]
                       }]
            },
            "alerts": [{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name1",
                    "severity": "critical",
                    "description": "Alert description",
                    "group": "alert_group"
                }
            },{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name2",
                    "severity": "warning",
                    "description": "Alert description",
                    "group": "alert_group"
                }
            },{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name3",
                    "severity": "critical",
                    "description": "Alert description",
                    "group": "alert_group"
                }
            }]
        }]
        }
        exit_status = self.engine_rboa.treat_data(data, "report.html")

        assert exit_status[0]["status"] == rboa_engine.exit_codes["WRONG_REPORT_PERIOD"]["status"]

        report_groups = self.session.query(ReportGroup).all()

        assert len(report_groups) == 1

        assert report_groups[0].name == "report_group"
        assert report_groups[0].description == "Group of reports for testing"
        
        reports = self.session.query(Report).all()

        assert len(reports) == 1
        assert reports[0].name == "report.html"
        assert reports[0].generation_mode == "MANUAL"
        assert reports[0].relative_path == None
        assert reports[0].validity_start == None
        assert reports[0].validity_stop == None
        assert reports[0].triggering_time.isoformat() == "2018-07-05T02:07:03"
        assert reports[0].generation_start == None
        assert reports[0].generation_stop == None
        assert reports[0].generated == None
        assert reports[0].compressed == None
        assert reports[0].generation_progress == 100
        assert reports[0].metadata_ingestion_progress == None
        assert reports[0].generator == "report_generator"
        assert reports[0].generator_version == "1.0"
        assert reports[0].generation_error == True
        assert reports[0].report_group_uuid == report_groups[0].report_group_uuid

        report_statuses = self.session.query(ReportStatus).all()

        assert len(report_statuses) == 1

        assert report_statuses[0].status == rboa_engine.exit_codes["WRONG_REPORT_PERIOD"]["status"]
        assert report_statuses[0].log == "The metadata of the report report.html associated to the generator report_generator with version 1.0 has a validity period which its stop (2018-06-05T08:07:36) is lower than its start (2018-07-05T02:07:03)"
        assert report_statuses[0].report_uuid == reports[0].report_uuid
        
        alert_groups = self.session.query(AlertGroup).all()

        assert len(alert_groups) == 0

        alerts = self.session.query(Alert).all()

        assert len(alerts) == 0

        report_alerts = self.session.query(ReportAlert).all()

        assert len(report_alerts) == 0

    def test_wrong_generation_period(self):

        filename = "report.html"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/html_inputs/" + filename

        data = {"operations": [{
            "mode": "insert",
            "report": {"name": filename,
                       "group": "report_group",
                       "group_description": "Group of reports for testing",
                       "path": file_path,
                       "compress": "true",
                       "generation_mode": "MANUAL",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "triggering_time": "2018-07-05T02:07:03",
                       "generation_start": "2018-08-05T02:07:10",
                       "generation_stop": "2018-07-05T02:15:10",
                       "generator": "report_generator",
                       "generator_version": "1.0",
                       "values": [{"name": "VALUES",
                                   "type": "object",
                                   "values": [
                                       {"type": "text",
                                        "name": "TEXT",
                                        "value": "TEXT"},
                                       {"type": "boolean",
                                        "name": "BOOLEAN",
                                        "value": "true"},
                                       {"type": "double",
                                        "name": "DOUBLE",
                                        "value": "0.9"},
                                       {"type": "timestamp",
                                        "name": "TIMESTAMP",
                                        "value": "20180712T00:00:00"},
                                       {"type": "geometry",
                                        "name": "GEOMETRY",
                                        "value": "29.012974905944 -118.33483458667 28.8650301641571 -118.372028380632 28.7171766138274 -118.409121707686 28.5693112139334 -118.44612300623 28.4213994944367 -118.483058731035 28.2734085660472 -118.519970531113 28.1253606038163 -118.556849863134 27.9772541759126 -118.593690316835 27.8291247153939 -118.630472520505 27.7544158362332 -118.64900551674 27.7282373644786 -118.48032600682 27.7015162098732 -118.314168792268 27.6742039940042 -118.150246300849 27.6462511775992 -117.98827485961 27.6176070520608 -117.827974178264 27.5882197156561 -117.669066835177 27.5580360448116 -117.511277813618 27.5270016492436 -117.354334035359 27.4950608291016 -117.197963963877 27.4621565093409 -117.041897175848 27.4282301711374 -116.885863967864 27.3932217651372 -116.729594956238 27.3570696128269 -116.572820673713 27.3197103000253 -116.415271199941 27.2810785491022 -116.256675748617 27.241107085821 -116.09676229722 27.1997272484913 -115.935260563566 27.1524952198729 -115.755436839005 27.2270348347386 -115.734960009089 27.3748346522356 -115.694299254844 27.5226008861849 -115.653563616829 27.6702779354428 -115.612760542177 27.8178690071708 -115.571901649363 27.9653506439026 -115.531000691074 28.1127600020619 -115.490011752733 28.2601469756437 -115.44890306179 28.4076546372628 -115.407649898021 28.455192866856 -115.589486631416 28.4968374106496 -115.752807970928 28.5370603096399 -115.91452902381 28.575931293904 -116.074924211465 28.6135193777855 -116.234273707691 28.6498895688451 -116.392847129762 28.6851057860975 -116.550917254638 28.7192301322012 -116.708756374584 28.752323018501 -116.866636764481 28.7844432583843 -117.024831047231 28.8156481533955 -117.183612605748 28.8459935678779 -117.343255995623 28.8755339855462 -117.504037348554 28.9043225601122 -117.666234808407 28.9324111491586 -117.830128960026 28.9598503481156 -117.996003330616 28.9866878706574 -118.164136222141 29.012974905944 -118.33483458667"}
                                   ]
                       }]
            },
            "alerts": [{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name1",
                    "severity": "critical",
                    "description": "Alert description",
                    "group": "alert_group"
                }
            },{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name2",
                    "severity": "warning",
                    "description": "Alert description",
                    "group": "alert_group"
                }
            },{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name3",
                    "severity": "critical",
                    "description": "Alert description",
                    "group": "alert_group"
                }
            }]
        }]
        }
        exit_status = self.engine_rboa.treat_data(data, "report.html")

        assert exit_status[0]["status"] == rboa_engine.exit_codes["WRONG_GENERATION_PERIOD"]["status"]

        report_groups = self.session.query(ReportGroup).all()

        assert len(report_groups) == 1

        assert report_groups[0].name == "report_group"
        assert report_groups[0].description == "Group of reports for testing"
        
        reports = self.session.query(Report).all()

        assert len(reports) == 1
        assert reports[0].name == "report.html"
        assert reports[0].generation_mode == "MANUAL"
        assert reports[0].relative_path == None
        assert reports[0].validity_start.isoformat() == "2018-06-05T02:07:03"
        assert reports[0].validity_stop.isoformat() == "2018-06-05T08:07:36"
        assert reports[0].triggering_time.isoformat() == "2018-07-05T02:07:03"
        assert reports[0].generation_start == None
        assert reports[0].generation_stop == None
        assert reports[0].generated == None
        assert reports[0].compressed == None
        assert reports[0].generation_progress == 100
        assert reports[0].metadata_ingestion_progress == None
        assert reports[0].generator == "report_generator"
        assert reports[0].generator_version == "1.0"
        assert reports[0].generation_error == True
        assert reports[0].report_group_uuid == report_groups[0].report_group_uuid

        report_statuses = self.session.query(ReportStatus).all()

        assert len(report_statuses) == 1

        assert report_statuses[0].status == rboa_engine.exit_codes["WRONG_GENERATION_PERIOD"]["status"]
        assert report_statuses[0].log == "The metadata of the report report.html associated to the generator report_generator with version 1.0 has a generation period which its stop (2018-07-05T02:15:10) is lower than its start (2018-08-05T02:07:10)"
        assert report_statuses[0].report_uuid == reports[0].report_uuid
        
        alert_groups = self.session.query(AlertGroup).all()

        assert len(alert_groups) == 0

        alerts = self.session.query(Alert).all()

        assert len(alerts) == 0

        report_alerts = self.session.query(ReportAlert).all()

        assert len(report_alerts) == 0

    def test_non_existent_file(self):

        filename = "non_existent_file.html"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/html_inputs/" + filename

        data = {"operations": [{
            "mode": "insert",
            "report": {"name": filename,
                       "group": "report_group",
                       "group_description": "Group of reports for testing",
                       "path": file_path,
                       "compress": "true",
                       "generation_mode": "MANUAL",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "triggering_time": "2018-07-05T02:07:03",
                       "generation_start": "2018-07-05T02:07:10",
                       "generation_stop": "2018-07-05T02:15:10",
                       "generator": "report_generator",
                       "generator_version": "1.0",
                       "values": [{"name": "VALUES",
                                   "type": "object",
                                   "values": [
                                       {"type": "text",
                                        "name": "TEXT",
                                        "value": "TEXT"},
                                       {"type": "boolean",
                                        "name": "BOOLEAN",
                                        "value": "true"},
                                       {"type": "double",
                                        "name": "DOUBLE",
                                        "value": "0.9"},
                                       {"type": "timestamp",
                                        "name": "TIMESTAMP",
                                        "value": "20180712T00:00:00"},
                                       {"type": "geometry",
                                        "name": "GEOMETRY",
                                        "value": "29.012974905944 -118.33483458667 28.8650301641571 -118.372028380632 28.7171766138274 -118.409121707686 28.5693112139334 -118.44612300623 28.4213994944367 -118.483058731035 28.2734085660472 -118.519970531113 28.1253606038163 -118.556849863134 27.9772541759126 -118.593690316835 27.8291247153939 -118.630472520505 27.7544158362332 -118.64900551674 27.7282373644786 -118.48032600682 27.7015162098732 -118.314168792268 27.6742039940042 -118.150246300849 27.6462511775992 -117.98827485961 27.6176070520608 -117.827974178264 27.5882197156561 -117.669066835177 27.5580360448116 -117.511277813618 27.5270016492436 -117.354334035359 27.4950608291016 -117.197963963877 27.4621565093409 -117.041897175848 27.4282301711374 -116.885863967864 27.3932217651372 -116.729594956238 27.3570696128269 -116.572820673713 27.3197103000253 -116.415271199941 27.2810785491022 -116.256675748617 27.241107085821 -116.09676229722 27.1997272484913 -115.935260563566 27.1524952198729 -115.755436839005 27.2270348347386 -115.734960009089 27.3748346522356 -115.694299254844 27.5226008861849 -115.653563616829 27.6702779354428 -115.612760542177 27.8178690071708 -115.571901649363 27.9653506439026 -115.531000691074 28.1127600020619 -115.490011752733 28.2601469756437 -115.44890306179 28.4076546372628 -115.407649898021 28.455192866856 -115.589486631416 28.4968374106496 -115.752807970928 28.5370603096399 -115.91452902381 28.575931293904 -116.074924211465 28.6135193777855 -116.234273707691 28.6498895688451 -116.392847129762 28.6851057860975 -116.550917254638 28.7192301322012 -116.708756374584 28.752323018501 -116.866636764481 28.7844432583843 -117.024831047231 28.8156481533955 -117.183612605748 28.8459935678779 -117.343255995623 28.8755339855462 -117.504037348554 28.9043225601122 -117.666234808407 28.9324111491586 -117.830128960026 28.9598503481156 -117.996003330616 28.9866878706574 -118.164136222141 29.012974905944 -118.33483458667"}
                                   ]
                       }]
            },
            "alerts": [{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name1",
                    "severity": "critical",
                    "description": "Alert description",
                    "group": "alert_group"
                }
            },{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name2",
                    "severity": "warning",
                    "description": "Alert description",
                    "group": "alert_group"
                }
            },{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name3",
                    "severity": "critical",
                    "description": "Alert description",
                    "group": "alert_group"
                }
            }]
        }]
        }
        exit_status = self.engine_rboa.treat_data(data, "non_existent_file.html")

        assert exit_status[0]["status"] == rboa_engine.exit_codes["FILE_DOES_NOT_EXIST"]["status"]

        report_groups = self.session.query(ReportGroup).all()

        assert len(report_groups) == 1

        assert report_groups[0].name == "report_group"
        assert report_groups[0].description == "Group of reports for testing"
        
        reports = self.session.query(Report).all()

        assert len(reports) == 1
        assert reports[0].name == "non_existent_file.html"
        assert reports[0].generation_mode == "MANUAL"
        assert reports[0].relative_path == None
        assert reports[0].validity_start.isoformat() == "2018-06-05T02:07:03"
        assert reports[0].validity_stop.isoformat() == "2018-06-05T08:07:36"
        assert reports[0].triggering_time.isoformat() == "2018-07-05T02:07:03"
        assert reports[0].generation_start.isoformat() == "2018-07-05T02:07:10"
        assert reports[0].generation_stop.isoformat() == "2018-07-05T02:15:10"
        assert reports[0].generated == None
        assert reports[0].compressed == None
        assert reports[0].generation_progress == 100
        assert reports[0].metadata_ingestion_progress == 60
        assert reports[0].generator == "report_generator"
        assert reports[0].generator_version == "1.0"
        assert reports[0].generation_error == True
        assert reports[0].report_group_uuid == report_groups[0].report_group_uuid

        report_statuses = self.session.query(ReportStatus).all()

        assert len(report_statuses) == 2

        report_statuses.sort(key=lambda x:x.time_stamp)

        assert report_statuses[-1].status == rboa_engine.exit_codes["FILE_DOES_NOT_EXIST"]["status"]
        assert report_statuses[-1].log == "The report with path /eboa/src/tests/html_inputs/non_existent_file.html does not exist"
        assert report_statuses[0].report_uuid == reports[0].report_uuid
        
        alert_groups = self.session.query(AlertGroup).all()

        assert len(alert_groups) == 1

        assert alert_groups[0].name == "alert_group"

        alerts = self.session.query(Alert).all()

        assert len(alerts) == 3

        alerts.sort(key=lambda x:x.name)
        assert alerts[0].name == "alert_name1"

        report_alerts = self.session.query(ReportAlert).all()

        assert len(report_alerts) == 0


    def test_insert_same_report_twice(self):

        filename = "report.html"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/html_inputs/" + filename
        
        data = {"operations": [{
            "mode": "insert",
            "report": {"name": filename,
                       "group": "report_group",
                       "group_description": "Group of reports for testing",
                       "path": file_path,
                       "compress": "true",
                       "generation_mode": "MANUAL",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "triggering_time": "2018-07-05T02:07:03",
                       "generation_start": "2018-07-05T02:07:10",
                       "generation_stop": "2018-07-05T02:15:10",
                       "generator": "report_generator",
                       "generator_version": "1.0",
                       "values": [{"name": "VALUES",
                                   "type": "object",
                                   "values": [
                                       {"type": "text",
                                        "name": "TEXT",
                                        "value": "TEXT"},
                                       {"type": "boolean",
                                        "name": "BOOLEAN",
                                        "value": "true"},
                                       {"type": "double",
                                        "name": "DOUBLE",
                                        "value": "0.9"},
                                       {"type": "timestamp",
                                        "name": "TIMESTAMP",
                                        "value": "20180712T00:00:00"},
                                       {"type": "geometry",
                                        "name": "GEOMETRY",
                                        "value": "29.012974905944 -118.33483458667 28.8650301641571 -118.372028380632 28.7171766138274 -118.409121707686 28.5693112139334 -118.44612300623 28.4213994944367 -118.483058731035 28.2734085660472 -118.519970531113 28.1253606038163 -118.556849863134 27.9772541759126 -118.593690316835 27.8291247153939 -118.630472520505 27.7544158362332 -118.64900551674 27.7282373644786 -118.48032600682 27.7015162098732 -118.314168792268 27.6742039940042 -118.150246300849 27.6462511775992 -117.98827485961 27.6176070520608 -117.827974178264 27.5882197156561 -117.669066835177 27.5580360448116 -117.511277813618 27.5270016492436 -117.354334035359 27.4950608291016 -117.197963963877 27.4621565093409 -117.041897175848 27.4282301711374 -116.885863967864 27.3932217651372 -116.729594956238 27.3570696128269 -116.572820673713 27.3197103000253 -116.415271199941 27.2810785491022 -116.256675748617 27.241107085821 -116.09676229722 27.1997272484913 -115.935260563566 27.1524952198729 -115.755436839005 27.2270348347386 -115.734960009089 27.3748346522356 -115.694299254844 27.5226008861849 -115.653563616829 27.6702779354428 -115.612760542177 27.8178690071708 -115.571901649363 27.9653506439026 -115.531000691074 28.1127600020619 -115.490011752733 28.2601469756437 -115.44890306179 28.4076546372628 -115.407649898021 28.455192866856 -115.589486631416 28.4968374106496 -115.752807970928 28.5370603096399 -115.91452902381 28.575931293904 -116.074924211465 28.6135193777855 -116.234273707691 28.6498895688451 -116.392847129762 28.6851057860975 -116.550917254638 28.7192301322012 -116.708756374584 28.752323018501 -116.866636764481 28.7844432583843 -117.024831047231 28.8156481533955 -117.183612605748 28.8459935678779 -117.343255995623 28.8755339855462 -117.504037348554 28.9043225601122 -117.666234808407 28.9324111491586 -117.830128960026 28.9598503481156 -117.996003330616 28.9866878706574 -118.164136222141 29.012974905944 -118.33483458667"}
                                   ]
                       }]
            },
            "alerts": [{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name1",
                    "severity": "critical",
                    "description": "Alert description",
                    "group": "alert_group"
                }
            },{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name2",
                    "severity": "warning",
                    "description": "Alert description",
                    "group": "alert_group"
                }
            },{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name3",
                    "severity": "critical",
                    "description": "Alert description",
                    "group": "alert_group"
                }
            }]
        }]
        }
        exit_status = self.engine_rboa.treat_data(data, filename)

        assert exit_status[0]["status"] == rboa_engine.exit_codes["OK"]["status"]

        exit_status = self.engine_rboa.treat_data(data, filename)

        assert exit_status[0]["status"] == rboa_engine.exit_codes["OK"]["status"]
