"""
Automated tests for the export submodule

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import unittest

# Import engine of the DDBB
import eboa.engine.engine as eboa_engine
from eboa.engine.engine import Engine
from eboa.engine.query import Query

# Import export
from eboa.engine import export

# Import EBOA errors
from eboa.engine.errors import ErrorParsingParameters

class TestExport(unittest.TestCase):
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

    def test_export_events_wrong_structure(self):
        """
        Method to test the export_events function with wrong structure argument
        """

        test_success = False
        structure = "not_a_dict"
        try:
            export.export_events(structure, [])
        except ErrorParsingParameters:
            test_success = True
        # end try

        assert test_success

    def test_export_events_wrong_group(self):
        """
        Method to test the export_events function with wrong group argument
        """

        test_success = False
        structure = {}
        group = {}
        try:
            export.export_events(structure, [], group = group)
        except ErrorParsingParameters:
            test_success = True
        # end try

        assert test_success

    def test_export_events_wrong_include_ers(self):
        """
        Method to test the export_events function with wrong include_ers argument
        """

        test_success = False
        structure = {}
        group = {}
        include_ers = "not_bool"
        try:
            export.export_events(structure, [], group = group, include_ers = include_ers)
        except ErrorParsingParameters:
            test_success = True
        # end try

        assert test_success

    def test_export_events_wrong_include_annotations(self):
        """
        Method to test the export_events function with wrong include_annotations argument
        """

        test_success = False
        structure = {}
        group = {}
        include_annotations = "not_bool"
        try:
            export.export_events(structure, [], group = group, include_annotations = include_annotations)
        except ErrorParsingParameters:
            test_success = True
        # end try

        assert test_success

    def test_export_events(self):
        """
        Method to test the export_events function with no optional arguments
        """

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.xml",
                       "reception_time": "2018-01-01T00:00:00",
                       "generation_time": "2018-01-01T00:00:00",
                       "validity_start": "2018-01-01T00:00:00",
                       "validity_stop": "2018-01-02T00:00:00",
                       "priority": 30},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-01-01T04:00:00",
                "stop": "2018-01-01T05:00:00",
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
                                      "value": "29.012974905944 -118.33483458667 28.8650301641571 -118.372028380632 28.7171766138274 -118.409121707686 28.5693112139334 -118.44612300623 28.4213994944367 -118.483058731035 28.2734085660472 -118.519970531113 28.1253606038163 -118.556849863134 27.9772541759126 -118.593690316835 27.8291247153939 -118.630472520505 27.7544158362332 -118.64900551674 27.7282373644786 -118.48032600682 27.7015162098732 -118.314168792268 27.6742039940042 -118.150246300849 27.6462511775992 -117.98827485961 27.6176070520608 -117.827974178264 27.5882197156561 -117.669066835177 27.5580360448116 -117.511277813618 27.5270016492436 -117.354334035359 27.4950608291016 -117.197963963877 27.4621565093409 -117.041897175848 27.4282301711374 -116.885863967864 27.3932217651372 -116.729594956238 27.3570696128269 -116.572820673713 27.3197103000253 -116.415271199941 27.2810785491022 -116.256675748617 27.241107085821 -116.09676229722 27.1997272484913 -115.935260563566 27.1524952198729 -115.755436839005 27.2270348347386 -115.734960009089 27.3748346522356 -115.694299254844 27.5226008861849 -115.653563616829 27.6702779354428 -115.612760542177 27.8178690071708 -115.571901649363 27.9653506439026 -115.531000691074 28.1127600020619 -115.490011752733 28.2601469756437 -115.44890306179 28.4076546372628 -115.407649898021 28.455192866856 -115.589486631416 28.4968374106496 -115.752807970928 28.5370603096399 -115.91452902381 28.575931293904 -116.074924211465 28.6135193777855 -116.234273707691 28.6498895688451 -116.392847129762 28.6851057860975 -116.550917254638 28.7192301322012 -116.708756374584 28.752323018501 -116.866636764481 28.7844432583843 -117.024831047231 28.8156481533955 -117.183612605748 28.8459935678779 -117.343255995623 28.8755339855462 -117.504037348554 28.9043225601122 -117.666234808407 28.9324111491586 -117.830128960026 28.9598503481156 -117.996003330616 28.9866878706574 -118.164136222141 29.012974905944 -118.33483458667"}
                                 ]
                                 }
                            ]
                            }
                           ],
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
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {
                    "name": "ANNOTATION_CNF",
                    "system": "SYSTEM"
                },
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
                                      "value": "29.012974905944 -118.33483458667 28.8650301641571 -118.372028380632 28.7171766138274 -118.409121707686 28.5693112139334 -118.44612300623 28.4213994944367 -118.483058731035 28.2734085660472 -118.519970531113 28.1253606038163 -118.556849863134 27.9772541759126 -118.593690316835 27.8291247153939 -118.630472520505 27.7544158362332 -118.64900551674 27.7282373644786 -118.48032600682 27.7015162098732 -118.314168792268 27.6742039940042 -118.150246300849 27.6462511775992 -117.98827485961 27.6176070520608 -117.827974178264 27.5882197156561 -117.669066835177 27.5580360448116 -117.511277813618 27.5270016492436 -117.354334035359 27.4950608291016 -117.197963963877 27.4621565093409 -117.041897175848 27.4282301711374 -116.885863967864 27.3932217651372 -116.729594956238 27.3570696128269 -116.572820673713 27.3197103000253 -116.415271199941 27.2810785491022 -116.256675748617 27.241107085821 -116.09676229722 27.1997272484913 -115.935260563566 27.1524952198729 -115.755436839005 27.2270348347386 -115.734960009089 27.3748346522356 -115.694299254844 27.5226008861849 -115.653563616829 27.6702779354428 -115.612760542177 27.8178690071708 -115.571901649363 27.9653506439026 -115.531000691074 28.1127600020619 -115.490011752733 28.2601469756437 -115.44890306179 28.4076546372628 -115.407649898021 28.455192866856 -115.589486631416 28.4968374106496 -115.752807970928 28.5370603096399 -115.91452902381 28.575931293904 -116.074924211465 28.6135193777855 -116.234273707691 28.6498895688451 -116.392847129762 28.6851057860975 -116.550917254638 28.7192301322012 -116.708756374584 28.752323018501 -116.866636764481 28.7844432583843 -117.024831047231 28.8156481533955 -117.183612605748 28.8459935678779 -117.343255995623 28.8755339855462 -117.504037348554 28.9043225601122 -117.666234808407 28.9324111491586 -117.830128960026 28.9598503481156 -117.996003330616 28.9866878706574 -118.164136222141 29.012974905944 -118.33483458667"}
                                 ]
                                 }
                            ]
                            }
                           ],
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
            }]
        }]}

        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.query_eboa.get_events()

        assert len(events) == 1

        structure = {}
        export.export_events(structure, events)
        
        assert structure == {
            "events": {
                str(events[0].event_uuid): {
                    "event_uuid": str(events[0].event_uuid),
                    "start": "2018-01-01T04:00:00",
                    "stop": "2018-01-01T05:00:00",
                    "ingestion_time": events[0].ingestion_time.isoformat(),
                    "gauge": {"gauge_uuid": str(events[0].gauge.gauge_uuid),
                              "dim_signature": "dim_signature",
                              "name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "description": None},
                    "source": {"source_uuid": str(events[0].source.source_uuid),
                               "name": "source1.xml"},
                    "values": [{"name": "VALUES",
                                "type": "object",
                                "values": [
                                    {"name": "TEXT",
                                     "type": "text",
                                     "value": "TEXT"},
                                    {"name": "BOOLEAN",
                                     "type": "boolean",
                                     "value": "True"},
                                    {"name": "BOOLEAN2",
                                     "type": "boolean",
                                     "value": "False"},
                                    {"name": "DOUBLE",
                                     "type": "double",
                                     "value": "0.9"},
                                    {"name": "TIMESTAMP",
                                     "type": "timestamp",
                                     "value": "2018-07-12T00:00:00"},
                                    {"name": "VALUES2",
                                     "type": "object",
                                     "values": [{"name": "GEOMETRY",
                                                 "type": "object",

                                                 "type": "geometry",
                                                 "value": "POLYGON ((29.012974905944 -118.33483458667, 28.8650301641571 -118.372028380632, 28.7171766138274 -118.409121707686, 28.5693112139334 -118.44612300623, 28.4213994944367 -118.483058731035, 28.2734085660472 -118.519970531113, 28.1253606038163 -118.556849863134, 27.9772541759126 -118.593690316835, 27.8291247153939 -118.630472520505, 27.7544158362332 -118.64900551674, 27.7282373644786 -118.48032600682, 27.7015162098732 -118.314168792268, 27.6742039940042 -118.150246300849, 27.6462511775992 -117.98827485961, 27.6176070520608 -117.827974178264, 27.5882197156561 -117.669066835177, 27.5580360448116 -117.511277813618, 27.5270016492436 -117.354334035359, 27.4950608291016 -117.197963963877, 27.4621565093409 -117.041897175848, 27.4282301711374 -116.885863967864, 27.3932217651372 -116.729594956238, 27.3570696128269 -116.572820673713, 27.3197103000253 -116.415271199941, 27.2810785491022 -116.256675748617, 27.241107085821 -116.09676229722, 27.1997272484913 -115.935260563566, 27.1524952198729 -115.755436839005, 27.2270348347386 -115.734960009089, 27.3748346522356 -115.694299254844, 27.5226008861849 -115.653563616829, 27.6702779354428 -115.612760542177, 27.8178690071708 -115.571901649363, 27.9653506439026 -115.531000691074, 28.1127600020619 -115.490011752733, 28.2601469756437 -115.44890306179, 28.4076546372628 -115.407649898021, 28.455192866856 -115.589486631416, 28.4968374106496 -115.752807970928, 28.5370603096399 -115.91452902381, 28.575931293904 -116.074924211465, 28.6135193777855 -116.234273707691, 28.6498895688451 -116.392847129762, 28.6851057860975 -116.550917254638, 28.7192301322012 -116.708756374584, 28.752323018501 -116.866636764481, 28.7844432583843 -117.024831047231, 28.8156481533955 -117.183612605748, 28.8459935678779 -117.343255995623, 28.8755339855462 -117.504037348554, 28.9043225601122 -117.666234808407, 28.9324111491586 -117.830128960026, 28.9598503481156 -117.996003330616, 28.9866878706574 -118.164136222141, 29.012974905944 -118.33483458667))"}
                                                ]
                                     }
                                ]
                                }
                               ],
                    "links_to_me": [],
                    "alerts": [
                        {"event_alert_uuid": str(events[0].alerts[0].event_alert_uuid),
                         "message": "Alert message",
                         "validated": None,
                         "ingestion_time": events[0].alerts[0].ingestion_time.isoformat(),
                         "generator": "test",
                         "notified": None,
                         "solved": None,
                         "solved_time": None,
                         "notification_time": events[0].alerts[0].notification_time.isoformat(),
                         "justification": None,
                         "definition": {"alert_uuid": str(events[0].alerts[0].alertDefinition.alert_uuid),
                                        "name": "alert_name1",
                                        "severity": 4,
                                        "description": "Alert description",
                                        "group": "alert_group"},
                         "event_uuid": str(events[0].event_uuid)
                         }
                    ],
                    "explicit_reference": {"uuid": str(events[0].explicitRef.explicit_ref_uuid),
                                           "name": "EXPLICIT_REFERENCE"}
                }
            },
            "explicit_references": {
                str(events[0].explicitRef.explicit_ref_uuid): {
                    "explicit_ref_uuid": str(events[0].explicitRef.explicit_ref_uuid),
                    "ingestion_time": events[0].explicitRef.ingestion_time.isoformat(),
                    "explicit_ref": "EXPLICIT_REFERENCE",
                    "alerts": [],
                    "annotations": [
                        {"annotation_uuid": str(events[0].explicitRef.annotations[0].annotation_uuid),
                         "explicit_reference": {"uuid": str(events[0].explicitRef.explicit_ref_uuid),
                                                "name": "EXPLICIT_REFERENCE"},
                         "ingestion_time": events[0].explicitRef.annotations[0].ingestion_time.isoformat(),
                         "configuration": {"uuid": str(events[0].explicitRef.annotations[0].annotationCnf.annotation_cnf_uuid),
                                           "dim_signature": "dim_signature",
                                           "name": "ANNOTATION_CNF",
                                           "system": "SYSTEM",
                                           "description": None},
                         "source": {"source_uuid": str(events[0].source.source_uuid),
                                    "name": "source1.xml"},
                         "values": [{"name": "VALUES",
                                     "type": "object",
                                     "values": [
                                         {"name": "TEXT",
                                          "type": "text",
                                          "value": "TEXT"},
                                         {"name": "BOOLEAN",
                                          "type": "boolean",
                                          "value": "True"},
                                         {"name": "BOOLEAN2",
                                          "type": "boolean",
                                          "value": "False"},
                                         {"name": "DOUBLE",
                                          "type": "double",
                                          "value": "0.9"},
                                         {"name": "TIMESTAMP",
                                          "type": "timestamp",
                                          "value": "2018-07-12T00:00:00"},
                                         {"name": "VALUES2",
                                          "type": "object",
                                          "values": [{"name": "GEOMETRY",
                                                      "type": "object",

                                                 "type": "geometry",
                                                      "value": "POLYGON ((29.012974905944 -118.33483458667, 28.8650301641571 -118.372028380632, 28.7171766138274 -118.409121707686, 28.5693112139334 -118.44612300623, 28.4213994944367 -118.483058731035, 28.2734085660472 -118.519970531113, 28.1253606038163 -118.556849863134, 27.9772541759126 -118.593690316835, 27.8291247153939 -118.630472520505, 27.7544158362332 -118.64900551674, 27.7282373644786 -118.48032600682, 27.7015162098732 -118.314168792268, 27.6742039940042 -118.150246300849, 27.6462511775992 -117.98827485961, 27.6176070520608 -117.827974178264, 27.5882197156561 -117.669066835177, 27.5580360448116 -117.511277813618, 27.5270016492436 -117.354334035359, 27.4950608291016 -117.197963963877, 27.4621565093409 -117.041897175848, 27.4282301711374 -116.885863967864, 27.3932217651372 -116.729594956238, 27.3570696128269 -116.572820673713, 27.3197103000253 -116.415271199941, 27.2810785491022 -116.256675748617, 27.241107085821 -116.09676229722, 27.1997272484913 -115.935260563566, 27.1524952198729 -115.755436839005, 27.2270348347386 -115.734960009089, 27.3748346522356 -115.694299254844, 27.5226008861849 -115.653563616829, 27.6702779354428 -115.612760542177, 27.8178690071708 -115.571901649363, 27.9653506439026 -115.531000691074, 28.1127600020619 -115.490011752733, 28.2601469756437 -115.44890306179, 28.4076546372628 -115.407649898021, 28.455192866856 -115.589486631416, 28.4968374106496 -115.752807970928, 28.5370603096399 -115.91452902381, 28.575931293904 -116.074924211465, 28.6135193777855 -116.234273707691, 28.6498895688451 -116.392847129762, 28.6851057860975 -116.550917254638, 28.7192301322012 -116.708756374584, 28.752323018501 -116.866636764481, 28.7844432583843 -117.024831047231, 28.8156481533955 -117.183612605748, 28.8459935678779 -117.343255995623, 28.8755339855462 -117.504037348554, 28.9043225601122 -117.666234808407, 28.9324111491586 -117.830128960026, 28.9598503481156 -117.996003330616, 28.9866878706574 -118.164136222141, 29.012974905944 -118.33483458667))"}
                                                     ]
                                          }
                                     ]
                                     }
                                    ],
                         "alerts": [
                             {"annotation_alert_uuid": str(events[0].explicitRef.annotations[0].alerts[0].annotation_alert_uuid),
                              "message": "Alert message",
                              "validated": None,
                              "ingestion_time": events[0].explicitRef.annotations[0].alerts[0].ingestion_time.isoformat(),
                              "generator": "test",
                              "notified": None,
                              "solved": None,
                              "solved_time": None,
                              "notification_time": events[0].explicitRef.annotations[0].alerts[0].notification_time.isoformat(),
                              "justification": None,
                              "definition": {"alert_uuid": str(events[0].explicitRef.annotations[0].alerts[0].alertDefinition.alert_uuid),
                                             "name": "alert_name1",
                                             "severity": 4,
                                             "description": "Alert description",
                                             "group": "alert_group"},
                              "annotation_uuid": str(events[0].explicitRef.annotations[0].annotation_uuid)
                              }
                         ],
                         "explicit_reference": {"uuid": str(events[0].explicitRef.annotations[0].explicitRef.explicit_ref_uuid),
                                                "name": "EXPLICIT_REFERENCE"}
                         }
                    ]
                }
            }
        }

    def test_export_events_no_events(self):
        """
        Method to test the export_events function with no optional arguments with no events in DDBB
        """

        structure = {}
        export.export_events(structure, [], group = "event_group")
        
        assert structure == {
            "event_groups": {
                "event_group": []
            },
            "events": {
            }
        }

    def test_export_events_with_links(self):
        """
        Method to test the export_events function with links
        """

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source1.xml",
                            "reception_time": "2018-01-01T00:00:00",
                            "generation_time": "2018-01-01T00:00:00",
                            "validity_start": "2018-01-01T00:00:00",
                            "validity_stop": "2018-01-02T00:00:00",
                            "priority": 30},
                 "events": [{
                     "link_ref": "EVENT_1",
                     "explicit_reference": "EXPLICIT_REFERENCE",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "SIMPLE_UPDATE"},
                     "start": "2018-01-01T04:00:00",
                     "stop": "2018-01-01T05:00:00",
                     "links": [{
                         "link": "EVENT_2",
                         "link_mode": "by_ref",
                         "name": "EVENT_LINK",
                         "back_ref": "EVENT_LINK_BACK_REF"
                     }]
                 },{
                     "link_ref": "EVENT_2",
                     "explicit_reference": "EXPLICIT_REFERENCE",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "SIMPLE_UPDATE"},
                     "start": "2018-01-01T05:00:00",
                     "stop": "2018-01-01T06:00:00"
                 }]
        }]}

        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.query_eboa.get_events(order_by={"field": "start", "descending": False})

        assert len(events) == 2
        
        structure = {}
        export.export_events(structure, events, group = "event_group")

        assert structure == {
            "event_groups": {
                "event_group": [str(events[0].event_uuid), str(events[1].event_uuid)]
            },
            "events": {
                str(events[0].event_uuid): {
                    "event_uuid": str(events[0].event_uuid),
                    "start": "2018-01-01T04:00:00",
                    "stop": "2018-01-01T05:00:00",
                    "ingestion_time": events[0].ingestion_time.isoformat(),
                    "gauge": {"gauge_uuid": str(events[0].gauge.gauge_uuid),
                              "dim_signature": "dim_signature",
                              "name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "description": None},
                    "source": {"source_uuid": str(events[0].source.source_uuid),
                               "name": "source1.xml"},
                    "values": [],
                    "links_to_me": [{"event_uuid_link": str(events[1].event_uuid),
                                     "name": "EVENT_LINK_BACK_REF"}],
                    "alerts": [],
                    "explicit_reference": {"uuid": str(events[0].explicitRef.explicit_ref_uuid),
                                           "name": "EXPLICIT_REFERENCE"}},
                str(events[1].event_uuid): {
                    "event_uuid": str(events[1].event_uuid),
                    "start": "2018-01-01T05:00:00",
                    "stop": "2018-01-01T06:00:00",
                    "ingestion_time": events[1].ingestion_time.isoformat(),
                    "gauge": {"gauge_uuid": str(events[1].gauge.gauge_uuid),
                              "dim_signature": "dim_signature",
                              "name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "description": None},
                    "source": {"source_uuid": str(events[1].source.source_uuid),
                               "name": "source1.xml"},
                    "values": [],
                    "links_to_me": [{"event_uuid_link": str(events[0].event_uuid),
                                     "name": "EVENT_LINK"}],
                    "alerts": [],
                    "explicit_reference": {"uuid": str(events[1].explicitRef.explicit_ref_uuid),
                                           "name": "EXPLICIT_REFERENCE"}}
            },
            "explicit_references": {
                str(events[0].explicitRef.explicit_ref_uuid): {
                    "explicit_ref_uuid": str(events[0].explicitRef.explicit_ref_uuid),
                    "ingestion_time": events[0].explicitRef.ingestion_time.isoformat(),
                    "explicit_ref": "EXPLICIT_REFERENCE",
                    "alerts": [],
                    "annotations": []
                }
            }
        }

    def test_export_events_with_group(self):
        """
        Method to test the export_events function with group
        """

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source1.xml",
                            "reception_time": "2018-01-01T00:00:00",
                            "generation_time": "2018-01-01T00:00:00",
                            "validity_start": "2018-01-01T00:00:00",
                            "validity_stop": "2018-01-02T00:00:00",
                            "priority": 30},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "SIMPLE_UPDATE"},
                     "start": "2018-01-01T04:00:00",
                     "stop": "2018-01-01T05:00:00"
                 }]
        }]}

        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.query_eboa.get_events()

        assert len(events) == 1
        
        structure = {}
        export.export_events(structure, events, group = "event_group")

        assert structure == {
            "event_groups": {
                "event_group": [str(events[0].event_uuid)]
            },
            "events": {
                str(events[0].event_uuid): {
                    "event_uuid": str(events[0].event_uuid),
                    "start": "2018-01-01T04:00:00",
                    "stop": "2018-01-01T05:00:00",
                    "ingestion_time": events[0].ingestion_time.isoformat(),
                    "gauge": {"gauge_uuid": str(events[0].gauge.gauge_uuid),
                              "dim_signature": "dim_signature",
                              "name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "description": None},
                    "source": {"source_uuid": str(events[0].source.source_uuid),
                               "name": "source1.xml"},
                    "values": [],
                    "links_to_me": [],
                    "alerts": [],
                    "explicit_reference": {"uuid": str(events[0].explicitRef.explicit_ref_uuid),
                                           "name": "EXPLICIT_REFERENCE"}}},
            "explicit_references": {
                str(events[0].explicitRef.explicit_ref_uuid): {
                    "explicit_ref_uuid": str(events[0].explicitRef.explicit_ref_uuid),
                    "ingestion_time": events[0].explicitRef.ingestion_time.isoformat(),
                    "explicit_ref": "EXPLICIT_REFERENCE",
                    "alerts": [],
                    "annotations": []
                }
            }
        }

    def test_export_events_without_explicit_references(self):
        """
        Method to test the export_events function without explicit_references
        """

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source1.xml",
                            "reception_time": "2018-01-01T00:00:00",
                            "generation_time": "2018-01-01T00:00:00",
                            "validity_start": "2018-01-01T00:00:00",
                            "validity_stop": "2018-01-02T00:00:00",
                            "priority": 30},
                 "events": [{
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "SIMPLE_UPDATE"},
                     "start": "2018-01-01T04:00:00",
                     "stop": "2018-01-01T05:00:00"
                 }]
        }]}

        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.query_eboa.get_events()

        assert len(events) == 1
        
        structure = {}
        export.export_events(structure, events, include_ers = False)

        assert structure == {
            "events": {
                str(events[0].event_uuid): {
                    "event_uuid": str(events[0].event_uuid),
                    "start": "2018-01-01T04:00:00",
                    "stop": "2018-01-01T05:00:00",
                    "ingestion_time": events[0].ingestion_time.isoformat(),
                    "gauge": {"gauge_uuid": str(events[0].gauge.gauge_uuid),
                              "dim_signature": "dim_signature",
                              "name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "description": None},
                    "source": {"source_uuid": str(events[0].source.source_uuid),
                               "name": "source1.xml"},
                    "values": [],
                    "links_to_me": [],
                    "alerts": [],
                }
            }
        }

    def test_export_annotations_wrong_structure(self):
        """
        Method to test the export_annotations function with wrong structure argument
        """

        test_success = False
        structure = "not_a_dict"
        try:
            export.export_annotations(structure, [])
        except ErrorParsingParameters:
            test_success = True
        # end try

        assert test_success

    def test_export_annotations_wrong_group(self):
        """
        Method to test the export_annotations function with wrong group argument
        """

        test_success = False
        structure = {}
        group = {}
        try:
            export.export_annotations(structure, [], group = group)
        except ErrorParsingParameters:
            test_success = True
        # end try

        assert test_success

    def test_export_annotations_wrong_include_ers(self):
        """
        Method to test the export_annotations function with wrong include_ers argument
        """

        test_success = False
        structure = {}
        group = {}
        include_ers = "not_bool"
        try:
            export.export_annotations(structure, [], group = group, include_ers = include_ers)
        except ErrorParsingParameters:
            test_success = True
        # end try

        assert test_success

    def test_export_annotations_wrong_include_annotations(self):
        """
        Method to test the export_annotations function with wrong include_annotations argument
        """

        test_success = False
        structure = {}
        group = {}
        include_annotations = "not_bool"
        try:
            export.export_annotations(structure, [], group = group, include_annotations = include_annotations)
        except ErrorParsingParameters:
            test_success = True
        # end try

        assert test_success

    def test_export_annotations(self):
        """
        Method to test the export_annotations function with no optional arguments
        """

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.xml",
                       "reception_time": "2018-01-01T00:00:00",
                       "generation_time": "2018-01-01T00:00:00",
                       "validity_start": "2018-01-01T00:00:00",
                       "validity_stop": "2018-01-02T00:00:00",
                       "priority": 30},
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {
                    "name": "ANNOTATION_CNF",
                    "system": "SYSTEM"
                },
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
                                      "value": "29.012974905944 -118.33483458667 28.8650301641571 -118.372028380632 28.7171766138274 -118.409121707686 28.5693112139334 -118.44612300623 28.4213994944367 -118.483058731035 28.2734085660472 -118.519970531113 28.1253606038163 -118.556849863134 27.9772541759126 -118.593690316835 27.8291247153939 -118.630472520505 27.7544158362332 -118.64900551674 27.7282373644786 -118.48032600682 27.7015162098732 -118.314168792268 27.6742039940042 -118.150246300849 27.6462511775992 -117.98827485961 27.6176070520608 -117.827974178264 27.5882197156561 -117.669066835177 27.5580360448116 -117.511277813618 27.5270016492436 -117.354334035359 27.4950608291016 -117.197963963877 27.4621565093409 -117.041897175848 27.4282301711374 -116.885863967864 27.3932217651372 -116.729594956238 27.3570696128269 -116.572820673713 27.3197103000253 -116.415271199941 27.2810785491022 -116.256675748617 27.241107085821 -116.09676229722 27.1997272484913 -115.935260563566 27.1524952198729 -115.755436839005 27.2270348347386 -115.734960009089 27.3748346522356 -115.694299254844 27.5226008861849 -115.653563616829 27.6702779354428 -115.612760542177 27.8178690071708 -115.571901649363 27.9653506439026 -115.531000691074 28.1127600020619 -115.490011752733 28.2601469756437 -115.44890306179 28.4076546372628 -115.407649898021 28.455192866856 -115.589486631416 28.4968374106496 -115.752807970928 28.5370603096399 -115.91452902381 28.575931293904 -116.074924211465 28.6135193777855 -116.234273707691 28.6498895688451 -116.392847129762 28.6851057860975 -116.550917254638 28.7192301322012 -116.708756374584 28.752323018501 -116.866636764481 28.7844432583843 -117.024831047231 28.8156481533955 -117.183612605748 28.8459935678779 -117.343255995623 28.8755339855462 -117.504037348554 28.9043225601122 -117.666234808407 28.9324111491586 -117.830128960026 28.9598503481156 -117.996003330616 28.9866878706574 -118.164136222141 29.012974905944 -118.33483458667"}
                                 ]
                                 }
                            ]
                            }
                           ],
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
            }]
        }]}

        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        annotations = self.query_eboa.get_annotations()

        assert len(annotations) == 1

        structure = {}
        export.export_annotations(structure, annotations)
        
        assert structure == {
            "annotations": {
                str(annotations[0].explicitRef.annotations[0].annotation_uuid): {
                    "annotation_uuid": str(annotations[0].explicitRef.annotations[0].annotation_uuid),
                    "explicit_reference": {"uuid": str(annotations[0].explicitRef.explicit_ref_uuid),
                                           "name": "EXPLICIT_REFERENCE"},
                    "ingestion_time": annotations[0].explicitRef.annotations[0].ingestion_time.isoformat(),
                    "configuration": {"uuid": str(annotations[0].explicitRef.annotations[0].annotationCnf.annotation_cnf_uuid),
                                      "dim_signature": "dim_signature",
                                      "name": "ANNOTATION_CNF",
                                      "system": "SYSTEM",
                                      "description": None},
                    "source": {"source_uuid": str(annotations[0].source.source_uuid),
                               "name": "source1.xml"},
                    "values": [{"name": "VALUES",
                                "type": "object",
                                "values": [
                                    {"name": "TEXT",
                                     "type": "text",
                                     "value": "TEXT"},
                                    {"name": "BOOLEAN",
                                     "type": "boolean",
                                     "value": "True"},
                                    {"name": "BOOLEAN2",
                                     "type": "boolean",
                                     "value": "False"},
                                    {"name": "DOUBLE",
                                     "type": "double",
                                     "value": "0.9"},
                                    {"name": "TIMESTAMP",
                                     "type": "timestamp",
                                     "value": "2018-07-12T00:00:00"},
                                    {"name": "VALUES2",
                                     "type": "object",
                                     "values": [{"name": "GEOMETRY",
                                                 "type": "object",

                                                 "type": "geometry",
                                                 "value": "POLYGON ((29.012974905944 -118.33483458667, 28.8650301641571 -118.372028380632, 28.7171766138274 -118.409121707686, 28.5693112139334 -118.44612300623, 28.4213994944367 -118.483058731035, 28.2734085660472 -118.519970531113, 28.1253606038163 -118.556849863134, 27.9772541759126 -118.593690316835, 27.8291247153939 -118.630472520505, 27.7544158362332 -118.64900551674, 27.7282373644786 -118.48032600682, 27.7015162098732 -118.314168792268, 27.6742039940042 -118.150246300849, 27.6462511775992 -117.98827485961, 27.6176070520608 -117.827974178264, 27.5882197156561 -117.669066835177, 27.5580360448116 -117.511277813618, 27.5270016492436 -117.354334035359, 27.4950608291016 -117.197963963877, 27.4621565093409 -117.041897175848, 27.4282301711374 -116.885863967864, 27.3932217651372 -116.729594956238, 27.3570696128269 -116.572820673713, 27.3197103000253 -116.415271199941, 27.2810785491022 -116.256675748617, 27.241107085821 -116.09676229722, 27.1997272484913 -115.935260563566, 27.1524952198729 -115.755436839005, 27.2270348347386 -115.734960009089, 27.3748346522356 -115.694299254844, 27.5226008861849 -115.653563616829, 27.6702779354428 -115.612760542177, 27.8178690071708 -115.571901649363, 27.9653506439026 -115.531000691074, 28.1127600020619 -115.490011752733, 28.2601469756437 -115.44890306179, 28.4076546372628 -115.407649898021, 28.455192866856 -115.589486631416, 28.4968374106496 -115.752807970928, 28.5370603096399 -115.91452902381, 28.575931293904 -116.074924211465, 28.6135193777855 -116.234273707691, 28.6498895688451 -116.392847129762, 28.6851057860975 -116.550917254638, 28.7192301322012 -116.708756374584, 28.752323018501 -116.866636764481, 28.7844432583843 -117.024831047231, 28.8156481533955 -117.183612605748, 28.8459935678779 -117.343255995623, 28.8755339855462 -117.504037348554, 28.9043225601122 -117.666234808407, 28.9324111491586 -117.830128960026, 28.9598503481156 -117.996003330616, 28.9866878706574 -118.164136222141, 29.012974905944 -118.33483458667))"}
                                                ]
                                     }
                                ]
                                }
                               ],
                    "alerts": [
                        {"annotation_alert_uuid": str(annotations[0].alerts[0].annotation_alert_uuid),
                         "message": "Alert message",
                         "validated": None,
                         "ingestion_time": annotations[0].alerts[0].ingestion_time.isoformat(),
                         "generator": "test",
                         "notified": None,
                         "solved": None,
                         "solved_time": None,
                         "notification_time": annotations[0].alerts[0].notification_time.isoformat(),
                         "justification": None,
                         "definition": {"alert_uuid": str(annotations[0].alerts[0].alertDefinition.alert_uuid),
                                        "name": "alert_name1",
                                        "severity": 4,
                                        "description": "Alert description",
                                        "group": "alert_group"},
                         "annotation_uuid": str(annotations[0].annotation_uuid)
                         }
                    ],
                    "explicit_reference": {"uuid": str(annotations[0].explicitRef.explicit_ref_uuid),
                                           "name": "EXPLICIT_REFERENCE"}
                }
            },
            "explicit_references": {
                str(annotations[0].explicitRef.explicit_ref_uuid): {
                    "explicit_ref_uuid": str(annotations[0].explicitRef.explicit_ref_uuid),
                    "ingestion_time": annotations[0].explicitRef.ingestion_time.isoformat(),
                    "explicit_ref": "EXPLICIT_REFERENCE",
                    "alerts": [],
                    "annotations": [
                        {"annotation_uuid": str(annotations[0].explicitRef.annotations[0].annotation_uuid),
                         "explicit_reference": {"uuid": str(annotations[0].explicitRef.explicit_ref_uuid),
                                                "name": "EXPLICIT_REFERENCE"},
                         "ingestion_time": annotations[0].explicitRef.annotations[0].ingestion_time.isoformat(),
                         "configuration": {"uuid": str(annotations[0].explicitRef.annotations[0].annotationCnf.annotation_cnf_uuid),
                                           "dim_signature": "dim_signature",
                                           "name": "ANNOTATION_CNF",
                                           "system": "SYSTEM",
                                           "description": None},
                         "source": {"source_uuid": str(annotations[0].source.source_uuid),
                                    "name": "source1.xml"},
                         "values": [{"name": "VALUES",
                                     "type": "object",
                                     "values": [
                                         {"name": "TEXT",
                                          "type": "text",
                                          "value": "TEXT"},
                                         {"name": "BOOLEAN",
                                          "type": "boolean",
                                          "value": "True"},
                                         {"name": "BOOLEAN2",
                                          "type": "boolean",
                                          "value": "False"},
                                         {"name": "DOUBLE",
                                          "type": "double",
                                          "value": "0.9"},
                                         {"name": "TIMESTAMP",
                                          "type": "timestamp",
                                          "value": "2018-07-12T00:00:00"},
                                         {"name": "VALUES2",
                                          "type": "object",
                                          "values": [{"name": "GEOMETRY",
                                                      "type": "object",

                                                 "type": "geometry",
                                                      "value": "POLYGON ((29.012974905944 -118.33483458667, 28.8650301641571 -118.372028380632, 28.7171766138274 -118.409121707686, 28.5693112139334 -118.44612300623, 28.4213994944367 -118.483058731035, 28.2734085660472 -118.519970531113, 28.1253606038163 -118.556849863134, 27.9772541759126 -118.593690316835, 27.8291247153939 -118.630472520505, 27.7544158362332 -118.64900551674, 27.7282373644786 -118.48032600682, 27.7015162098732 -118.314168792268, 27.6742039940042 -118.150246300849, 27.6462511775992 -117.98827485961, 27.6176070520608 -117.827974178264, 27.5882197156561 -117.669066835177, 27.5580360448116 -117.511277813618, 27.5270016492436 -117.354334035359, 27.4950608291016 -117.197963963877, 27.4621565093409 -117.041897175848, 27.4282301711374 -116.885863967864, 27.3932217651372 -116.729594956238, 27.3570696128269 -116.572820673713, 27.3197103000253 -116.415271199941, 27.2810785491022 -116.256675748617, 27.241107085821 -116.09676229722, 27.1997272484913 -115.935260563566, 27.1524952198729 -115.755436839005, 27.2270348347386 -115.734960009089, 27.3748346522356 -115.694299254844, 27.5226008861849 -115.653563616829, 27.6702779354428 -115.612760542177, 27.8178690071708 -115.571901649363, 27.9653506439026 -115.531000691074, 28.1127600020619 -115.490011752733, 28.2601469756437 -115.44890306179, 28.4076546372628 -115.407649898021, 28.455192866856 -115.589486631416, 28.4968374106496 -115.752807970928, 28.5370603096399 -115.91452902381, 28.575931293904 -116.074924211465, 28.6135193777855 -116.234273707691, 28.6498895688451 -116.392847129762, 28.6851057860975 -116.550917254638, 28.7192301322012 -116.708756374584, 28.752323018501 -116.866636764481, 28.7844432583843 -117.024831047231, 28.8156481533955 -117.183612605748, 28.8459935678779 -117.343255995623, 28.8755339855462 -117.504037348554, 28.9043225601122 -117.666234808407, 28.9324111491586 -117.830128960026, 28.9598503481156 -117.996003330616, 28.9866878706574 -118.164136222141, 29.012974905944 -118.33483458667))"}
                                                     ]
                                          }
                                     ]
                                     }
                                    ],
                         "alerts": [
                             {"annotation_alert_uuid": str(annotations[0].alerts[0].annotation_alert_uuid),
                              "message": "Alert message",
                              "validated": None,
                              "ingestion_time": annotations[0].alerts[0].ingestion_time.isoformat(),
                              "generator": "test",
                              "notified": None,
                              "solved": None,
                              "solved_time": None,
                              "notification_time": annotations[0].alerts[0].notification_time.isoformat(),
                              "justification": None,
                              "definition": {"alert_uuid": str(annotations[0].alerts[0].alertDefinition.alert_uuid),
                                             "name": "alert_name1",
                                             "severity": 4,
                                             "description": "Alert description",
                                             "group": "alert_group"},
                              "annotation_uuid": str(annotations[0].annotation_uuid)
                              }
                         ],
                         "explicit_reference": {"uuid": str(annotations[0].explicitRef.explicit_ref_uuid),
                                                "name": "EXPLICIT_REFERENCE"}
                         }
                    ]
                }
            }
        }

    def test_export_annotations_with_group(self):
        """
        Method to test the export_annotations function with group
        """

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
            "source": {"name": "source1.xml",
                       "reception_time": "2018-01-01T00:00:00",
                       "generation_time": "2018-01-01T00:00:00",
                       "validity_start": "2018-01-01T00:00:00",
                       "validity_stop": "2018-01-02T00:00:00",
                       "priority": 30},
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {
                    "name": "ANNOTATION_CNF",
                    "system": "SYSTEM"
                }
            }]
        }]}

        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        annotations = self.query_eboa.get_annotations()

        assert len(annotations) == 1
        
        structure = {}
        export.export_annotations(structure, annotations, group = "annotation_group")

        assert structure == {
            "annotation_groups": {
                "annotation_group": [str(annotations[0].annotation_uuid)]
            },
            "annotations": {
                str(annotations[0].explicitRef.annotations[0].annotation_uuid): {
                    "annotation_uuid": str(annotations[0].explicitRef.annotations[0].annotation_uuid),
                    "explicit_reference": {"uuid": str(annotations[0].explicitRef.explicit_ref_uuid),
                                           "name": "EXPLICIT_REFERENCE"},
                    "ingestion_time": annotations[0].explicitRef.annotations[0].ingestion_time.isoformat(),
                    "configuration": {"uuid": str(annotations[0].explicitRef.annotations[0].annotationCnf.annotation_cnf_uuid),
                                      "dim_signature": "dim_signature",
                                      "name": "ANNOTATION_CNF",
                                      "system": "SYSTEM",
                                      "description": None},
                    "source": {"source_uuid": str(annotations[0].source.source_uuid),
                               "name": "source1.xml"},
                    "values": [],
                    "alerts": [],
                }
            },
            "explicit_references": {
                str(annotations[0].explicitRef.explicit_ref_uuid): {
                    "explicit_ref_uuid": str(annotations[0].explicitRef.explicit_ref_uuid),
                    "ingestion_time": annotations[0].explicitRef.ingestion_time.isoformat(),
                    "explicit_ref": "EXPLICIT_REFERENCE",
                    "alerts": [],
                    "annotations": [{
                        "annotation_uuid": str(annotations[0].explicitRef.annotations[0].annotation_uuid),
                        "explicit_reference": {"uuid": str(annotations[0].explicitRef.explicit_ref_uuid),
                                               "name": "EXPLICIT_REFERENCE"},
                        "ingestion_time": annotations[0].explicitRef.annotations[0].ingestion_time.isoformat(),
                        "configuration": {"uuid": str(annotations[0].explicitRef.annotations[0].annotationCnf.annotation_cnf_uuid),
                                          "dim_signature": "dim_signature",
                                          "name": "ANNOTATION_CNF",
                                          "system": "SYSTEM",
                                          "description": None},
                        "source": {"source_uuid": str(annotations[0].source.source_uuid),
                                   "name": "source1.xml"},
                        "values": [],
                        "alerts": [],
                    }]
                }
            }
        }
        
    def test_export_annotations_without_explicit_references(self):
        """
        Method to test the export_annotations function without explicit_references
        """

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
            "source": {"name": "source1.xml",
                       "reception_time": "2018-01-01T00:00:00",
                       "generation_time": "2018-01-01T00:00:00",
                       "validity_start": "2018-01-01T00:00:00",
                       "validity_stop": "2018-01-02T00:00:00",
                       "priority": 30},
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {
                    "name": "ANNOTATION_CNF",
                    "system": "SYSTEM"
                }
            }]
        }]}

        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        annotations = self.query_eboa.get_annotations()

        assert len(annotations) == 1
        
        structure = {}
        export.export_annotations(structure, annotations, include_ers = False, group = "annotation_group")

        assert structure == {
            "annotation_groups": {
                "annotation_group": [str(annotations[0].annotation_uuid)]
            },
            "annotations": {
                str(annotations[0].explicitRef.annotations[0].annotation_uuid): {
                    "annotation_uuid": str(annotations[0].explicitRef.annotations[0].annotation_uuid),
                    "explicit_reference": {"uuid": str(annotations[0].explicitRef.explicit_ref_uuid),
                                           "name": "EXPLICIT_REFERENCE"},
                    "ingestion_time": annotations[0].explicitRef.annotations[0].ingestion_time.isoformat(),
                    "configuration": {"uuid": str(annotations[0].explicitRef.annotations[0].annotationCnf.annotation_cnf_uuid),
                                      "dim_signature": "dim_signature",
                                      "name": "ANNOTATION_CNF",
                                      "system": "SYSTEM",
                                      "description": None},
                    "source": {"source_uuid": str(annotations[0].source.source_uuid),
                               "name": "source1.xml"},
                    "values": [],
                    "alerts": [],
                }
            }
        }

    def test_export_annotations_no_annotations(self):
        """
        Method to test the export_annotations function with no optional arguments with no annotations in DDBB
        """

        structure = {}
        export.export_annotations(structure, [], group = "annotation_group")
        
        assert structure == {
            "annotation_groups": {
                "annotation_group": []
            },
            "annotations": {
            }
        }
        
    def test_export_ers_wrong_structure(self):
        """
        Method to test the export_ers function with wrong structure argument
        """

        test_success = False
        structure = "not_a_dict"
        try:
            export.export_ers(structure, [])
        except ErrorParsingParameters:
            test_success = True
        # end try

        assert test_success

    def test_export_ers_wrong_group(self):
        """
        Method to test the export_ers function with wrong group argument
        """

        test_success = False
        structure = {}
        group = {}
        try:
            export.export_ers(structure, [], group = group)
        except ErrorParsingParameters:
            test_success = True
        # end try

        assert test_success

    def test_export_ers_wrong_include_annotations(self):
        """
        Method to test the export_ers function with wrong include_annotations argument
        """

        test_success = False
        structure = {}
        group = {}
        include_annotations = "not_bool"
        try:
            export.export_ers(structure, [], group = group, include_annotations = include_annotations)
        except ErrorParsingParameters:
            test_success = True
        # end try

        assert test_success

    def test_export_ers(self):
        """
        Method to test the export_ers function with no optional arguments
        """

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.xml",
                       "reception_time": "2018-01-01T00:00:00",
                       "generation_time": "2018-01-01T00:00:00",
                       "validity_start": "2018-01-01T00:00:00",
                       "validity_stop": "2018-01-02T00:00:00",
                       "priority": 30},
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {
                    "name": "ANNOTATION_CNF",
                    "system": "SYSTEM"
                },
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
                                      "value": "29.012974905944 -118.33483458667 28.8650301641571 -118.372028380632 28.7171766138274 -118.409121707686 28.5693112139334 -118.44612300623 28.4213994944367 -118.483058731035 28.2734085660472 -118.519970531113 28.1253606038163 -118.556849863134 27.9772541759126 -118.593690316835 27.8291247153939 -118.630472520505 27.7544158362332 -118.64900551674 27.7282373644786 -118.48032600682 27.7015162098732 -118.314168792268 27.6742039940042 -118.150246300849 27.6462511775992 -117.98827485961 27.6176070520608 -117.827974178264 27.5882197156561 -117.669066835177 27.5580360448116 -117.511277813618 27.5270016492436 -117.354334035359 27.4950608291016 -117.197963963877 27.4621565093409 -117.041897175848 27.4282301711374 -116.885863967864 27.3932217651372 -116.729594956238 27.3570696128269 -116.572820673713 27.3197103000253 -116.415271199941 27.2810785491022 -116.256675748617 27.241107085821 -116.09676229722 27.1997272484913 -115.935260563566 27.1524952198729 -115.755436839005 27.2270348347386 -115.734960009089 27.3748346522356 -115.694299254844 27.5226008861849 -115.653563616829 27.6702779354428 -115.612760542177 27.8178690071708 -115.571901649363 27.9653506439026 -115.531000691074 28.1127600020619 -115.490011752733 28.2601469756437 -115.44890306179 28.4076546372628 -115.407649898021 28.455192866856 -115.589486631416 28.4968374106496 -115.752807970928 28.5370603096399 -115.91452902381 28.575931293904 -116.074924211465 28.6135193777855 -116.234273707691 28.6498895688451 -116.392847129762 28.6851057860975 -116.550917254638 28.7192301322012 -116.708756374584 28.752323018501 -116.866636764481 28.7844432583843 -117.024831047231 28.8156481533955 -117.183612605748 28.8459935678779 -117.343255995623 28.8755339855462 -117.504037348554 28.9043225601122 -117.666234808407 28.9324111491586 -117.830128960026 28.9598503481156 -117.996003330616 28.9866878706574 -118.164136222141 29.012974905944 -118.33483458667"}
                                 ]
                                 }
                            ]
                            }
                           ],
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
            }]
        }]}

        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        ers = self.query_eboa.get_explicit_refs()

        assert len(ers) == 1

        structure = {}
        export.export_ers(structure, ers)
        
        assert structure == {
            "explicit_references": {
                str(ers[0].explicit_ref_uuid): {
                    "explicit_ref_uuid": str(ers[0].explicit_ref_uuid),
                    "ingestion_time": ers[0].ingestion_time.isoformat(),
                    "explicit_ref": "EXPLICIT_REFERENCE",
                    "alerts": [],
                    "annotations": [
                        {"annotation_uuid": str(ers[0].annotations[0].annotation_uuid),
                         "explicit_reference": {"uuid": str(ers[0].explicit_ref_uuid),
                                                "name": "EXPLICIT_REFERENCE"},
                         "ingestion_time": ers[0].annotations[0].ingestion_time.isoformat(),
                         "configuration": {"uuid": str(ers[0].annotations[0].annotationCnf.annotation_cnf_uuid),
                                           "dim_signature": "dim_signature",
                                           "name": "ANNOTATION_CNF",
                                           "system": "SYSTEM",
                                           "description": None},
                         "source": {"source_uuid": str(ers[0].annotations[0].source.source_uuid),
                                    "name": "source1.xml"},
                         "values": [{"name": "VALUES",
                                     "type": "object",
                                     "values": [
                                         {"name": "TEXT",
                                          "type": "text",
                                          "value": "TEXT"},
                                         {"name": "BOOLEAN",
                                          "type": "boolean",
                                          "value": "True"},
                                         {"name": "BOOLEAN2",
                                          "type": "boolean",
                                          "value": "False"},
                                         {"name": "DOUBLE",
                                          "type": "double",
                                          "value": "0.9"},
                                         {"name": "TIMESTAMP",
                                          "type": "timestamp",
                                          "value": "2018-07-12T00:00:00"},
                                         {"name": "VALUES2",
                                          "type": "object",
                                          "values": [{"name": "GEOMETRY",
                                                      "type": "object",

                                                 "type": "geometry",
                                                      "value": "POLYGON ((29.012974905944 -118.33483458667, 28.8650301641571 -118.372028380632, 28.7171766138274 -118.409121707686, 28.5693112139334 -118.44612300623, 28.4213994944367 -118.483058731035, 28.2734085660472 -118.519970531113, 28.1253606038163 -118.556849863134, 27.9772541759126 -118.593690316835, 27.8291247153939 -118.630472520505, 27.7544158362332 -118.64900551674, 27.7282373644786 -118.48032600682, 27.7015162098732 -118.314168792268, 27.6742039940042 -118.150246300849, 27.6462511775992 -117.98827485961, 27.6176070520608 -117.827974178264, 27.5882197156561 -117.669066835177, 27.5580360448116 -117.511277813618, 27.5270016492436 -117.354334035359, 27.4950608291016 -117.197963963877, 27.4621565093409 -117.041897175848, 27.4282301711374 -116.885863967864, 27.3932217651372 -116.729594956238, 27.3570696128269 -116.572820673713, 27.3197103000253 -116.415271199941, 27.2810785491022 -116.256675748617, 27.241107085821 -116.09676229722, 27.1997272484913 -115.935260563566, 27.1524952198729 -115.755436839005, 27.2270348347386 -115.734960009089, 27.3748346522356 -115.694299254844, 27.5226008861849 -115.653563616829, 27.6702779354428 -115.612760542177, 27.8178690071708 -115.571901649363, 27.9653506439026 -115.531000691074, 28.1127600020619 -115.490011752733, 28.2601469756437 -115.44890306179, 28.4076546372628 -115.407649898021, 28.455192866856 -115.589486631416, 28.4968374106496 -115.752807970928, 28.5370603096399 -115.91452902381, 28.575931293904 -116.074924211465, 28.6135193777855 -116.234273707691, 28.6498895688451 -116.392847129762, 28.6851057860975 -116.550917254638, 28.7192301322012 -116.708756374584, 28.752323018501 -116.866636764481, 28.7844432583843 -117.024831047231, 28.8156481533955 -117.183612605748, 28.8459935678779 -117.343255995623, 28.8755339855462 -117.504037348554, 28.9043225601122 -117.666234808407, 28.9324111491586 -117.830128960026, 28.9598503481156 -117.996003330616, 28.9866878706574 -118.164136222141, 29.012974905944 -118.33483458667))"}
                                                     ]
                                          }
                                     ]
                                     }
                                    ],
                         "alerts": [
                             {"annotation_alert_uuid": str(ers[0].annotations[0].alerts[0].annotation_alert_uuid),
                              "message": "Alert message",
                              "validated": None,
                              "ingestion_time": ers[0].annotations[0].alerts[0].ingestion_time.isoformat(),
                              "generator": "test",
                              "notified": None,
                              "solved": None,
                              "solved_time": None,
                              "notification_time": ers[0].annotations[0].alerts[0].notification_time.isoformat(),
                              "justification": None,
                              "definition": {"alert_uuid": str(ers[0].annotations[0].alerts[0].alertDefinition.alert_uuid),
                                             "name": "alert_name1",
                                             "severity": 4,
                                             "description": "Alert description",
                                             "group": "alert_group"},
                              "annotation_uuid": str(ers[0].annotations[0].annotation_uuid)
                              }
                         ],
                         "explicit_reference": {"uuid": str(ers[0].annotations[0].explicitRef.explicit_ref_uuid),
                                                "name": "EXPLICIT_REFERENCE"}
                         }
                    ]
                }
            }
        }
        
    def test_export_ers_no_events(self):
        """
        Method to test the export_ers function with no optional arguments with no explicit_reference in DDBB
        """

        structure = {}
        export.export_ers(structure, [], group = "explicit_reference_group")
        
        assert structure == {
            "explicit_reference_groups": {
                "explicit_reference_group": []
            },
            "explicit_references": {
            }
        }
