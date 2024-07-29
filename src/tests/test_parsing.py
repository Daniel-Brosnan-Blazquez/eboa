"""
Automated tests for the parsing submodule

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import unittest

# Import parsing
import eboa.engine.parsing as parsing

# Import exceptions
from eboa.engine.errors import ErrorParsingDictionary

class TestParsing(unittest.TestCase):

    def test_parse_maximal_insert_doc(self):
        data = {"operations": [
            {
                "mode": "insert",
                "dim_signature": {
                    "exec": "test_exec1",
                    "name": "test_dim_signature1",
                    "version": "1.0"
                },
                "source": {
                    "reception_time": "2018-06-06T13:33:29",
                    "generation_time": "2018-06-06T13:33:29",
                    "reported_generation_time": "2018-06-06T13:33:29",
                    "name": "test_simple_update.json",
                    "validity_start": "2018-06-05T02:07:03",
                    "validity_stop": "2018-06-05T02:07:36",
                    "reported_validity_start": "2018-06-05T02:07:03",
                    "reported_validity_stop": "2018-06-05T02:07:36",
                    "ingestion_completeness": {
                        "check": "true",
                        "message": ""                        
                    },
                    "ingested": "false"
                },
                "explicit_references": [
                    {
                        "group": "test_explicit_ref_group1",
                        "links": [
                            {
                                "back_ref": "test_link_bak_ref_name1",
                                "link": "test_explicit_ref2",
                                "name": "test_link_name1"
                            }
                        ],
                        "name": "test_explicit_ref1"
                    }
                ],
                "events": [
                    {
                        "explicit_reference": "test_explicit_ref1",
                        "gauge": {
                            "insertion_type": "SIMPLE_UPDATE",
                            "name": "test_gauge_name1",
                            "system": "test_gauge_system1"
                        },
                        "key": "test_key1",
                        "links": [
                            {
                                "link": "event_link_id1",
                                "link_mode": "by_ref",
                                "name": "test_link_name1"
                            },
                            {
                                "back_ref": "test_link_bak_ref_name2",
                                "link": "event_link_id2",
                                "link_mode": "by_ref",
                                "name": "test_link_name2"
                            }
                        ],
                        "start": "2018-06-05T02:07:03",
                        "stop": "2018-06-05T02:07:36",
                        "values": [
                            {
                                "name": "test_object_name1",
                                "type": "object",
                                "values": [
                                    {
                                        "name": "test_text_name1",
                                        "type": "text",
                                        "value": "test text1"
                                    },
                                    {
                                        "name": "test_timestamp_name1",
                                        "type": "timestamp",
                                        "value": "2018-06-06T13:33:29"
                                    },
                                    {
                                        "name": "test_boolean_name1",
                                        "type": "boolean",
                                        "value": "false"
                                    },
                                    {
                                        "name": "test_double_name1",
                                        "type": "double",
                                        "value": "0.91234"
                                    },
                                    {
                                        "name": "test_object_name2",
                                        "type": "object",
                                        "values": [
                                            {
                                                "name": "test_text_name2",
                                                "type": "text",
                                                "value": "test text2"
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]}
                ],
                "annotations": [
                    {
                        "explicit_reference": "test_explicit_ref1",
                        "annotation_cnf": {
                            "name": "test_gauge_name1",
                            "system": "test_gauge_system1"
                        },
                        "values": [
                            {
                                "name": "test_object_name1",
                                "type": "object",
                                "values": [
                                    {
                                        "name": "test_text_name1",
                                        "type": "text",
                                        "value": "test text1"
                                    },
                                    {
                                        "name": "test_timestamp_name1",
                                        "type": "timestamp",
                                        "value": "2018-06-06T13:33:29"
                                    },
                                    {
                                        "name": "test_boolean_name1",
                                        "type": "boolean",
                                        "value": "false"
                                    },
                                    {
                                        "name": "test_double_name1",
                                        "type": "double",
                                        "value": "0.91234"
                                    },
                                    {
                                        "name": "test_object_name2",
                                        "type": "object",
                                        "values": [
                                            {
                                                "name": "test_text_name2",
                                                "type": "text",
                                                "value": "test text2"
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]}
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
                    },
                    "entity": {
                        "reference_mode": "by_ref",
                        "reference": "EVENT_REF",
                        "type": "event"
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
                    },
                    "entity": {
                        "reference_mode": "by_ref",
                        "reference": "EXPLICIT_REFERENCE_EVENT",
                        "type": "explicit_ref"
                    }
                },{
                    "message": "Alert message",
                    "generator": "test",
                    "notification_time": "2018-06-05T08:07:36",
                    "justification": "JUSTIFICATION",
                    "alert_cnf": {
                        "name": "alert_name3",
                        "severity": "critical",
                        "description": "Alert description",
                        "group": "alert_group"
                    },
                    "entity": {
                        "reference_mode": "by_ref",
                        "reference": "source.json",
                        "type": "source"
                    }
                }]
            }]
            }

        returned_value = parsing.validate_data_dictionary(data)
        assert returned_value == None

    def test_parse_minimal_insert_doc(self):
        data = {"operations": [
            {
                "mode": "insert",
                "dim_signature": {
                    "exec": "test_exec1",
                    "name": "test_dim_signature1",
                    "version": "1.0"
                },
                "source": {
                    "reception_time": "2018-06-06T13:33:29",
                    "generation_time": "2018-06-06T13:33:29",
                    "name": "test_simple_update.json",
                    "validity_start": "2018-06-05T02:07:03",
                    "validity_stop": "2018-06-05T02:07:36"
                }
            }
        ]}

        returned_value = parsing.validate_data_dictionary(data)
        assert returned_value == None

    def test_parse_no_mode(self):
        data = {"operations": [
            {
                "dim_signature": {
                    "exec": "test_exec1",
                    "name": "test_dim_signature1",
                    "version": "1.0"
                },
                "source": {
                    "generation_time": "2018-06-06T13:33:29",
                    "name": "test_simple_update.json",
                    "validity_start": "2018-06-05T02:07:03",
                    "validity_stop": "2018-06-05T02:07:36"
                }
            }
        ]}


        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_no_operations(self):
        data = {}

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_operations_not_dict(self):
        data = {"operations": "text"}

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_operation_not_dict(self):
        data = {"operations": [
            "text"
        ]}

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_operation_invalid_mode(self):
        data = {"operations": [
            {"mode": "invalid_mode"}
        ]}

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_invalid_insert(self):
        data = {"operations": [
            {"mode": "insert",
             "not_allowed_tag": 0}
        ]}

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_no_dim_signature(self):
        data = {"operations": [
            {"mode": "insert"}
        ]}

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_no_source(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             }
         }
        ]}

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_not_allowed_tags_dim_signature(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "not_allowed_tag": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {
                 "generation_time": "2018-06-06T13:33:29",
                 "name": "test_simple_update.json",
                 "validity_start": "2018-06-05T02:07:03",
                 "validity_stop": "2018-06-05T02:07:36"
             }
         }
        ]}

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_dim_signature_no_exec(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {
                 "generation_time": "2018-06-06T13:33:29",
                 "name": "test_simple_update.json",
                 "validity_start": "2018-06-05T02:07:03",
                 "validity_stop": "2018-06-05T02:07:36"
             }
         }
        ]}

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_dim_signature_no_name(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "version": "1.0"
             },
             "source": {
                 "generation_time": "2018-06-06T13:33:29",
                 "name": "test_simple_update.json",
                 "validity_start": "2018-06-05T02:07:03",
                 "validity_stop": "2018-06-05T02:07:36"
             }
         }
        ]}

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_dim_signature_no_version(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1"
             },
             "source": {
                 "generation_time": "2018-06-06T13:33:29",
                 "name": "test_simple_update.json",
                 "validity_start": "2018-06-05T02:07:03",
                 "validity_stop": "2018-06-05T02:07:36"
             }
         }
        ]}

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_dim_signature_exec_not_str(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": 0,
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {
                 "generation_time": "2018-06-06T13:33:29",
                 "name": "test_simple_update.json",
                 "validity_start": "2018-06-05T02:07:03",
                 "validity_stop": "2018-06-05T02:07:36"
             }
         }
        ]}

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_dim_signature_name_not_str(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": 0,
                 "version": "1.0"
             },
             "source": {
                 "generation_time": "2018-06-06T13:33:29",
                 "name": "test_simple_update.json",
                 "validity_start": "2018-06-05T02:07:03",
                 "validity_stop": "2018-06-05T02:07:36"
             }
         }
        ]}

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_dim_signature_version_not_str(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": 0
             },
             "source": {
                 "generation_time": "2018-06-06T13:33:29",
                 "name": "test_simple_update.json",
                 "validity_start": "2018-06-05T02:07:03",
                 "validity_stop": "2018-06-05T02:07:36"
             }
         }
        ]}

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_source_no_name(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {
                 "generation_time": "2018-06-06T13:33:29",
                 "validity_start": "2018-06-05T02:07:03",
                 "validity_stop": "2018-06-05T02:07:36"
             }
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_source_no_generation_time(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {
                 "name": "test_simple_update.json",
                 "validity_start": "2018-06-05T02:07:03",
                 "validity_stop": "2018-06-05T02:07:36"
             }
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_source_no_validity_start(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {
                 "generation_time": "2018-06-06T13:33:29",
                 "name": "test_simple_update.json",
                 "validity_stop": "2018-06-05T02:07:36"
             }
         }
        ]}

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_source_no_validity_stop(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {
                 "generation_time": "2018-06-06T13:33:29",
                 "name": "test_simple_update.json",
                 "validity_start": "2018-06-05T02:07:03",
             }
         }
        ]}

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_source_name_no_str(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {
                 "generation_time": "2018-06-06T13:33:29",
                 "name": ["not_a_string"],
                 "validity_start": "2018-06-05T02:07:03",
                 "validity_stop": "2018-06-05T02:07:36"
             }
         }
        ]}

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_source_generation_time_no_date(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {
                 "generation_time": "not_a_date",
                 "name": "test_simple_update.json",
                 "validity_start": "2018-06-05T02:07:03",
                 "validity_stop": "2018-06-05T02:07:36"
             }
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_source_validity_start_no_date(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {
                 "generation_time": "2018-06-06T13:33:29",
                 "name": "test_simple_update.json",
                 "validity_start": "not_a_date",
                 "validity_stop": "2018-06-05T02:07:36"
             }
         }
        ]}

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_source_validity_stop_no_date(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {
                 "generation_time": "2018-06-06T13:33:29",
                 "name": "test_simple_update.json",
                 "validity_start": "2018-06-05T02:07:03",
                 "validity_stop": "not_a_date"
             }
         }
        ]}

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_source_no_allowed_items(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {
                 "no_allowed_item": ""
             }
         }
        ]}

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_explicit_references_not_list(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "explicit_references": "not_a_list"
         }
        ]}

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_explicit_references_list_not_containing_dict(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "explicit_references": ["not_a_dict"]

         }
        ]}

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_explicit_references_dict_not_valid_item(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "explicit_references": [{"not_a_valid_item": ""}]

         }
        ]}

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_explicit_references_no_name(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "explicit_references": [{
                 "links": [{"link": "EXPLICIT_REFERENCE_LINK"}]
             }]
         }
        ]}

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_explicit_references_name_no_str(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "explicit_references": [{
                 "name": ["no_str"]
             }]         
         }
        ]}

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_explicit_references_group_no_str(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "explicit_references": [{
                 "name": "EXPLICIT_REFERENCE",
                 "group": ["not_a_str"]
             }]
         }
        ]}

        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_explicit_references_links_no_list(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "explicit_references": [{
                 "name": "EXPLICIT_REFERENCE",
                 "links": "not_a_list"
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_explicit_references_link_not_valid_item(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "explicit_references": [{
                 "name": "EXPLICIT_REFERENCE",
                 "links": [{"not_valid_item": ""}]
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_explicit_references_link_no_link_inside(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "explicit_references": [{
                 "name": "EXPLICIT_REFERENCE",
                 "links": [{}]
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_explicit_references_link_no_name_inside(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "explicit_references": [{
                 "name": "EXPLICIT_REFERENCE",
                 "links": [{"link": "EXPLICIT_REFERENCE_LINK"}]
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_explicit_references_link_name_no_str(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "explicit_references": [{
                 "name": "EXPLICIT_REFERENCE",
                 "links": [{"link": "EXPLICIT_REFERENCE_LINK",
                            "name": ["no_str"]}]
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_explicit_references_link_link_no_str(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "explicit_references": [{
                 "name": "EXPLICIT_REFERENCE",
                 "links": [{"link": ["no_str"],
                            "name": "LINK_NAME"}]
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_explicit_references_link_back_ref_no_valid(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "explicit_references": [{
                 "name": "EXPLICIT_REFERENCE",
                 "links": [{"link": "EXPLICIT_REFERENCE_LINK",
                            "name": "LINK_NAME",
                            "back_ref": False}]
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_events_no_list(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "events": "not_a_list"
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_events_list_no_dict(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "events": ["not_a_dict"]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_events_no_valid_item(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "events": [{"not_a_valid_item": ""}]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_events_no_gauge(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "events": [{}]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_events_no_start(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "events": [{
                 "gauge": {"name": "GAUGE_NAME",
                           "system": "GAUGE_SYSTEM",
                           "insertion_type": "SIMPLE_UPDATE"},
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_events_no_stop(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "events": [{
                 "gauge": {"name": "GAUGE_NAME",
                           "system": "GAUGE_SYSTEM",
                           "insertion_type": "SIMPLE_UPDATE"},
                 "start": "2018-06-05T02:07:03",
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_events_start_no_date(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "events": [{
                 "gauge": {"name": "GAUGE_NAME",
                           "system": "GAUGE_SYSTEM",
                           "insertion_type": "SIMPLE_UPDATE"},
                 "start": "not_a_date",
                 "stop": "2018-06-05T02:07:03"
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_events_stop_no_date(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "events": [{
                 "gauge": {"name": "GAUGE_NAME",
                           "system": "GAUGE_SYSTEM",
                           "insertion_type": "SIMPLE_UPDATE"},
                 "start": "2018-06-05T02:07:03",
                 "stop": "no_date"
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True


    def test_parse_insert_operation_event_explicit_reference_no_str(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "events": [{
                 "explicit_reference": ["no_str"],
                 "gauge": {"name": "GAUGE_NAME",
                           "system": "GAUGE_SYSTEM",
                           "insertion_type": "SIMPLE_UPDATE"},
                 "start": "2018-06-05T02:07:03",
                 "stop": "2018-06-05T08:07:03"
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_event_key_no_str(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "events": [{
                 "key": ["no_str"],
                 "explicit_reference": "EXPLICIT_REFERENCE",
                 "gauge": {"name": "GAUGE_NAME",
                           "system": "GAUGE_SYSTEM",
                           "insertion_type": "SIMPLE_UPDATE"},
                 "start": "2018-06-05T02:07:03",
                 "stop": "2018-06-05T08:07:03"
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_gauge_no_dict(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "events": [{
                 "explicit_reference": "EXPLICIT_REFERENCE",
                 "gauge": "no_dict",
                 "start": "2018-06-05T02:07:03",
                 "stop": "2018-06-05T08:07:03"
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_gauge_no_valid_item(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "events": [{
                 "explicit_reference": "EXPLICIT_REFERENCE",
                 "gauge": {"no_valid_item": ""},
                 "start": "2018-06-05T02:07:03",
                 "stop": "2018-06-05T08:07:03"
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_gauge_no_insertion_type(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "events": [{
                 "explicit_reference": "EXPLICIT_REFERENCE",
                 "gauge": {"name": "GAUGE_NAME",
                           "system": "GAUGE_SYSTEM"},
                 "start": "2018-06-05T02:07:03",
                 "stop": "2018-06-05T08:07:03"
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_gauge_no_name(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "events": [{
                 "explicit_reference": "EXPLICIT_REFERENCE",
                 "gauge": {"system": "GAUGE_SYSTEM",
                           "insertion_type": "SIMPLE_UPDATE"},
                 "start": "2018-06-05T02:07:03",
                 "stop": "2018-06-05T08:07:03"
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_gauge_name_no_str(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "events": [{
                 "explicit_reference": "EXPLICIT_REFERENCE",
                 "gauge": {"name": ["no_str"],
                           "system": "GAUGE_SYSTEM",
                           "insertion_type": "SIMPLE_UPDATE"},
                 "start": "2018-06-05T02:07:03",
                 "stop": "2018-06-05T08:07:03"
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_gauge_no_valid_insertion_type(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "events": [{
                 "explicit_reference": "EXPLICIT_REFERENCE",
                 "gauge": {"name": "GAUGE_NAME",
                           "system": "GAUGE_SYSTEM",
                           "insertion_type": "NOT_VALID"},
                 "start": "2018-06-05T02:07:03",
                 "stop": "2018-06-05T08:07:03"
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_gauge_system_no_str(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "events": [{
                 "explicit_reference": "EXPLICIT_REFERENCE",
                 "gauge": {"name": "GAUGE_NAME",
                           "system": ["no_str"],
                           "insertion_type": "SIMPLE_UPDATE"},
                 "start": "2018-06-05T02:07:03",
                 "stop": "2018-06-05T08:07:03"
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_event_links_no_list(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "events": [{
                 "gauge": {"name": "GAUGE_NAME",
                           "system": "GAUGE_SYSTEM",
                           "insertion_type": "SIMPLE_UPDATE"},
                 "start": "2018-06-05T02:07:03",
                 "stop": "2018-06-05T08:07:03",
                "links": "not_a_list"
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_event_links_no_valid_item(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "events": [{
                 "gauge": {"name": "GAUGE_NAME",
                           "system": "GAUGE_SYSTEM",
                           "insertion_type": "SIMPLE_UPDATE"},
                 "start": "2018-06-05T02:07:03",
                 "stop": "2018-06-05T08:07:03",
                "links": [{
                    "not_valid_item": ""
                }]
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_event_links_no_link(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "events": [{
                 "gauge": {"name": "GAUGE_NAME",
                           "system": "GAUGE_SYSTEM",
                           "insertion_type": "SIMPLE_UPDATE"},
                 "start": "2018-06-05T02:07:03",
                 "stop": "2018-06-05T08:07:03",
                "links": [{"link_mode": "by_ref",
                    "name": "EVENT_LINK_NAME"
                }]
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_event_links_no_name(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "events": [{
                 "gauge": {"name": "GAUGE_NAME",
                           "system": "GAUGE_SYSTEM",
                           "insertion_type": "SIMPLE_UPDATE"},
                 "start": "2018-06-05T02:07:03",
                 "stop": "2018-06-05T08:07:03",
                "links": [{
                    "link": "EVENT_LINK",
                    "link_mode": "by_ref"
                }]
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_event_links_no_link_mode(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "events": [{
                 "gauge": {"name": "GAUGE_NAME",
                           "system": "GAUGE_SYSTEM",
                           "insertion_type": "SIMPLE_UPDATE"},
                 "start": "2018-06-05T02:07:03",
                 "stop": "2018-06-05T08:07:03",
                "links": [{
                    "link": "EVENT_LINK",
                    "name": "EVENT_LINK_NAME"
                }]
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_event_links_name_no_str(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "events": [{
                 "gauge": {"name": "GAUGE_NAME",
                           "system": "GAUGE_SYSTEM",
                           "insertion_type": "SIMPLE_UPDATE"},
                 "start": "2018-06-05T02:07:03",
                 "stop": "2018-06-05T08:07:03",
                "links": [{
                    "link": "EVENT_LINK",
                    "link_mode": "by_ref",
                    "name": ["no_str"]
                }]
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_event_links_link_no_str(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "events": [{
                 "gauge": {"name": "GAUGE_NAME",
                           "system": "GAUGE_SYSTEM",
                           "insertion_type": "SIMPLE_UPDATE"},
                 "start": "2018-06-05T02:07:03",
                 "stop": "2018-06-05T08:07:03",
                "links": [{
                    "link": ["no_str"],
                    "link_mode": "by_ref",
                    "name": "EVENT_LINK_NAME"
                }]
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_event_links_link_mode_not_valid(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "events": [{
                 "gauge": {"name": "GAUGE_NAME",
                           "system": "GAUGE_SYSTEM",
                           "insertion_type": "SIMPLE_UPDATE"},
                 "start": "2018-06-05T02:07:03",
                 "stop": "2018-06-05T08:07:03",
                "links": [{
                    "link": "EVENT_LINK",
                    "link_mode": "not_valid",
                    "name": "EVENT_LINK_NAME"
                }]
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_event_links_back_ref_not_valid(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "events": [{
                 "gauge": {"name": "GAUGE_NAME",
                           "system": "GAUGE_SYSTEM",
                           "insertion_type": "SIMPLE_UPDATE"},
                 "start": "2018-06-05T02:07:03",
                 "stop": "2018-06-05T08:07:03",
                "links": [{
                    "link": "EVENT_LINK",
                    "link_mode": "by_ref",
                    "name": "EVENT_LINK_NAME",
                    "back_ref": False
                }]
             }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_annotations_no_list(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
            "annotations": "not_list"
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_annotations_contains_no_dict(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
             "annotations": ["not_a_dict"]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_annotations_no_valid_item(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
            "annotations": [{"not_valid_item": ""}]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_annotations_no_annotation_cnf(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE"
            }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_annotations_no_explicit_reference(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
            "annotations": [{
                "annotation_cnf": {"name": "GAUGE_NAME",
                                   "system": "GAUGE_SYSTEM"}
            }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_annotations_explicit_reference_no_str(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
            "annotations": [{
                "explicit_reference": ["not_str"],
                "annotation_cnf": {"name": "GAUGE_NAME",
                                   "system": "GAUGE_SYSTEM"}
            }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_annotation_cnf_no_dict(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {"not_a_dict"}
            }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_annotation_cnf_no_valid_item(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {"no_valid_item": ""}
            }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_annotation_cnf_no_name(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {"system": "GAUGE_SYSTEM"}
            }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_annotation_cnf_name_no_str(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {"name": ["no_str"],
                                   "system": "GAUGE_SYSTEM"}
            }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_annotation_cnf_system_no_str(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {"name": "GAUGE_NAME",
                                   "system": ["no_str"]}
            }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_values_no_list(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {"name": "GAUGE_NAME",
                                   "system": "GAUGE_SYSTEM"},
                "values": "no_list"
            }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_values_no_dict(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {"name": "GAUGE_NAME",
                                   "system": "GAUGE_SYSTEM"},
                "values": ["no_dict"]
            }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_values_no_valid_item(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {"name": "GAUGE_NAME",
                                   "system": "GAUGE_SYSTEM"},
                "values": [{"no_valid_item": "VALUES"}]
            }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_values_no_name(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {"name": "GAUGE_NAME",
                                   "system": "GAUGE_SYSTEM"},
                "values": [{}]
            }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_values_no_type(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {"name": "GAUGE_NAME",
                                   "system": "GAUGE_SYSTEM"},
                "values": [{"name": "VALUES"}]
            }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_values_name_no_str(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {"name": "GAUGE_NAME",
                                   "system": "GAUGE_SYSTEM"},
                "values": [{"type": "double",
                            "name": ["no_str"]}]
            }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_values_type_no_valid(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {"name": "GAUGE_NAME",
                                   "system": "GAUGE_SYSTEM"},
                "values": [{"name": "VALUES",
                            "type": "not_valid_type"}]
            }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_values_and_value(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {"name": "GAUGE_NAME",
                                   "system": "GAUGE_SYSTEM"},
                "values": [{"name": "VALUES",
                            "type": "object",
                            "values": [
                                {"type": "double",
                                 "name": "NOT_A_DOUBLE",
                                 "value": "NOT_A_DOUBLE"}],
                            "value": ""
                        }]
            }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True

    def test_parse_insert_operation_value_no_str(self):
        data = {"operations": [
            {"mode": "insert",
             "dim_signature": {
                 "exec": "test_exec1",
                 "name": "test_dim_signature1",
                 "version": "1.0"
             },
             "source": {"name": "source.xml",
                        "generation_time": "2018-07-05T02:07:03",
                        "validity_start": "2018-06-05T02:07:03",
                        "validity_stop": "2018-06-05T08:07:36"},
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {"name": "GAUGE_NAME",
                                   "system": "GAUGE_SYSTEM"},
                "values": [{"name": "VALUES",
                            "type": "object",
                            "values": [
                                {"type": "text",
                                 "name": "NOT_A_STR",
                                 "value": ["NOT_A_STR"]}]
                        }]
            }]
         }
        ]}
        try:
            parsing.validate_data_dictionary(data)
        except ErrorParsingDictionary:
            assert True == True
        except:
            assert False == True
