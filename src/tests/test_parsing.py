"""
Automated tests for the parsing submodule

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
# Import python utilities
import unittest

# Import parsing
import gsdm.engine.parsing as parsing

# Import exceptions
from gsdm.engine.errors import ErrorParsingDictionary

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
                    "generation_time": "2018-06-06T13:33:29",
                    "name": "test_simple_update.json",
                    "validity_start": "2018-06-05T02:07:03",
                    "validity_stop": "2018-06-05T02:07:36"
                },
                "explicit_references": [
                    {
                        "group": "test_explicit_ref_group1",
                        "links": [
                            {
                                "back_ref": "true",
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
                                "back_ref": "false",
                                "link": "event_link_id1",
                                "link_mode": "by_ref",
                                "name": "test_link_name1"
                            },
                            {
                                "back_ref": "true",
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
                ]
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
