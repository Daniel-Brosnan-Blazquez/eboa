# Import python utilities#
from dateutil import parser
import os
import sys
import unittest
import datetime

# Import parsing
import ingestions.functions.date_functions as date_functions

class MergeTesting(unittest.TestCase):

    #Also an empty list
    def test_empty_timeline(self):
        timeline = []
        timeline_res = date_functions.merge_timeline(timeline)
        assert timeline_res == []

        #Same result than input
    def test_single_segment_timeline(self):
        timeline = [{"start":parser.parse("2018-11-21T20:55:17"),"stop":parser.parse("2018-11-21T20:58:17"),"id":"test"}]
        timeline_res = date_functions.merge_timeline(timeline)
        assert timeline_res == [{"start":parser.parse("2018-11-21T20:55:17"),"stop":parser.parse("2018-11-21T20:58:17"),"ids":["test"]}]

        #Two groups of segments, one that contains to segments where they intersect partially and the other one where there are a one-point intersection and a fully covered intersection
        #First group                    Second group
        #|--------|                        |---------------------------|
        #    |--------|                           |--------|           |--------|
    def test_multiple_segments_timeline(self):
        timeline = [{"start":parser.parse("2018-11-21T20:55:00"),"stop":parser.parse("2018-11-21T20:58:00"),"id":"testing_Id_1"},
                    {"start":parser.parse("2018-11-21T20:57:30"),"stop":parser.parse("2018-11-21T20:59:45"),"id":"testing_Id_2"},
                    {"start":parser.parse("2018-11-21T21:30:00"), "stop":parser.parse("2018-11-22T01:00:00"),"id":"testing_Id_3"},
                    {"start":parser.parse("2018-11-21T23:12:14"),"stop":parser.parse("2018-11-22T00:00:00"),"id":"testing_Id_4"},
                    {"start":parser.parse("2018-11-22T00:00:00"),"stop":parser.parse("2019-02-05T14:14:14"), "id":"testing_Id_5"}]

        timeline_res = date_functions.merge_timeline(timeline)

        assert timeline_res == [{'start': datetime.datetime(2018, 11, 21, 20, 55), 'stop': datetime.datetime(2018, 11, 21, 20, 59, 45), 'ids': ['testing_Id_1', 'testing_Id_2']}, {'start': datetime.datetime(2018, 11, 21, 21, 30), 'stop': datetime.datetime(2019, 2, 5, 14, 14, 14), 'ids': ['testing_Id_3', 'testing_Id_4', 'testing_Id_5']}]
