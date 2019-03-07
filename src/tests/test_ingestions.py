# Import python utilities
from dateutil import parser
import os
import sys
import unittest
import datetime

# Import parsing
import ingestions.functions.date_functions as date_functions

class TestMergeTimelines(unittest.TestCase):

    def test_empty_timeline(self):
        """
        Empty list
        """
        timeline = []
        timeline_res = date_functions.merge_timeline(timeline)
        assert timeline_res == []

    def test_single_segment_timeline(self):
        """
        Same result than input
        """
        timeline = [{"start":parser.parse("2018-11-21T20:55:17"),"stop":parser.parse("2018-11-21T20:58:17"),"id":"test"}]
        timeline_res = date_functions.merge_timeline(timeline)
        assert timeline_res == [{"start":parser.parse("2018-11-21T20:55:17"),"stop":parser.parse("2018-11-21T20:58:17"),"id":["test"]}]

    def test_multiple_segments_timeline(self):
        """
        Two groups of segments, one that contains to segments where they intersect partially and the other one where there are a one-point intersection and a fully covered intersection
        First group                    Second group
        |--------|                        |---------------------------|
            |--------|                           |--------|           |--------|
        """

        timeline = [{"start":parser.parse("2018-11-21T20:55:00"),"stop":parser.parse("2018-11-21T20:58:00"),"id":"testing_Id_1"},
                    {"start":parser.parse("2018-11-21T20:57:30"),"stop":parser.parse("2018-11-21T20:59:45"),"id":"testing_Id_2"},
                    {"start":parser.parse("2018-11-21T21:30:00"), "stop":parser.parse("2018-11-22T01:00:00"),"id":"testing_Id_3"},
                    {"start":parser.parse("2018-11-21T23:12:14"),"stop":parser.parse("2018-11-22T00:00:00"),"id":"testing_Id_4"},
                    {"start":parser.parse("2018-11-22T00:00:00"),"stop":parser.parse("2019-02-05T14:14:14"), "id":"testing_Id_5"}]

        timeline_res = date_functions.merge_timeline(timeline)

        assert timeline_res == [{'start': datetime.datetime(2018, 11, 21, 20, 55), 'stop': datetime.datetime(2018, 11, 21, 20, 59, 45), 'id': ['testing_Id_1', 'testing_Id_2']}, {'start': datetime.datetime(2018, 11, 21, 21, 30), 'stop': datetime.datetime(2019, 2, 5, 14, 14, 14), 'id': ['testing_Id_3', 'testing_Id_4', 'testing_Id_5']}]

class TestDifferenceTimelines(unittest.TestCase):

    def test_empty_timelines(self):
        """
        Empty timelines
        """
        timeline1 = []
        timeline2 = []
        timeline_res = date_functions.difference_timelines(timeline1, timeline2)
        assert timeline_res == []

    def test_single_segment_timeline1(self):
        """
        Same result than input in timeline1
        """
        timeline1 = [{"start":parser.parse("2018-01-01T00:00:00"),"stop":parser.parse("2018-01-02T00:00:00"),"id":"timeline1"}]
        timeline2 = []
        timeline_res = date_functions.difference_timelines(timeline1, timeline2)
        assert timeline_res == [{"start":parser.parse("2018-01-01T00:00:00"),"stop":parser.parse("2018-01-02T00:00:00"),"id":"timeline1", "timeline":timeline1}]

    def test_single_segment_timeline2(self):
        """
        Same result than input in timeline2
        """
        timeline1 = []
        timeline2 = [{"start":parser.parse("2018-01-01T00:00:00"),"stop":parser.parse("2018-01-02T00:00:00"),"id":"timeline2"}]
        timeline_res = date_functions.difference_timelines(timeline1, timeline2)
        assert timeline_res == [{"start":parser.parse("2018-01-01T00:00:00"),"stop":parser.parse("2018-01-02T00:00:00"),"id":"timeline2", "timeline":timeline2}]

    def test_same_segments_in_both_timelines(self):
        """
        Empty result because there is no difference
        """
        timeline1 = [{"start":parser.parse("2018-01-01T00:00:00"),"stop":parser.parse("2018-01-02T00:00:00"),"id":"timeline1"}]
        timeline2 = [{"start":parser.parse("2018-01-01T00:00:00"),"stop":parser.parse("2018-01-02T00:00:00"),"id":"timeline2"}]
        timeline_res = date_functions.difference_timelines(timeline1, timeline2)
        assert timeline_res == []

    def test_segment_in_timeline1_covers_more(self):
        """
        Result are the margins of the segment of timeline1 compared to the segment in timelin2
        """
        timeline1 = [{"start":parser.parse("2018-01-01T00:00:00"),"stop":parser.parse("2018-01-02T00:00:00"),"id":"timeline1"}]
        timeline2 = [{"start":parser.parse("2018-01-01T12:00:00"),"stop":parser.parse("2018-01-01T18:00:00"),"id":"timeline2"}]
        timeline_res = date_functions.difference_timelines(timeline1, timeline2)
        assert timeline_res == [{"start":parser.parse("2018-01-01T00:00:00"),"stop":parser.parse("2018-01-01T12:00:00"),"id":"timeline1", "timeline":timeline1},{"start":parser.parse("2018-01-01T18:00:00"),"stop":parser.parse("2018-01-02T00:00:00"),"id":"timeline1", "timeline":timeline1}]

    def test_segment_in_timeline2_covers_more(self):
        """
        Result are the margins of the segment of timeline2 compared to the segment in timelin1
        """
        timeline1 = [{"start":parser.parse("2018-01-01T12:00:00"),"stop":parser.parse("2018-01-01T18:00:00"),"id":"timeline1"}]
        timeline2 = [{"start":parser.parse("2018-01-01T00:00:00"),"stop":parser.parse("2018-01-02T00:00:00"),"id":"timeline2"}]
        timeline_res = date_functions.difference_timelines(timeline1, timeline2)
        assert timeline_res == [{"start":parser.parse("2018-01-01T00:00:00"),"stop":parser.parse("2018-01-01T12:00:00"),"id":"timeline2", "timeline":timeline2},{"start":parser.parse("2018-01-01T18:00:00"),"stop":parser.parse("2018-01-02T00:00:00"),"id":"timeline2", "timeline":timeline2}]

    def test_segment_in_timeline1_covers_more_on_left(self):
        """
        Result are the margins of the segment of timeline2 compared to the segment in timelin1
        """
        timeline1 = [{"start":parser.parse("2018-01-01T00:00:00"),"stop":parser.parse("2018-01-02T00:00:00"),"id":"timeline1"}]
        timeline2 = [{"start":parser.parse("2018-01-01T12:00:00"),"stop":parser.parse("2018-01-02T00:00:00"),"id":"timeline2"}]
        timeline_res = date_functions.difference_timelines(timeline1, timeline2)
        assert timeline_res == [{"start":parser.parse("2018-01-01T00:00:00"),"stop":parser.parse("2018-01-01T12:00:00"),"id":"timeline1", "timeline":timeline1}]

    def test_segment_in_timeline2_covers_more_on_left(self):
        """
        Result are the margins of the segment of timeline2 compared to the segment in timelin1
        """
        timeline1 = [{"start":parser.parse("2018-01-01T12:00:00"),"stop":parser.parse("2018-01-02T00:00:00"),"id":"timeline1"}]
        timeline2 = [{"start":parser.parse("2018-01-01T00:00:00"),"stop":parser.parse("2018-01-02T00:00:00"),"id":"timeline2"}]
        timeline_res = date_functions.difference_timelines(timeline1, timeline2)
        assert timeline_res == [{"start":parser.parse("2018-01-01T00:00:00"),"stop":parser.parse("2018-01-01T12:00:00"),"id":"timeline2", "timeline":timeline2}]

    def test_segment_in_timeline1_covers_more_on_left_segment_in_timeline2_covers_more_on_right(self):
        """
        Result are the margins of the segment of timeline2 compared to the segment in timelin1
        """
        timeline1 = [{"start":parser.parse("2018-01-01T00:00:00"),"stop":parser.parse("2018-01-01T18:00:00"),"id":"timeline1"}]
        timeline2 = [{"start":parser.parse("2018-01-01T12:00:00"),"stop":parser.parse("2018-01-02T00:00:00"),"id":"timeline2"}]
        timeline_res = date_functions.difference_timelines(timeline1, timeline2)
        assert timeline_res == [{"start":parser.parse("2018-01-01T00:00:00"),"stop":parser.parse("2018-01-01T12:00:00"),"id":"timeline1", "timeline":timeline1},{"start":parser.parse("2018-01-01T18:00:00"),"stop":parser.parse("2018-01-02T00:00:00"),"id":"timeline2", "timeline":timeline2}]

    def test_timelines_do_not_overlap(self):
        """
        Result are the segments of timeline1 and timelin2
        """
        timeline1 = [{"start":parser.parse("2018-01-01T00:00:00"),"stop":parser.parse("2018-01-02T00:00:00"),"id":"timeline1"}]
        timeline2 = [{"start":parser.parse("2018-01-03T00:00:00"),"stop":parser.parse("2018-01-04T00:00:00"),"id":"timeline2"}]
        timeline_res = date_functions.difference_timelines(timeline1, timeline2)
        assert timeline_res == [{"start":parser.parse("2018-01-01T00:00:00"),"stop":parser.parse("2018-01-02T00:00:00"),"id":"timeline1", "timeline":timeline1},{"start":parser.parse("2018-01-03T00:00:00"),"stop":parser.parse("2018-01-04T00:00:00"),"id":"timeline2", "timeline":timeline2}]
