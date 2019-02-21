"""
Helper module for managing dates in the EBOA component

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
from dateutil import parser

###########
# Functions using the timelines with the structure defined by identifiers and start and stop vlaues
###########

def intersect_timelines (timeline1, timeline2):
    """
    Method to obtain the segments from timeline1 intersecting with timeline2
    PRE: the segments of the timelines are ordered in time (by the start value)
    """
    timeline = []
    for segment1 in timeline1:
        for segment2 in timeline2:
            if segment1["stop"] > segment2["start"] and segment1["start"] < segment2["stop"]:
                start = segment2["start"]
                if segment1["start"] > segment2["start"]:
                    start = segment1["start"]
                # end if
                stop = segment2["stop"]
                if segment1["stop"] < segment2["stop"]:
                    stop = segment1["stop"]
                # end if
                timeline.append({"id1": segment1["id"],
                                 "id2": segment2["id"],
                                 "start": start,
                                 "stop": stop
                                 })
            # end if
        # end for
    # end for
    return timeline
# end def

def difference_timelines (timeline1, timeline2):
    """
    Method to obtain the difference in segments between the timeline1 and the timeline2
    PRE: the segments of the timelines are ordered in time (by the start value)
    """
    timeline = []
    i1 = 0
    i2 = 0
    if len(timeline1) > 0:
        use_i1 = True
    else:
        use_i1 = False
        start1 = datetime.datetime.max
        stop1 = datetime.datetime.max
        id1 = ""
    # end if
    if len(timeline2) > 0:
        use_i2 = True
    else:
        use_i2 = False
        start2 = datetime.datetime.max
        stop2 = datetime.datetime.max
        id2 = ""
    # end if

    while i1 < len(timeline1) or i2 < len(timeline2):
        if use_i1:
            start1 = timeline1[i1]["start"]
            stop1 = timeline1[i1]["stop"]
            id1 = timeline1[i1]["id"]
        # end if
        if use_i2:
            start2 = timeline1[i2]["start"]
            stop2 = timeline1[i2]["stop"]
            id2 = timeline1[i2]["id"]
        # end if

        create_difference_segment = True

        if start1 < start2:
            difference_start = start1
            difference_timeline = timeline1
            difference_id = id1
            if stop1 <= start2:
                use_i1 = True
                i1 += 1
                difference_stop = stop1
            else:
                use_i1 = False
                difference_stop = start2
                start1 = start2
            # end if
        elif start1 > start2:
            difference_start = start2
            difference_timeline = timeline2
            difference_id = id2
            if stop2 <= start1:
                use_i2 = True
                i2 += 1
                start2 = timeline2[i2]["start"]
                stop2 = timeline2[i2]["stop"]
                id2 = timeline2[i2]["id"]
                difference_stop = stop2
            else:
                use_i2 = False
                difference_stop = start1
                start2 = start1
            # end if
        else:
            # start1 = start2
            if stop1 > stop2:
                i2 += 1
                use_i2 = True
                start1 = stop2
            elif stop1 < stop2:
                i1 += 1
                use_i1 = True
                start2 = stop1
            else:
                i1 += 1
                i2 += 1
                use_i1 = True
                use_i2 = True
            # end if
            create_difference_segment = False
        # end if
        if create_difference_segment:
            timeline.append({
                "timeline": difference_timeline,
                "id": difference_id,
                "start": difference_start,
                "stop": difference_stop
            })
        # end if
    # end while
    return timeline
# end def

def sort_timeline_by_start(timeline):
    """
    Method to order a timeline of date segments by the start value of each segment
    """
    # Validate the format of the timeline

    return sorted(timeline, key=lambda segment: segment["start"])

def get_timeline_duration(timeline):
    """
    Method to get the duration of the timeline (summing up the duration of every event)
    Input: list of segments structured for the usage of this component
    Output: duration of the timeline in seconds

    """
    return sum([(segment["stop"] - segment["start"]).total_seconds() for segment in timeline])

###########
# Functions using the timelines with the structure of data to be inserted into the EBOA
###########

def convert_input_events_to_date_segments(timeline):
    """
    Method to convert from events to be inserted into EBOA to segments to be managed by this component
    Input: list of events with a structure accepted by EBOA
    Output: list of elements accepted by this component:
    {
    "id": event["link_ref"],
    "start": event["start"],
    "stop": event["stop"]
    }
    the result is sorted by the start value
    """
    # Validate the format of the timeline

    date_segments = [{"id": event["link_ref"], "start": parser.parse(event["start"]), "stop": parser.parse(event["stop"])} for event in timeline]

    return sorted(date_segments, key=lambda segment: segment["start"])

###########
# Functions using the timelines with the structure of data extracted from the EBOA
###########

def convert_eboa_events_to_date_segments(timeline):
    """
    Method to convert from events extracted from EBOA to segments to be managed by this component
    Input: list of events structured as the datamodel of EBOA
    Output: list of elements accepted by this component:
    {
    "id": event.event_uuid,
    "start": event.start,
    "stop": event.stop
    }
    the result is sorted by the start value
    """
    # Validate the format of the timeline

    date_segments = [{"id": event.event_uuid, "start": event.start, "stop": event.stop} for event in timeline]

    return sorted(date_segments, key=lambda segment: segment["start"])

def merge_timeline(timeline):
    """
    Method to obtain a merged timeline between the timeline1 and the timeline2
    PRE: the segments of the timelines are ordered in time (by the start value)
    """
    timeline_aux = []
    segment1 = 0
    i = 0
    while segment1 < len(timeline):
        ids = []
        jumper = 1
        i += 1
        timeline_aux.append({"start":timeline[segment1]["start"],
                             "stop":timeline[segment1]["stop"],
                             "ids": []
                            })
        timeline_aux[i-1]["ids"].append(timeline[segment1]["id"])
        segment2 = segment1 + 1
        while segment2 < len(timeline):
            if timeline_aux[i-1]["stop"] >= timeline[segment2]["start"]:
                timeline_aux[i-1]["ids"].append(timeline[segment2]["id"])
                if timeline_aux[i-1]["stop"] < timeline[segment2]["stop"]:
                    timeline_aux[i-1]["stop"] = timeline[segment2]["stop"]
                    jumper += 1
                #end if
                else:
                    jumper += 1
                #end else
                segment2 = segment2 + 1
            #end if
            else:
                break
            #end else
        segment1 += jumper
        #end while
    #end while
    return timeline_aux

def get_eboa_timeline_duration(timeline):
    """
    Method to get the duration of the timeline (summing up the duration of every event)
    Input: list of events structured as the datamodel of EBOA
    Output: duration of the timeline in seconds

    """

    return sum([(event.stop - event.start).total_seconds() for event in timeline])
