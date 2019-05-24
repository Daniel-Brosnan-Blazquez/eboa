"""
Helper module for managing EBOA entities in the EBOA component

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
from dateutil import parser
import datetime
from lxml import etree, objectify

###########
# Functions for helping with geometries
###########
def correct_list_of_coordinates_for_ds (list_of_coordinates):
    """
    Method to correct the format of a given list of coordinates for a datastrip
    :param list_of_coordinates: list with coordinates
    :type list_of_coordinates: list

    :return: list_of_coordinates
    :rtype: str

    """
    if type(list_of_coordinates) != list:
        raise
    # end if
    result_list_of_coordinates = []
    # Minimum accpeted number of coordinates is 2
    if len(list_of_coordinates) > 1:
        first_longitude = list_of_coordinates[0]
        first_latitude = list_of_coordinates[1]
        result_list_of_coordinates.append(first_longitude)
        result_list_of_coordinates.append(first_latitude)
        i = 1
        while i < len(list_of_coordinates)/2:
            longitude = list_of_coordinates[i*2]
            if longitude == first_longitude:
                break
            elif ((i*2) + 1) < len(list_of_coordinates):
                latitude = list_of_coordinates[(i*2) + 1]
                result_list_of_coordinates.append(longitude)
                result_list_of_coordinates.append(latitude)
            # end if
            i += 1
        # end while
        result_list_of_coordinates.append(first_longitude)
        result_list_of_coordinates.append(first_latitude)
    # end if
        
    return result_list_of_coordinates

def correct_list_of_coordinates_for_gr_tl (list_of_coordinates):
    """
    Method to correct the format of a given list of coordinates for a granule or a tile
    :param list_of_coordinates: list with coordinates
    :type list_of_coordinates: list

    :return: list_of_coordinates
    :rtype: str

    """
    if type(list_of_coordinates) != list:
        raise
    # end if
    result_list_of_coordinates = []
    # Minimum accpeted number of coordinates is 2
    if len(list_of_coordinates) > 1:
        first_longitude = list_of_coordinates[0]
        first_latitude = list_of_coordinates[1]
        result_list_of_coordinates.append(first_longitude)
        result_list_of_coordinates.append(first_latitude)
        i = 1
        while i < len(list_of_coordinates)/2:
            longitude = list_of_coordinates[i*3]
            if longitude == first_longitude:
                break
            elif ((i*3) + 1) < len(list_of_coordinates):
                latitude = list_of_coordinates[(i*3) + 1]
                result_list_of_coordinates.append(longitude)
                result_list_of_coordinates.append(latitude)
            # end if
            i += 1
        # end while
        result_list_of_coordinates.append(first_longitude)
        result_list_of_coordinates.append(first_latitude)
    # end if
        
    return result_list_of_coordinates

def list_of_coordinates_to_str_geometry (list_of_coordinates):
    """
    Method to receive a string of coordinates and return the same list but with a correct format
    :param list_of_coordinates: list with coordinates
    :type list_of_coordinates: list

    :return: geometry
    :rtype: str

    """
    result_geometry = ""
    i = 0
    for coordinate in list_of_coordinates:
        if i == 0:
            result_geometry = coordinate
        else:
            result_geometry = result_geometry + " " + coordinate
        # end if
        i += 1
    # end for
        
    return result_geometry

###########
# Functions for helping with the ingestion of data
###########
def insert_event_for_ingestion(event, source, list_of_events):
    """
    Method to insert an event into a list for ingestion so that some checks are performed before

    :param event: dictionary with the structure of the event to insert
    :type event: dict
    :param source: dictionary with the structure of the source to insert
    :type source: dict
    :param list_of_events: list of events
    :type parent: list
    """
    # Discard events that are not inside the validity period
    if parser.parse(event["start"]) >= parser.parse(source["validity_stop"]) or parser.parse(event["stop"]) <= parser.parse(source["validity_start"]):
        return
    elif parser.parse(event["start"]) < parser.parse(source["validity_start"]) and parser.parse(event["stop"]) <= parser.parse(source["validity_stop"]) and parser.parse(event["stop"]) > parser.parse(source["validity_start"]):
        event["start"] = source["validity_start"]
    elif parser.parse(event["stop"]) > parser.parse(source["validity_stop"]) and parser.parse(event["start"]) < parser.parse(source["validity_stop"]) and parser.parse(event["start"]) >= parser.parse(source["validity_start"]):
        event["stop"] = source["validity_stop"]
    elif parser.parse(event["start"]) < parser.parse(source["validity_start"]) and parser.parse(event["stop"]) > parser.parse(source["validity_stop"]):
        event["start"] = source["validity_start"]
        event["stop"] = source["validity_stop"]
    # end if
    list_of_events.append(event)
    return

###########
# Functions for helping with trating XML files
###########
def remove_namespaces(file_path, new_file_path):
    """
    Method to insert an event into a list for ingestion so that some checks are performed before

    """
    parser = etree.XMLParser(remove_blank_text=True)
    tree = etree.parse(file_path, parser)
    root = tree.getroot()
    
    for elem in root.getiterator():
        if not hasattr(elem.tag, 'find'):
            continue
        # end if
        i = elem.tag.find('}')
        if i >= 0:
            elem.tag = elem.tag[i+1:]
        # end if
    # end for

    objectify.deannotate(root, cleanup_namespaces=True)

    tree.write(new_file_path,
               pretty_print=True, xml_declaration=True, encoding='UTF-8')

    return

###########
# Functions for computing operations between dates
###########
def dates_difference (minuend, subtrahend):
    """
    Method to perform the difference between two dates
    :param minuend: first date in the substruction
    :type date: str
    :param subtrahend: second date in the substruction
    :type date: str

    :return: seconds of difference
    :rtype: float

    """    
    return (parser.parse(minuend) - parser.parse(subtrahend)).total_seconds()

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
                timeline.append({
                    "id": str(segment1["id"]) + "#" + str(segment2["id"]),
                    "id1": segment1["id"],
                    "id2": segment2["id"],
                    "start": start,
                    "stop": stop
                })
            # end if
        # end for
    # end for
    return timeline
# end def

def get_intersected_timeline_with_idx (timeline, idx):
    """
    Method to obtain the segments of the timeline result of the intersection operation with id corresponding to the timeline 1 or timeline 2
    PRE: the segments of the timeline have the structured returned by the intersect_timelines operation
    """
    
    intersected_timeline_with_idx = []
    for segment in timeline:
        intersected_timeline_with_idx.append({
            "id": segment["id" + str(idx)],
            "start": segment["start"],
            "stop": segment["stop"]
        })

    return intersected_timeline_with_idx
# end def

def intersect_many_timelines (timelines):
    """
    Method to obtain the timeline intersecting all the timelines in the received list
    PRE: the segments of the timelines are ordered in time (by the start value)

    :param timelines: list of timelines
    :type timelines: list

    :return: timeline
    :rtype: timeline with the intersected segments in all timelines

    """
    intersected_timeline = []
    i = 0
    for timeline in timelines:
        if i == 0:
            intersected_timeline = timeline
        else:
            intersected_timeline = intersect_timelines(timeline, intersected_timeline)
        # end if
        i += 1
    # end for

    return intersected_timeline
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
        if use_i1 and i1 < len(timeline1):
            start1 = timeline1[i1]["start"]
            stop1 = timeline1[i1]["stop"]
            id1 = timeline1[i1]["id"]
        elif use_i1:
            start1 = datetime.datetime.max
            stop1 = datetime.datetime.max
            id1 = ""
        # end if
        if use_i2 and i2 < len(timeline2):
            start2 = timeline2[i2]["start"]
            stop2 = timeline2[i2]["stop"]
            id2 = timeline2[i2]["id"]
        elif use_i2:
            start2 = datetime.datetime.max
            stop2 = datetime.datetime.max
            id2 = ""
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
                use_i1 = False
                use_i2 = True
                start1 = stop2
            elif stop1 < stop2:
                i1 += 1
                use_i1 = True
                use_i2 = False
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

def merge_timeline(timeline):
    """
    Method to obtain a merged timeline from the given timeline
    PRE: the segments of the given timeline are ordered in time (by the start value)
    """
    timeline_aux = []
    segment1 = 0
    i = 0
    while segment1 < len(timeline):
        jumper = 1
        i += 1
        timeline_aux.append({"start":timeline[segment1]["start"],
                             "stop":timeline[segment1]["stop"],
                             "id": []
                            })
        timeline_aux[i-1]["id"].append(timeline[segment1]["id"])
        segment2 = segment1 + 1
        while segment2 < len(timeline):
            if timeline_aux[i-1]["stop"] >= timeline[segment2]["start"]:
                timeline_aux[i-1]["id"].append(timeline[segment2]["id"])
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
        #end while
        segment1 += jumper
    #end while
    return timeline_aux

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

def get_greater_segment(timeline):
    """
    Method to get the segment corresponding to the greater duration
    Input: list of segments structured for the usage of this component
    Output: segment with the greater duration

    """
    segments_duration = [{"id": segment["id"],
                          "start": segment["start"],
                          "stop": segment["stop"],
                          "duration": (segment["stop"] - segment["start"]).total_seconds()} for segment in timeline]
    
    sorted_segments_by_duration = sorted(segments_duration, key=lambda segment: segment["duration"])

    return sorted_segments_by_duration[-1]

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

def get_eboa_timeline_duration(timeline):
    """
    Method to get the duration of the timeline (summing up the duration of every event)
    Input: list of events structured as the datamodel of EBOA
    Output: duration of the timeline in seconds

    """

    return sum([(event.stop - event.start).total_seconds() for event in timeline])
