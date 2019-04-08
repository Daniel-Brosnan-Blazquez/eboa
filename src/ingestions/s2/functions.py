"""
Helper module for the ingestions of files of Sentinel-2

Written by DEIMOS Space S.L. (dibb)

module eboa
"""

# Import python utilities
import math
import datetime
from dateutil import parser

import ingestions.functions.date_functions as date_functions

# Import debugging
from eboa.debugging import debug

###
# Acquisition ingestions' helpers
###

# Uncomment for debugging reasons
#@debug
def convert_from_gps_to_utc(date):
    """
    Method to convert a date in GPS precission to UTC
    :param date: date in GPS precission and ISO format
    :type date: str

    :return: date coverted in ISO 8601
    :rtype: str

    """

    date_datetime = parser.parse(date)

    if date_datetime > datetime.datetime(2015, 6, 30, 23, 59, 59) and date_datetime <= datetime.datetime(2016, 12, 31, 23, 59, 59):
        correction = -17
    elif date_datetime > datetime.datetime(2016, 12, 31, 23, 59, 59):
        correction = -18
    else:
        correction = -16

    return str(date_datetime + datetime.timedelta(seconds=correction))

# Uncomment for debugging reasons
#@debug
def convert_from_datetime_gps_to_datetime_utc(date):
    """
    Method to convert a date in GPS precission to UTC
    :param date: date in GPS precission and ISO format
    :type date: str

    :return: date coverted in ISO 8601
    :rtype: str

    """

    if date > datetime.datetime(2015, 6, 30, 23, 59, 59) and date <= datetime.datetime(2016, 12, 31, 23, 59, 59):
        correction = -17
    elif date > datetime.datetime(2016, 12, 31, 23, 59, 59):
        correction = -18
    else:
        correction = -16

    return date + datetime.timedelta(seconds=correction)

# Uncomment for debugging reasons
#@debug
def get_vcid_mode(vcid):
    """
    Method to convert the VCID number into the storage mode
    :param vcid: VCID number
    :type vcid: str

    :return: mode
    :rtype: str

    """

    correspondence = {
        "2": "SAD",
        "3": "HKTM",
        "4": "NOMINAL",
        "5": "NRT",
        "6": "RT",
        "20": "NOMINAL",
        "21": "NRT",
        "22": "RT"
    }

    return correspondence[vcid]


# Uncomment for debugging reasons
#@debug
def get_vcid_apid_configuration(vcid):
    """
    Method to obtain the APID configuration related to the VCID number
    :param vcid: VCID number
    :type vcid: str

    :return: apid_configuration
    :rtype: dict

    """

    # Second half swath
    apids_second_half_swath = {
        "min_apid": 0,
        "max_apid": 92,
    }
    # First half swath
    apids_first_half_swath = {
        "min_apid": 256,
        "max_apid": 348,
    }

    correspondence = {
        "4": apids_second_half_swath,
        "5": apids_second_half_swath,
        "6": apids_second_half_swath,
        "20": apids_first_half_swath,
        "21": apids_first_half_swath,
        "22": apids_first_half_swath
    }

    return correspondence[vcid]

# Uncomment for debugging reasons
#@debug
def get_band_detector(apid):
    """
    Method to obtain the band and detector numbers related to the APID number

    The detector and the bands are determined from APID
        APID RANGE     DETECTOR
           0-15           12
           16-31           11
           32-47           10
           48-63           9
           64-79           8
           80-95           7

           256-271           6
           272-287           5
           288-303           4
           304-319           3
           320-335           2
           336-351           1

        APID MOD 16     BAND
             0           1
             1           2
             2           3
             3           4
             4           5
             5           6
             6           7
             7           8
             8           8a
             9           9
             10          10
             11          11
             12          12

    :param apid: APID number
    :type vcid: str

    :return: band_detector_configuration
    :rtype: dict

    """

    if int(apid) < 256:
        detector = 12 - math.floor(int(apid)/16)
    else:
        detector = 12 - (math.floor((int(apid) - 256)/16) + 6)
    # end if

    raw_band = (int(apid) % 16) + 1
    if raw_band == 9:
        band = "8a"
    elif raw_band > 9:
        band = raw_band - 1
    else:
        band = raw_band
    # end if

    return {"detector": str(detector), "band": str(band)}

# Uncomment for debugging reasons
#@debug
def get_counter_threshold(band):
    """
    Method to obtain the counter threshold related to the band number
        BAND     COUNTER THRESHOLD    METRES
           1              23            60
           2-4           143            10
           5-7            71            20
           8             143            10
           8a             71            20
           9-10           23            60
           11-12          71            20

    :param band: band number
    :type band: str

    :return: counter_threshold
    :rtype: int

    """

    if band == "2" or band == "3" or band == "4" or band == "8":
        counter_threshold = 143
    elif band == "1" or band == "9" or band == "10":
        counter_threshold = 23
    else:
        counter_threshold = 71
    # end if

    return counter_threshold

# Uncomment for debugging reasons
#@debug
def get_counter_threshold_from_apid(apid):
    """
    Method to obtain the counter threshold related to the apid number

    :param apid: apid number
    :type apid: str

    :return: counter_threshold
    :rtype: int

    """

    band_detector = get_band_detector(apid)

    return get_counter_threshold(band_detector["band"])

def get_apid_numbers():
    """
    Method to obtain the APID numbers used

    :return: list of APID numbers
    :rtype: list

    """
    apids = []
    for i in range(6):
        for j in range (13):
            apids.append(i*16 + j)
        # end for
    # end for
    for i in range(6):
        for j in range (13):
            apids.append((i*16 + j) + 256)
        # end for
    # end for
    return apids

###
# Date helpers
###

# Uncomment for debugging reasons
#@debug
def three_letter_to_iso_8601(date):
    """
    Method to convert a date in three letter format to a date in ISO 8601 format
    :param date: date in three letter format (DD-MMM-YYYY HH:MM:SS.ssssss)
    :type date: str

    :return: date in ISO 8601 format (YYYY-MM-DDTHH:MM:SS)
    :rtype: str

    """
    month = {
        "JAN": "01",
        "FEB": "02",
        "MAR": "03",
        "APR": "04",
        "MAY": "05",
        "JUN": "06",
        "JUL": "07",
        "AUG": "08",
        "SEP": "09",
        "OCT": "10",
        "NOV": "11",
        "DEC": "12"
    }
    year = date[7:11]
    month = month[date[3:6]]
    day = date[0:2]
    hours = date[12:14]
    minutes = date[15:17]
    seconds = date[18:20]
    microseconds = date[21:27]

    return year + "-" + month + "-" + day + "T" + hours + ":" + minutes + ":" + seconds + "." + microseconds

def L0_L1A_L1B_processing(source, engine, query, granule_timeline, list_of_events, datastrip, granule_timeline_per_detector, list_of_operations, system, version, filename):
    """
    Method to generate the events for the levels L0 and L1B
    :param source: information of the source
    :type source: dict
    :param engine: object to access the engine of the EBOA
    :type engine: Engine
    :param query: object to access the query interface of the EBOA
    :type query: Query
    :param granule_timeline: list of granule segments to be processed
    :type granule_timeline: list
    :param list_of_events: list to store the events to be inserted into the eboa
    :type list_of_events: list
    :param datastrip: datastrip
    :type datastrip: str
    :param granule_timeline_per_detector: dict containing the granule segments per detector
    :type granule_timeline_per_detector: dict
    :param list_of_operations: list of operations to be inserted into EBOA
    :type list_of_operations: list
    :param level: level of the outputs being processed
    :type level: str
    :param system: center where data has been processed
    :type system: str
    :param version: version of the processor used
    :type version: str
    :param filename: name of the processor file
    :type version: str

    :return: None

    """
    general_status = "COMPLETE"
    granule_timeline_sorted = date_functions.sort_timeline_by_start(granule_timeline)
    datablocks = date_functions.merge_timeline(granule_timeline_sorted)
    # Obtain the satellite
    satellite = source["name"][0:3]

    # Obtain the production level from the datastrip
    level = datastrip[13:16].replace("_","")

    # Completeness operations for the completeness analysis of the plan
    planning_processing_completeness_operation = {
        "mode": "insert",
        "dim_signature": {
            "name": "PROCESSING_" + satellite,
            "exec": "planning_processing_" + filename,
            "version": version
        },
        "events": []
    }
    # Completeness operations for the completeness analysis of the received imaging
    isp_validity_processing_completeness_operation = {
        "mode": "insert",
        "dim_signature": {
            "name": "PROCESSING_" + satellite,
            "exec": "processing_received_" + filename,
            "version": version
        },
        "events": []
    }

    for datablock in datablocks:
        status = "COMPLETE"

        # Obtain the gaps from the processing per detector
        processing_gaps = {}
        granule_timeline_per_detector_sorted = {}
        datablocks_per_detector = {}
        intersected_datablock_per_detector = {}
        for detector in granule_timeline_per_detector:
            granule_timeline_per_detector_sorted[detector] = date_functions.sort_timeline_by_start(granule_timeline_per_detector[detector])
            datablocks_per_detector[detector] = date_functions.merge_timeline(granule_timeline_per_detector_sorted[detector])
            datablock_for_extracting_gaps = {
                "id": datablock["id"],
                "start": datablock["start"] + datetime.timedelta(seconds=6),
                "stop": datablock["stop"] - datetime.timedelta(seconds=6)
            }
            intersected_datablock_per_detector[detector] = date_functions.intersect_timelines([datablock_for_extracting_gaps], datablocks_per_detector[detector])
            processing_gaps[detector] = date_functions.difference_timelines(intersected_datablock_per_detector[detector],[datablock_for_extracting_gaps])
            if len(processing_gaps[detector]) == 0:
                del processing_gaps[detector]
            # end if
        # end for

        # Obtain the gaps existing during the reception per detector
        isp_gaps = query.get_events(gauge_names = {"filter": "ISP_GAP", "op": "like"},
                                    value_filters = [{"name": {"str": "satellite", "op": "like"}, "type": "text", "value": {"op": "like", "value": satellite}}],
                                    start_filters = [{"date": datablock["stop"].isoformat(), "op": "<"}],
                                    stop_filters = [{"date": datablock["start"].isoformat(), "op": ">"}])
        data_isp_gaps = {}
        for gap in isp_gaps:
            detector = [value.value for value in gap.eventDoubles if value.name == "detector"][0]
            if detector not in data_isp_gaps:
                data_isp_gaps[str(int(detector))] = []
            # end if
            data_isp_gaps[str(int(detector))].append({
                "id": gap.event_uuid,
                "start": gap.start,
                "stop": gap.stop
            })
        # end for

        # Merge gaps per detector
        data_merged_isp_gaps = {}
        for detector in data_isp_gaps:
            data_merged_isp_gaps[detector] = date_functions.merge_timeline(data_isp_gaps[detector])
        # end for

        gaps_due_to_reception_issues = {}
        gaps_due_to_processing_issues = {}
        for detector in processing_gaps:
            status="INCOMPLETE"
            general_status = "INCOMPLETE"
            if detector in data_merged_isp_gaps:
                gaps_due_to_reception_issues[detector] = date_functions.intersect_timelines(processing_gaps[detector], data_merged_isp_gaps[detector])
                gaps_due_to_processing_issues[detector] = date_functions.difference_timelines(processing_gaps[detector], gaps_due_to_reception_issues[detector])
            else:
                gaps_due_to_processing_issues[detector] = processing_gaps[detector]
            # end if
        # end for

        processing_validity_link_ref = "PROCESSING_VALIDITY_" + datablock["start"].isoformat()
        # Create gap events
        def create_processing_gap_events(gaps, gap_source):
            for detector in gaps:
                for gap in gaps[detector]:
                    gap_event = {
                        "key": datastrip + "_" + "processing_validity",
                        "explicit_reference": datastrip,
                        "gauge": {
                            "insertion_type": "EVENT_KEYS",
                            "name": "PROCESSING_GAP",
                            "system": system
                        },
                        "links": [{
                                 "link": processing_validity_link_ref,
                                 "link_mode": "by_ref",
                                 "name": "PROCESSING_GAP",
                                 "back_ref": "PROCESSING_VALIDITY"
                                 }
                             ],
                         "start": gap["start"].isoformat(),
                         "stop": gap["stop"].isoformat(),
                         "values": [{
                             "name": "details",
                             "type": "object",
                             "values": [{
                                "type": "double",
                                "value": detector,
                                "name": "detector"
                                },{
                                "type": "text",
                                "value": gap_source,
                                "name": "source"
                            },{
                                "type": "text",
                                "value": level,
                                "name": "level"
                            },{
                                "type": "text",
                                "value": satellite,
                                "name": "satellite"
                            }]
                         }]
                    }
                    list_of_events.append(gap_event)
                # end for
            # end for
        # end def

        create_processing_gap_events(gaps_due_to_reception_issues, "reception")
        create_processing_gap_events(gaps_due_to_processing_issues, "processing")

        # Obtain the planned imaging
        corrected_planned_imagings = query.get_events(gauge_names = {"filter": "PLANNED_CUT_IMAGING_CORRECTION", "op": "like"},
                                                      gauge_systems = {"filter": satellite, "op": "like"},
                                                      start_filters = [{"date": datablock["stop"].isoformat(), "op": "<"}],
                                                      stop_filters = [{"date": datablock["start"].isoformat(), "op": ">"}])

        links_processing_validity = []
        links_planning_processing_completeness = []
        links_processing_reception_completeness = []
        planning_matching_status = "NO_MATCHED_PLANNED_IMAGING"
        reception_matching_status = "NO_MATCHED_ISP_VALIDITY"
        sensing_orbit = ""
        downlink_orbit = ""

        # Planning completeness
        if len(corrected_planned_imagings) > 0:
            corrected_planned_imaging = corrected_planned_imagings[0]
            planned_imaging_uuid = [event_link.event_uuid_link for event_link in corrected_planned_imaging.eventLinks if event_link.name == "PLANNED_EVENT"][0]
            planning_matching_status = "MATCHED_PLANNED_IMAGING"
            sensing_orbit_values = query.get_event_values_interface(value_type="double",
                                                                    value_filters=[{"name": {"op": "like", "str": "start_orbit"}, "type": "double"}],
                                                                    event_uuids = {"op": "in", "filter": [planned_imaging_uuid]})
            sensing_orbit = str(sensing_orbit_values[0].value)

            links_processing_validity.append({
                "link": str(planned_imaging_uuid),
                "link_mode": "by_uuid",
                "name": "PROCESSING_VALIDITY",
                "back_ref": "PLANNED_IMAGING"
            })
            links_planning_processing_completeness.append({
                "link": str(planned_imaging_uuid),
                "link_mode": "by_uuid",
                "name": "PROCESSING_COMPLETENESS",
                "back_ref": "PLANNED_IMAGING"
            })

            value = {
                "name": "processing_completeness_" + level+ "_began",
                "type": "object",
                "values": []
            }
            exit_status = engine.insert_event_values(planned_imaging_uuid, value)
            if exit_status["inserted"] == True:

                planned_imaging_event = query.get_events(event_uuids = {"op": "in", "filter": [planned_imaging_uuid]})
                planning_processing_completeness_generation_time = planned_imaging_event[0].source.generation_time.isoformat()

                # Insert the linked COMPLETENESS event for the automatic completeness check
                planning_event_values = corrected_planned_imaging.get_structured_values()
                planning_event_values[0]["values"] = planning_event_values[0]["values"] + [
                    {"name": "status",
                     "type": "text",
                     "value": "MISSING"}
                ]

                # Add margin of 4 seconds to each side of the segment to avoid false alerts
                start = corrected_planned_imaging.start + datetime.timedelta(seconds=4)
                stop = corrected_planned_imaging.stop - datetime.timedelta(seconds=4)

                planning_processing_completeness_operation["events"].append({
                    "gauge": {
                            "insertion_type": "SIMPLE_UPDATE",
                        "name": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_" + level,
                        "system": satellite
                    },
                    "start": start.isoformat(),
                    "stop": stop.isoformat(),
                    "links": [
                        {
                            "link": str(planned_imaging_uuid),
                            "link_mode": "by_uuid",
                            "name": "PROCESSING_COMPLETENESS",
                            "back_ref": "PLANNED_IMAGING"
                        }],
                    "values": planning_event_values
                })
            # end if
        # end if

        # Obtain ISP_VALIDITY events
        isp_validities = query.get_events(gauge_names = {"filter":"ISP_VALIDITY", "op":"like"},
                                          value_filters = [{"name": {"str": "satellite", "op": "like"}, "type": "text", "value": {"op": "like", "value": satellite}}],
                                          start_filters = [{"date": datablock["stop"].isoformat(), "op": "<"}],
                                          stop_filters = [{"date": datablock["start"].isoformat(), "op": ">"}])
        # Received Imaging Completeness
        if len(isp_validities) > 0:
            reception_matching_status = "MATCHED_ISP_VALIDITY"
            isp_validity_segments = date_functions.convert_eboa_events_to_date_segments(isp_validities)
            intersected_isp_validities = date_functions.get_intersected_timeline_with_idx(date_functions.intersect_timelines([datablock], isp_validity_segments), 2)
            greater_isp_validity_segment = date_functions.get_greater_segment(intersected_isp_validities)
            isp_validity = [isp_validity for isp_validity in isp_validities if isp_validity.event_uuid == greater_isp_validity_segment["id"]][0]

            isp_validity_uuid = isp_validity.event_uuid

            downlink_orbit_values = query.get_event_values_interface(value_type="double",
                                                                    value_filters=[{"name": {"op": "like", "str": "downlink_orbit"}, "type": "double"}],
                                                                     event_uuids = {"op": "in", "filter": [isp_validity_uuid]})
            downlink_orbit = str(downlink_orbit_values[0].value)

            links_processing_validity.append({
                "link": str(isp_validity_uuid),
                "link_mode": "by_uuid",
                "name": "PROCESSING_VALIDITY",
                "back_ref": "ISP_VALIDITY"
            })
            links_processing_reception_completeness.append({
                "link": str(isp_validity_uuid),
                "link_mode": "by_uuid",
                "name": "PROCESSING_COMPLETENESS",
                "back_ref": "ISP_VALIDITY"
            })

            value = {
                "name": "received_imaging_completeness_" + level+ "_began",
                "type": "object",
                "values": []
            }
            exit_status = engine.insert_event_values(isp_validity_uuid, value)
            if exit_status["inserted"] == True:

                isp_validity_processing_completeness_generation_time = isp_validity.source.generation_time.isoformat()

                # Insert the linked COMPLETENESS event for the automatic completeness check
                isp_validity_values = isp_validity.get_structured_values()
                isp_validity_values[0]["values"] = [value for value in isp_validity_values[0]["values"] if value["name"] != "status"] + [
                    {"name": "status",
                     "type": "text",
                     "value": "MISSING"}
                ]

                # Add margin of 6 second to each side of the segment to avoid false alerts
                start = isp_validity.start + datetime.timedelta(seconds=6)
                stop = isp_validity.stop - datetime.timedelta(seconds=6)

                isp_validity_processing_completeness_operation["events"].append({
                    "gauge": {
                            "insertion_type": "SIMPLE_UPDATE",
                        "name": "ISP_VALIDITY_PROCESSING_COMPLETENESS_" + level,
                        "system": satellite
                    },
                    "start": start.isoformat(),
                    "stop": stop.isoformat(),
                    "links": [
                        {
                            "link": str(isp_validity_uuid),
                            "link_mode": "by_uuid",
                            "name": "PROCESSING_COMPLETENESS",
                            "back_ref": "ISP_VALIDITY"
                        }],
                    "values": isp_validity_values
                })

            # end if
        # end if

        links_planning_processing_completeness.append({
            "link": processing_validity_link_ref,
            "link_mode": "by_ref",
            "name": "PROCESSING_COMPLETENESS",
            "back_ref": "PROCESSING_VALIDITY"
        })
        planning_processing_completeness_event = {
            "explicit_reference": datastrip,
            "gauge": {
                "insertion_type": "INSERT_and_ERASE_per_EVENT",
                "name": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_" + level,
                "system": satellite
            },
            "links": links_planning_processing_completeness,
             "start": datablock["start"].isoformat(),
             "stop": datablock["stop"].isoformat(),
             "values": [{
                 "name": "details",
                 "type": "object",
                 "values": [{
                    "type": "text",
                    "value": status,
                    "name": "status"
                 },{
                     "type": "text",
                     "value": level,
                     "name": "level"
                 },{
                     "type": "text",
                     "value": satellite,
                     "name": "satellite"
                 },{
                     "name": "processing_centre",
                     "type": "text",
                     "value": system
                 },{
                     "name": "matching_plan_status",
                     "type": "text",
                     "value": planning_matching_status
                 },{
                     "name": "matching_reception_status",
                     "type": "text",
                     "value": reception_matching_status
                 }]
             }]
        }
        if sensing_orbit != "":
            planning_processing_completeness_event["values"][0]["values"].append({
                "name": "sensing_orbit",
                "type": "double",
                "value": sensing_orbit
            })
        # end if
        if downlink_orbit != "":
            planning_processing_completeness_event["values"][0]["values"].append({
                "name": "downlink_orbit",
                "type": "double",
                "value": downlink_orbit
            })
        # end if
        list_of_events.append(planning_processing_completeness_event)

        links_processing_reception_completeness.append({
            "link": processing_validity_link_ref,
            "link_mode": "by_ref",
            "name": "PROCESSING_COMPLETENESS",
            "back_ref": "PROCESSING_VALIDITY"
        })
        processing_reception_completeness_event = {
            "explicit_reference": datastrip,
            "gauge": {
                "insertion_type": "INSERT_and_ERASE_per_EVENT",
                "name": "ISP_VALIDITY_PROCESSING_COMPLETENESS_" + level,
                "system": satellite
            },
            "links": links_processing_reception_completeness,
             "start": datablock["start"].isoformat(),
             "stop": datablock["stop"].isoformat(),
             "values": [{
                 "name": "details",
                 "type": "object",
                 "values": [{
                    "type": "text",
                    "value": status,
                    "name": "status"
                 },{
                     "type": "text",
                     "value": level,
                     "name": "level"
                 },{
                     "type": "text",
                     "value": satellite,
                     "name": "satellite"
                 },{
                     "name": "processing_centre",
                     "type": "text",
                     "value": system
                 },{
                     "name": "matching_plan_status",
                     "type": "text",
                     "value": planning_matching_status
                 },{
                     "name": "matching_reception_status",
                     "type": "text",
                     "value": reception_matching_status
                 }]
             }]
        }
        if sensing_orbit != "":
            processing_reception_completeness_event["values"][0]["values"].append({
                "name": "sensing_orbit",
                "type": "double",
                "value": sensing_orbit
            })
        # end if
        if downlink_orbit != "":
            processing_reception_completeness_event["values"][0]["values"].append({
                "name": "downlink_orbit",
                "type": "double",
                "value": downlink_orbit
            })
        # end if

        list_of_events.append(processing_reception_completeness_event)
        processing_validity_event = {
            "key": datastrip + "_" + "processing_validity",
            "link_ref": processing_validity_link_ref,
            "explicit_reference": datastrip,
            "gauge": {
                "insertion_type": "EVENT_KEYS",
                "name": "PROCESSING_VALIDITY",
                "system": system
            },
            "links": links_processing_validity,
             "start": datablock["start"].isoformat(),
             "stop": datablock["stop"].isoformat(),
             "values": [{
                 "name": "details",
                 "type": "object",
                 "values": [{
                    "type": "text",
                    "value": status,
                    "name": "status"
                 },{
                     "type": "text",
                     "value": level,
                     "name": "level"
                 },{
                     "type": "text",
                     "value": satellite,
                     "name": "satellite"
                 },{
                     "name": "processing_centre",
                     "type": "text",
                     "value": system
                 },{
                     "name": "matching_plan_status",
                     "type": "text",
                     "value": planning_matching_status
                 },{
                     "name": "matching_reception_status",
                     "type": "text",
                     "value": reception_matching_status
                 }],
             }]
        }

        if sensing_orbit != "":
            processing_validity_event["values"][0]["values"].append({
                "name": "sensing_orbit",
                "type": "double",
                "value": sensing_orbit
            })
        # end if
        if downlink_orbit != "":
            processing_validity_event["values"][0]["values"].append({
                "name": "downlink_orbit",
                "type": "double",
                "value": downlink_orbit
            })
        # end if

        list_of_events.append(processing_validity_event)

    # end for

    if len(planning_processing_completeness_operation["events"]) > 0:
        completeness_event_starts = [event["start"] for event in planning_processing_completeness_operation["events"]]
        completeness_event_starts.sort()
        completeness_event_stops = [event["stop"] for event in planning_processing_completeness_operation["events"]]
        completeness_event_stops.sort()

        # Source for the completeness planning operation adjusting the validity to the events
        planning_processing_completeness_operation["source"] = {
            "name": source["name"],
            "generation_time": planning_processing_completeness_generation_time,
            "validity_start": completeness_event_starts[0],
            "validity_stop": completeness_event_stops[-1]
        }

        list_of_operations.append(planning_processing_completeness_operation)
    # end if

    if len(isp_validity_processing_completeness_operation["events"]) > 0:
        completeness_event_starts = [event["start"] for event in isp_validity_processing_completeness_operation["events"]]
        completeness_event_starts.sort()
        completeness_event_stops = [event["stop"] for event in isp_validity_processing_completeness_operation["events"]]
        completeness_event_stops.sort()

        # Source for the completeness received imaging operation adjusting the validity to the events
        isp_validity_processing_completeness_operation["source"] = {
            "name": source["name"],
            "generation_time": isp_validity_processing_completeness_generation_time,
            "validity_start": completeness_event_starts[0],
            "validity_stop": completeness_event_stops[-1]
        }

        list_of_operations.append(isp_validity_processing_completeness_operation)
    # end if

    return general_status
# end def

def L1C_L2A_processing(source, engine, query, list_of_events, processing_validity_events, datastrip, list_of_operations, system, version, filename):
    """
    Method to generate the events for the levels L1C and L2A
    :param source: information of the source
    :type source: dict
    :param engine: object to access the engine of the EBOA
    :type engine: Engine
    :param query: object to access the query interface of the EBOA
    :type query: Query
    :param list_of_events: list to store the events to be inserted into the eboa
    :type list_of_events: list
    :param processing_validity_events: dict containing the events linked to the sensing date from the datablock analysed
    :type processing_validity_events: dict
    :param datastrip: datastrip
    :type datastrip: str
    :param list_of_operations: list of operations to be inserted into EBOA
    :type list_of_operations: list
    :param system: center where data has been processed
    :type system: str
    :param version: version of the processor used
    :type version: str
    :param filename: name of the processor file
    :type version: str

    :return: None

    """
    gaps = []
    planned_cut_imagings = []
    isp_validities = []
    general_status = "COMPLETE"

    # Obtain the satellite
    satellite = source["name"][0:3]

    # Obtain the production level from the datastrip
    level = datastrip[13:16].replace("_","")

    # Completeness operations for the completeness analysis of the plan
    planning_processing_completeness_operation = {
        "mode": "insert",
        "dim_signature": {
            "name": "PROCESSING_" + satellite,
            "exec": "planning_processing_" + filename,
            "version": version
        },
        "events": []
    }
    # Completeness operations for the completeness analysis of the received imaging
    isp_validity_processing_completeness_operation = {
        "mode": "insert",
        "dim_signature": {
            "name": "PROCESSING_" + satellite,
            "exec": "processing_received_" + filename,
            "version": version
        },
        "events": []
    }

    # Classify the events obtained from the datatrip linked events
    for processing_validity_event in processing_validity_events:
        status = "COMPLETE"
        linking_to_processing_validity_events = query.get_linking_events(event_uuids = {"filter": [processing_validity_event.event_uuid], "op": "in"},
                                                              link_names = {"filter": ["PROCESSING_GAP", "PLANNED_IMAGING", "ISP_VALIDITY"], "op": "in"})

        for event in linking_to_processing_validity_events["linking_events"]:
            if event.gauge.name.startswith("PROCESSING_GAP"):
                gaps.append(event)
            # end if
            if event.gauge.name.startswith("PLANNED_CUT_IMAGING"):
                planned_cut_imagings.append(event)
            # end elif
            if event.gauge.name.startswith("ISP_VALIDITY"):
                isp_validities.append(event)
        # end for

        # If gaps, status is incomplete
        processing_validity_link_ref = "PROCESSING_VALIDITY_" + processing_validity_event.start.isoformat()
        if len(gaps) > 0:
            status = "INCOMPLETE"
            general_status = "INCOMPLETE"

            for gap in gaps:
                values = gap.get_structured_values()
                value_level = [value for value in values[0]["values"] if value["name"] == "level"][0]
                value_level["value"] = level
                gap_event = {
                    "key": datastrip + "_" + "processing_validity",
                    "explicit_reference": datastrip,
                    "gauge": {
                        "insertion_type": "EVENT_KEYS",
                        "name": "PROCESSING_GAP",
                        "system": system
                    },
                    "links": [{
                             "link": processing_validity_link_ref,
                             "link_mode": "by_ref",
                             "name": "PROCESSING_GAP",
                             "back_ref": "PROCESSING_VALIDITY"
                             }
                         ],
                     "start": gap.start.isoformat(),
                     "stop": gap.stop.isoformat(),
                     "values": values
                }
                list_of_events.append(gap_event)
            # end for
        # end if

        links_processing_validity = []
        links_planning_processing_completeness = []
        links_processing_reception_completeness = []
        planning_matching_status = "NO_MATCHED_PLANNED_IMAGING"
        reception_matching_status = "NO_MATCHED_ISP_VALIDITY"
        sensing_orbit = ""
        downlink_orbit = ""

        # Planning completeness
        if len(planned_cut_imagings) > 0:
            planned_imaging = planned_cut_imagings[0]
            planned_imaging_uuid = planned_imaging.event_uuid
            planning_matching_status = "MATCHED_PLANNED_IMAGING"
            sensing_orbit_values = query.get_event_values_interface(value_type="double",
                                                                    value_filters=[{"name": {"op": "like", "str": "start_orbit"}, "type": "double"}],
                                                                    event_uuids = {"op": "in", "filter": [planned_imaging_uuid]})
            sensing_orbit = str(sensing_orbit_values[0].value)

            links_processing_validity.append({
                "link": str(planned_imaging_uuid),
                "link_mode": "by_uuid",
                "name": "PROCESSING_VALIDITY",
                "back_ref": "PLANNED_IMAGING"
            })
            links_planning_processing_completeness.append({
                "link": str(planned_imaging_uuid),
                "link_mode": "by_uuid",
                "name": "PROCESSING_COMPLETENESS",
                "back_ref": "PLANNED_IMAGING"
            })

            value = {
                "name": "processing_completeness_" + level + "_began",
                "type": "object",
                "values": []
            }

            exit_status = engine.insert_event_values(planned_imaging_uuid, value)
            if exit_status["inserted"] == True:
                corrected_planned_imaging_uuid = [event_link.event_uuid_link for event_link in planned_imaging.eventLinks if event_link.name == "TIME_CORRECTION"][0]
                corrected_planned_imaging_event = query.get_events(event_uuids = {"op": "in", "filter": [corrected_planned_imaging_uuid]})

                planning_processing_completeness_generation_time = planned_imaging.source.generation_time.isoformat()

                planning_event_values = planned_imaging.get_structured_values()
                planning_event_values[0]["values"] = planning_event_values[0]["values"] + [
                    {"name": "status",
                     "type": "text",
                     "value": "MISSING"}
                ]

                # Add margin of 4 seconds to each side of the segment to avoid false alerts
                start = corrected_planned_imaging_event[0].start + datetime.timedelta(seconds=4)
                stop = corrected_planned_imaging_event[0].stop - datetime.timedelta(seconds=4)

                planning_processing_completeness_operation["events"].append({
                    "gauge": {
                            "insertion_type": "SIMPLE_UPDATE",
                        "name": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_" + level + "",
                        "system": satellite
                    },
                    "start": start.isoformat(),
                    "stop": stop.isoformat(),
                    "links": [
                        {
                            "link": str(planned_imaging_uuid),
                            "link_mode": "by_uuid",
                            "name": "PROCESSING_COMPLETENESS",
                            "back_ref": "PLANNED_IMAGING"
                        }],
                    "values": planning_event_values
                })
            # end if
        # end if

        # Received Imaging Completeness
        if len(isp_validities) > 0:
            reception_matching_status = "MATCHED_ISP_VALIDITY"
            isp_validity = isp_validities[0]

            isp_validity_uuid = isp_validity.event_uuid

            downlink_orbit_values = query.get_event_values_interface(value_type="double",
                                                                    value_filters=[{"name": {"op": "like", "str": "downlink_orbit"}, "type": "double"}],
                                                                     event_uuids = {"op": "in", "filter": [isp_validity_uuid]})
            downlink_orbit = str(downlink_orbit_values[0].value)

            links_processing_validity.append({
                "link": str(isp_validity_uuid),
                "link_mode": "by_uuid",
                "name": "PROCESSING_VALIDITY",
                "back_ref": "ISP_VALIDITY"
            })
            links_processing_reception_completeness.append({
                "link": str(isp_validity_uuid),
                "link_mode": "by_uuid",
                "name": "PROCESSING_COMPLETENESS",
                "back_ref": "ISP_VALIDITY"
            })

            value = {
                "name": "received_imaging_completeness_" + level+ "_began",
                "type": "object",
                "values": []
            }
            exit_status = engine.insert_event_values(isp_validity_uuid, value)
            if exit_status["inserted"] == True:

                isp_validity_processing_completeness_generation_time = isp_validity.source.generation_time.isoformat()

                # Insert the linked COMPLETENESS event for the automatic completeness check
                isp_validity_values = isp_validity.get_structured_values()
                isp_validity_values[0]["values"] = [value for value in isp_validity_values[0]["values"] if value["name"] != "status"] + [
                    {"name": "status",
                     "type": "text",
                     "value": "MISSING"}
                ]

                # Add margin of 6 second to each side of the segment to avoid false alerts
                start = isp_validity.start + datetime.timedelta(seconds=6)
                stop = isp_validity.stop - datetime.timedelta(seconds=6)

                isp_validity_processing_completeness_operation["events"].append({
                    "gauge": {
                            "insertion_type": "SIMPLE_UPDATE",
                        "name": "ISP_VALIDITY_PROCESSING_COMPLETENESS_" + level,
                        "system": satellite
                    },
                    "start": start.isoformat(),
                    "stop": stop.isoformat(),
                    "links": [
                        {
                            "link": str(isp_validity_uuid),
                            "link_mode": "by_uuid",
                            "name": "PROCESSING_COMPLETENESS",
                            "back_ref": "ISP_VALIDITY"
                        }],
                    "values": isp_validity_values
                })

            # end if
        # end if

        links_planning_processing_completeness.append({
            "link": processing_validity_link_ref,
            "link_mode": "by_ref",
            "name": "PROCESSING_COMPLETENESS",
            "back_ref": "PROCESSING_VALIDITY"
        })
        planning_processing_completeness_event = {
            "explicit_reference": datastrip,
            "gauge": {
                "insertion_type": "INSERT_and_ERASE_per_EVENT",
                "name": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_" + level,
                "system": satellite
            },
            "links": links_planning_processing_completeness,
             "start": processing_validity_event.start.isoformat(),
             "stop": processing_validity_event.stop.isoformat(),
             "values": [{
                 "name": "details",
                 "type": "object",
                 "values": [{
                    "type": "text",
                    "value": status,
                    "name": "status"
                 },{
                     "type": "text",
                     "value": level,
                     "name": "level"
                 },{
                     "type": "text",
                     "value": satellite,
                     "name": "satellite"
                 },{
                     "name": "processing_centre",
                     "type": "text",
                     "value": system
                 },{
                     "name": "matching_plan_status",
                     "type": "text",
                     "value": planning_matching_status
                 },{
                     "name": "matching_reception_status",
                     "type": "text",
                     "value": reception_matching_status
                 }]
             }]
        }

        if sensing_orbit != "":
            planning_processing_completeness_event["values"][0]["values"].append({
                "name": "sensing_orbit",
                "type": "double",
                "value": sensing_orbit
            })
        # end if
        if downlink_orbit != "":
            planning_processing_completeness_event["values"][0]["values"].append({
                "name": "downlink_orbit",
                "type": "double",
                "value": downlink_orbit
            })
        # end if

        list_of_events.append(planning_processing_completeness_event)

        links_processing_reception_completeness.append({
            "link": processing_validity_link_ref,
            "link_mode": "by_ref",
            "name": "PROCESSING_COMPLETENESS",
            "back_ref": "PROCESSING_VALIDITY"
        })
        processing_reception_completeness_event = {
            "explicit_reference": datastrip,
            "gauge": {
                "insertion_type": "INSERT_and_ERASE_per_EVENT",
                "name": "ISP_VALIDITY_PROCESSING_COMPLETENESS_" + level,
                "system": satellite
            },
            "links": links_processing_reception_completeness,
             "start": processing_validity_event.start.isoformat(),
             "stop": processing_validity_event.stop.isoformat(),
             "values": [{
                 "name": "details",
                 "type": "object",
                 "values": [{
                    "type": "text",
                    "value": status,
                    "name": "status"
                 },{
                     "type": "text",
                     "value": level,
                     "name": "level"
                 },{
                     "type": "text",
                     "value": satellite,
                     "name": "satellite"
                 },{
                     "name": "processing_centre",
                     "type": "text",
                     "value": system
                 },{
                     "name": "matching_plan_status",
                     "type": "text",
                     "value": planning_matching_status
                 },{
                     "name": "matching_reception_status",
                     "type": "text",
                     "value": reception_matching_status
                 }]
             }]
        }

        if sensing_orbit != "":
            processing_reception_completeness_event["values"][0]["values"].append({
                "name": "sensing_orbit",
                "type": "double",
                "value": sensing_orbit
            })
        # end if
        if downlink_orbit != "":
            processing_reception_completeness_event["values"][0]["values"].append({
                "name": "downlink_orbit",
                "type": "double",
                "value": downlink_orbit
            })
        # end if

        list_of_events.append(processing_reception_completeness_event)

        processing_validity_event = {
            "key": datastrip + "_" + "processing_validity",
            "link_ref": processing_validity_link_ref,
            "explicit_reference": datastrip,
            "gauge": {
                "insertion_type": "EVENT_KEYS",
                "name": "PROCESSING_VALIDITY",
                "system": system
            },
            "links": links_processing_validity,
             "start": processing_validity_event.start.isoformat(),
             "stop": processing_validity_event.stop.isoformat(),
             "values": [{
                 "name": "details",
                 "type": "object",
                 "values": [{
                    "type": "text",
                    "value": status,
                    "name": "status"
                 },{
                     "type": "text",
                     "value": level,
                     "name": "level"
                 },{
                     "type": "text",
                     "value": satellite,
                     "name": "satellite"
                 },{
                     "name": "processing_centre",
                     "type": "text",
                     "value": system
                 },{
                     "name": "matching_plan_status",
                     "type": "text",
                     "value": planning_matching_status
                 },{
                     "name": "matching_reception_status",
                     "type": "text",
                     "value": reception_matching_status
                 }],
             }]
        }

        if sensing_orbit != "":
            processing_validity_event["values"][0]["values"].append({
                "name": "sensing_orbit",
                "type": "double",
                "value": sensing_orbit
            })
        # end if
        if downlink_orbit != "":
            processing_validity_event["values"][0]["values"].append({
                "name": "downlink_orbit",
                "type": "double",
                "value": downlink_orbit
            })
        # end if

        list_of_events.append(processing_validity_event)

        if len(planning_processing_completeness_operation["events"]) > 0:
            completeness_event_starts = [event["start"] for event in planning_processing_completeness_operation["events"]]
            completeness_event_starts.sort()
            completeness_event_stops = [event["stop"] for event in planning_processing_completeness_operation["events"]]
            completeness_event_stops.sort()

            # Source for the completeness planning operation adjusting the validity to the events
            planning_processing_completeness_operation["source"] = {
                "name": source["name"],
                "generation_time": planning_processing_completeness_generation_time,
                "validity_start": completeness_event_starts[0],
                "validity_stop": completeness_event_stops[-1]
            }

            list_of_operations.append(planning_processing_completeness_operation)
        # end if

        if len(isp_validity_processing_completeness_operation["events"]) > 0:
            completeness_event_starts = [event["start"] for event in isp_validity_processing_completeness_operation["events"]]
            completeness_event_starts.sort()
            completeness_event_stops = [event["stop"] for event in isp_validity_processing_completeness_operation["events"]]
            completeness_event_stops.sort()

            # Source for the completeness received imaging operation adjusting the validity to the events
            isp_validity_processing_completeness_operation["source"] = {
                "name": source["name"],
                "generation_time": isp_validity_processing_completeness_generation_time,
                "validity_start": completeness_event_starts[0],
                "validity_stop": completeness_event_stops[-1]
            }

            list_of_operations.append(isp_validity_processing_completeness_operation)
        # end if


    # end if


    return general_status
# end def
