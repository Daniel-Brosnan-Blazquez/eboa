"""
Ingestion module for the DPC files of Sentinel-2

Written by DEIMOS Space S.L. (femd)

module eboa
"""

# Import python utilities
import os
import argparse
from dateutil import parser
import datetime
import json
import sys
from tempfile import mkstemp

# Import xml parser
from lxml import etree

# Import engine
import eboa.engine.engine as eboa_engine
from eboa.engine.engine import Engine

# Import ingestion helpers
import eboa.engine.ingestion as ingestion
import ingestions.s2.functions as functions
import ingestions.functions.date_functions as date_functions
import ingestions.s2.xpath_functions as xpath_functions

# Import query
from eboa.engine.query import Query

# Import debugging
from eboa.debugging import debug

# Import logging
from eboa.logging import Log

logging_module = Log()
logger = logging_module.logger

version = "1.0"

def L0_L1B_processing(source, engine, query, granule_timeline, list_of_events, ds_output, granule_timeline_per_detector, list_of_operations, system):
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
    :param ds_output: datastrip
    :type ds_output: str
    :param granule_timeline_per_detector: dict containing the granule segments per detector
    :type granule_timeline_per_detector: dict
    :param list_of_operations: list of operations to be inserted into EBOA
    :type list_of_operations: list
    :param level: level of the outputs being processed
    :type level: str
    :param system: center where data has been processed
    :type system: str

    :return: None

    """

    granule_timeline_sorted = date_functions.sort_timeline_by_start(granule_timeline)
    datablocks = date_functions.merge_timeline(granule_timeline_sorted)
    if len(datablocks) > 0:

        # Obtain the satellite
        satellite = source["name"][0:3]

        # Obtain the production level from the datastrip
        level = ds_output[13:16].replace("_","")

        # Completeness operations for the completeness analysis of the plan
        planning_processing_completeness_operation = {
            "mode": "insert",
            "dim_signature": {
                "name": "PROCESSING_" + satellite,
                "exec": "planning_processing_" + os.path.basename(__file__),
                "version": version
            },
            "events": []
        }
        # Completeness operations for the completeness analysis of the received imaging
        isp_validity_processing_completeness_operation = {
            "mode": "insert",
            "dim_signature": {
                "name": "PROCESSING_" + satellite,
                "exec": "processing_received_" + os.path.basename(__file__),
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
                    data_isp_gaps[detector] = {}
                # end if
                data_isp_gaps[detector].append({
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
                if detector in data_merged_isp_gaps:
                    gaps_due_to_reception_issues[detector] = date_functions.intersect_timelines(processing_gaps[detector], data_merged_isp_gaps[detector])
                    gaps_due_to_processing_issues[detector] = date_functions.difference_timelines(processing_gaps[detector], gaps_due_to_reception_issues[detector])
                else:
                    gaps_due_to_processing_issues[detector] = processing_gaps[detector]
                # end if
            # end for

            processing_validity_link_ref = "PROCESSING_VALIDITY_" + datablock["start"].isoformat()
            # Create gap events
            def create_processing_gap_events(gaps, source):
                for detector in gaps:
                    for gap in gaps[detector]:
                        gap_source = "processing"
                        gap_event = {
                            "explicit_reference": ds_output,
                            "gauge": {
                                "insertion_type": "SIMPLE_UPDATE",
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
                                                                        event_uuids = {"op": "in", "filter": [planned_imaging_uuid]})
                downlink_orbit = str(sensing_orbit_values[0].value)

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
                "explicit_reference": ds_output,
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
                         "name": "sensing_orbit",
                         "type": "text",
                         "value": sensing_orbit
                     },{
                         "name": "downlink_orbit",
                         "type": "text",
                         "value": downlink_orbit
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
            list_of_events.append(planning_processing_completeness_event)

            links_processing_reception_completeness.append({
                "link": processing_validity_link_ref,
                "link_mode": "by_ref",
                "name": "PROCESSING_COMPLETENESS",
                "back_ref": "PROCESSING_VALIDITY"
            })
            processing_reception_completeness_event = {
                "explicit_reference": ds_output,
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
                         "name": "sensing_orbit",
                         "type": "text",
                         "value": sensing_orbit
                     },{
                         "name": "downlink_orbit",
                         "type": "text",
                         "value": downlink_orbit
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
            list_of_events.append(processing_reception_completeness_event)

            processing_validity_event = {
                "link_ref": processing_validity_link_ref,
                "explicit_reference": ds_output,
                "gauge": {
                    "insertion_type": "SIMPLE_UPDATE",
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
                         "name": "sensing_orbit",
                         "type": "text",
                         "value": sensing_orbit
                     },{
                         "name": "downlink_orbit",
                         "type": "text",
                         "value": downlink_orbit
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

    # end if

    return status
# end def

def L1C_L2A_processing(source, engine, query, list_of_events, processing_validity_events, ds_output, list_of_operations, system):
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
    :param ds_output: datastrip
    :type ds_output: str
    :param list_of_operations: list of operations to be inserted into EBOA
    :type list_of_operations: list
    :param system: center where data has been processed
    :type system: str

    :return: None

    """
    gaps = []
    planned_cut_imagings = []
    isp_validities = []
    status = "COMPLETE"
    # Classify the events obtained from the datatrip linked events
    if len(processing_validity_events["prime_events"]) > 0:

        # Obtain the satellite
        satellite = source["name"][0:3]

        # Obtain the production level from the datastrip
        level = ds_output[13:16].replace("_","")

        # Completeness operations for the completeness analysis of the plan
        planning_processing_completeness_operation = {
            "mode": "insert",
            "dim_signature": {
                "name": "PROCESSING_" + satellite,
                "exec": "planning_processing_" + os.path.basename(__file__),
                "version": version
            },
            "events": []
        }
        # Completeness operations for the completeness analysis of the received imaging
        isp_validity_processing_completeness_operation = {
            "mode": "insert",
            "dim_signature": {
                "name": "PROCESSING_" + satellite,
                "exec": "processing_received_" + os.path.basename(__file__),
                "version": version
            },
            "events": []
        }

        processing_validity_event = processing_validity_events["prime_events"][0]

        for event in processing_validity_events["linking_events"]:
            if event.gauge.name.startswith("PROCESSING_GAP"):
                gaps.append(event)
            # end if
            elif event.gauge.name.startswith("PLANNED_CUT_IMAGING"):
                planned_cut_imagings.append(event)
            # end elif
            elif event.gauge.name.startswith("ISP_VALIDITY"):
                isp_validities.append(event)
        # end for

        # If gaps, status is incomplete
        processing_validity_link_ref = "PROCESSING_VALIDITY_" + processing_validity_event.start.isoformat()
        if len(gaps) > 0:
            status = "INCOMPLETE"

            for gap in gaps:
                gap_event = {
                    "explicit_reference": ds_output,
                    "gauge": {
                        "insertion_type": "SIMPLE_UPDATE",
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
                     "values": gap.get_structured_values()
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
                        "name": "PLANNED_IMAGING_" + level + "_COMPLETENESS",
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
                                                                    event_uuids = {"op": "in", "filter": [planned_imaging_uuid]})
            downlink_orbit = str(sensing_orbit_values[0].value)

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
            "explicit_reference": ds_output,
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
                     "name": "sensing_orbit",
                     "type": "text",
                     "value": sensing_orbit
                 },{
                     "name": "downlink_orbit",
                     "type": "text",
                     "value": downlink_orbit
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
        list_of_events.append(planning_processing_completeness_event)

        links_processing_reception_completeness.append({
            "link": processing_validity_link_ref,
            "link_mode": "by_ref",
            "name": "PROCESSING_COMPLETENESS",
            "back_ref": "PROCESSING_VALIDITY"
        })
        processing_reception_completeness_event = {
            "explicit_reference": ds_output,
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
                     "name": "sensing_orbit",
                     "type": "text",
                     "value": sensing_orbit
                 },{
                     "name": "downlink_orbit",
                     "type": "text",
                     "value": downlink_orbit
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
        list_of_events.append(processing_reception_completeness_event)

        processing_validity_event = {
            "link_ref": processing_validity_link_ref,
            "explicit_reference": ds_output,
            "gauge": {
                "insertion_type": "SIMPLE_UPDATE",
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
                     "name": "sensing_orbit",
                     "type": "text",
                     "value": sensing_orbit
                 },{
                     "name": "downlink_orbit",
                     "type": "text",
                     "value": downlink_orbit
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


    return status
# end def


def process_file(file_path, engine, query):
    """
    Function to process the file and insert its relevant information
    into the DDBB of the eboa

    :param file_path: path to the file to be processed
    :type file_path: str
    :param engine: object to access the engine of the EBOA
    :type engine: Engine
    :param query: object to access the query interface of the EBOA
    :type query: Query
    """
    file_name = os.path.basename(file_path)

    # Remove namespaces
    (_, new_file_path) = new_file = mkstemp()
    ingestion.remove_namespaces(file_path, new_file_path)

    # Parse file
    parsed_xml = etree.parse(new_file_path)
    xpath_xml = etree.XPathEvaluator(parsed_xml)

    list_of_explicit_references = []
    list_of_annotations = []
    list_of_events = []
    list_of_timelines = []
    list_of_configuration_events = []
    list_of_configuration_explicit_references = []
    list_of_operations = []
    query_upper_level = False

    # Obtain the satellite
    satellite = file_name[0:3]
    # Obtain the station
    system = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Source/System")[0].text
    # Obtain the creation date
    creation_date = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Source/Creation_Date")[0].text.split("=")[1]
    # Obtain the validity start
    validity_start = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Start")[0].text.split("=")[1]
    # Obtain the validity stop
    validity_stop = xpath_xml("/Earth_Explorer_File/Earth_Explorer_Header/Fixed_Header/Validity_Period/Validity_Stop")[0].text.split("=")[1]
    # Obtain the workplan current status
    workplan_current_status = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SUPERVISION_INFO/WORKPLAN_CURRENT_STATUS")[0].text
    # Obtain the workplan message
    workplan_message = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SUPERVISION_INFO/WORKPLAN_MESSAGE")[0].text
    # Obtain the workplan start datetime
    workplan_start_datetime = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SUPERVISION_INFO/WORKPLAN_START_DATETIME")[0].text
    # Obtain the workplan end datetime
    workplan_end_datetime = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SUPERVISION_INFO/WORKPLAN_END_DATETIME")[0].text
    # Obtain a list of the mrfs
    mrf_list = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SYNTHESIS_INFO/Product_Report/List_Of_MRFs/MRF")
    # Obtain a list of the steps
    steps_list = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/DATA/STEP_INFO")
    # Source for the main operation
    source = {
        "name": file_name,
        "generation_time": creation_date,
        "validity_start": validity_start,
        "validity_stop": validity_stop
    }

    # Loop through each output node that contains a datastrip (excluding the auxiliary data)
    for output_msi in xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SYNTHESIS_INFO/Product_Report/*[contains(name(),'Output_Products') and boolean(child::DATA_STRIP_ID)]") :
        granule_timeline_per_detector = {}
        granule_timeline = []
        # Obtain the datastrip
        ds_output = output_msi.find("DATA_STRIP_ID").text
        # Obtain the sensing identifier from the datastrip
        sensing_identifier = ds_output[41:57]
        # Obtain the baseline from the datastrip
        baseline = ds_output[58:]
        # Obtain the production level from the datastrip
        level = ds_output[13:16].replace("_","")

        # Obtain the input datastrip if exists
        input = xpath_xml("/Earth_Explorer_File/Data_Block/SUP_WORKPLAN_REPORT/SPECIFIC_HEADER/SYNTHESIS_INFO/Product_Report/Input_Products/DATA_STRIP_ID")
        if len(input) > 0:
            ds_input = input[0].text
        # end if

        baseline_annotation = {
        "explicit_reference": ds_output,
        "annotation_cnf": {
            "name": "PRODUCTION_BASELINE",
            "system": system
            },
        "values": [{
            "name": "details",
            "type": "object",
            "values": [
                {"name": "baseline",
                 "type": "text",
                 "value": baseline
                 }]
            }]
        }
        list_of_annotations.append(baseline_annotation)

        sensing_identifier_annotation = {
        "explicit_reference": ds_output,
        "annotation_cnf": {
            "name": "SENSING_IDENTIFIER",
            "system": system
            },
        "values": [{
            "name": "details",
            "type": "object",
            "values": [
                {"name": "sensing_identifier",
                 "type": "text",
                 "value": sensing_identifier
                }]
            }]
        }
        list_of_annotations.append(sensing_identifier_annotation)

        explicit_reference = {
           "group": level + "_DS",
           "links": [{
               "back_ref": "INPUT_DATASTRIP",
               "link": "OUTPUT_DATASTRIP",
               "name": ds_input
               }
           ],
           "name": ds_output
        }
        list_of_explicit_references.append(explicit_reference)

        # Loop over each granule in the ouput
        for granule in output_msi.xpath("*[name() = 'GRANULES_ID' and contains(text(),'_GR_')]"):
            # Obtain the granule id
            granule_t = granule.text
            level_gr = granule_t[13:16].replace("_","")
            granule_sensing_date = granule_t[42:57]
            detector = granule_t[59:61]

            # Create a segment for each granule with a margin calculated to get whole scenes
            start= parser.parse(granule_sensing_date)
            stop = start + datetime.timedelta(seconds=5)
            granule_segment = {
                "start": start,
                "stop": stop,
                "id": granule_t
            }

            # Create a dictionary containing all the granules for each detector
            granule_timeline.append(granule_segment)
            if detector not in granule_timeline_per_detector:
                granule_timeline_per_detector[detector] = []
            # end if
            granule_timeline_per_detector[detector].append(granule_segment)
            explicit_reference = {
                "group": level_gr + "_GR",
                "links": [{
                    "back_ref": "DATASTRIP",
                    "link": "GRANULE",
                    "name": ds_output
                    }
                ],
                "name": granule_t
            }
            list_of_explicit_references.append(explicit_reference)
        # end for

        # Loop over each tile in the output
        for tile in output_msi.xpath("*[name() = 'GRANULES_ID' and contains(text(),'_TL_')]"):
            # Obtain the tile id
            tile_t = tile.text
            level_tl = tile_t[13:16]
            level_tl.replace("_","")

            explicit_reference = {
                "group": level_tl + "_TL",
                "links": [{
                    "back_ref": "DATASTRIP",
                    "link": "TILE",
                    "name": ds_output
                    }
                ],
                "name": tile_t
            }
            list_of_explicit_references.append(explicit_reference)
        # end for

        # Loop over each TCI in the ouput
        for true_color in output_msi.xpath("*[name() = 'GRANULES_ID' and contains(text(),'_TC_')]"):
            # Obtain the true color imaging id
            true_color_t = true_color.text
            level_tc = true_color_t[13:16]
            level_tc.replace("_","")

            explicit_reference = {
                "group": level_tc + "_TC",
                "links": [{
                    "back_ref": "DATASTRIP",
                    "link": "TCI",
                    "name": ds_output
                    }
                ],
                "name": true_color_t
            }
            list_of_explicit_references.append(explicit_reference)
        # end_for

        if level == "L1B" or level == "L0":
            L0_L1B_processing(source, engine, query, granule_timeline,list_of_events,ds_output,granule_timeline_per_detector, list_of_operations, system)
        elif (level == "L1C" or level == "L2A") and query_upper_level is False:
            query_upper_level = True
            upper_level_ers = query.get_explicit_refs(annotation_cnf_names = {"filter": "SENSING_IDENTIFIER", "op": "like"},
                                                      groups = {"filter": ["L0_DS", "L1B_DS"], "op": "in"},
                                                      annotation_value_filters = [{"name": {"str": "sensing_identifier", "op": "like"}, "type": "text", "value": {"op": "like", "value": sensing_identifier}}])
            upper_level_ers_same_satellite = [er.explicit_ref for er in upper_level_ers if er.explicit_ref[0:3] == satellite]
            upper_level_er = [er for er in upper_level_ers_same_satellite if er[13:16] == "L1B"]
            if len(upper_level_er) == 0:
                upper_level_er = [er for er in upper_level_ers_same_satellite if er[13:16] == "L0_"]
            # end if
            if len(upper_level_er) > 0:
                er = upper_level_er[0]

                processing_validity_events = query.get_linking_events(gauge_names = {"filter": ["PROCESSING_VALIDITY","PROCESSING_VALIDITY"], "op": "in"},
                                                                      explicit_refs = {"filter": er, "op": "like"},
                                                                      value_filters = [{"name": {"str": "level", "op": "like"}, "type": "text", "value": {"op": "in", "value": ["L0", "L1B"]}}],
                                                                      link_names = {"filter": ["PROCESSING_GAP", "PLANNED_IMAGING", "ISP_VALIDITY"], "op": "in"},
                                                                      return_prime_events = True)
            
                L1C_L2A_processing(source, engine, query, list_of_events, processing_validity_events, ds_output, list_of_operations, system)
            # end if
        # end if

        event_timeliness = {
            "explicit_reference": ds_output,
            "gauge": {
                "insertion_type": "SIMPLE_UPDATE",
                "name": "TIMELINESS",
                "system": system
            },
            "start": steps_list[0].find("PROCESSING_START_DATETIME").text[:-1],
            "stop": steps_list[-1].find("PROCESSING_END_DATETIME").text[:-1]
        }
        list_of_events.append(event_timeliness)

        # Steps
        for step in steps_list:
            if step.find("EXEC_STATUS").text == 'COMPLETED':
                event_step = {
                    "explicit_reference": ds_output,
                    "gauge": {
                        "insertion_type": "SIMPLE_UPDATE",
                        "name": "STEP_INFO",
                        "system": system
                    },
                    "start": step.find("PROCESSING_START_DATETIME").text[:-1],
                    "stop": step.find("PROCESSING_END_DATETIME").text[:-1],
                    "values": [{
                        "name": "details",
                        "type": "object",
                        "values": [{
                                   "name": "id",
                                   "type": "text",
                                   "value": step.get("id")
                                   },{
                                   "name": "exec_mode",
                                   "type": "text",
                                   "value": step.find("SUBSYSTEM_INFO/STEP_REPORT/GENERAL_INFO/EXEC_MODE").text
                        }]
                    }]
                }
                list_of_events.append(event_step)
            # end if
        # end for

        for mrf in mrf_list:
            explicit_reference = {
                "group": "MISSION_CONFIGURATION",
                "links": [{
                    "back_ref": "DATASTRIP",
                    "link": "CONFIGURATION",
                    "name": ds_output
                    }
                ],
                "name": mrf.find("Id").text
            }
            list_of_configuration_explicit_references.append(explicit_reference)
        # end for
    # end for

    for mrf in mrf_list:
        # Only if the mrf does not exist in the DB
        mrfsDB = query.get_events(explicit_refs = {"op": "like", "filter": mrf.find("Id").text})
        if len(mrfsDB) is 0:
            # If the date is correct, else the date is set to a maximum value
            try:
                stop = str(parser.parse(mrf.find("ValidityStop").text[:-1]))
            # end if
            except:
                stop = str(datetime.datetime.max)
            # end except
            event_mrf={
                "key":mrf.find("Id").text,
                "explicit_reference": mrf.find("Id").text,
                "gauge": {
                    "insertion_type": "EVENT_KEYS",
                    "name": "MRF_VALIDITY",
                    "system": system
                },
                "start": mrf.find("ValidityStart").text[:-1],
                "stop": stop,
                "values": [{
                    "name": "details",
                    "type": "object",
                    "values": [{
                          "name": "generation_time",
                          "type": "timestamp",
                          "value": mrf.find("Id").text[25:40]
                          }]
                    }]
                }
            list_of_configuration_events.append(event_mrf)
        # end if
    # end for

    # Adjust the validity period to the events in the operation
    if len(list_of_events) > 0:
        event_starts = [event["start"] for event in list_of_events]
        event_starts.sort()
        if source["validity_start"] > event_starts[0]:
            source["validity_start"] = event_starts[0]
        # end if
        event_stops = [event["stop"] for event in list_of_events]
        event_stops.sort()
        if source["validity_stop"] < event_stops[-1]:
            source["validity_stop"] = event_stops[-1]
        # end if
        list_of_operations.append({
            "mode": "insert",
            "dim_signature": {
                  "name": "PROCESSING_" + satellite,
                  "exec": os.path.basename(__file__),
                  "version": version
            },
            "source": source,
            "annotations": list_of_annotations,
            "explicit_references": list_of_explicit_references,
            "events": list_of_events,
            })

     # end if

    # Adjust the validity period to the events in the operation
    if len(list_of_configuration_events) > 0:
        source = {
            "name": file_name,
            "generation_time": creation_date
        }

        event_starts = [event["start"] for event in list_of_configuration_events]
        event_starts.sort()
        source["validity_start"] = event_starts[0]
        event_stops = [event["stop"] for event in list_of_configuration_events]
        event_stops.sort()
        source["validity_stop"] = event_stops[-1]
        list_of_operations.append({
            "mode": "insert",
            "dim_signature": {
                  "name": "PROCESSING_"  + satellite,
                  "exec": "configuration_" + os.path.basename(__file__),
                  "version": version
            },
            "source": source,
            "explicit_references": list_of_configuration_explicit_references,
            "events": list_of_configuration_events,
        })

     # end if

    data = {"operations": list_of_operations}

    os.remove(new_file_path)

    return data

def insert_data_into_DDBB(data, filename, engine):
    # Treat data
    returned_value = engine.treat_data(data, filename)
    if returned_value == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]:
        logger.error("The file {} could not be validated".format(filename))
    # end if

    return returned_value

def command_process_file(file_path, output_path = None):
    engine = Engine()
    query = Query()

    # Process file
    data = process_file(file_path, engine, query)

    # Validate data
    filename = os.path.basename(file_path)

    returned_value = 0

    # Treat data
    if output_path == None:
        returned_value = insert_data_into_DDBB(data, filename, engine)
    else:
        with open(output_path, "w") as write_file:
            json.dump(data, write_file, indent=4)
    # end if

    return returned_value

if __name__ == "__main__":
    """
    This will be not here. After all the needs are identified, the ingestions a priori will have only the method command_process_file and this method will be called from outside
    """
    args_parser = argparse.ArgumentParser(description='Process MPL_SPs.')
    args_parser.add_argument('-f', dest='file_path', type=str, nargs=1,
                             help='path to the file to process', required=True)
    args_parser.add_argument('-o', dest='output_path', type=str, nargs=1,
                             help='path to the output file', required=False)
    args = args_parser.parse_args()
    file_path = args.file_path[0]
    output_path = None
    if args.output_path != None:
        output_path = args.output_path[0]
    # end if

    # Before calling to the processor there should be a validation of
    # the file following a schema. Schema not available for STNACQs

    returned_value = command_process_file(file_path, output_path)

    logger.info("The ingestion has been performed and the exit status is {}".format(returned_value))
