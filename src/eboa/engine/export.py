"""
Export definition for providing formatted extracted data

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
from geoalchemy2.shape import to_shape

import logging
from eboa.logging import Log

logging = Log()
logger = logging.logger

def build_values_structure(values, structure, position = 0, parent_level = -1, parent_position = 0):
    """
    """
    object_entity_list = [value for value in values if value.parent_level == parent_level and value.parent_position == parent_position and value.position == position]
    
    if len(object_entity_list) > 0:
        object_entity = object_entity_list[0]
        if str(type(object_entity)) in ["<class 'eboa.datamodel.events.EventObject'>", "<class 'eboa.datamodel.annotations.AnnotationObject'>"]:
            object_entity_structure = {"name": object_entity.name,
                                       "type": "object",
                                       "values": []
            }

            child_values = sorted([value for value in values if value.parent_level == parent_level + 1 and value.parent_position == position], key=lambda x: x.position)
            structure.append(object_entity_structure)
            for value in child_values:
                if str(type(value)) in ["<class 'eboa.datamodel.events.EventBoolean'>", "<class 'eboa.datamodel.annotations.AnnotationBoolean'>"]:
                    value_type = "boolean"
                elif str(type(value)) in ["<class 'eboa.datamodel.events.EventDouble'>", "<class 'eboa.datamodel.annotations.AnnotationDouble'>"]:
                    value_type = "double"
                elif str(type(value)) in ["<class 'eboa.datamodel.events.EventTimestamp'>", "<class 'eboa.datamodel.annotations.AnnotationTimestamp'>"]:
                    value_type = "timestamp"
                elif str(type(value)) in ["<class 'eboa.datamodel.events.EventGeometry'>", "<class 'eboa.datamodel.annotations.AnnotationGeometry'>"]:
                    value_type = "geometry"
                elif str(type(value)) in ["<class 'eboa.datamodel.events.EventObject'>", "<class 'eboa.datamodel.annotations.AnnotationObject'>"]:
                    value_type = "object"
                else:
                    value_type = "text"
                # end if

                if value_type != "object":
                    value_content = str(value.value)
                    if value_type == "geometry":
                        value_content = to_shape(value.value).wkt
                    # end if

                    object_entity_structure["values"].append({"name": value.name,
                                                              "type": value_type,
                                                              "value": value_content
                                                          })
                else:
                    build_values_structure(values, object_entity_structure["values"], value.position, parent_level + 1, position)
                # end if
            # end for
        # end if
    # end if
    return

    # def get_source(self, name):
    #     """
    #     """
    #     # Get start of the query
    #     start_query = datetime.datetime.now()

    #     # Generate metadata of the query
    #     data = {}
    #     data["mode"] = "query"
    #     data["request"] = {"name": "get_source",
    #                        "parameters": {"name": name}
    #                    }
                
    #     # Get the source from the DDBB
    #     source = self.get_sources([name])

    #     if len(source) == 0:
    #         # Log that the name provided does not exist into DDBB
    #         logger.error("There is no source into the DDBB with name {}".format(name))
    #         return -1
    #     # end if

    #     data["source"] = {"source_uuid": source[0].source_uuid,
    #                       "name": source[0].name,
    #                       "generation_time": source[0].generation_time,
    #                       "validity_start": source[0].validity_start,
    #                       "validity_stop": source[0].validity_stop,
    #                       "ingestion_time": source[0].ingestion_time,
    #                       "ingestion_duration": source[0].ingestion_duration.total_seconds()
    #                   }

    #     processor_version = source[0].processor_version

    #     # Get the status of the source from the DDBB
    #     source_status = self.get_sources_statuses([source[0].source_uuid])

    #     # Get the dim_signature from the DDBB
    #     dim_signature = self.get_dim_signatures([source[0].dim_signature_uuid])

    #     data["dim_signature"] = {"name": dim_signature[0].dim_signature,
    #                              "version": processor_version,
    #                              "exec": dim_signature[0].dim_exec_name
    #                   }

    #     # Get the events from the DDBB
    #     events = self.get_events(source_uuids = [source[0].source_uuid])

    #     if len(events) > 0:
    #         data["events"] = []
    #         for event in events:
    #             event_info = {"event_uuid": event.event_uuid,
    #                           "start": event.start,
    #                           "stop": event.stop,
    #                           "ingestion_time": event.ingestion_time
    #                       }
    #             values = self.get_event_values([event.event_uuid])
    #             if len (values) > 0:
    #                 event_info["values"] = []
    #                 self._build_values_structure(values, event_info["values"])
    #             # end if
    #             data["events"].append(event_info)
    #         # end for
    #     # en if

    #     # Get the annotations from the DDBB
    #     annotations = self.get_annotations([source[0].source_uuid])
    #     if len(annotations) > 0:
    #         data["annotations"] = []
    #         for annotation in annotations:
    #             annotation_info = {"annotation_uuid": annotation.annotation_uuid,
    #                           "ingestion_time": annotation.ingestion_time,
    #                           "visible": annotation.visible
    #                       }
    #             values = self.get_annotation_values([annotation.annotation_uuid])
    #             if len (values) > 0:
    #                 annotation_info["values"] = []
    #                 self._build_values_structure(values, annotation_info["values"])
    #             # end if
    #             data["annotations"].append(annotation_info)
    #         # end for
    #     # en if

    #     # Annotate the stop of the query
    #     stop_query = datetime.datetime.now()

    #     data["request"]["duration"] = (stop_query - start_query).total_seconds()

    #     return data

    # def get_source_xml(self, name, output):
    #     """
    #     """
    #     # Get the data from DDBB
    #     data = self.get_source(name)

    #     # Get start of the query
    #     start_parsing = datetime.datetime.now()

    #     ops = etree.Element("ops")

    #     query = etree.SubElement(ops, "query", mode=data["mode"])

    #     # Include the request
    #     request = etree.SubElement(query, "request", name="get_source_xml", duration=str(data["request"]["duration"]))
    #     if "parameters" in data["request"]:
    #         parameters = etree.SubElement(request, "parameters")
    #         for parameter in data["request"]["parameters"]:
    #             tag = etree.SubElement(parameters, parameter)
    #             tag.text = data["request"]["parameters"][parameter]
    #         # end for
    #     # end if

    #     # Include the DIM signature
    #     etree.SubElement(query, "dim_signature", name=data["dim_signature"]["name"], 
    #                      version=data["dim_signature"]["version"], 
    #                      exec=data["dim_signature"]["exec"])
    #     # Include the DIM processing
    #     etree.SubElement(query, "source", name=data["source"]["name"], 
    #                      generation_time=str(data["source"]["generation_time"]), 
    #                      validity_start=str(data["source"]["validity_start"]), 
    #                      validity_stop=str(data["source"]["validity_stop"]), 
    #                      ingestion_time=str(data["source"]["ingestion_time"]), 
    #                      ingestion_duration=str(data["source"]["ingestion_duration"]))

    #     data_xml = etree.SubElement(query, "data")

    #     # Include events
    #     for event in data["events"]:
    #         event_xml = etree.SubElement(data_xml, "event", id=str(event["event_uuid"]), start=str(event["start"]),
    #                          stop=str(event["stop"]), ingestion_time=str(event["ingestion_time"]))
    #         if "values" in event:
    #             self._transform_value_to_xml(event["values"][0], event_xml)
    #     # end for

    #     # Include annotations
    #     for annotation in data["annotations"]:
    #         annotation_xml = etree.SubElement(data_xml, "annotation",
    #                                           id=str(annotation["annotation_uuid"]),
    #                                           ingestion_time=str(annotation["ingestion_time"]))
    #         if "values" in annotation:
    #             self._transform_value_to_xml(annotation["values"][0], annotation_xml)
    #     # end for

    #     # Annotate the duration of the parsing
    #     parsing_duration = (datetime.datetime.now() - start_parsing).total_seconds()
    #     request.set("parsing_duration", str(parsing_duration))

    #     etree.ElementTree(ops).write(output, pretty_print=True)
    #     return

    # def _transform_value_to_xml(self, values, node):
    #     """
    #     """
    #     values_xml = etree.SubElement(node, "values", name=values["name"])

    #     for item in values["values"]:
    #         if item["type"] == "object":
    #             self._transform_value_to_xml(item, values_xml)
    #         else:
    #             value = etree.SubElement(values_xml, "value", name=item["name"],
    #                              type=item["type"])
    #             if item["type"] == "geometry":
    #                 value.text = to_shape(item["value"]).wkt.replace("POLYGON ((", "").replace("))", "").replace(",", "")
    #             else:
    #                 value.text = str(item["value"])
    #             # end if
    #         # end if
    #     # end for

    #     return
