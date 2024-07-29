"""
Functions definition for the engine component

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import os
from dateutil import parser
import re
import random

# Import SQLalchemy entities
from sqlalchemy.exc import IntegrityError, InternalError

# Import exceptions
from eboa.engine.errors import WrongValue, OddNumberOfCoordinates

# Import SQLAlchemy utilities
import uuid

# Import debugging
from eboa.debugging import debug, race_condition

# Import datamodel
from eboa.datamodel.alerts import Alert, AlertGroup

# Import alert severity codes
from eboa.engine.alerts import alert_severity_codes

def insert_values(values, entity_uuid, list_values, position = 0, parent_level = -1, parent_position = 0, positions = None):
    """
    Method to insert the values associated to events or annotations

    :param values: list of values to be inserted
    :type values: list
    :param entity_uuid: identifier of the event or annotation
    :type entity_uuid: uuid
    :param list_values: list with the inserted values for later bulk ingestion
    :type list_values: list
    :param position: value position inside the structure of values
    :type position: int
    :param parent_level: level of the parent value inside the structure of values
    :type parent_level: int
    :param parent_position: position of the parent value inside the correspoding level of the structure of values
    :type parent_position: int
    :param positions: counter of the positions per level
    :type positions: dict
    """

    if positions == None:
        positions = {}
    # end if
    if not parent_level in positions:
        # List for keeping track of the positions occupied in the level (parent_level = level - 1)
        positions[parent_level] = position
    # end if
    for item in values:
        if item["type"] == "object":
            if not "objects" in list_values:
                list_values["objects"] = []
            # end if
            list_values["objects"].append(dict([("name", item.get("name")),
                                               ("position",  positions[parent_level]),
                                               ("parent_level",  parent_level),
                                               ("parent_position",  parent_position),
                                               (entity_uuid["name"], entity_uuid["id"])]
            ))
            insert_values(item["values"], entity_uuid, list_values, 0, parent_level + 1, positions[parent_level], positions)
        else:
            value = bool(str(item.get("value")))
            if item["type"] == "boolean":
                if not "booleans" in list_values:
                    list_values["booleans"] = []
                # end if
                if item.get("value").lower() == "true":
                    value = True
                elif item.get("value").lower() == "false":
                    value = False
                else:
                    raise WrongValue("The value {} cannot be converted to the specified type {}".format(item.get("value"), item["type"]))
                list_to_use = list_values["booleans"]
            elif item["type"] == "text":
                value = str(item.get("value"))
                if not "texts" in list_values:
                    list_values["texts"] = []
                # end if
                list_to_use = list_values["texts"]
            elif item["type"] == "double":
                try:
                    value = float(item.get("value"))
                except ValueError:
                    raise WrongValue("The value {} cannot be converted to the specified type {}".format(item.get("value"), item["type"]))
                # end try
                if not "doubles" in list_values:
                    list_values["doubles"] = []
                # end if
                list_to_use = list_values["doubles"]
            elif item["type"] == "timestamp":
                try:
                    value = parser.parse(item.get("value"))
                except ValueError:
                    raise WrongValue("The value {} cannot be converted to the specified type {}".format(item.get("value"), item["type"]))
                # end try
                if not "timestamps" in list_values:
                    list_values["timestamps"] = []
                # end if
                list_to_use = list_values["timestamps"]
            elif item["type"] == "geometry":
                if re.match("POLYGON((.*))", item.get("value")):
                    value = item.get("value")
                else:
                    list_coordinates = item.get("value").split(" ")
                    if len (list_coordinates) % 2 != 0:
                        raise OddNumberOfCoordinates("The geometry value {} contains an odd number of coordinates".format(item.get("value")))
                    # end if
                    coordinates = 0
                    value = "POLYGON(("
                    for coordinate in list_coordinates:
                        if coordinates == 2:
                            value = value + ","
                            coordinates = 0
                        # end if
                        try:
                            float(coordinate)
                        except ValueError:
                            raise WrongValue("The coordinate {} inside the geometry cannot be converted to the specified type {}. Note: coordinates inside the geometry should be separated by white spaces".format(coordinate, "float"))
                        # end try
                        value = value + coordinate
                        coordinates += 1
                        if coordinates == 1:
                            value = value + " "
                        # end if
                    # end for
                    value = value + "))"
                # end if                        
                if not "geometries" in list_values:
                    list_values["geometries"] = []
                # end if
                list_to_use = list_values["geometries"]
            # end if
            list_to_use.append(dict([("name", item.get("name")),
                                     ("value", value),
                                     ("position",  positions[parent_level]),
                                     ("parent_level",  parent_level),
                                     ("parent_position",  parent_position),
                                     (entity_uuid["name"], entity_uuid["id"])]
                                ))
        # end if
        positions[parent_level] += 1
    # end for

    return

def insert_alert_groups(session, operation):
    """
    Method to insert the groups of alerts
    """
    returned_alert_groups = {}
    alert_groups = [alert.get("alert_cnf").get("group") for alert in operation.get("alerts") or []]
    unique_alert_groups = sorted(set(alert_groups))
    for alert_group in unique_alert_groups or []:
        session.begin_nested()
        id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        alert_group_ddbb = AlertGroup(id, alert_group)
        session.add(alert_group_ddbb)
        try:
            race_condition()
            session.commit()
        except IntegrityError:
            # The explicit reference group exists already into DDBB
            session.rollback()
            alert_group_ddbb = session.query(AlertGroup).filter(AlertGroup.name == alert_group).first()
            pass
        # end try
        returned_alert_groups[alert_group] = alert_group_ddbb
    # end for

    return returned_alert_groups

def insert_alert_cnfs(session, operation, alert_groups):
    """
    Method to insert the alert configurations
    """
    returned_alert_cnfs = {}
    alert_cnfs = [(alert.get("alert_cnf").get("name"), alert.get("alert_cnf").get("description"), alert.get("alert_cnf").get("severity"), alert.get("alert_cnf").get("group")) for alert in operation.get("alerts") or []]
    unique_alert_cnfs = sorted(set(alert_cnfs))
    for alert_cnf in unique_alert_cnfs:
        name = alert_cnf[0]
        description = alert_cnf[1]
        severity = alert_severity_codes[alert_cnf[2]]
        group = alert_groups[alert_cnf[3]]
        returned_alert_cnfs[name] = session.query(Alert).filter(Alert.name == name).first()
        if not returned_alert_cnfs[name]:
            session.begin_nested()
            id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
            group
            returned_alert_cnfs[name] = Alert(id, name, severity, group, description)
            session.add(returned_alert_cnfs[name])
            try:
                race_condition()
                session.commit()
            except IntegrityError:
                # The alert has been inserted between the query and the insertion. Roll back transaction for
                # re-using the session
                session.rollback()
                # Get the stored alert configuration
                returned_alert_cnfs[name] = session.query(Alert).filter(Alert.name == name).first()
                pass
            # end try
        # end if
    # end for

    return returned_alert_cnfs
