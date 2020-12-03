#!/usr/bin/env python3
"""
Ingestion module of the health status of the host machine

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import psutil
import datetime
import uuid
import argparse
import os
import random
import json
import glob

# Import xml parser
from lxml import etree

# Import engine functions
from eboa.engine.functions import get_resources_path, get_schemas_path
from eboa.engine.engine import Engine
import eboa.engine.engine as eboa_engine
from eboa.datamodel.base import engine as datamodel_engine

# Import SQLalchemy entities
from sqlalchemy import create_engine

# Import logging
from eboa.logging import Log

logging_module = Log(name = os.path.basename(__file__))
logger = logging_module.logger

version="1.0"

"""
Errors definition for the health ingestion

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class HealthConfigCannotBeRead(Error):
    """Exception raised when the health configuration file cannot be read.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class HealthConfigDoesNotPassSchema(Error):
    """Exception raised when the health configuration does not pass the schema.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

def get_health_conf():
    # schema_path = get_schemas_path() + "/health_schema.xsd"
    # parsed_schema = etree.parse(schema_path)
    # schema = etree.XMLSchema(parsed_schema)
    # Get configuration
    try:
        configuration_xml = etree.parse(get_resources_path() + "/health.xml")
    except etree.XMLSyntaxError as e:
        logger.error("The health configuration file ({}) cannot be read".format(get_resources_path() + "/health.xml"))
        raise HealthConfigCannotBeRead("The health configuration file ({}) cannot be read".format(get_resources_path() + "/health.xml"))
    # end try

    # valid = schema.validate(configuration_xml)
    # if not valid:
    #     logger.error("The health configuration file ({}) does not pass the schema ({})".format(get_resources_path() + "/health.xml", get_schemas_path() + "/health_schema.xsd"))
    #     raise HealthConfigDoesNotPassSchema("The health configuration file ({}) does not pass the schema ({})".format(get_resources_path() + "/health.xml", get_schemas_path() + "/health_schema.xsd"))
    # # end if

    health_xpath = etree.XPathEvaluator(configuration_xml)

    return health_xpath

def main():

    args_parser = argparse.ArgumentParser(description='Ingestion of health status.')
    args_parser.add_argument("-o", dest="output_path", type=str, nargs=1,
                             help="path to the output file", required=False)

    args = args_parser.parse_args()
    output_path = None
    if args.output_path != None:
        output_path = args.output_path[0]
        # Check if file exists
        if not os.path.isdir(os.path.dirname(output_path)):
            print("The specified path to the output file {} does not exist".format(output_path))
            exit(-1)
        # end if
    # end if

    # Read configuration
    health_xpath = get_health_conf()

    now_datetime = datetime.datetime.now()
    now = now_datetime.isoformat()
    
    # Data to be inserted
    values = []
    alerts = []

    id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
    source = "boa_health_" + str(id) + ".json"

    month_odd = now_datetime.month % 2
    key = "BOA_HEALTH_" + str(month_odd) + "-" + str(now_datetime.day) + "T" + str(now_datetime.hour) + ":" + str(now_datetime.minute)

    dim_signature = "BOA_HEALTH"
    
    data = {"operations": [{
        "mode": "insert",
        "dim_signature": {
            "name": dim_signature,
            "exec": os.path.basename(__file__),
            "version": version
        },
        "source": {
            "name": source,
            "reception_time": now,
            "generation_time": now,
            "validity_start": now,
            "validity_stop": now
        },
        "events": [{
            "key": key,
            "link_ref": "boa_health_" + str(id),
            "gauge": {
                "insertion_type": "EVENT_KEYS",
                "name": "BOA_HEALTH"
            },
            "start": now,
            "stop": now,
            "values": values
        }],
        "alerts": alerts
    }]}

    ###
    # Alerts' configuration
    ###
    # General CPU
    general_cpu = health_xpath("/health/alerts/alert[@id = 'general_cpu_max_threshold']")
    general_cpu_max_threshold = 90
    if len(general_cpu) > 0:
        general_cpu_max_threshold = float(general_cpu[0].get("threshold"))
    # end if
    
    # Particular CPU
    particular_cpu = health_xpath("/health/alerts/alert[@id = 'particular_cpu_max_threshold']")
    particular_cpu_max_threshold = 90
    if len(particular_cpu) > 0:
        particular_cpu_max_threshold = float(particular_cpu[0].get("threshold"))
    # end if

    # Memory
    memory = health_xpath("/health/alerts/alert[@id = 'memory_max_threshold']")
    memory_max_threshold = 75
    if len(memory) > 0:
        memory_max_threshold = float(memory[0].get("threshold"))
    # end if

    # Swap
    swap = health_xpath("/health/alerts/alert[@id = 'swap_max_threshold']")
    swap_max_threshold = 1
    if len(swap) > 0:
        swap_max_threshold = float(swap[0].get("threshold"))
    # end if

    # Ddbb_Connections
    ddbb_connections = health_xpath("/health/alerts/alert[@id = 'ddbb_connections_max_threshold']")
    ddbb_connections_max_threshold = 300
    if len(ddbb_connections) > 0:
        ddbb_connections_max_threshold = float(ddbb_connections[0].get("threshold"))
    # end if

    # Disk usage
    disk_usage = health_xpath("/health/alerts/alert[@id = 'disk_max_threshold']")
    disk_usage_max_threshold = 90
    if len(disk_usage) > 0:
        disk_usage_max_threshold = float(disk_usage[0].get("threshold"))
    # end if

    ###
    # CPU
    ###
    # Get utilization per CPU
    cpu_times_percent_per_cpu = psutil.cpu_times_percent(interval=1, percpu=True)
    
    cpu_user = 0
    cpu_nice = 0
    cpu_system = 0
    cpu_idle = 0
    cpu_iowait = 0
    cpu_irq = 0
    cpu_softirq = 0
    cpu_steal = 0
    cpu_guest = 0
    cpu_guest_nice = 0
    general_usage = 0
    for i, cpu_times_percent in enumerate(cpu_times_percent_per_cpu):

        ### Commented on 20191202 for reducing the amount of not needed information inserted 
        # if (100 - cpu_times_percent.idle) >= particular_cpu_max_threshold:
        #     alerts.append({
        #         "message": "CPU {}: maximum usage threshold ({}) reached with a value of {} %".format(i, particular_cpu_max_threshold, 100 - cpu_times_percent.idle),
        #         "generator": os.path.basename(__file__),
        #         "notification_time": now,
        #         "alert_cnf": {
        #             "name": "PARTICULAR_CPU_MAX_THRESHOLD_REACHED",
        #             "severity": "warning",
        #             "description": "Alert refers to a high CPU usage value in a particular node",
        #             "group": "BOA_HEALTH"
        #         },
        #         "entity": {
        #             "reference_mode": "by_ref",
        #             "reference": "boa_health_" + str(id),
        #             "type": "event"
        #         }
        #     })
        # # end if
        ### End Commented on 20191202 for reducing the amount of not needed information inserted
        
        general_usage += (100 - cpu_times_percent.idle)
        cpu_user += cpu_times_percent.user
        cpu_nice += cpu_times_percent.nice
        cpu_system += cpu_times_percent.system
        cpu_idle += cpu_times_percent.idle
        cpu_iowait += cpu_times_percent.iowait
        cpu_irq += cpu_times_percent.irq
        cpu_softirq += cpu_times_percent.softirq
        cpu_steal += cpu_times_percent.steal
        cpu_guest += cpu_times_percent.guest
        cpu_guest_nice += cpu_times_percent.guest_nice

        ### Commented on 20191202 for reducing the amount of not needed information inserted 
    #     values.append({
    #         "name": "information_for_cpu_" + str(i),
    #         "type": "object",
    #         "values": [
    #             {"name": "cpu_number",
    #              "type": "double",
    #              "value": str(i)},
    #             {"name": "cpu_" + str(i) + "_user",
    #              "type": "double",
    #              "value": str(cpu_times_percent.user)},
    #             {"name": "cpu_" + str(i) + "_nice",
    #              "type": "double",
    #              "value": str(cpu_times_percent.nice)},
    #             {"name": "cpu_" + str(i) + "_system",
    #              "type": "double",
    #              "value": str(cpu_times_percent.system)},
    #             {"name": "cpu_" + str(i) + "_idle",
    #              "type": "double",
    #              "value": str(cpu_times_percent.idle)},
    #             {"name": "cpu_" + str(i) + "_iowait",
    #              "type": "double",
    #              "value": str(cpu_times_percent.iowait)},
    #             {"name": "cpu_" + str(i) + "_irq",
    #              "type": "double",
    #              "value": str(cpu_times_percent.irq)},
    #             {"name": "cpu_" + str(i) + "_softirq",
    #              "type": "double",
    #              "value": str(cpu_times_percent.softirq)},
    #             {"name": "cpu_" + str(i) + "_steal",
    #              "type": "double",
    #              "value": str(cpu_times_percent.steal)},
    #             {"name": "cpu_" + str(i) + "_guest",
    #              "type": "double",
    #              "value": str(cpu_times_percent.guest)},
    #             {"name": "cpu_" + str(i) + "_guest_nice",
    #              "type": "double",
    #              "value": str(cpu_times_percent.guest_nice)},
    #             {"name": "cpu_" + str(i) + "_usage_percentage",
    #              "type": "double",
    #              "value": str(100 - cpu_times_percent.idle)}
    #         ]
    #     })
        ### End Commented on 20191202 for reducing the amount of not needed information inserted
    # end for
        
    values.append({
        "name": "general_cpu_information",
        "type": "object",
        "values": [
            {"name": "cpu_user",
             "type": "double",
             "value": str(cpu_user / (i + 1))},
            {"name": "cpu_nice",
             "type": "double",
             "value": str(cpu_nice / (i + 1))},
            {"name": "cpu_system",
             "type": "double",
             "value": str(cpu_system / (i + 1))},
            {"name": "cpu_idle",
             "type": "double",
             "value": str(cpu_idle / (i + 1))},
            {"name": "cpu_iowait",
             "type": "double",
             "value": str(cpu_iowait / (i + 1))},
            {"name": "cpu_irq",
             "type": "double",
             "value": str(cpu_irq / (i + 1))},
            {"name": "cpu_softirq",
             "type": "double",
             "value": str(cpu_softirq / (i + 1))},
            {"name": "cpu_steal",
             "type": "double",
             "value": str(cpu_steal / (i + 1))},
            {"name": "cpu_guest",
             "type": "double",
             "value": str(cpu_guest / (i + 1))},
            {"name": "cpu_guest_nice",
             "type": "double",
             "value": str(cpu_guest_nice / (i + 1))},
            {"name": "cpu_usage_percentage",
             "type": "double",
             "value": str(general_usage / (i + 1))}
        ]
    })

    if (general_usage / (i + 1)) >= general_cpu_max_threshold:
        alerts.append({
            "message": "General CPU: maximum usage threshold ({}) reached with a value of {} %".format(general_cpu_max_threshold, (general_usage / (i + 1))),
            "generator": os.path.basename(__file__),
            "notification_time": now,
            "alert_cnf": {
                "name": "GENERAL_CPU_MAX_THRESHOLD_REACHED",
                "severity": "warning",
                "description": "Alert refers to a high CPU usage value in general",
                "group": "BOA_HEALTH"
            },
            "entity": {
                "reference_mode": "by_ref",
                "reference": "boa_health_" + str(id),
                "type": "event"
            }
        })
    # end if

    # Get number of processes in the system run queue averaged over the last minute
    average_load = psutil.getloadavg()[0]

    values.append({
        "name": "average_load_last_minute",
        "type": "double",
        "value": str(average_load)
    })

    ###
    # MEMORY
    ###
    # Get the statistics of the memory usage
    memory_usage = psutil.virtual_memory()

    values.append({
        "name": "memory_information",
        "type": "object",
        "values": [
            {"name": "memory_total",
             "type": "double",
             "value": str(memory_usage.total)},
            {"name": "memory_available",
             "type": "double",
             "value": str(memory_usage.available)},
            {"name": "memory_usage_percentage",
             "type": "double",
             "value": str((memory_usage.used / memory_usage.total) * 100)},
            {"name": "memory_used",
             "type": "double",
             "value": str(memory_usage.used)},
            {"name": "memory_free",
             "type": "double",
             "value": str(memory_usage.free)},
            {"name": "memory_active",
             "type": "double",
             "value": str(memory_usage.active)},
            {"name": "memory_inactive",
             "type": "double",
             "value": str(memory_usage.inactive)},
            {"name": "memory_buffers",
             "type": "double",
             "value": str(memory_usage.buffers)},
            {"name": "memory_buffers_percentage",
             "type": "double",
             "value": str((memory_usage.buffers / memory_usage.total) * 100)},
            {"name": "memory_cached",
             "type": "double",
             "value": str(memory_usage.cached)},
            {"name": "memory_cached_percentage",
             "type": "double",
             "value": str((memory_usage.cached / memory_usage.total) * 100)},
            {"name": "memory_shared",
             "type": "double",
             "value": str(memory_usage.shared)},
            {"name": "memory_slab",
             "type": "double",
             "value": str(memory_usage.slab)}
        ]
    })

    if ((memory_usage.used / memory_usage.total) * 100) >= memory_max_threshold:
        alerts.append({
            "message": "Memory: maximum usage threshold ({}) reached with a value of {} %".format(memory_max_threshold, (memory_usage.used / memory_usage.total) * 100),
            "generator": os.path.basename(__file__),
            "notification_time": now,
            "alert_cnf": {
                "name": "MEMORY_MAX_THRESHOLD_REACHED",
                "severity": "critical",
                "description": "Alert refers to a high memory usage value",
                "group": "BOA_HEALTH"
            },
            "entity": {
                "reference_mode": "by_ref",
                "reference": "boa_health_" + str(id),
                "type": "event"
            }
        })
    # end if
    
    swap_usage = psutil.swap_memory()

    values.append({
        "name": "swap_information",
        "type": "object",
        "values": [
            {"name": "swap_total",
             "type": "double",
             "value": str(swap_usage.total)},
            {"name": "swap_usage_percentage",
             "type": "double",
             "value": str((swap_usage.used / swap_usage.total) * 100)},
            {"name": "swap_used",
             "type": "double",
             "value": str(swap_usage.used)},
            {"name": "swap_free",
             "type": "double",
             "value": str(swap_usage.free)}
        ]
    })

    if ((swap_usage.used / swap_usage.total) * 100) >= swap_max_threshold:
        alerts.append({
            "message": "Swap: maximum usage threshold ({}) reached with a value of {} %".format(swap_max_threshold, (swap_usage.used / swap_usage.total) * 100),
            "generator": os.path.basename(__file__),
            "notification_time": now,
            "alert_cnf": {
                "name": "SWAP_MAX_THRESHOLD_REACHED",
                "severity": "fatal",
                "description": "Alert refers to a high swap usage value",
                "group": "BOA_HEALTH"
            },
            "entity": {
                "reference_mode": "by_ref",
                "reference": "boa_health_" + str(id),
                "type": "event"
            }
        })
    # end if

    ###
    # DISKs
    ###
    # Get the statistics of the disk usage
    disks = psutil.disk_partitions()
    disks_usage = {}
    for disk in disks:
        disk_mountpoint = disk.mountpoint
        disk_usage = psutil.disk_usage(disk_mountpoint)

        values.append({
            "name": "information_for_disk" + disk_mountpoint.replace("/", "_"),
            "type": "object",
            "values": [
                {"name": "disk_mountpoint",
                 "type": "text",
                 "value": disk_mountpoint},
                {"name": disk_mountpoint.replace("/", "_") + "_total",
                 "type": "double",
                 "value": str(disk_usage.total)},
                {"name": disk_mountpoint.replace("/", "_") + "_used",
                 "type": "double",
                 "value": str(disk_usage.used)},
                {"name": disk_mountpoint.replace("/", "_") + "_free",
                 "type": "double",
                 "value": str(disk_usage.free)},
                {"name": disk_mountpoint.replace("/", "_") + "_usage_percentage",
                 "type": "double",
                 "value": str(disk_usage.percent)}
            ]
        })

        if disk_usage.percent >= disk_usage_max_threshold:
            alerts.append({
                "message": "Disk {}: maximum usage threshold ({}) reached with a value of {} %".format(disk_mountpoint, disk_usage_max_threshold, disk_usage.percent),
                "generator": os.path.basename(__file__),
                "notification_time": now,
                "alert_cnf": {
                    "name": "DISK_MAX_THRESHOLD_REACHED",
                    "severity": "fatal",
                    "description": "Alert refers to a high disk usage value",
                    "group": "BOA_HEALTH"
                },
                "entity": {
                    "reference_mode": "by_ref",
                    "reference": "boa_health_" + str(id),
                    "type": "event"
                }
            })
        # end if

    # end if

    ###
    # BOOT TIME
    ###
    # Get boot time
    boot_time = datetime.datetime.fromtimestamp(psutil.boot_time()).isoformat()

    values.append({
        "name": "boot_time",
        "type": "timestamp",
        "value": boot_time
    })

    ###
    # PROCESSES
    ###
    # Get information of the running processes
    pids = psutil.pids()
    processes = {}
    for pid in pids:
        try:
            p = psutil.Process(pid)
            values.append({
                "name": "information_for_process_" + str(pid),
                "type": "object",
                "values": [
                    {"name": "pid",
                     "type": "double",
                     "value": str(p.pid)},
                    {"name": "ppid",
                     "type": "double",
                     "value": str(p.ppid())},
                    {"name": "command",
                     "type": "text",
                     "value": " ".join(str(x) for x in p.cmdline())},
                    {"name": "status",
                     "type": "text",
                     "value": p.status()},
                    {"name": "create_time",
                     "type": "timestamp",
                     "value": datetime.datetime.fromtimestamp(p.create_time()).isoformat()},
                    {"name": "cpu_percentage",
                     "type": "double",
                     "value": str(p.cpu_percent(interval=1.0))},
                    {"name": "memory_percentage",
                     "type": "double",
                     "value": str(p.memory_percent())},
                    {"name": "number_of_threads",
                     "type": "double",
                     "value": str(p.num_threads())},
                    {"name": "number_of_fds",
                     "type": "double",
                     "value": str(p.num_fds())}
                ]
            })

        except psutil.NoSuchProcess:
            pass
        # end try
    # end for

    ###
    # FOLDERs
    ###
    for folder in health_xpath("/health/folders/folder"):
        recursive = folder.get("recursive")
        if recursive != None and recursive.lower() == "true":
            files_inside_dir = [file for file in glob.glob(folder.get("path") + "/**", recursive=True) if os.path.isfile(file)]
            number_of_files = len(files_inside_dir)
        else:
            try:
                number_of_files = len(os.listdir(folder.get("path")))
            except FileNotFoundError:
                continue
            # end try
        # end if
        values.append({
            "name": "information_for_folder_" + folder.get("name"),
            "type": "object",
            "values": [
                {"name": "folder_name",
                 "type": "text",
                 "value": folder.get("name")},
                {"name": folder.get("name") + "_path",
                 "type": "text",
                 "value": folder.get("path")},
                {"name": folder.get("name") + "_number_of_files",
                 "type": "double",
                 "value": str(number_of_files)}
            ]
        })

        if number_of_files >= int(folder.get("threshold_num_files")):
            alerts.append({
                "message": "Folder {}: maximum number of files threshold ({}) reached with a value of {}".format(folder.get("name"), folder.get("threshold_num_files"), number_of_files),
                "generator": os.path.basename(__file__),
                "notification_time": now,
                "alert_cnf": {
                    "name": "FOLDER_MAX_THRESHOLD_REACHED",
                    "severity": "critical",
                    "description": "Alert refers to a high number of files inside the folder",
                    "group": "BOA_HEALTH"
                },
                "entity": {
                    "reference_mode": "by_ref",
                    "reference": "boa_health_" + str(id),
                    "type": "event"
                }
            })
        # end if
        
    # end for
    
    ###
    # DDBB CONNECTIONs
    ###
    number_of_parallel_connections_to_ddbb = datamodel_engine.execute("select count(*) from pg_stat_activity;").first()[0]

    values.append({
        "name": "number_of_parallel_connections_to_ddbb",
        "type": "double",
        "value": str(number_of_parallel_connections_to_ddbb)
    })

    if number_of_parallel_connections_to_ddbb >= ddbb_connections_max_threshold:
        alerts.append({
            "message": "DDBB connections: maximum database connections threshold ({}) reached with a value of {} %".format(ddbb_connections_max_threshold, number_of_parallel_connections_to_ddbb),
            "generator": os.path.basename(__file__),
            "notification_time": now,
            "alert_cnf": {
                "name": "CONNECTIONS_TO_DDBB_MAX_THRESHOLD_REACHED",
                "severity": "critical",
                "description": "Alert refers to a high number of parallel connections to the DDBB",
                "group": "BOA_HEALTH"
            },
            "entity": {
                "reference_mode": "by_ref",
                "reference": "boa_health_" + str(id),
                "type": "event"
            }
        })
    # end if

    if args.output_path != None:
        with open(output_path, "w") as write_file:
            json.dump(data, write_file, indent=4)
    else:
        engine = Engine()
        returned_statuses = engine.treat_data(data, source)
        if not returned_statuses[0]["status"] in [eboa_engine.exit_codes["OK"]["status"], eboa_engine.exit_codes["SOURCE_ALREADY_INGESTED"]["status"]]:
            logger.error("The health monitoring information provided in file {} has failed for the DIM signature {} using the processor {} with status {}".format(source,
                                                                                                                                        dim_signature,
                                                                                                                                        os.path.basename(__file__),
                                                                                                                                        returned_statuses[0]["status"]))
        # end if
        else:
            logger.info("The health monitoring information provided in file {} has been correctly ingested into DDBB".format(source))
    # end if
    
    return

if __name__ == "__main__":

    main()

