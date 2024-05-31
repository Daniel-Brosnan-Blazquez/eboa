#!/usr/bin/env python3
"""
Tool for aggregating metrics for prometheus

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import argparse
import tempfile
import shutil
import os

# Import logging
from eboa.logging import Log

logging = Log(name = __name__, log_name = "eboa_metrics.log")
logger = logging.logger

def execute_aggregator(metrics_folder_path, output_metrics_file_path):
    """Function to aggregate BOA metrics
    
    :param metrics_folder_path: path to the folder of metrics to be aggregated
    :type metrics_folder_path: str
    :param output_metrics_file_path: path to the output metrics file
    :type output_metrics_file_path: str
    """

    logger.info(f"Aggregating BOA metrics available in the folder {metrics_folder_path} into the file {output_metrics_file_path}")

    # Create temporary folder where metrics to be aggregated are first moved
    temporary_folder = tempfile.TemporaryDirectory()
    temporary_folder_path = temporary_folder.name
    for metrics_file_path in os.listdir(metrics_folder_path):
        logger.info(f"Moving metrics file {metrics_file_path} to the temporary folder {temporary_folder_path}")
        shutil.move(os.path.join(metrics_folder_path, metrics_file_path), temporary_folder_path)
    # end for
    
    # Temporary file is moved to the final destination once closed
    temporary_file = tempfile.NamedTemporaryFile(delete=False)
    temporary_file_path = temporary_file.name
    temporary_file_descriptor = open(temporary_file_path,"w+")

    for metrics_file_path in os.listdir(temporary_folder_path):
        logger.info(f"Aggregating metrics file {metrics_file_path} into file {temporary_file_path}")
        f = open(os.path.join(temporary_folder_path, metrics_file_path),"r")
        metrics = f.read()
        temporary_file_descriptor.write(metrics)
    # end for

    temporary_file_descriptor.close()
    
    shutil.move(temporary_file_path, output_metrics_file_path)

    logger.info(f"Aggregated metrics file {temporary_file_path} moved to the final path {output_metrics_file_path}")

    return

def main():

    args_parser = argparse.ArgumentParser(description='BOA metrics aggregation tool.')
    args_parser.add_argument("-i", dest="metrics_folder_path", type=str, nargs=1,
                             help="path to the folder of metrics to be aggregated", required=True)
    args_parser.add_argument("-o", dest="output_metrics_file_path", type=str, nargs=1,
                             help="path to the output aggregated metrics file", required=True)

    args = args_parser.parse_args()
    output_metrics_file_path = args.output_metrics_file_path[0]
    metrics_folder_path = args.metrics_folder_path[0]

    if not os.path.isdir(metrics_folder_path):
        log = f"The metrics aggregator {os.path.basename(__file__)} has been triggered with an input folder ({metrics_folder_path}) which does not exist"
        logger.error(log)
        raise Exception(log)
    # end if

    execute_aggregator(metrics_folder_path, output_metrics_file_path)
    
    exit(0)

if __name__ == "__main__":

    main()
