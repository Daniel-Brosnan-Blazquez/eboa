#!/usr/bin/env python3
"""
Script for deciding if continue launching triggering command to ingest data into EBOA

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import datetime
import os

# Import engine
from eboa.engine.query import Query

# Import auxiliary functions
from eboa.datamodel.functions import read_configuration

# Import logging
from eboa.logging import Log

logging_module = Log(name = os.path.basename(__file__))
logger = logging_module.logger

config = read_configuration()
maximum_parallel_ingestions = config["MAXIMUM_PARALLEL_INGESTIONS"]

def main():
    query = Query()

    # Get sources not ingested in the last 2 hours (to avoid blocking the chain by blocked ingestions)
    # 2 hours is the time for notifying alerts and so the system will be protected
    stop = datetime.datetime.now()
    start = stop + datetime.timedelta(minutes=-30)
    sources = query.get_sources(ingested = False,
                                ingestion_error = {"filter": True, "op": "!="},
                                dim_signatures = {"filter": "PENDING_SOURCES", "op": "=="},
                                generation_time_filters = [{"date": start.isoformat(), "op": ">"},
                                                                             {"date": stop.isoformat(), "op": "<"}])

    return_value = 0
    if len(sources) >= maximum_parallel_ingestions:
        logger.error("The system has reached the maximum number of parallel ingestions set as {}".format(maximum_parallel_ingestions))
        return_value = -1
    # end if

    query.close_session()

    exit(return_value)

if __name__ == "__main__":

    main()

