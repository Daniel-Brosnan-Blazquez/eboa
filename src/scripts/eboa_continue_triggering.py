#!/usr/bin/env python3
"""
Script for deciding if continue launching triggering command to ingest data into EBOA

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import engine
from eboa.engine.query import Query

# Import auxiliary functions
from eboa.datamodel.functions import read_configuration

# Import logging
from eboa.logging import Log

logging_module = Log()
logger = logging_module.logger

config = read_configuration()
maximum_parallel_ingestions = config["MAXIMUM_PARALLEL_INGESTIONS"]

def main():
    query = Query()

    sources = query.get_sources(ingested = False)

    return_value = 0
    if len(sources) > maximum_parallel_ingestions:
        logger.error("The system has reached the maximum number of parallel ingestions set as {}".format(maximum_parallel_ingestions))
        return_value = -1
    # end if

    query.close_session()

    exit(return_value)

if __name__ == "__main__":

    main()

