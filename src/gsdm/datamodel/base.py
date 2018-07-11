"""
Connection configuration for the gsdm component

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
# Import SQLalchemy entities
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Import exceptions
from .errors import GsdmResourcesPathNotAvailable

# Import python utilities
import os
import json

if "GSDM_RESOURCES_PATH" in os.environ:
    # Get the path to the resources of the gsdm
    gsdm_resources_path = os.environ["GSDM_RESOURCES_PATH"]
    # Get configuration
    with open(gsdm_resources_path + "config/datamodel.json") as json_data_file:
        config = json.load(json_data_file)
else:
    raise GsdmResourcesPathNotAvailable("The environment variable GSDM_RESOURCES_PATH is not defined")
# end if

db_configuration = config["DDBB_CONFIGURATION"]

db_uri = "{db_api}://{user}@{host}:{port}/{database}".format(**db_configuration)

engine = create_engine(db_uri, pool_size=db_configuration["pool_size"], max_overflow=db_configuration["max_overflow"])
Session = sessionmaker(bind=engine)

Base = declarative_base()
