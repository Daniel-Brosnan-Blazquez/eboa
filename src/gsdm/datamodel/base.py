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
from gsdm.datamodel.errors import GsdmResourcesPathNotAvailable

# Import auxiliary functions
from gsdm.datamodel.functions import read_configuration

config = read_configuration()

db_configuration = config["DDBB_CONFIGURATION"]

db_uri = "{db_api}://{user}@{host}:{port}/{database}".format(**db_configuration)

engine = create_engine(db_uri, pool_size=db_configuration["pool_size"], max_overflow=db_configuration["max_overflow"])
Session = sessionmaker(bind=engine)

Base = declarative_base()
