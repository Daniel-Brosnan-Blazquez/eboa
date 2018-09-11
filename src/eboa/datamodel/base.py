"""
Connection configuration for the eboa component

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import SQLalchemy entities
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Import exceptions
from eboa.datamodel.errors import EboaResourcesPathNotAvailable

# Import auxiliary functions
from eboa.datamodel.functions import read_configuration

config = read_configuration()

db_configuration = config["DDBB_CONFIGURATION"]

db_uri = "{db_api}://{user}@{host}:{port}/{database}".format(**db_configuration)

engine = create_engine(db_uri, pool_size=db_configuration["pool_size"], max_overflow=db_configuration["max_overflow"])
Session = sessionmaker(bind=engine)

Base = declarative_base()
