"""
Connection configuration for the uboa component

Written by DEIMOS Space S.L. (dibb)

module uboa
"""
# Import SQLalchemy entities
import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Import auxiliary functions
from uboa.datamodel.functions import read_configuration

config = read_configuration()

db_configuration = config["DDBB_CONFIGURATION"]

db_uri = "{db_api}://{user}@{host}:{port}/{database}".format(**db_configuration)

engine = create_engine(db_uri, pool_size=db_configuration["pool_size"], max_overflow=db_configuration["max_overflow"])
Session = sessionmaker(bind=engine)

Base = declarative_base()
