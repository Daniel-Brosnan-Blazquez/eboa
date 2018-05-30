"""
Connection configuration for the gsdm component

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

dbConfiguration = {
    'user': 'e2espm',
    'host': 'localhost',
    'port': 5432,
    'database': 'gsdmdb',
    'dbApi': 'postgres',
}

dbUri = "{dbApi}://{user}@{host}:{port}/{database}".format(**dbConfiguration)

engine = create_engine('postgresql://e2espm@localhost:5432/gsdmdb', pool_size=20, max_overflow=20)
Session = sessionmaker(bind=engine)

Base = declarative_base()
