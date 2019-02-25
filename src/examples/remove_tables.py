"""
Test: remove tables

Written by DEIMOS Space S.L. (dibb)

module eboa
"""

# Import engine of the DDBB
from eboa.datamodel.base import Session, engine, Base

# Clear all tables before executing the test
for table in reversed(Base.metadata.sorted_tables):
    engine.execute(table.delete())
# end for
