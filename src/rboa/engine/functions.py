"""
Functions definition for the engine component

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import os

# Import exceptions
from rboa.engine.errors import RboaArchivePathNotAvailable

def get_rboa_archive_path():
    """
    Method to obtain the path to the archive of rboa
    """
    rboa_archive_path = None
    if "RBOA_ARCHIVE_PATH" in os.environ:
        # Get the path to the resources of the eboa
        rboa_archive_path = os.environ["RBOA_ARCHIVE_PATH"]

    else:
        raise RboaArchivePathNotAvailable("The environment variable RBOA_ARCHIVE_PATH is not defined")
    # end if

    return rboa_archive_path
