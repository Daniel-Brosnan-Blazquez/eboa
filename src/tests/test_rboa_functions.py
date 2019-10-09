"""
Automated tests for the functions used by engine submodule

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import os
import unittest

# Import auxiliary functions
from rboa.engine.functions import get_rboa_archive_path

# Import exceptions
from rboa.engine.errors import RboaArchivePathNotAvailable

class TestFunctions(unittest.TestCase):

    def test_no_rboa_archive_path(self):

        if "RBOA_ARCHIVE_PATH" in os.environ.keys():
            rboa_archive_path = os.environ["RBOA_ARCHIVE_PATH"]
            del os.environ["RBOA_ARCHIVE_PATH"]
        # end if

        try:
            get_rboa_archive_path()
        except RboaArchivePathNotAvailable:
            assert True == True
        except:
            assert False == True

        if "RBOA_ARCHIVE_PATH" in os.environ.keys():
            os.environ["RBOA_ARCHIVE_PATH"] = rboa_archive_path
        # end if
