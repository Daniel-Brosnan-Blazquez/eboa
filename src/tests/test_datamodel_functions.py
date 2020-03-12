"""
Automated tests for the functions used by datamodel submodule

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import os
import unittest

# Import auxiliary functions
from eboa.datamodel.functions import read_configuration

# Import exceptions
from eboa.datamodel.errors import EboaResourcesPathNotAvailable

class TestDatamodel(unittest.TestCase):

    def test_no_eboa_resources_path(self):

        if "EBOA_RESOURCES_PATH" in os.environ.keys():
            eboa_resources_path = os.environ["EBOA_RESOURCES_PATH"]
            del os.environ["EBOA_RESOURCES_PATH"]
        # end if

        try:
            read_configuration()
        except EboaResourcesPathNotAvailable:
            assert True == True
        except:
            assert False == True

        os.environ["EBOA_RESOURCES_PATH"] = eboa_resources_path

