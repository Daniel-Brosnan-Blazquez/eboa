"""
Automated tests for the functions used by engine submodule

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
# Import python utilities
import os
import unittest

# Import auxiliary functions
from gsdm.engine.functions import read_configuration

# Import exceptions
from gsdm.engine.errors import GsdmResourcesPathNotAvailable

class TestDatamodel(unittest.TestCase):

    def test_no_gsdm_resources_path(self):

        if "GSDM_RESOURCES_PATH" in os.environ.keys():
            gsdm_resources_path = os.environ["GSDM_RESOURCES_PATH"]
            del os.environ["GSDM_RESOURCES_PATH"]
        # end if

        try:
            read_configuration()
        except GsdmResourcesPathNotAvailable:
            assert True == True
        except:
            assert False == True

        if "GSDM_RESOURCES_PATH" in os.environ.keys():
            os.environ["GSDM_RESOURCES_PATH"] = gsdm_resources_path
        # end if
