"""
Errors definition for the datamodel module

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class GsdmResourcesPathNotAvailable(Error):
    """Exception raised when the environment variable GSDM_RESOURCES_PATH has not been defined.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
