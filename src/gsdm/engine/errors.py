"""
Errors definition for the engine module

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class SourceAlreadyIngested(Error):
    """Exception raised when a file is received more than once.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class WrongPeriod(Error):
    """Exception raised when a period is defined with a stop date lower than the start date.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class WrongEventLink(Error):
    """Exception raised when a link has been defined for an event but the reference is not found on any other event to link with.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class WrongValue(Error):
    """Exception raised when a provided value cannot be converted to its specified type.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class OddNumberOfCoordinates(Error):
    """Exception raised when a provided geometry has an odd number of coordinates.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class GsdmResourcesPathNotAvailable(Error):
    """Exception raised when the environment variable GSDM_RESOURCES_PATH has not been defined.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class WrongGeometry(Error):
    """Exception raised when the geometry value is not accepted by the engine

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class ErrorParsingDictionary(Error):
    """Exception raised when the parsing has found a non supported structure

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
