"""
Errors definition for the engine module

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class RboaArchivePathNotAvailable(Error):
    """Exception raised when the environment variable RBOA_ARCHIVE_PATH has not been defined.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class WrongGenerationPeriod(Error):
    """Exception raised when the generation period is defined with a stop date lower than the start date.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
