"""
Errors definition for the triggering module

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class ReportingConfigCannotBeRead(Error):
    """Exception raised when the reporting configuration file cannot be read.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class ReportingConfigDoesNotPassSchema(Error):
    """Exception raised when the reporting configuration does not pass the schema.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class WrongReportingPeriod(Error):
    """Exception raised when the received period has a start value greater than its stop.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
