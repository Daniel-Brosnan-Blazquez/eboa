"""
Errors definition for the triggering module

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class TriggeringConfigCannotBeRead(Error):
    """Exception raised when the triggering configuration file cannot be read.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class TriggeringConfigDoesNotPassSchema(Error):
    """Exception raised when the triggering configuration does not pass the schema.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class FileDoesNotMatchAnyRule(Error):
    """Exception raised when the file does not match any rule inside the triggering configuration.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
