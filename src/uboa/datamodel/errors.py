"""
Errors definition for the datamodel module

Written by Daniel Brosnan Blázquez

module uboa
"""
class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class WrongParameter(Error):
    """Exception raised when the received parameter is not correct

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
