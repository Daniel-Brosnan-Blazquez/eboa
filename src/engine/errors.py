"""
Errors definition for the engine module

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class InputError(Error):
    """Exception raised for errors in the input.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
