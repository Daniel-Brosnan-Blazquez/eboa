"""
Errors definition for the engine module

Written by DEIMOS Space S.L. (dibb)

module uboa
"""
class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class RolesDuplicated(Error):
    """Exception raised when a role is specified twice to be inserted

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class RoleAlreadyInserted(Error):
    """Exception raised when a role has to be inserted but it was already in the DDBB

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class UsersDuplicated(Error):
    """Exception raised when a user is specified twice to be inserted

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class UserAlreadyInserted(Error):
    """Exception raised when a user has to be inserted but it was already in the DDBB

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class EmailNotCorrect(Error):
    """Exception raised when a the specified email has an incorrect format

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class UsernameNotCorrect(Error):
    """Exception raised when a the specified username has an incorrect format

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class ErrorParsingDictionary(Error):
    """Exception raised when the input cannot be validated

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

