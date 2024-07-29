"""
Errors definition for the engine module

Written by DEIMOS Space S.L. (dibb)

module eboa
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

class WrongReportedValidityPeriod(Error):
    """Exception raised when the reported validity period is defined with a stop date lower than the start date.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class UndefinedEventLink(Error):
    """Exception raised when a link has been defined for an event which does not exist with the corresponding link reference.

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

class EboaResourcesPathNotAvailable(Error):
    """Exception raised when the environment variable EBOA_RESOURCES_PATH has not been defined.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class EboaLogPathNotAvailable(Error):
    """Exception raised when the environment variable EBOA_LOG_PATH has not been defined.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class EboaSchemasPathNotAvailable(Error):
    """Exception raised when the environment variable EBOA_SCHEMAS_PATH has not been defined.

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

class ErrorParsingParameters(Error):
    """Exception raised when the parsing has found a non supported structure in the parameters received

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class ErrorParsingFilters(Error):
    """Exception raised when the filters passed to a method are not accepted

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class DuplicatedEventLinkRef(Error):
    """Exception raised when there is more than one event with the same link reference identifier.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class LinksInconsistency(Error):
    """Exception raised when there are clashes on the defined links for the events.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class InputError(Error):
    """Exception raised when the specified inputs have a wrong type.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message


class DuplicatedValues(Error):
    """Exception raised when the specified there are values duplicated.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class UndefinedEntityReference(Error):
    """Exception raised when an alert has been defined for an entity which does not exist with the corresponding reference.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class FilePathDoesNotExist(Error):
    """Exception raised when a file path does not exist.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class PriorityNotDefined(Error):
    """Exception raised when there is one or more events with the insertion type ...with_PRIORITY
    and the related source has not defined the priority.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class DuplicatedSetCounter(Error):
    """Exception raised when there is one or more events with the insertion type SET_COUNTER associated to the same gauge.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class MixedOperationsWithCounter(Error):
    """Exception raised when there are events with the insertion type SET_COUNTER and UPDATE_COUNTER mixed associated to the same gauge.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class IncorrectTle(Error):
    """Exception raised when the format of the received TLE is not correct.
    Correct format is:
    SATELLITE-INDICATOR
    1 NNNNNC NNNNNAAA NNNNN.NNNNNNNN +.NNNNNNNN +NNNNN-N +NNNNN-N N NNNNN
    2 NNNNN NNN.NNNN NNN.NNNN NNNNNNN NNN.NNNN NNN.NNNN NN.NNNNNNNNNNNNNN

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, tle):
        tle_format = "SATELLITE-INDICATOR\n1 NNNNNC NNNNNAAA NNNNN.NNNNNNNN +.NNNNNNNN +NNNNN-N +NNNNN-N N NNNNN\n2 NNNNN NNN.NNNN NNN.NNNN NNNNNNN NNN.NNNN NNN.NNNN NN.NNNNNNNNNNNNNN"

        self.message = "\nReceived TLE:\n{}\n is incorrect. TLE should have the following format:\n{} ".format(tle, tle_format)

        super().__init__(self.message)
