"""
Helper module for the ingestions of files of Sentinel-2 using functions in XPATH

Written by DEIMOS Space S.L. (dibb)

module eboa
"""

# Import debugging
from eboa.debugging import debug

# Import ingestion helpers
import ingestions.s2.functions as functions
import ingestions.functions.date_functions as date_functions

###
# Date helpers
###

#@debug
def three_letter_to_iso_8601(dummy, date):
    """
    Method to convert a date in three letter format to a date in ISO 8601 format from XPATH
    :param dummy: parameter not used by lxml
    :type dummy: None
    :param date: date in three letter format (DD-MMM-YYYY HH:MM:SS.ssssss)
    :type date: str

    :return: date in ISO 8601 format (YYYY-MM-DDTHH:MM:SS)
    :rtype: str

    """    

    return functions.three_letter_to_iso_8601(date)

#@debug
def dates_difference(dummy, minuend, subtrahend):
    """
    Method to perform the difference between two dates from XPATH
    :param dummy: parameter not used by lxml
    :type dummy: None
    :param minuend: first date in the substruction
    :type date: str
    :param subtrahend: second date in the substruction
    :type date: str

    :return: seconds of difference
    :rtype: float

    """    

    return date_functions.dates_difference(minuend, subtrahend)

#@debug
def get_counter_threshold_from_apid(dummy, apid):
    """
    Method to obtain the counter threshold of the related apid
    :param dummy: parameter not used by lxml
    :type dummy: None
    :param apid: apid number
    :type apid: str

    :return: counter_threshold
    :rtype: int

    """    

    return functions.get_counter_threshold_from_apid(apid)
