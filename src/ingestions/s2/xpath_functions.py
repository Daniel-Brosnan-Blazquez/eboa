"""
Helper module for the ingestions of files of Sentinel-2 using functions in XPATH

Written by DEIMOS Space S.L. (dibb)

module eboa
"""

# Import debugging
from eboa.debugging import debug

# Import ingestion helpers
import ingestions.s2.functions as functions

###
# Date helpers
###

# Uncomment for debugging reasons
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
