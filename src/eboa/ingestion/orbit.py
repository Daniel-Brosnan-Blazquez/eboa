"""
Helper module for managing satellite orbits in the EBOA component

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
from dateutil import parser
import datetime
from lxml import etree, objectify
import re

# Import SGP4 utilities for orbit propagation
from sgp4.api import Satrec

# Import eboa utilities
from eboa.engine.errors import IncorrectTle

###########
# Functions for managing the orbit of a satellite
###########
def get_orbit(tle):
    """
    Method to obtain the orbit of a satellite from its TLE
    :param tle: TLE of the satellite with the following format:
    SATELLITE-INDICATOR
    1 NNNNNC NNNNNAAA NNNNN.NNNNNNNN +.NNNNNNNN +NNNNN-N +NNNNN-N N NNNNN
    2 NNNNN NNN.NNNN NNN.NNNN NNNNNNN NNN.NNNN NNN.NNNN NN.NNNNNNNNNNNNNN
    :type tle: str

    :return: orbit
    :rtype: sgp4.model.Satellite

    """

    # Verify TLE has three lines
    if not re.match(".*\n.*\n.*", tle):
        raise IncorrectTle(tle)
    # end if

    # Obtain first line of TLE
    first_line = tle.split("\n")[1]

    # Verify first line 
    if not re.match("1 ...... ........ .............. .......... ......-. ......-. . .....", first_line):
        raise IncorrectTle(tle)
    # end if

    second_line = tle.split("\n")[2]

    # Verify second line 
    if not re.match("2 ..... ........ ........ ....... ........ ........ .................", second_line):
        raise IncorrectTle(tle)
    # end if

    orbit = Satrec.twoline2rv(first_line, second_line)

    return orbit
