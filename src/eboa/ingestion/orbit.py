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
import math

# Import SGP4 utilities for orbit propagation
from sgp4.api import Satrec, jday

# Import eboa utilities
from eboa.engine.errors import IncorrectTle

# Import astropy utilities
from astropy.coordinates import SkyCoord, ITRS
from astropy.time import Time

###########
# Functions for managing the orbit of a satellite
###########
def verify_tle(tle_string):
    """
    Method to verify the content of a tle

    :param tle_string: TLE of the satellite with the following format:
    SATELLITE-INDICATOR
    1 NNNNNC NNNNNAAA NNNNN.NNNNNNNN +.NNNNNNNN +NNNNN-N +NNNNN-N N NNNNN
    2 NNNNN NNN.NNNN NNN.NNNN NNNNNNN NNN.NNNN NNN.NNNN NN.NNNNNNNNNNNNNN
    :type tle_string: str

    :return: (first_line, second_line) tuple with the content of the TLE
    :rtype: tuple
    """

    # Verify TLE has three lines
    if not tle_string or not re.match(".*\n.*\n.*", tle_string):
        raise IncorrectTle(tle_string)
    # end if

    # Obtain first line of TLE
    first_line = tle_string.split("\n")[1]

    # Verify first line 
    if not re.match("1 ...... ........ .............. .......... ......-. ......-. . .....", first_line):
        raise IncorrectTle(tle_string)
    # end if

    second_line = tle_string.split("\n")[2]

    # Verify second line 
    if not re.match("2 ..... ........ ........ ....... ........ ........ .................", second_line):
        raise IncorrectTle(tle_string)
    # end if

    return first_line, second_line
    
def get_orbit(tle_string):
    """
    Method to obtain the orbit of a satellite from its TLE
    :param tle_string: TLE of the satellite with the following format:
    SATELLITE-INDICATOR
    1 NNNNNC NNNNNAAA NNNNN.NNNNNNNN +.NNNNNNNN +NNNNN-N +NNNNN-N N NNNNN
    2 NNNNN NNN.NNNN NNN.NNNN NNNNNNN NNN.NNNN NNN.NNNN NN.NNNNNNNNNNNNNN
    :type tle_string: str

    :return: orbit
    :rtype: sgp4.model.Satellite
    """

    first_line, second_line = verify_tle(tle_string)

    orbit = Satrec.twoline2rv(first_line, second_line)

    return orbit

def get_ephemeris(satellite_orbit, time):
    '''
    Function to obtain the ephemeris of a satellite at the specified time.

    :param satellite_orbit: object defining the orbit of the satellite
    :type satellite_orbit: orbit object
    :param time: time of interest in ISO 8601 format
    :type time: str

    :return: (error, position, velocity) tuple corresponding to the ephemeris of the satellite
    :rtype: tuple
    '''
    
    time_datetime = parser.parse(time)
    jd, fr = jday(time_datetime.year, time_datetime.month, time_datetime.day,
                  time_datetime.hour, time_datetime.minute,
                  time_datetime.second)

    return satellite_orbit.sgp4(jd, fr)

def get_orbit_duration(tle_string):
    """
    Method to obtain the orbit duration from a tle

    :param tle_string: TLE of the satellite with the following format:
    SATELLITE-INDICATOR
    1 NNNNNC NNNNNAAA NNNNN.NNNNNNNN +.NNNNNNNN +NNNNN-N +NNNNN-N N NNNNN
    2 NNNNN NNN.NNNN NNN.NNNN NNNNNNN NNN.NNNN NNN.NNNN NN.NNNNNNNNNNNNNN
    :type tle_string: str

    :return: orbit duration in minutes
    :rtype: float
    """
    
    first_line, second_line = verify_tle(tle_string)

    # Obtain mean motion from TLE
    mean_motion = float(second_line[52:63])

    # Obtain orbit duration from mean motion
    orbit_duration = (24*60) / mean_motion

    return orbit_duration    

def get_semimajor(tle_string):
    """
    Method to obtain the semimajor from a tle

    :param tle_string: TLE of the satellite with the following format:
    SATELLITE-INDICATOR
    1 NNNNNC NNNNNAAA NNNNN.NNNNNNNN +.NNNNNNNN +NNNNN-N +NNNNN-N N NNNNN
    2 NNNNN NNN.NNNN NNN.NNNN NNNNNNN NNN.NNNN NNN.NNNN NN.NNNNNNNNNNNNNN
    :type tle_string: str

    :return: semimajor axis value
    :rtype: float
    """

    # Standard gravitational parameter for the earth
    mu = 398600.0;
    
    first_line, second_line = verify_tle(tle_string)

    # Obtain mean motion from TLE
    mean_motion = float(second_line[52:63])

    # Obtain semimajor from mean motion
    # References:
    # https://space.stackexchange.com/questions/18289/how-to-get-semi-major-axis-from-tle
    # https://smallsats.org/2012/12/06/two-line-element-set-tle/
    semimajor = (mu/(mean_motion*2*math.pi/(86400))**2)**(1/3);

    return semimajor

###########
# Functions for managing the reference frames of the positions of the satellite
###########
def satellite_positions_to_fixed(inertial_satellite_positions, epochs):
    '''
    Function to transform satellite positions in the Earth inertial frame (TEME) to the Earth fixed frame

    :param inertial_satellite_positions: list of satellite positions with the format [x1, y1, z1, ..., xn, yn, zn]
    :type inertial_satellite_positions: list
    :param epochs: list of epochs associated to the satellite positions
    :type epochs: list of strings with format "YYYY-MM-DDThh:mm:ss.000"

    :return: satellite positions in the Earth fixed frame with the format [x1, y1, z1, ..., xn, yn, zn]
    :rtype: list
    '''

    # Obtain satellite positions referenced in the Earth fixed frame
    satellite_positions = []
    i = 0
    j = 0
    while i < len(inertial_satellite_positions):
        time = Time(epochs[j], format="isot", scale="utc")
        inertial_satellite_position = SkyCoord(x=inertial_satellite_positions[i], y=inertial_satellite_positions[i+1], z=inertial_satellite_positions[i+2], frame="teme", unit=("km", "km", "km"), representation_type="cartesian", obstime=time)
        fixed_satellite_position = inertial_satellite_position.transform_to(ITRS())

        # Store X, Y, Z values referenced in the Earth fixed frame
        satellite_positions.append(fixed_satellite_position.earth_location.x.value)
        satellite_positions.append(fixed_satellite_position.earth_location.y.value)
        satellite_positions.append(fixed_satellite_position.earth_location.z.value)
        i += 3
        j += 1
    # end while

    return satellite_positions

