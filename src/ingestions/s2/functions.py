"""
Helper module for the ingestions of files of Sentinel-2

Written by DEIMOS Space S.L. (dibb)

module eboa
"""

# Import python utilities
import math
import datetime
from dateutil import parser

# Import debugging
from eboa.debugging import debug

###
# Acquisition ingestions' helpers
###

# Uncomment for debugging reasons
#@debug
def convert_from_gps_to_utc(date):
    """
    Method to convert a date in GPS precission to UTC
    :param date: date in GPS precission and ISO format
    :type date: str

    :return: date coverted in ISO 8601
    :rtype: str

    """
    
    date_datetime = parser.parse(date)

    if date_datetime > datetime.datetime(2015, 6, 30, 23, 59, 59) and date_datetime <= datetime.datetime(2016, 12, 31, 23, 59, 59):
        correction = -17
    elif date_datetime > datetime.datetime(2016, 12, 31, 23, 59, 59):
        correction = -18
    else:
        correction = -16

    return str(date_datetime + datetime.timedelta(seconds=correction))

# Uncomment for debugging reasons
#@debug
def convert_from_datetime_gps_to_datetime_utc(date):
    """
    Method to convert a date in GPS precission to UTC
    :param date: date in GPS precission and ISO format
    :type date: str

    :return: date coverted in ISO 8601
    :rtype: str

    """
    
    if date > datetime.datetime(2015, 6, 30, 23, 59, 59) and date <= datetime.datetime(2016, 12, 31, 23, 59, 59):
        correction = -17
    elif date > datetime.datetime(2016, 12, 31, 23, 59, 59):
        correction = -18
    else:
        correction = -16

    return date + datetime.timedelta(seconds=correction)

# Uncomment for debugging reasons
#@debug
def get_vcid_mode(vcid):
    """
    Method to convert the VCID number into the storage mode
    :param vcid: VCID number
    :type vcid: str

    :return: mode
    :rtype: str

    """

    correspondence = {
        "2": "SAD",
        "3": "HKTM",
        "4": "NOMINAL",
        "5": "NRT",
        "6": "RT",
        "20": "NOMINAL",
        "21": "NRT",
        "22": "RT"
    }

    return correspondence[vcid]


# Uncomment for debugging reasons
#@debug
def get_vcid_apid_configuration(vcid):
    """
    Method to obtain the APID configuration related to the VCID number
    :param vcid: VCID number
    :type vcid: str

    :return: apid_configuration
    :rtype: dict

    """

    # Second half swath
    apids_second_half_swath = {
        "min_apid": 0,
        "max_apid": 92,
    }
    # First half swath
    apids_first_half_swath = {
        "min_apid": 256,
        "max_apid": 348,
    }

    correspondence = {
        "4": apids_second_half_swath,
        "5": apids_second_half_swath,
        "6": apids_second_half_swath,
        "20": apids_first_half_swath,
        "21": apids_first_half_swath,
        "22": apids_first_half_swath
    }

    return correspondence[vcid]

# Uncomment for debugging reasons
#@debug
def get_band_detector(apid):
    """
    Method to obtain the band and detector numbers related to the APID number

    The detector and the bands are determined from APID
        APID RANGE     DETECTOR 
           0-15           12
           16-31           11
           32-47           10
           48-63           9
           64-79           8
           80-95           7

           256-271           6
           272-287           5
           288-303           4
           304-319           3
           320-335           2
           336-351           1

        APID MOD 16     BAND 
             0           1
             1           2
             2           3
             3           4
             4           5
             5           6
             6           7
             7           8
             8           8a
             9           9
             10          10
             11          11
             12          12

    :param apid: APID number
    :type vcid: str

    :return: band_detector_configuration
    :rtype: dict

    """
    
    if int(apid) < 256:
        detector = 12 - math.floor(int(apid)/16)
    else:
        detector = 12 - (math.floor((int(apid) - 256)/16) + 6)
    # end if
    
    raw_band = (int(apid) % 16) + 1
    if raw_band == 9:
        band = "8a"
    elif raw_band > 9:
        band = raw_band - 1
    else:
        band = raw_band
    # end if

    return {"detector": str(detector), "band": str(band)}

# Uncomment for debugging reasons
#@debug
def get_counter_threshold(band):
    """
    Method to obtain the counter threshold related to the band number
        BAND     COUNTER THRESHOLD    METRES
           1              23            60
           2-4           143            10
           5-7            71            20
           8             143            10
           8a             71            20
           9-10           23            60
           11-12          71            20    

    :param band: band number
    :type band: str

    :return: counter_threshold
    :rtype: int

    """
    
    if band == "2" or band == "3" or band == "4" or band == "8":
        counter_threshold = 143
    elif band == "1" or band == "9" or band == "10":
        counter_threshold = 23
    else:
        counter_threshold = 71
    # end if
    
    return counter_threshold

# Uncomment for debugging reasons
#@debug
def get_counter_threshold_from_apid(apid):
    """
    Method to obtain the counter threshold related to the apid number

    :param apid: apid number
    :type apid: str

    :return: counter_threshold
    :rtype: int

    """

    band_detector = get_band_detector(apid)
    
    return get_counter_threshold(band_detector["band"])

def get_apid_numbers():
    """
    Method to obtain the APID numbers used

    :return: list of APID numbers
    :rtype: list

    """
    apids = []
    for i in range(6):
        for j in range (13):
            apids.append(i*16 + j)
        # end for
    # end for
    for i in range(6):
        for j in range (13):
            apids.append((i*16 + j) + 256)
        # end for
    # end for
    return apids

###
# Date helpers
###

# Uncomment for debugging reasons
#@debug
def three_letter_to_iso_8601(date):
    """
    Method to convert a date in three letter format to a date in ISO 8601 format
    :param date: date in three letter format (DD-MMM-YYYY HH:MM:SS.ssssss)
    :type date: str

    :return: date in ISO 8601 format (YYYY-MM-DDTHH:MM:SS)
    :rtype: str

    """    
    month = {
        "JAN": "01",
        "FEB": "02",
        "MAR": "03",
        "APR": "04",
        "MAY": "05",
        "JUN": "06",
        "JUL": "07",
        "AUG": "08",
        "SEP": "09",
        "OCT": "10",
        "NOV": "11",
        "DEC": "12"
    }
    year = date[7:11]
    month = month[date[3:6]]
    day = date[0:2]
    hours = date[12:14]
    minutes = date[15:17]
    seconds = date[18:20]
    microseconds = date[21:27]

    return year + "-" + month + "-" + day + "T" + hours + ":" + minutes + ":" + seconds + "." + microseconds
