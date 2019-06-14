#!/usr/bin/env python3
"""
Script for converting MJD to UTC

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import argparse
import re

# Import astropy
from astropy.time import Time

def main():

    args_parser = argparse.ArgumentParser(description='EBOA MJD to UTC.')
    args_parser.add_argument('-m', dest='mjd', type=str, nargs=1,
                             help='MJD to convert to UTC', required=True)

    args = args_parser.parse_args()
    mjd = args.mjd[0]

    try:
        mjd = float(mjd)
    except:
        print("The provided MJD must be convertable to float")
        exit(-1)
    # end if

    t0 = Time("2000-01-01T00:00:00", format='isot', scale='utc')
    date_mjd = mjd + t0.mjd
    date_time = Time(date_mjd, format='mjd', scale='utc')
    date_time.format = "isot"

    print("The MJD {} is equal to the following date in UTC: {}".format(mjd, date_time.value))

    exit(0)
    
if __name__ == "__main__":

    main()

