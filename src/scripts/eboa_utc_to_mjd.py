#!/usr/bin/env python3
"""
Script for converting UTC to MJD

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import argparse
import re

# Import astropy
from astropy.time import Time

def main():

    args_parser = argparse.ArgumentParser(description='EBOA UTC to MJD.')
    args_parser.add_argument('-d', dest='date', type=str, nargs=1,
                             help='UTC date to convert to MJD', required=True)

    args = args_parser.parse_args()
    date = args.date[0]

    if not re.match("....-..-..T..:..:...*", date):
        print("The date must have the format ....-..-..T..:..:...*")
        exit(-1)
    # end if

    t0 = Time("2000-01-01T00:00:00", format='isot', scale='utc')
    date_time = Time(date, format='isot', scale='utc')
    date_mjd = date_time.mjd - t0.mjd

    print("The date {} in UTC is equal to the following MJD: {}".format(date, date_mjd))

    exit(0)
    
if __name__ == "__main__":

    main()

