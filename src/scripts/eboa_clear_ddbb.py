#!/usr/bin/env python3
"""
Script for deciding if continue launching triggering command to ingest data into EBOA

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import engine
from eboa.engine.query import Query

def main():

    continue_erase = input("This script will erase all the data in the DDBB. Are you sure you want to continue [Ny]")
    if continue_erase == "y":
        query = Query()
        query.clear_db()
        print("The DDBB has been erased")
    else:
        print("The script is not going to do anything. The DDBB will not be erased")

    exit(0)

if __name__ == "__main__":

    main()

