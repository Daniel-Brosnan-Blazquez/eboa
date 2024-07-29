#!/bin/bash
#################################################################
#
# Init DDBB of the uboa
#
# Written by DEIMOS Space S.L. (dibb)
#
# module uboa
#################################################################
USAGE="Usage: `basename $0` -f datamodel_file -p port -h host"
DATAMODEL_FILE=""
PORT="5432"
HOST="localhost"

while getopts f:p:h: option
do
    case "${option}"
        in
        f) DATAMODEL_FILE=${OPTARG};;
        p) PORT=${OPTARG};;
        h) HOST=${OPTARG};;
        ?) echo -e $USAGE
            exit -1
    esac
done

# Check that option -f has been specified
if [ "$DATAMODEL_FILE" == "" ];
then
    echo "ERROR: The option -f has to be provided"
    echo $USAGE
    exit -1
fi

# Check that the sql file for filling up the DDBB exists
if [ ! -f $DATAMODEL_FILE ];
then
    echo "ERROR: The file $DATAMODEL_FILE provided does not exist"
    exit -1
fi

# Check that there are no connections to the DDBB if exists
DATABASE=`psql -p "$PORT" -h "$HOST" -t -U postgres -c "SELECT count(*) FROM pg_database WHERE datname='uboadb'";`
if [ $DATABASE -eq 1 ];
then
    CONNECTIONS=`psql -p "$PORT" -h "$HOST" -U postgres -t -c "SELECT count(*) FROM pg_stat_activity where datname = 'uboadb';"`
    if [ $CONNECTIONS -ne 0 ];
    then
        echo "ERROR: There are $((CONNECTIONS + 0)) active connections to the DDBB"
        exit -1
    fi
fi

# Remove DDBB if it exists
if [ $DATABASE -eq 1 ];
then
    # Drop the DDBB
    psql -p $PORT -h $HOST -U postgres -c "DROP DATABASE uboadb;"

    # Drop the uboa role
    psql -p $PORT -h $HOST -U postgres -c "DROP ROLE uboa;"
fi

# Create DDBB
psql -p $PORT -h $HOST -U postgres -c "CREATE DATABASE uboadb;"

# Fill DDBB
psql -p $PORT -h $HOST -U postgres -d uboadb -f $DATAMODEL_FILE
status=$?

if [ $status -ne 0 ];
then
    echo "ERROR: It was not possible to fill up the DDBB"
    exit -1
fi

echo "DDBB has been initiated correctly!"

exit 0
