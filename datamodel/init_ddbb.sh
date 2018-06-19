#################################################################
#
# Init DDBB of the gsdm
#
# Written by DEIMOS Space S.L. (dibb)
#
# module gsdm
#################################################################
USAGE="Usage: `basename $0` -f datamodel_file"
DATAMODEL_FILE=""

while getopts f: option
do
    case "${option}"
        in
        f) DATAMODEL_FILE=${OPTARG};;
        ?) echo -e $USAGE
            exit -1
    esac
done

# Check that the init of the DDBB is being executed by Postgres
if [ "$(whoami)" != "postgres" ] && [ "$(whoami)" != "root" ]; then
        echo "ERROR: Script must be run as user: postgres"
        exit -1
fi

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
DATABASE=`psql -t -U postgres -c "SELECT count(*) FROM pg_database WHERE datname='gsdmdb'";`
if [ $DATABASE -eq 1 ];
then
    CONNECTIONS=`psql -U postgres -t -c "SELECT count(*) FROM pg_stat_activity where datname = 'gsdmdb';"`
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
    psql -U postgres -c "DROP DATABASE gsdmdb;"

    # Drop the gsdm role
    psql -U postgres -c "DROP ROLE gsdm;"
fi

# Create DDBB
psql -U postgres -c "CREATE DATABASE gsdmdb;"

# Add extenstion for postgis
psql -U postgres -d gsdmdb -c "CREATE EXTENSION postgis;"

# Fill DDBB
psql -U postgres -d gsdmdb -f $DATAMODEL_FILE
status=$?

if [ $status -ne 0 ];
then
    echo "ERROR: It was not possible to fill up the DDBB"
    exit -1
fi

echo "DDBB has been initiated correctly!"

exit 0
