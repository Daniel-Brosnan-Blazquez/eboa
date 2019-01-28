#################################################################
#
# Modify existing DDBB of the eboa with a SQL file with differences
#
# Written by DEIMOS Space S.L. (dibb)
#
# module eboa
#################################################################
USAGE="Usage: `basename $0` -f diff_sql_file -d ddbb -p port -h host"
DIFF_SQL_FILE=""
PORT="5432"
HOST="localhost"

while getopts f:d:p:h: option
do
    case "${option}"
        in
        f) DIFF_SQL_FILE=${OPTARG};;
        d) DDBB=${OPTARG};;
        p) PORT=${OPTARG};;
        h) HOST=${OPTARG};;
        ?) echo -e $USAGE
            exit -1
    esac
done

# Check that the init of the DDBB is being executed by Postgres or root
if [ "$(whoami)" != "postgres" ] && [ "$(whoami)" != "root" ]; then
        echo "ERROR: Script must be run as user: postgres"
        exit -1
fi

# Check that option -f has been specified
if [ "$DIFF_SQL_FILE" == "" ];
then
    echo "ERROR: The option -f has to be provided"
    echo $USAGE
    exit -1
fi

# Check that option -d has been specified
if [ "$DDBB" == "" ];
then
    echo "ERROR: The option -d has to be provided"
    echo $USAGE
    exit -1
fi

# Check that the sql file for modifying the DDBB exists
if [ ! -f $DIFF_SQL_FILE ];
then
    echo "ERROR: The file $DIFF_SQL_FILE provided does not exist"
    exit -1
fi

# Check that there are no connections to the DDBB if exists
DATABASE=`psql -p "$PORT" -h "$HOST" -t -U postgres -c "SELECT count(*) FROM pg_database WHERE datname='$DDBB'";`
if [ $DATABASE -eq 1 ];
then
    CONNECTIONS=`psql -p "$PORT" -h "$HOST" -U postgres -t -c "SELECT count(*) FROM pg_stat_activity where datname = '$DDBB';"`
    if [ $CONNECTIONS -ne 0 ];
    then
        echo "ERROR: There are $((CONNECTIONS + 0)) active connections to the DDBB"
        exit -1
    fi
fi

# Modify DDBB
psql -p $PORT -h $HOST -U postgres -d $DDBB -f $DIFF_SQL_FILE
status=$?

if [ $status -ne 0 ];
then
    echo "ERROR: It was not possible to modify the DDBB"
    exit -1
fi

echo "DDBB has been modified correctly!"

exit 0
