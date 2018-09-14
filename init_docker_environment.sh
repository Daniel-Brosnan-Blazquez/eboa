#################################################################
#
# Init docker environment of the eboa
#
# Written by DEIMOS Space S.L. (dibb)
#
# module eboa
#################################################################

USAGE="Usage: `basename $0` -p path_to_eboa"
PATH_TO_EBOA=""

while getopts p: option
do
    case "${option}"
        in
        p) PATH_TO_EBOA=${OPTARG};;
        ?) echo -e $USAGE
            exit -1
    esac
done

# Check that option -p has been specified
if [ "$PATH_TO_EBOA" == "" ];
then
    echo "ERROR: The option -p has to be provided"
    echo $USAGE
    exit -1
fi

# Check that the path to the eboa project exists
if [ ! -d $PATH_TO_EBOA ];
then
    echo "ERROR: The directory $PATH_TO_EBOA provided does not exist"
    exit -1
fi

######
# Create EBOA database container
######
# Remove eboa database container if it already exists
docker stop eboa-database-container
# Execute container
docker run  --name eboa-database-container -d mdillon/postgis

######
# Create EBOA container
######
# Remove eboa image and container if it already exists
docker stop eboa-container
docker rm eboa-container
docker rmi eboa
find . -name *pyc -delete
docker build -t eboa .
# Initialize the Postgis database
docker run -it --name eboa-container --link eboa-database-container:eboa -d -v $PATH_TO_EBOA:/eboa eboa
docker exec -it eboa-container bash -c "pip3 install -e /eboa/src"
# Initialize the EBOA database inside the postgis-database container
status=255
while true
do
    echo "Trying to initialize database..."
    docker exec -it eboa-container bash -c '/eboa/datamodel/init_ddbb.sh -h $EBOA_PORT_5432_TCP_ADDR -p $EBOA_PORT_5432_TCP_PORT -f /eboa/datamodel/eboa_data_model.sql'
    status=$?
    if [ $status -ne 0 ]
    then
        echo "Server is not ready yet..."
        # Wait for the server to be initialize
        sleep 1
    else
        echo "Database has been initialized..."
        break
    fi
done
# Change port and address configuration of the eboa defined by the postgis container
docker exec -it eboa-container bash -c 'sed -i "s/localhost/$EBOA_PORT_5432_TCP_ADDR/" /eboa/src/config/datamodel.json'
docker exec -it eboa-container bash -c 'sed -i "s/5432/$EBOA_PORT_5432_TCP_PORT/" /eboa/src/config/datamodel.json'

