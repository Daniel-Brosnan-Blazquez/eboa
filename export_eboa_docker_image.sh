#################################################################
#
# Export eboa image
#
# Written by DEIMOS Space S.L. (dibb)
#
# module eboa
#################################################################

USAGE="Usage: `basename $0` -v version"
VERSION=""

while getopts v: option
do
    case "${option}"
        in
        v) VERSION=${OPTARG};;
        ?) echo -e $USAGE
            exit -1
    esac
done

# Check that option -v has been specified
if [ "$VERSION" == "" ];
then
    echo "ERROR: The option -v has to be provided"
    echo $USAGE
    exit -1
fi

# Commit changes to the image
docker commit -m "EBOA installation" eboa-container eboa:0.1.0
# Save image into an archive
docker save eboa:0.1.0 > eboa.tar
