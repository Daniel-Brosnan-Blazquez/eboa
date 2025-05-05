# Start EBOA for running infinitely
echo
echo "####################"
echo "Initialize EBOA DDBB"
echo "####################"
while true
do
    echo "Trying to initialize EBOA, SBOA and UBOA databases..."
    boa_init.py -e -s -u -y
    status=$?
    if [ $status -ne 0 ]
    then
        echo "Server is not ready yet..."
        # Wait for the server to be initialize
        sleep 1
    else
        echo "Databases have been initialized... :D"
        break
    fi
done
