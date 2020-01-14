PIDFILE="./data_server.pid"
echo $PIDFILE
if [ ! -f "$PIDFILE" ]
then
    echo "no data-server to stop (could not find file $PIDFILE)"
else
    echo "pid " $(cat "$PIDFILE")
    kill -9 $(cat "$PIDFILE")
    rm -f "$PIDFILE"
    echo STOPPED
fi
