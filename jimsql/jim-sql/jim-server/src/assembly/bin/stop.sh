#!/bin/sh
if [ "$1" == "pid" ]
then
    PIDPROC=`cat ./jim.pid`
else
    PIDPROC=`ps -ef | grep 'io.jimdb.sql.server.JimBootstrap' | grep -v 'grep'| awk '{print $2}'`
fi

if [ -z "$PIDPROC" ];then
 echo "jim.server is not running"
 exit 0
fi

echo "PIDPROC: "$PIDPROC
for PID in $PIDPROC
do
if kill $PID
   then echo "process jim.server(Pid:$PID) was force stopped at " `date`
fi
done
echo stop finished.
