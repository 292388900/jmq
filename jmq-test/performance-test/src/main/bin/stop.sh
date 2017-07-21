#!/bin/sh

PIDPROC=`ps -ef | grep 'com.ipd.jmq.test.performance' | grep -v 'grep'| awk '{print $2}'`

if [ -z "$PIDPROC" ];then
 echo "performance-test is not running"
 exit 0
fi

echo "PIDPROC: "$PIDPROC
for PID in $PIDPROC
do
if kill $PID
   then echo "process performance-test (Pid:$PID) was force stopped at " `date`
fi
done
echo stop finished.