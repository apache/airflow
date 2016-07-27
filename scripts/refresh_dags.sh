#!/usr/bin/env bash

NUM_WORKER_PROCS_EXPECTED=2
WORKER_STARTUP_WAIT=5 # if some workers are starting up, how long to wait before to checking again
WORKER_REFRESH_WAIT=30 # if all workers are ready, how long to wait before refreshing a worker

NUM_MASTER_PROCS_FOUND=$(ps aux | grep "gunicorn: master" | grep -v "grep" | wc -l | sed -e 's/^[[:space:]]*//')

if [ "$NUM_MASTER_PROCS_FOUND" -ne "1" ]
then
    echo "Expected 1 gunicorn master process, found $NUM_MASTER_PROCS_FOUND"
    exit 1
fi

MASTER_PID=$(ps aux | grep "gunicorn: master" | grep -v "grep" | awk '{print $2}')
echo $MASTER_PID


# State transition diagram, where each state is [$NUM_READY_WORKER_PROCS_FOUND / $NUM_WORKER_PROCS_FOUND].
# We expect most time to be spent in [n / n]. The horizontal transition at ? happens when the new worker
# parses all the dags.
#
#    V ------------------------------------------------------------------------------------------------------|
# [n / n] ----$WORKER_REFRESH_WAIT----> [n / n + 1]  ----$WORKER_STARTUP_WAIT----?----> [n + 1 / n + 1] ---- |
#                                            ^-----------------------------------|
#
while true
do
    NUM_WORKER_PROCS_FOUND=$(ps aux | grep "gunicorn: worker" | grep -v grep | wc -l | sed -e 's/^[[:space:]]*//')
    NUM_READY_WORKER_PROCS_FOUND=$(ps aux | grep "\[ready\] gunicorn: worker" | grep -v grep | wc -l | sed -e 's/^[[:space:]]*//')

    echo "found [$NUM_READY_WORKER_PROCS_FOUND / $NUM_WORKER_PROCS_FOUND] workers"

    if [ $NUM_WORKER_PROCS_FOUND -gt $NUM_READY_WORKER_PROCS_FOUND ]
    then
        echo "some workers are starting up, waiting..."
        sleep $WORKER_STARTUP_WAIT
        continue
    fi

    if [ $NUM_WORKER_PROCS_FOUND -gt $NUM_WORKER_PROCS_EXPECTED ]
    then
        echo "killing a worker"
        kill -TTOU $MASTER_PID
        sleep 1
        continue
    fi

    if [ $NUM_WORKER_PROCS_FOUND -eq $NUM_WORKER_PROCS_EXPECTED ]
    then
        sleep $WORKER_REFRESH_WAIT
        echo "doing a refresh"
        kill -TTIN $MASTER_PID
        continue
    fi

done
