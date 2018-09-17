#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOTDIR="$(dirname $(dirname ${DIR}))"

export AIRFLOW__CORE__DAGS_FOLDER="/tmp/dags"
export AIRFLOW_HOME=${AIRFLOW_HOME:-/home/airflow}

# Generate the `airflow` executable if needed
which airflow > /dev/null || python setup.py develop

# Cleaning up after potential previous run
echo Removing all dags from the dags folder
rm -vf ${AIRFLOW__CORE__DAGS_FOLDER}/*
mkdir -pv ${AIRFLOW__CORE__DAGS_FOLDER}/

echo "Killing scheduler and webserver daemons (just in case)"

function kill_process_and_wait() {
    export PID_FILE_NAME=$1
    PID=$(cat /app/${PID_FILE_NAME}.pid 2>/dev/null)
    while [ -f /app/${PID_FILE_NAME}.pid ]; do
        kill ${PID} 2>/dev/null
        echo "Sleeping until ${PID_FILE_NAME} gets killed"
        sleep 1
        if ps -p ${PID} > /dev/null
        then
           echo "Process ${PID} is running"
        else
           rm -f /app/${PID_FILE_NAME}.pid
           break
        fi
    done
}


kill_process_and_wait airflow-scheduler
kill_process_and_wait airflow-webserver-monitor
kill_process_and_wait airflow-webserver

for i in "$@"
do
case ${i} in
    -v=*|--vars=*)
    INT_TEST_VARS="${i#*=}"
    shift # past argument=value
    ;;
    -d=*|--dags=*)
    INT_TEST_DAGS="${i#*=}"
    shift # past argument=value
    ;;
    *)
          # unknown option
    ;;
esac
done
echo "VARIABLES  = ${INT_TEST_VARS}"
echo "DAGS       = ${INT_TEST_DAGS}"
if [[ -n $1 ]]; then
    echo "Last line of file specified as non-opt/last argument:"
    tail -1 $1
fi

# Remove square brackets if they exist
TEMP=${INT_TEST_VARS//[}
SANITIZED_VARIABLES=${TEMP//]}
echo ""
echo "========= AIRFLOW VARIABLES =========="
echo ${SANITIZED_VARIABLES}
echo ""


IFS=',' read -ra ENVS <<< "${SANITIZED_VARIABLES}"
for item in "${ENVS[@]}"; do
    IFS='=' read -ra ENV <<< "$item"
    airflow variables -s "${ENV[0]}" "${ENV[1]}"
    echo "Set Airflow variable:"" ${ENV[0]}"" ${ENV[1]}"
done

INT_TEST_DAGS=${INT_TEST_DAGS:-${AIRFLOW_HOME}/incubator-airflow/airflow/contrib/example_dags/*.py}
INT_TEST_VARS=${INT_TEST_VARS:-"[PROJECT_ID=project,LOCATION=europe-west1,SOURCE_REPOSITORY=https://example.com,ENTRYPOINT=helloWorld]"}

echo "Running test DAGs from: ${AIRFLOW__CORE__DAGS_FOLDER}"
cp -v ${INT_TEST_DAGS} ${AIRFLOW__CORE__DAGS_FOLDER}/

airflow initdb
airflow webserver --daemon
airflow scheduler --daemon

sleep 5

function get_dag_state() {
    tmp=$(airflow dag_state $1 $(date -d "1 day ago" '+%m-%dT00:00:00+00:00'))
    result=$(echo "$tmp" | tail -1)
    echo ${result}
}

results=()
while read -r name ; do
    echo "Unpausing $name"
    airflow unpause ${name}
    while [ "$(get_dag_state ${name})" = "running" ]
    do
        echo "Sleeping 1s..."
        sleep 1
        continue
    done
    res=$(get_dag_state ${name})
    if ! [[ ${res} = "success" ]]; then
        res="failed"
    fi
    echo ">>> FINISHED $name: "${res}
    results+=("$name:"${res})
done < <(ls ${AIRFLOW__CORE__DAGS_FOLDER} | grep '.*py$' | grep -Po '.*(?=\.)')
# `ls ...` -> Get all .py files and remove the file extension from the names
# ^ Process substitution to avoid the sub-shell and interact with array outside of the loop
# https://unix.stackexchange.com/a/407794/78408

echo ""
echo "===== RESULTS: ====="
for item in "${results[@]}"
do
    echo ${item}
done
echo ""

for item in "${results[@]}"
do
    IFS=':' read -ra NAMES <<< "$item"
    if [[ ${NAMES[1]} = "failed" ]]; then
        dir_name="${NAMES[0]}"
        for entry in "${AIRFLOW_HOME}/logs/${dir_name}/*"
        do
            echo ""
            echo ""
            echo "===== ERROR LOG [START]: ${dir_name} ===== "
            echo ${entry}
            echo ""
            tail -n 50 ${entry}/$(date -d "1 day ago" '+%Y-%m-%dT00:00:00+00:00')/1.log
            echo ""
            echo "===== ERROR LOG [END]: ${dir_name} ===== "
            echo ""
        done
    fi
done

echo "NUMBER OF TESTS RUN: ${#results[@]}"

for item in "${results[@]}"
do
    if ! [[ ${item} = *"success"* ]]; then
        echo "STATUS: TESTS FAILED"
        exit 1
    fi
done

echo "STATUS: ALL TESTS SUCCEEDED"
exit 0
