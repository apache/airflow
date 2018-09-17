#!/usr/bin/env bash

#
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

set -o verbose
set +x

pwd

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOTDIR="$(dirname $(dirname ${DIR}))"


export AIRFLOW__CORE__DAGS_FOLDER="/tmp/dags"
export AIRFLOW_HOME=${AIRFLOW_HOME:=/app}

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

# For impersonation tests on Travis, make airflow accessible to other users via the global PATH
# (which contains /usr/local/bin)
sudo ln -sf "${VIRTUAL_ENV}/bin/airflow" /usr/local/bin/
sudo chown -R airflow.airflow "${HOME}/.config"
# kdc init happens in setup_kdc.sh
kinit -kt ${KRB5_KTNAME} airflow

# For impersonation tests running on SQLite on Travis, make the database world readable so other
# users can update it
AIRFLOW_DB="$HOME/airflow.db"

if [ -f "${AIRFLOW_DB}" ]; then
  sudo chown airflow.airflow "${AIRFLOW_DB}"
  chmod a+rw "${AIRFLOW_DB}"
  chmod g+rwx "${AIRFLOW_HOME}"
else
  touch "${AIRFLOW_DB}"
fi

if [ ! -z "${GCLOUD_SERVICE_KEY_BASE64}" ]; then

    echo "Initializing the DB"
    yes | airflow initdb
    yes | airflow resetdb
    echo "Installing lsb_release"
    echo
    sudo apt-get update && sudo apt-get install -y --no-install-recommends lsb-core
    echo
    echo "Extracting the key"
    echo ${GCLOUD_SERVICE_KEY_BASE64} | base64 --decode > /tmp/key.json
    KEY_DIR=/tmp
    GCP_SERVICE_ACCOUNT_KEY_NAME=key.json

    echo
    echo "Installing gcloud CLI"
    echo
    export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)"
    echo "deb http://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -
    sudo apt-get update && sudo apt-get install -y --no-install-recommends google-cloud-sdk
    echo
    echo "Activating service account with ${KEY_DIR}/${GCP_SERVICE_ACCOUNT_KEY_NAME}"
    echo
    export GOOGLE_APPLICATION_CREDENTIALS=${KEY_DIR}/${GCP_SERVICE_ACCOUNT_KEY_NAME}
    gcloud auth activate-service-account \
       --key-file="${KEY_DIR}/${GCP_SERVICE_ACCOUNT_KEY_NAME}"
    ACCOUNT=$(cat "${KEY_DIR}/${GCP_SERVICE_ACCOUNT_KEY_NAME}" | \
      python -c 'import json, sys; info=json.load(sys.stdin); print(info["client_email"])')
    PROJECT=$(cat "${KEY_DIR}/${GCP_SERVICE_ACCOUNT_KEY_NAME}" | \
      python -c 'import json, sys; info=json.load(sys.stdin); print(info["project_id"])')
    echo "ACCOUNT=${ACCOUNT}"
    echo "PROJECT=${PROJECT}"
    gcloud config set account "${ACCOUNT}"
    gcloud config set project "${PROJECT}"
    python ${DIR}/_setup_gcp_connection.py "${PROJECT}"
else
    echo "Skipping integration tests as no GCLOUD_SERVICE_KEY_BASE64 defined."\
         "Set the variable to base64-encoded service account private key .json file"
    exit 0
fi

AIRFLOW_HOME=${AIRFLOW_HOME:-/app}
INT_TEST_DAGS=${INT_TEST_DAGS:-${AIRFLOW_HOME}/airflow/contrib/example_dags/*.py}
INT_TEST_VARS=${INT_TEST_VARS:-"[PROJECT_ID=project,LOCATION=europe-west1,SOURCE_REPOSITORY=https://example.com,ENTRYPOINT=helloWorld]"}

echo "AIRFLOW_HOME = ${AIRFLOW_HOME}"
echo "VARIABLES    = ${INT_TEST_VARS}"
echo "DAGS         = ${INT_TEST_DAGS}"

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

echo "Running test DAGs from: ${INT_TEST_DAGS}"
cp -v ${INT_TEST_DAGS} ${AIRFLOW__CORE__DAGS_FOLDER}/

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
