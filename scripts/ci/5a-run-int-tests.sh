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

pwd

echo "Using travis airflow.cfg"
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cp -f ${DIR}/airflow_travis.cfg ~/unittests.cfg

ROOTDIR="$(dirname $(dirname ${DIR}))"
export AIRFLOW__CORE__DAGS_FOLDER="$ROOTDIR/tests/dags"

# add test/contrib to PYTHONPATH
export PYTHONPATH=${PYTHONPATH:-$ROOTDIR/tests/test_utils}

# environment
export AIRFLOW_HOME=${AIRFLOW_HOME:=${HOME}}
export AIRFLOW__CORE__UNIT_TEST_MODE=True

# configuration test
export AIRFLOW__TESTSECTION__TESTKEY=testvalue

# any argument received is overriding the default nose execution arguments:
nose_args=$@

# Generate the `airflow` executable if needed
which airflow > /dev/null || python setup.py develop

# For impersonation tests on Travis, make airflow accessible to other users via the global PATH
# (which contains /usr/local/bin)
sudo ln -sf "${VIRTUAL_ENV}/bin/airflow" /usr/local/bin/

# kdc init happens in setup_kdc.sh
kinit -kt ${KRB5_KTNAME} airflow

# For impersonation tests running on SQLite on Travis, make the database world readable so other
# users can update it
AIRFLOW_DB="$HOME/airflow.db"

if [ -f "${AIRFLOW_DB}" ]; then
  chmod a+rw "${AIRFLOW_DB}"
  chmod g+rwx "${AIRFLOW_HOME}"
fi

if [ ! -z "${GCLOUD_SERVICE_KEY}" ]; then

    echo "Installing lsb_release"
    echo
    sudo apt-get update && sudo apt-get install -y --no-install-recommends lsb-core
    echo
    echo "Starting the integration tests"
    echo ${GCLOUD_SERVICE_KEY} | base64 --decode > /tmp/key.json
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
    sudo gcloud auth activate-service-account \
       --key-file="${KEY_DIR}/${GCP_SERVICE_ACCOUNT_KEY_NAME}"
    ACCOUNT=$(cat "${KEY_DIR}/${GCP_SERVICE_ACCOUNT_KEY_NAME}" | \
      python -c 'import json, sys; info=json.load(sys.stdin); print info["client_email"]')
    PROJECT=$(cat "${KEY_DIR}/${GCP_SERVICE_ACCOUNT_KEY_NAME}" | \
      python -c 'import json, sys; info=json.load(sys.stdin); print info["project_id"]')
    sudo gcloud config set account "${ACCOUNT}"
    sudo gcloud config set project "${PROJECT}"
    echo "Initializing the DB"
    yes | sudo airflow initdb
    yes | sudo airflow resetdb
    python ${DIR}/_setup_gcp_connection.py "${PROJECT}"
    echo
    echo "Service account activated"
    echo
    rm -vf /tmp/key.json
else
    echo "Skipping integration tests as no GCLOUD_SERVICE_KEY defined"
    exit 0
fi

AIRFLOW_HOME=${AIRFLOW_HOME:-/home/airflow}
INT_TEST_DAGS=${INT_TEST_DAGS:-${AIRFLOW_HOME}/incubator-airflow/airflow/contrib/example_dags/*.py}
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
    sudo airflow variables -s "${ENV[0]}" "${ENV[1]}"
    echo "Set Airflow variable:"" ${ENV[0]}"" ${ENV[1]}"
done

echo "Running test DAGs from: ${INT_TEST_DAGS}"
rm -fv ${AIRFLOW_HOME}/dags/*
cp -v ${INT_TEST_DAGS} ${AIRFLOW_HOME}/dags/

nohup sudo airflow webserver &
sleep 2
nohup sudo airflow scheduler &
sleep 2

function get_dag_state() {
    tmp=$(sudo airflow dag_state $1 $(date -d "1 day ago" '+%m-%dT00:00:00+00:00'))
    result=$(echo "$tmp" | tail -1)
    echo ${result}
}

results=()
while read -r name ; do
    echo "Unpausing $name"
    sudo airflow unpause ${name}
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
done < <(ls /home/airflow/dags | grep '.*py$' | grep -Po '.*(?=\.)')
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
