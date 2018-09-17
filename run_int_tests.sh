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

echo ""
echo "========= AIRFLOW VARIABLES =========="
echo $1
echo ""

IFS=',' read -ra ENVS <<< "$1"
for item in "${ENVS[@]}"; do
    IFS='=' read -ra ENV <<< "$item"
    airflow variables -s "${ENV[0]}" "${ENV[1]}"
    echo "Set Airflow variable:"" ${ENV[0]}"" ${ENV[1]}"
done

shift

echo "Running test DAGs from: $@"
cp $@ /home/airflow/dags/

airflow initdb
tmux new-session -d -s webserver 'airflow webserver'
sleep 2
tmux new-session -d -s scheduler 'airflow scheduler'
sleep 2

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
done < <(ls /home/airflow/dags | grep '.*py$' | grep -Po '.*(?=\.)')
# `ls ...` -> Get all .py files and remove the file extension from the names
# ^ Process substitution to avoid the subshell and interact with array outside of the loop
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

for item in "${results[@]}"
do
    if ! [[ ${item} = *"success"* ]]; then
        echo "STATUS: TESTS FAILED"
        exit 1
    fi
done

echo "STATUS: ALL TESTS SUCCEEDED"
exit 0
