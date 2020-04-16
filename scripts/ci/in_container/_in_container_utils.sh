#!/usr/bin/env bash
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

function assert_in_container() {
    export VERBOSE=${VERBOSE:="false"}
    if [[ ! -f /.dockerenv ]]; then
        echo >&2
        echo >&2 "You are not inside the Airflow docker container!"
        echo >&2 "You should only run this script in the Airflow docker container as it may override your files."
        echo >&2 "Learn more about how we develop and test airflow in:"
        echo >&2 "https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst"
        echo >&2
        exit 1
    fi
}

function in_container_script_start() {
    if [[ ${VERBOSE_COMMANDS:="false"} == "true" ]]; then
        set -x
    fi
}

function in_container_script_end() {
    #shellcheck disable=2181
    EXIT_CODE=$?
    if [[ ${EXIT_CODE} != 0 ]]; then
        echo "###########################################################################################"
        echo "                   EXITING ${0} WITH STATUS CODE ${EXIT_CODE}"
        echo "###########################################################################################"

    fi
    if [[ ${VERBOSE_COMMANDS} == "true" ]]; then
        set +x
    fi
}

#
# Cleans up PYC files (in case they come in mounted folders)
#
function in_container_cleanup_pyc() {
    set +o pipefail
    sudo find . \
        -path "./airflow/www/node_modules" -prune -o \
        -path "./airflow/www_rbac/node_modules" -prune -o \
        -path "./.eggs" -prune -o \
        -path "./docs/_build" -prune -o \
        -path "./build" -prune -o \
        -name "*.pyc" | grep ".pyc$" | sudo xargs rm -f
    set -o pipefail
}

#
# Cleans up __pycache__ directories (in case they come in mounted folders)
#
function in_container_cleanup_pycache() {
    set +o pipefail
    find . \
        -path "./airflow/www/node_modules" -prune -o \
        -path "./airflow/www_rbac/node_modules" -prune -o \
        -path "./.eggs" -prune -o \
        -path "./docs/_build" -prune -o \
        -path "./build" -prune -o \
        -name "__pycache__" | grep "__pycache__" | sudo xargs rm -rvf
    set -o pipefail
}

#
# Fixes ownership of files generated in container - if they are owned by root, they will be owned by
# The host user.
#
function in_container_fix_ownership() {
    set +o pipefail
    sudo find . -user root \
    | sudo xargs chown -v "${HOST_USER_ID}.${HOST_GROUP_ID}" --no-dereference >/dev/null 2>&1
    set -o pipefail
}

function in_container_go_to_airflow_sources() {
    pushd "${AIRFLOW_SOURCES}"  &>/dev/null || exit 1
}

function in_container_basic_sanity_check() {
    assert_in_container
    in_container_go_to_airflow_sources
    in_container_cleanup_pyc
    in_container_cleanup_pycache
}

export DISABLE_CHECKS_FOR_TESTS="missing-docstring,no-self-use,too-many-public-methods,protected-access"

function start_output_heartbeat() {
    MESSAGE=${1:-"Still working!"}
    INTERVAL=${2:=10}
    echo
    echo "Starting output heartbeat"
    echo

    bash 2> /dev/null <<EOF &
while true; do
  echo "\$(date): ${MESSAGE} "
  sleep ${INTERVAL}
done
EOF
    export HEARTBEAT_PID=$!
}

function stop_output_heartbeat() {
    kill "${HEARTBEAT_PID}"
    wait "${HEARTBEAT_PID}" || true 2> /dev/null
}

function setup_kerberos() {
    FQDN=$(hostname)
    ADMIN="admin"
    PASS="airflow"
    KRB5_KTNAME=/etc/airflow.keytab

    sudo cp "${MY_DIR}/krb5/krb5.conf" /etc/krb5.conf

    echo -e "${PASS}\n${PASS}" | \
        sudo kadmin -p "${ADMIN}/admin" -w "${PASS}" -q "addprinc -randkey airflow/${FQDN}" 2>&1 \
          | sudo tee "${AIRFLOW_HOME}/logs/kadmin_1.log" >/dev/null
    RES_1=$?

    sudo kadmin -p "${ADMIN}/admin" -w "${PASS}" -q "ktadd -k ${KRB5_KTNAME} airflow" 2>&1 \
          | sudo tee "${AIRFLOW_HOME}/logs/kadmin_2.log" >/dev/null
    RES_2=$?

    sudo kadmin -p "${ADMIN}/admin" -w "${PASS}" -q "ktadd -k ${KRB5_KTNAME} airflow/${FQDN}" 2>&1 \
          | sudo tee "${AIRFLOW_HOME}/logs``/kadmin_3.log" >/dev/null
    RES_3=$?

    if [[ ${RES_1} != 0 || ${RES_2} != 0 || ${RES_3} != 0 ]]; then
        exit 1
    else
        echo
        echo "Kerberos enabled and working."
        echo
        sudo chmod 0644 "${KRB5_KTNAME}"
    fi
}


function dump_container_logs() {
    echo "###########################################################################################"
    echo "                   Dumping logs from all the containers"
    echo "###########################################################################################"
    echo "  Docker processes:"
    echo "###########################################################################################"
    docker ps --no-trunc
    echo "###########################################################################################"
    for CONTAINER in $(docker ps -qa)
    do
        CONTAINER_NAME=$(docker inspect --format "{{.Name}}" "${CONTAINER}")
        echo "-------------------------------------------------------------------------------------------"
        echo " Docker inspect: ${CONTAINER_NAME}"
        echo "-------------------------------------------------------------------------------------------"
        echo
        docker inspect "${CONTAINER}"
        echo
        echo "-------------------------------------------------------------------------------------------"
        echo " Docker logs: ${CONTAINER_NAME}"
        echo "-------------------------------------------------------------------------------------------"
        echo
        docker logs "${CONTAINER}"
        echo
        echo "###########################################################################################"
    done
}

function dump_airflow_logs() {
    echo "###########################################################################################"
    echo "                   Dumping logs from all the airflow tasks"
    echo "###########################################################################################"
    pushd /root/airflow/ || exit 1
    tar -czf "${1}" logs
    popd || exit 1
    echo "###########################################################################################"
}


function send_docker_logs_to_file_io() {
    echo "##############################################################################"
    echo
    echo "   DUMPING LOG FILES FROM CONTAINERS AND SENDING THEM TO file.io"
    echo
    echo "##############################################################################"
    DUMP_FILE=/tmp/$(date "+%Y-%m-%d")_docker_${TRAVIS_BUILD_ID:="default"}_${TRAVIS_JOB_ID:="default"}.log.gz
    dump_container_logs 2>&1 | gzip >"${DUMP_FILE}"
    echo
    echo "   Logs saved to ${DUMP_FILE}"
    echo
    echo "##############################################################################"
    curl -F "file=@${DUMP_FILE}" https://file.io
}

function send_airflow_logs_to_file_io() {
    echo "##############################################################################"
    echo
    echo "   DUMPING LOG FILES FROM AIRFLOW AND SENDING THEM TO file.io"
    echo
    echo "##############################################################################"
    DUMP_FILE=/tmp/$(date "+%Y-%m-%d")_airflow_${TRAVIS_BUILD_ID:="default"}_${TRAVIS_JOB_ID:="default"}.log.tar.gz
    dump_airflow_logs "${DUMP_FILE}"
    echo
    echo "   Logs saved to ${DUMP_FILE}"
    echo
    echo "##############################################################################"
    curl -F "file=@${DUMP_FILE}" https://file.io
}


function dump_kind_logs() {
    echo "###########################################################################################"
    echo "                   Dumping logs from KIND"
    echo "###########################################################################################"

    FILE_NAME="${1}"
    kind --name "${CLUSTER_NAME}" export logs "${FILE_NAME}"
}


function send_kubernetes_logs_to_file_io() {
    echo "##############################################################################"
    echo
    echo "   DUMPING LOG FILES FROM KIND AND SENDING THEM TO file.io"
    echo
    echo "##############################################################################"
    DUMP_DIR_NAME=$(date "+%Y-%m-%d")_kind_${TRAVIS_BUILD_ID:="default"}_${TRAVIS_JOB_ID:="default"}
    DUMP_DIR=/tmp/${DUMP_DIR_NAME}
    dump_kind_logs "${DUMP_DIR}"
    tar -cvzf "${DUMP_DIR}.tar.gz" -C /tmp "${DUMP_DIR_NAME}"
    echo
    echo "   Logs saved to ${DUMP_DIR}.tar.gz"
    echo
    echo "##############################################################################"
    curl -F "file=@${DUMP_DIR}.tar.gz" https://file.io
}

function install_released_airflow_version() {
    pip uninstall -y apache-airflow || true
    find /root/airflow/ -type f -print0 | xargs rm -f --
    if [[ ${1} == "1.10.2" || ${1} == "1.10.1" ]]; then
        export SLUGIFY_USES_TEXT_UNIDECODE=yes
    fi
    rm -rf "${AIRFLOW_SOURCES}"/*.egg-info
    INSTALLS=("apache-airflow==${1}" "werkzeug<1.0.0")
    pip install --upgrade "${INSTALLS[@]}"
}
