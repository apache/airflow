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

#######################################################################################################
#
# Adds trap to the traps already set.
#
# Arguments:
#      trap to set
#      .... list of signals to handle
#######################################################################################################
function add_trap() {
    trap="${1}"
    shift
    for signal in "${@}"
    do
        # adding trap to exiting trap
        local handlers
        handlers="$( trap -p "${signal}" | cut -f2 -d \' )"
        # shellcheck disable=SC2064
        trap "${trap};${handlers}" "${signal}"
    done
}

function assert_in_container() {
    export VERBOSE=${VERBOSE:="false"}
    if [[ ! -f /.dockerenv ]]; then
        echo
        echo "${COLOR_RED_ERROR} You are not inside the Airflow docker container!  ${COLOR_RESET}"
        echo
        echo "You should only run this script in the Airflow docker container as it may override your files."
        echo "Learn more about how we develop and test airflow in:"
        echo "https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst"
        echo
        exit 1
    fi
}

function in_container_script_start() {
    OUT_FILE_PRINTED_ON_ERROR=$(mktemp)
    if [[ ${VERBOSE_COMMANDS:="false"} == "true" ]]; then
        set -x
    fi
}

function in_container_script_end() {
    #shellcheck disable=2181
    EXIT_CODE=$?
    if [[ ${EXIT_CODE} != 0 ]]; then
        if [[ "${PRINT_INFO_FROM_SCRIPTS=="true"}" == "true" ]] ;then
            if [[ -f ${OUT_FILE_PRINTED_ON_ERROR} ]]; then
                echo "###########################################################################################"
                echo
                echo "${COLOR_BLUE} EXIT CODE: ${EXIT_CODE} in container (See above for error message). Below is the output of the last action! ${COLOR_RESET}"
                echo
                echo "${COLOR_BLUE}***  BEGINNING OF THE LAST COMMAND OUTPUT *** ${COLOR_RESET}"
                cat "${OUT_FILE_PRINTED_ON_ERROR}"
                echo "${COLOR_BLUE}***  END OF THE LAST COMMAND OUTPUT ***  ${COLOR_RESET}"
                echo
                echo "${COLOR_BLUE} EXIT CODE: ${EXIT_CODE} in container. The actual error might be above the output!  ${COLOR_RESET}"
                echo
                echo "###########################################################################################"
            else
                echo "########################################################################################################################"
                echo "${COLOR_BLUE} [IN CONTAINER]   EXITING ${0} WITH EXIT CODE ${EXIT_CODE}  ${COLOR_RESET}"
                echo "########################################################################################################################"
            fi
        fi
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
        -name "__pycache__" | grep "__pycache__" | sudo xargs rm -rf
    set -o pipefail
}

#
# Fixes ownership of files generated in container - if they are owned by root, they will be owned by
# The host user. Only needed if the host is Linux - on Mac, ownership of files is automatically
# changed to the Host user via osxfs filesystem
#
function in_container_fix_ownership() {
    if [[ ${HOST_OS:=} == "Linux" ]]; then
        DIRECTORIES_TO_FIX=(
            "/tmp"
            "/files"
            "/root/.aws"
            "/root/.azure"
            "/root/.config/gcloud"
            "/root/.docker"
            "${AIRFLOW_SOURCES}"
        )
        if [[ ${VERBOSE} == "true" ]]; then
            echo "Fixing ownership of mounted files"
        fi
        sudo find "${DIRECTORIES_TO_FIX[@]}" -print0 -user root 2>/dev/null \
            | sudo xargs --null chown "${HOST_USER_ID}.${HOST_GROUP_ID}" --no-dereference ||
                true >/dev/null 2>&1
        if [[ ${VERBOSE} == "true" ]]; then
            echo "Fixed ownership of mounted files"
        fi
    fi
}

function in_container_clear_tmp() {
    if [[ ${VERBOSE} == "true" ]]; then
        echo "Cleaning ${AIRFLOW_SOURCES}/tmp from the container"
    fi
    rm -rf /tmp/*
    if [[ ${VERBOSE} == "true" ]]; then
        echo "Cleaned ${AIRFLOW_SOURCES}/tmp from the container"
    fi
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

export DISABLE_CHECKS_FOR_TESTS="missing-docstring,no-self-use,too-many-public-methods,protected-access,do-not-use-asserts"

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
    kill "${HEARTBEAT_PID}" || true
    wait "${HEARTBEAT_PID}" || true 2> /dev/null
}

function setup_kerberos() {
    FQDN=$(hostname)
    ADMIN="admin"
    PASS="airflow"
    KRB5_KTNAME=/etc/airflow.keytab

    sudo cp "${AIRFLOW_SOURCES}/scripts/in_container/krb5/krb5.conf" /etc/krb5.conf

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
        echo
        echo "Error when setting up Kerberos: ${RES_1} ${RES_2} ${RES_3}}!"
        echo
        exit 1
    else
        echo
        echo "Kerberos enabled and working."
        echo
        sudo chmod 0644 "${KRB5_KTNAME}"
    fi
}

function dump_airflow_logs() {
    local dump_file
    dump_file=/files/airflow_logs_$(date "+%Y-%m-%d")_${CI_BUILD_ID}_${CI_JOB_ID}.log.tar.gz
    echo "###########################################################################################"
    echo "                   Dumping logs from all the airflow tasks"
    echo "###########################################################################################"
    pushd "${AIRFLOW_HOME}" || exit 1
    tar -czf "${dump_file}" logs
    echo "                   Logs dumped to ${dump_file}"
    popd || exit 1
    echo "###########################################################################################"
}

function install_released_airflow_version() {
    pip uninstall -y apache-airflow || true
    find /root/airflow/ -type f -print0 | xargs -0 rm -f --
    if [[ ${1} == "1.10.2" || ${1} == "1.10.1" ]]; then
        export SLUGIFY_USES_TEXT_UNIDECODE=yes
    fi
    rm -rf "${AIRFLOW_SOURCES}"/*.egg-info
    INSTALLS=("apache-airflow==${1}" "werkzeug<1.0.0")
    pip install --upgrade "${INSTALLS[@]}"
}


function in_container_set_colors() {
    COLOR_BLUE=$'\e[34m'
    COLOR_GREEN=$'\e[32m'
    COLOR_GREEN_OK=$'\e[32mOK.'
    COLOR_RED=$'\e[31m'
    COLOR_RED_ERROR=$'\e[31mERROR:'
    COLOR_RESET=$'\e[0m'
    COLOR_YELLOW=$'\e[33m'
    COLOR_YELLOW_WARNING=$'\e[33mWARNING:'
    export COLOR_BLUE
    export COLOR_GREEN
    export COLOR_GREEN_OK
    export COLOR_RED
    export COLOR_RED_ERROR
    export COLOR_RESET
    export COLOR_YELLOW
    export COLOR_YELLOW_WARNING
}

export CI=${CI:="false"}
export GITHUB_ACTIONS=${GITHUB_ACTIONS:="false"}
