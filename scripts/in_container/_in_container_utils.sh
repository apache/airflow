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
    for signal in "${@}"; do
        # adding trap to exiting trap
        local handlers
        handlers="$(trap -p "${signal}" | cut -f2 -d \')"
        # shellcheck disable=SC2064
        trap "${trap};${handlers}" "${signal}"
    done
}

function assert_in_container() {
    export VERBOSE=${VERBOSE:="false"}
    if [[ ! -f /.dockerenv ]]; then
        echo
        echo "${COLOR_RED}ERROR: You are not inside the Airflow docker container!  ${COLOR_RESET}"
        echo
        echo "You should only run this script in the Airflow docker container as it may override your files."
        echo "Learn more about how we develop and test airflow in:"
        echo "https://github.com/apache/airflow/blob/main/contribuging-docs/README.rst"
        echo
        exit 1
    fi
}

function in_container_script_start() {
    if [[ ${VERBOSE_COMMANDS:="false"} == "true" || ${VERBOSE_COMMANDS} == "True" ]]; then
        set -x
    fi
}

function in_container_go_to_airflow_sources() {
    pushd "${AIRFLOW_SOURCES}" >/dev/null 2>&1 || exit 1
}

function in_container_basic_check() {
    assert_in_container
    in_container_go_to_airflow_sources
}

function dump_airflow_logs() {
    local dump_file
    dump_file=/files/airflow_logs_$(date "+%Y-%m-%d")_${CI_BUILD_ID}_${CI_JOB_ID}.log.tar.gz
    echo "###########################################################################################"
    echo "                   Dumping logs from all the airflow tasks"
    echo "###########################################################################################"
    pushd "${AIRFLOW_HOME}" >/dev/null 2>&1 || exit 1
    tar -czf "${dump_file}" logs
    echo "                   Logs dumped to ${dump_file}"
    popd >/dev/null 2>&1 || exit 1
    echo "###########################################################################################"
}

function in_container_set_colors() {
    COLOR_BLUE=$'\e[34m'
    COLOR_GREEN=$'\e[32m'
    COLOR_RED=$'\e[31m'
    COLOR_RESET=$'\e[0m'
    COLOR_YELLOW=$'\e[33m'
    export COLOR_BLUE
    export COLOR_GREEN
    export COLOR_RED
    export COLOR_RESET
    export COLOR_YELLOW
}

export CI=${CI:="false"}
export GITHUB_ACTIONS=${GITHUB_ACTIONS:="false"}
