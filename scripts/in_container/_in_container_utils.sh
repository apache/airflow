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

function in_container_get_packaging_tool() {
    ## IMPORTANT: IF YOU MODIFY THIS FUNCTION YOU SHOULD ALSO MODIFY CORRESPONDING FUNCTION IN
    ## `scripts/docker/common.sh`
    local PYTHON_BIN
    PYTHON_BIN=$(which python)
    if [[ ${AIRFLOW_USE_UV} == "true" ]]; then
        echo
        echo "${COLOR_BLUE}Using 'uv' to install Airflow${COLOR_RESET}"
        echo
        export PACKAGING_TOOL=""
        export PACKAGING_TOOL_CMD="uv pip"
        if [[ -z ${VIRTUAL_ENV=} ]]; then
            export EXTRA_INSTALL_FLAGS="--python ${PYTHON_BIN}"
            export EXTRA_UNINSTALL_FLAGS="--python ${PYTHON_BIN}"
        else
            export EXTRA_INSTALL_FLAGS=""
            export EXTRA_UNINSTALL_FLAGS=""
        fi
        export UPGRADE_EAGERLY="--upgrade --resolution highest"
        export UPGRADE_IF_NEEDED="--upgrade"
        UV_CONCURRENT_DOWNLOADS=$(nproc --all)
        export UV_CONCURRENT_DOWNLOADS
    else
        echo
        echo "${COLOR_BLUE}Using 'pip' to install Airflow${COLOR_RESET}"
        echo
        export PACKAGING_TOOL="pip"
        export PACKAGING_TOOL_CMD="pip"
        export EXTRA_INSTALL_FLAGS="--root-user-action ignore"
        export EXTRA_UNINSTALL_FLAGS="--yes"
        export UPGRADE_EAGERLY="--upgrade-strategy eager"
        export UPGRADE_IF_NEEDED="--upgrade --upgrade-strategy only-if-needed"
    fi
}

function in_container_basic_check() {
    assert_in_container
    in_container_get_packaging_tool
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
export AIRFLOW_USE_UV=${AIRFLOW_USE_UV:="false"}
