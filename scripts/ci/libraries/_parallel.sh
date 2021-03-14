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

function parallel::initialize_monitoring() {
    PARALLEL_MONITORED_DIR="$(mktemp -d)"
    export PARALLEL_MONITORED_DIR

    PARALLEL_JOBLOG="$(mktemp)"
    export PARALLEL_JOBLOG
}

function parallel::make_sure_gnu_parallel_is_installed() {
    start_end::group_start "Making sure GNU Parallels is installed"
    echo
    echo "Making sure you have GNU parallel installed"
    echo
    echo "You might need to provide root password if you do not have it"
    echo
    (command -v parallel || apt install parallel || sudo apt install parallel || brew install parallel) >/dev/null
    start_end::group_end "Making sure GNU Parallels is installed"
}

function parallel::kill_stale_semaphore_locks() {
    local pid
    echo
    echo "${COLOR_BLUE}Killing stale semaphore locks${COLOR_RESET}"
    echo
    for s in "${HOME}/.parallel/semaphores/id-${SEMAPHORE_NAME}/"*@*
    do
        pid="${s%%@*}"
        if [[ ${pid} != "-*" ]]; then
            kill -15 -- -"$(basename "${s%%@*}")" 2>/dev/null || true
            rm -f "${s}" 2>/dev/null
        fi
    done
}


# Periodical loop to print summary of all the processes run by parallel
function parallel::monitor_loop() {
    echo
    echo "Start monitoring of parallel execution in ${PARALLEL_MONITORED_DIR} directory."
    echo
    local progress_report_number=1
    local start_time
    local end_time
    start_time=${SECONDS}
    while true
    do
        echo
        echo "${COLOR_YELLOW}########### Monitoring progress start: ${progress_report_number} #################${COLOR_RESET}"
        echo
        echo "${COLOR_BLUE}########### STATISTICS #################"
        docker_engine_resources::print_overall_stats
        echo "########### STATISTICS #################${COLOR_RESET}"
        for directory in "${PARALLEL_MONITORED_DIR}"/*/*
        do
            parallel_process=$(basename "${directory}")

            echo "${COLOR_BLUE}### The last lines for ${parallel_process} process ###${COLOR_RESET}"
            echo
            tail -2 "${directory}/stdout" || true
            echo
            echo
        done
        echo
        echo "${COLOR_YELLOW}########### Monitoring progress end: ${progress_report_number} #################${COLOR_RESET}}"
        echo
        end_time=${SECONDS}
        echo "${COLOR_YELLOW}############## $((end_time - start_time)) seconds passed since start ####################### ${COLOR_RESET}"
        sleep 10
        progress_report_number=$((progress_report_number + 1))
    done
    echo "${COLOR_BLUE}########### STATISTICS #################"
    docker_engine_resources::print_overall_stats
    echo "########### STATISTICS #################${COLOR_RESET}"
}

# Monitors progress of parallel execution and periodically summarizes stdout entries created by
# the parallel execution. Sets PAPARALLEL_MONITORED_DIR which should be be passed as --results
# parameter to GNU parallel execution.
function parallel::monitor_progress() {
    echo "Parallel results are stored in: ${PARALLEL_MONITORED_DIR}"
    echo "Parallel joblog is stored in: ${PARALLEL_JOBLOG}"

    parallel::monitor_loop 2>/dev/null &

    # shellcheck disable=SC2034
    PARALLEL_MONITORING_PID=$!
    # shellcheck disable=SC2016
    traps::add_trap 'parallel::kill_monitor' EXIT
}


function parallel::kill_monitor() {
    kill -9 ${PARALLEL_MONITORING_PID} >/dev/null 2>&1 || true
}
