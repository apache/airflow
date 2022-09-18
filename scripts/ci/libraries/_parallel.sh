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


# Require SEMAPHORE_NAME


function parallel::initialize_monitoring() {
    PARALLEL_MONITORED_DIR="$(mktemp -d)"
    export PARALLEL_MONITORED_DIR

    PARALLEL_TAIL_LENGTH=${PARALLEL_TAIL_LENGTH:=2}
    export PARALLEL_TAIL_LENGTH
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
    rm -rf "${HOME}/.parallel"
}


# Periodical loop to print summary of all the processes run by parallel
function parallel::monitor_loop() {
    trap 'exit 0' TERM
    echo
    echo "Start monitoring of parallel execution in ${PARALLEL_MONITORED_DIR} directory."
    echo
    local progress_report_number=1
    local start_time
    local end_time
    # To continue supporting Bash v3 we can't use associative arrays - so use a
    # normal array and just check if the value is in it -- it will only ever be
    # a few items long so it won't be too expensive
    declare -a finished_jobs=()
    start_time=${SECONDS}
    while true
    do
        echo
        echo "${COLOR_YELLOW}########## Monitoring progress start: ${progress_report_number}  ##########${COLOR_RESET}"
        echo
        if [[ ${PR_LABELS} == *debug-ci-resources* || ${GITHUB_EVENT_NAME} == "push" ]]; then
            # Only print stats in `main` or when "debug-ci-resources" label is set on PR.
            echo "${COLOR_BLUE}########### STATISTICS #################"
            docker_engine_resources::print_overall_stats
            echo "########### STATISTICS #################${COLOR_RESET}"
        fi
        for directory in "${PARALLEL_MONITORED_DIR}"/*/*
        do
            parallel_process=$(basename "${directory}")
            if ( IFS=$'\x1F';  [[ "$IFS${finished_jobs[*]}$IFS" == *"$IFS${parallel_process}$IFS"* ]] ) ; then
              # Already finished, so don't print anything
              continue
            fi

            echo "${COLOR_BLUE}### The last ${PARALLEL_TAIL_LENGTH} lines for ${parallel_process} process: ${directory}/stdout ###${COLOR_RESET}"
            tail "-${PARALLEL_TAIL_LENGTH}" "${directory}/stdout" || true
            echo

            if [[ -s "${directory}/status" ]]; then
              finished_jobs+=("$parallel_process")
              # The last line of output (which we've already shown) will be a line about the success/failure
              # of this job
            fi

            echo

        done

        end_time=${SECONDS}
        echo "${COLOR_YELLOW}########## $((end_time - start_time)) seconds passed since start ##########${COLOR_RESET}"
        sleep 15
        progress_report_number=$((progress_report_number + 1))
    done
}

# Monitors progress of parallel execution and periodically summarizes stdout entries created by
# the parallel execution. Sets PAPARALLEL_MONITORED_DIR which should be be passed as --results
# parameter to GNU parallel execution.
function parallel::monitor_progress() {
    echo "Parallel results are stored in: ${PARALLEL_MONITORED_DIR}"
    parallel::monitor_loop 2>/dev/null &

    # shellcheck disable=SC2034
    PARALLEL_MONITORING_PID=$!
    # shellcheck disable=SC2016
    traps::add_trap 'parallel::kill_monitor' EXIT
}


function parallel::kill_monitor() {
    kill ${PARALLEL_MONITORING_PID} >/dev/null 2>&1 || true
}

# Outputs logs for successful test type
# $1 test type
function parallel::output_log_for_successful_job(){
    local job=$1
    local log_dir="${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}/${job}"
    start_end::group_start "${COLOR_GREEN}Output for successful ${job}${COLOR_RESET}"
    echo "${COLOR_GREEN}##### The ${job} succeeded ##### ${COLOR_RESET}"
    echo
    cat "${log_dir}"/stdout
    echo
    echo "${COLOR_GREEN}##### The ${job} succeeded ##### ${COLOR_RESET}"
    echo
    start_end::group_end
}

# Outputs logs for failed test type
# $1 test type
function parallel::output_log_for_failed_job(){
    local job=$1
    local log_dir="${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}/${job}"
    start_end::group_start "${COLOR_RED}Output: for failed ${job}${COLOR_RESET}"
    echo "${COLOR_RED}##### The ${job} failed ##### ${COLOR_RESET}"
    echo
    cat "${log_dir}"/stdout
    echo
    echo
    echo "${COLOR_RED}##### The ${job} failed ##### ${COLOR_RESET}"
    echo
    start_end::group_end
}

# Prints summary of jobs and returns status:
# 0 - all jobs succeeded (SKIPPED_FAILED_JOBS is not counted)
# >0 - number of failed jobs (except Quarantine)
function parallel::print_job_summary_and_return_status_code() {
    local return_code="0"
    local job
    local status_file
    for job_path in "${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}/"*
    do
        job="$(basename "${job_path}")"
        status_file="${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}/${job}/status"
        if [[ -s "${status_file}"  ]]; then
            status=$(cat "${status_file}")
        else
            echo "${COLOR_RED}Missing ${status_file} file"
            status="1"
        fi
        if [[ ${status} == "0" ]]; then
            parallel::output_log_for_successful_job "${job}"
        else
            parallel::output_log_for_failed_job "${job}"
            # SKIPPED_FAILED_JOB failure does not trigger whole test failure
            if [[ ${SKIPPED_FAILED_JOB=} != "${job}" ]]; then
                return_code=$((return_code + 1))
            fi
        fi
    done
    return "${return_code}"
}

function parallel::kill_all_running_docker_containers() {
    echo
    echo "${COLOR_BLUE}Kill all running docker containers${COLOR_RESET}"
    echo
    # shellcheck disable=SC2046
    docker kill $(docker ps -q) || true
}

function parallel::system_prune_docker() {
    echo
    echo "${COLOR_BLUE}System-prune docker${COLOR_RESET}"
    echo
    docker_v system prune --force --volumes
    echo
}

# Cleans up runner before test execution.
#  * Kills all running docker containers
#  * System prune to clean all the temporary/unnamed images and left-over volumes
#  * Print information about available space and memory
#  * Kills stale semaphore locks
function parallel::cleanup_runner() {
    start_end::group_start "Cleanup runner"
    parallel::kill_all_running_docker_containers
    parallel::system_prune_docker
    docker_engine_resources::check_all_resources
    docker_engine_resources::print_overall_stats
    parallel::kill_stale_semaphore_locks
    start_end::group_end
}

function parallel::make_sure_python_versions_are_specified() {
    if [[ -z "${CURRENT_PYTHON_MAJOR_MINOR_VERSIONS_AS_STRING=}" ]]; then
        echo
        echo "${COLOR_RED}The CURRENT_PYTHON_MAJOR_MINOR_VERSIONS_AS_STRING variable must be set and list python versions to use!${COLOR_RESET}"
        echo
        exit 1
    fi
    echo
    echo "${COLOR_BLUE}Running parallel builds for those Python versions: ${CURRENT_PYTHON_MAJOR_MINOR_VERSIONS_AS_STRING}${COLOR_RESET}"
    echo
}

function parallel::get_maximum_parallel_k8s_jobs() {
    docker_engine_resources::get_available_cpus_in_docker
    if [[ -n ${RUNS_ON=} && ${RUNS_ON} != *"self-hosted"* ]]; then
        echo
        echo "${COLOR_YELLOW}This is a GitHub Public runner - for now we are forcing max parallel K8S tests jobs to 1 for those${COLOR_RESET}"
        echo
        export MAX_PARALLEL_K8S_JOBS="1"
    else
        echo
        echo "${COLOR_YELLOW}This is a Self-Hosted runner - forcing max parallel jobs to 5${COLOR_RESET}"
        echo
        export MAX_PARALLEL_K8S_JOBS="3"
    fi
    export MAX_PARALLEL_K8S_JOBS
}
