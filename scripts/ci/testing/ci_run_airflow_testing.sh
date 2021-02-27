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

# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"

TESTING_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
readonly TESTING_DIR

if [[ -f ${BUILD_CACHE_DIR}/.skip_tests ]]; then
    echo
    echo "Skipping running tests !!!!!"
    echo
    exit
fi

# In case we see too many failures on regular PRs from our users using GitHub Public runners
# We can uncomment this and come back to sequential test-type execution
#if [[ ${RUNS_ON} != *"self-hosted"* ]]; then
#    echo
#    echo "${COLOR_YELLOW}This is a Github Public runner - for now we are forcing max parallel jobs to 1 for those${COLOR_RESET}"
#    echo "${COLOR_YELLOW}Until we fix memory usage to allow up to 2 parallel runs on those runners${COLOR_RESET}"
#    echo
#    # Forces testing in parallel in case the script is run on self-hosted runners
#    export MAX_PARALLEL_TEST_JOBS="1"
#fi

TEST_SLEEP_TIME="10"
TEST_SHOW_OUTPUT_LINES="2"
SEMAPHORE_NAME="tests"

function prepare_tests_to_run() {
    DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/files.yml")
    if [[ ${MOUNT_SELECTED_LOCAL_SOURCES} == "true" ]]; then
        DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/local.yml")
    fi
    if [[ ${MOUNT_ALL_LOCAL_SOURCES} == "true" ]]; then
        DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/local-all-sources.yml")
    fi

    if [[ ${GITHUB_ACTIONS} == "true" ]]; then
        DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/ga.yml")
    fi

    if [[ ${FORWARD_CREDENTIALS} == "true" ]]; then
        DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/forward-credentials.yml")
    fi

    if [[ -n ${INSTALL_AIRFLOW_VERSION=} || -n ${INSTALL_AIRFLOW_REFERENCE} ]]; then
        DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/remove-sources.yml")
    fi
    readonly DOCKER_COMPOSE_LOCAL

    if [[ -n "${FORCE_TEST_TYPE=}" ]]; then
        # Handle case where test type is forced from outside
        export TEST_TYPES="${FORCE_TEST_TYPE}"
    fi

    if [[ -z "${TEST_TYPES=}" ]]; then
        TEST_TYPES="Core Providers API CLI Integration Other WWW"
        echo
        echo "Test types not specified. Running all: ${TEST_TYPES}"
        echo
    fi

    if [[ -z "${FORCE_TEST_TYPE=}" ]]; then
        # Add Postgres/MySQL special test types in case we are running several test types
        if [[ ${BACKEND} == "postgres" ]]; then
            TEST_TYPES="${TEST_TYPES} Postgres"
        fi
        if [[ ${BACKEND} == "mysql" ]]; then
            TEST_TYPES="${TEST_TYPES} MySQL"
        fi
    fi
    readonly TEST_TYPES
}

# Runs test for a single test type. Based on the test type it will run additional integrations if needed
# You need to set variable TEST_TYPE - type of test to run
# ${@} - additional arguments to pass
function run_single_test_type() {
    echo
    echo "Waiting for semaphore for max ${MAX_PARALLEL_TEST_JOBS} jobs to run ${TEST_TYPE}"
    echo
    export TEST_TYPE
    exec parallel --ungroup --fg --semaphore --semaphorename "${SEMAPHORE_NAME}" --jobs "${MAX_PARALLEL_TEST_JOBS}" \
        "${TESTING_DIR}/ci_run_single_airflow_test_in_docker.sh" "${@}"
}

function kill_all_running_docker_containers() {
    echo
    echo "${COLOR_BLUE}Kill all running docker containers${COLOR_RESET}"
    echo
    # shellcheck disable=SC2046
    docker kill $(docker ps -q) || true
}

function system_prune_docker() {
    echo
    echo "${COLOR_BLUE}System-prune docker${COLOR_RESET}"
    echo
    docker system prune --force --volumes
    echo
}

function print_available_space_in_host() {
    echo "${COLOR_BLUE}Print available space${COLOR_RESET}"
    echo
    df --human
}

function print_available_memory_in_host() {
    echo
    echo "${COLOR_BLUE}Print available memory${COLOR_RESET}"
    echo
    free --human
}

function get_available_memory_in_docker() {
    MEMORY_AVAILABLE_FOR_DOCKER=$(docker run --entrypoint /bin/bash \
        "${AIRFLOW_CI_IMAGE}" -c \
        'echo $(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE) / (1024 * 1024)))')
    echo
    echo "${COLOR_BLUE}Memory available for Docker${COLOR_RESET}"
    echo
    echo "    ${MEMORY_AVAILABLE_FOR_DOCKER}"
    echo
    export MEMORY_AVAILABLE_FOR_DOCKER
}


function get_available_cpus_in_docker() {
    CPUS_AVAILABLE_FOR_DOCKER=$(docker run --entrypoint /bin/bash \
        "${AIRFLOW_CI_IMAGE}" -c \
        'grep -cE "cpu[0-9]+" </proc/stat')
    echo
    echo "${COLOR_BLUE}CPUS available for Docker${COLOR_RESET}"
    echo
    echo "    ${CPUS_AVAILABLE_FOR_DOCKER}"
    echo
    export CPUS_AVAILABLE_FOR_DOCKER
}

function get_maximum_parallel_test_jobs() {
    if [[ ${MAX_PARALLEL_TEST_JOBS=} != "" ]]; then
        echo
        echo "${COLOR_YELLOW}Maximum parallel test jobs forced vi MAX_PARALLEL_TEST_JOBS = ${MAX_PARALLEL_TEST_JOBS}${COLOR_RESET}"
        echo
    else
        MAX_PARALLEL_TEST_JOBS=${CPUS_AVAILABLE_FOR_DOCKER}
        echo
        echo "${COLOR_YELLOW}Maximum parallel test jobs set to number of CPUs available for Docker = ${MAX_PARALLEL_TEST_JOBS}${COLOR_RESET}"
        echo
    fi
    export MAX_PARALLEL_TEST_JOBS
}

function kill_stale_semaphore_locks() {
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


# Cleans up runner before test execution.
#  * Kills all running docker containers
#  * System prune to clean all the temporary/unnamed images and left-over volumes
#  * Print information about available space and memory
#  * Kills stale semaphore locks
function cleanup_runner() {
    start_end::group_start "Cleanup runner"
    kill_all_running_docker_containers
    system_prune_docker
    print_available_space_in_host
    print_available_memory_in_host
    get_available_memory_in_docker
    get_available_cpus_in_docker
    get_maximum_parallel_test_jobs
    kill_stale_semaphore_locks
    start_end::group_end
}


# Prints status of a parallel test type. Returns 0 if the test type is still running, 1 otherwise
#
# $1 - Test type
#
# The test_pids variable should contain mapping of test types to pids
# NOTE: It might be surprise that local variables (like test_pids) are visible to called functions
#       But this is how local variables in bash work (by design).
function print_test_type_status() {
    local test_type=$1
    local pid="${test_pids[${test_type}]}"
    local log_file="${CACHE_TMP_FILE_DIR}/output-${test_type}.log"
    if ps -p "${pid}" >/dev/null 2>/dev/null; then
        echo "${COLOR_BLUE}Test type ${test_type} in progress (last ${TEST_SHOW_OUTPUT_LINES} lines of the output shown).${COLOR_RESET}"
        echo
        tail -${TEST_SHOW_OUTPUT_LINES} "${log_file}"
        echo
        echo
        return 0
     else
        if wait "${pid}"; then
            echo
            echo "${COLOR_GREEN}Test type: ${test_type} succeeded.${COLOR_RESET}"
            echo
            return 1
        else
            echo
            echo "${COLOR_RED}Test type: ${test_type} failed.${COLOR_RESET}"
            echo
            return 1
        fi
    fi
}

# Prints status of all parallel test types. Returns 0 if any of the tests are still running, 1 otherwise
# test_types_to_run - test types that are checked
function print_all_test_types_status() {
    local still_running="false"
    local test_type
    echo "${COLOR_YELLOW}Progress for test types: ${COLOR_BLUE}${test_types_to_run}${COLOR_RESET}"
    echo
    for test_type in ${test_types_to_run}
    do
        if print_test_type_status "${test_type}" ; then
            still_running="true"
        fi
    done
    if [[ ${still_running} == "true" ]]; then
        return 0
    fi
    return 1
}

# Starts test types in parallel
# test_types_to_run - list of test types (it's not an array, it is space-separate list)
# ${@} - additional arguments to pass to test execution
function start_test_types_in_parallel() {
    local test_type
    start_end::group_start "Starting parallel tests: ${test_types_to_run}"

    for test_type in ${test_types_to_run}
    do
        log_file="${CACHE_TMP_FILE_DIR}/output-${test_type}.log"
        TEST_TYPE=${test_type} run_single_test_type "${@}" >"${log_file}" 2>&1 &
        pid=$!
        echo "The process started for ${test_type}. Log file ${log_file}"
        test_pids[${test_type}]=${pid}
    done
    start_end::group_end
}

# Monitors progress of test types running
# test_types_to_run - test types to monitor
# test_pids - map of test types to PIDs
function monitor_test_types() {
    start_end::group_start "Monitoring progress of running parallel tests: ${test_types_to_run}"
    while true;
    do
        sleep "${TEST_SLEEP_TIME}"
        if ! print_all_test_types_status; then
            break
        fi
    done
    start_end::group_end
}


# Checks status for all test types running.
# Returns 0 if any of the tests failed (with the exception of Quarantined test).
#
# * TEST_TYPES - all test types that were expected to run
# * test_pids - map of test types to PIDs
#
# output:
#   successful_tests - array of test types that succeeded
#   failed_tests - array of test types that failed
function check_test_types_status() {
    start_end::group_start "Checking status for all test types that were run: ${TEST_TYPES}"
    local test_type_exit_code="0"
    for test_type in ${TEST_TYPES}; do
        local pid="${test_pids[${test_type}]}"
        wait "${pid}"
        local temp_exit_code=$?
        if [[ ${temp_exit_code} == "0" ]]; then
            successful_tests+=("${test_type}")
        else
            failed_tests+=("${test_type}")
            if [[ ${test_type} != "Quarantined" ]]; then
                test_type_exit_code=${temp_exit_code}``
            fi
        fi
    done
    start_end::group_end
    return "${test_type_exit_code}"
}

# Outputs logs for successful test type
# $1 test type
function output_log_for_successful_test_type(){
    local test_test_type=$1
    local log_file="${CACHE_TMP_FILE_DIR}/output-${test_test_type}.log"
    start_end::group_start "${COLOR_GREEN}Output for successful ${test_test_type}${COLOR_RESET}"
    echo "${COLOR_GREEN}##### Test type ${test_test_type} succeeded ##### ${COLOR_RESET}"
    echo
    cat "${log_file}"
    echo
    echo "${COLOR_GREEN}##### Test type ${test_test_type} succeeded ##### ${COLOR_RESET}"
    echo
    start_end::group_end
}

# Outputs logs for failed test type
# $1 test type
function output_log_for_failed_test_type(){
    local test_test_type=$1
    local log_file="${CACHE_TMP_FILE_DIR}/output-${test_test_type}.log"
    start_end::group_start "${COLOR_RED}Output for failed ${test_test_type}${COLOR_RESET}"
    echo "${COLOR_RED}##### Test type ${test_test_type} failed ##### ${COLOR_RESET}"
    echo
    cat "${log_file}"
    echo
    echo "${COLOR_RED}##### Test type ${test_test_type} failed ##### ${COLOR_RESET}"
    echo
    start_end::group_end
}

function print_test_summary() {
    if [[ ${#successful_tests[@]} != 0 ]]; then
        echo
        echo "${COLOR_GREEN}Successful test types: ${successful_tests[*]} ${COLOR_RESET}"
        echo
        for test_type in "${successful_tests[@]}"
        do
            output_log_for_successful_test_type "${test_type}"
        done
    fi
    if [[ ${#failed_tests[@]} != 0 ]]; then
        echo
        echo "${COLOR_RED}Failed test types: ${failed_tests[*]}${COLOR_RESET}"
        echo
        for test_type in "${failed_tests[@]}"
        do
            output_log_for_failed_test_type "${test_type}"
        done
    fi
}

export MEMORY_REQUIRED_FOR_INTEGRATION_TEST_PARALLEL_RUN=33000

# Runs all test types in parallel depending on the number of CPUs available
# We monitors their progress, display the progress  and summarize the result when finished.
#
# In case there is not enough memory (MEMORY_REQUIRED_FOR_INTEGRATION_TEST_PARALLEL_RUN) available for
# the docker engine, the integration tests (which take a lot of memory for all the integrations)
# are run sequentially after all other tests were run in parallel.
#
# Input:
#   * TEST_TYPES  - contains all test types that should be executed
#   * MEMORY_REQUIRED_FOR_INTEGRATION_TEST_PARALLEL_RUN - memory in bytes required to run integration tests
#             in parallel to other tests
#   * MEMORY_AVAILABLE_FOR_DOCKER - memory that is available in docker (set by cleanup_runners)
#
function run_all_test_types_in_parallel() {
    local successful_tests=()
    local failed_tests=()
    local exit_code="0"
    local log_file
    local test_type
    local test_pids
    declare -A test_pids

    cleanup_runner

    echo
    echo "${COLOR_YELLOW}Running maximum ${MAX_PARALLEL_TEST_JOBS} test types in parallel"
    echo

    local run_integration_tests_separately="false"
    local test_types_to_run=${TEST_TYPES}

    if [[ ${test_types_to_run} == *"Integration"* ]]; then
        if (( MEMORY_AVAILABLE_FOR_DOCKER < MEMORY_REQUIRED_FOR_INTEGRATION_TEST_PARALLEL_RUN )) ; then
            # In case of Integration tests - they need more resources (Memory) thus we only run them in
            # parallel if we have more than 32 GB memory available. Otherwise we run them sequentially
            # after cleaning up the memory and stopping all docker instances
            echo ""
            echo "${COLOR_YELLOW}There is not enough memory to run Integration test in parallel${COLOR_RESET}"
            echo "${COLOR_YELLOW}   Available memory: ${MEMORY_AVAILABLE_FOR_DOCKER}${COLOR_RESET}"
            echo "${COLOR_YELLOW}   Required memory: ${MEMORY_REQUIRED_FOR_INTEGRATION_TEST_PARALLEL_RUN}${COLOR_RESET}"
            echo ""
            echo "${COLOR_YELLOW}Integration tests will be run separately at the end after cleaning up docker${COLOR_RESET}"
            echo ""
            test_types_to_run="${TEST_TYPES//Integration/}"
            run_integration_tests_separately="true"
        fi
    fi
    start_test_types_in_parallel "${@}"
    set +e
    monitor_test_types

    if [[ ${run_integration_tests_separately} == "true" ]]; then
        cleanup_runner
        test_types_to_run="Integration"
        start_test_types_in_parallel "${@}"
        monitor_test_types
    fi

    check_test_types_status
    exit_code=$?
    set -e
    print_test_summary
    return ${exit_code}
}

build_images::prepare_ci_build

build_images::rebuild_ci_image_if_needed_with_group

prepare_tests_to_run

echo
echo "Checking if you have parallel installed"
echo
echo "You might need to provide root password if you do not have it installed"
echo

(command -v parallel || apt install parallel || sudo apt install parallel || brew install parallel) >/dev/null

RUN_TESTS="true"
export RUN_TESTS

run_all_test_types_in_parallel "${@}"
