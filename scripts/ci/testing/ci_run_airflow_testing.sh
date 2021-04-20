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

# Enable automated tests execution
RUN_TESTS="true"
export RUN_TESTS

SKIPPED_FAILED_JOB="Quarantined"
export SKIPPED_FAILED_JOB

SEMAPHORE_NAME="tests"
export SEMAPHORE_NAME

# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"



# Starts test types in parallel
# test_types_to_run - list of test types (it's not an array, it is space-separate list)
# ${@} - additional arguments to pass to test execution
function run_test_types_in_parallel() {
    start_end::group_start "Monitoring tests: ${test_types_to_run}"
    parallel::monitor_progress
    mkdir -p "${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}"
    for TEST_TYPE in ${test_types_to_run}
    do
        export TEST_TYPE
        mkdir -p "${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}/${TEST_TYPE}"
        mkdir -p "${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}/${TEST_TYPE}"
        export JOB_LOG="${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}/${TEST_TYPE}/stdout"
        export PARALLEL_JOB_STATUS="${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}/${TEST_TYPE}/status"
        # Each test job will get SIGTERM followed by SIGTERM 200ms later and SIGKILL 200ms later after 25 mins
        # shellcheck disable=SC2086
        parallel --ungroup --bg --semaphore --semaphorename "${SEMAPHORE_NAME}" \
            --jobs "${MAX_PARALLEL_TEST_JOBS}" --timeout 1500 \
            "$( dirname "${BASH_SOURCE[0]}" )/ci_run_single_airflow_test_in_docker.sh" "${@}" >${JOB_LOG} 2>&1
    done
    parallel --semaphore --semaphorename "${SEMAPHORE_NAME}" --wait
    parallel::kill_monitor
    start_end::group_end
}

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
    parallel::cleanup_runner

    start_end::group_start "Determine how to run the tests"
    echo
    echo "${COLOR_YELLOW}Running maximum ${MAX_PARALLEL_TEST_JOBS} test types in parallel${COLOR_RESET}"
    echo

    local run_integration_tests_separately="false"
    # shellcheck disable=SC2153
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
            # Remove Integration from list of tests to run in parallel
            test_types_to_run="${test_types_to_run//Integration/}"
            run_integration_tests_separately="true"
        fi
    fi
    set +e
    start_end::group_end

    parallel::initialize_monitoring

    run_test_types_in_parallel "${@}"
    if [[ ${run_integration_tests_separately} == "true" ]]; then
        parallel::cleanup_runner
        test_types_to_run="Integration"
        run_test_types_in_parallel "${@}"
    fi
    set -e
    # this will exit with error code in case some of the non-Quarantined tests failed
    parallel::print_job_summary_and_return_status_code
}


testing::skip_tests_if_requested

build_images::prepare_ci_build

build_images::rebuild_ci_image_if_needed_with_group

parallel::make_sure_gnu_parallel_is_installed

testing::get_maximum_parallel_test_jobs

testing::get_test_types_to_run

testing::get_docker_compose_local

run_all_test_types_in_parallel "${@}"
