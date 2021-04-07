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
# shellcheck source=scripts/ci/libraries/_all_libs.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_all_libs.sh"
initialization::set_output_color_variables
export SEMAPHORE_NAME="image_tests"

export AIRFLOW_SOURCES="${AIRFLOW_SOURCES:=$( cd "$( dirname "${BASH_SOURCE[0]}" )/../../.." && pwd )}"
readonly AIRFLOW_SOURCES

DOCKER_EXAMPLES_DIR=${AIRFLOW_SOURCES}/docs/docker-stack/docker-examples/
export DOCKER_EXAMPLES_DIR

# Launches parallel building of images. Redirects output to log set the right directories
# $1 - name of the job
# $2 - bash file to execute in parallel
function run_image_test_job() {
    local file=$1

    local job_name=$2
    mkdir -p "${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}/${job_name}"
    export JOB_LOG="${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}/${job_name}/stdout"
    parallel --ungroup --bg --semaphore --semaphorename "${SEMAPHORE_NAME}" \
        --jobs "${MAX_PARALLEL_FROM_VARIABLE}" \
            "$(dirname "${BASH_SOURCE[0]}")/ci_run_prod_image_test.sh" "${job_name}" "${file}" >"${JOB_LOG}" 2>&1
}


function test_images() {
    if [[ ${CI=} == "true" ]]; then
        echo
        echo "Skipping the script builds on CI! "
        echo "They take very long time to build."
        echo
    else
        local scripts_to_test
        scripts_to_test=$(find "${DOCKER_EXAMPLES_DIR}" -type f -name '*.sh' )
        for file in ${scripts_to_test}
        do
            local job_name
            job_name=$(basename "${file}")
            run_image_test_job "${file}" "${job_name}"
        done
    fi
    local dockerfiles_to_test
    dockerfiles_to_test=$(find "${DOCKER_EXAMPLES_DIR}" -type f -name 'Dockerfile' )
    for file in ${dockerfiles_to_test}
    do
        local job_name
        job_name="$(basename "$(dirname "${file}")")"
        run_image_test_job "${file}" "${job_name}"
    done

}

cd "${AIRFLOW_SOURCES}" || exit 1

# Get as many parallel jobs as many python versions we have to work on
# Docker operations are not CPU bound
parallel::max_parallel_from_variable "${CURRENT_PYTHON_MAJOR_MINOR_VERSIONS_AS_STRING}"

parallel::make_sure_gnu_parallel_is_installed
parallel::kill_stale_semaphore_locks
parallel::initialize_monitoring

start_end::group_start "Testing image building"

parallel::monitor_progress
test_images

parallel --semaphore --semaphorename "${SEMAPHORE_NAME}" --wait
start_end::group_end

parallel::print_job_summary_and_return_status_code
