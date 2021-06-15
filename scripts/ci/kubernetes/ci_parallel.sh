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
set -euo pipefail

# We cannot perform full initialization because it will be done later in the "single run" scripts
# And some readonly variables are set there, therefore we only selectively reuse parallel lib needed
LIBRARIES_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../libraries/" && pwd)
# shellcheck source=scripts/ci/libraries/_all_libs.sh
source "${LIBRARIES_DIR}/_all_libs.sh"


function get_maximum_parallel_k8s_jobs() {
    docker_engine_resources::get_available_cpus_in_docker
    if [[ -n ${RUNS_ON=} && ${RUNS_ON} != *"self-hosted"* ]]; then
        echo
        echo "${COLOR_YELLOW}This is a Github Public runner - for now we are forcing max parallel K8S tests jobs to 1 for those${COLOR_RESET}"
        echo
        export MAX_PARALLEL_K8S_JOBS="1"
    else
        if [[ ${MAX_PARALLEL_K8S_JOBS=} != "" ]]; then
            echo
            echo "${COLOR_YELLOW}Maximum parallel k8s jobs forced vi MAX_PARALLEL_K8S_JOBS = ${MAX_PARALLEL_K8S_JOBS}${COLOR_RESET}"
            echo
        else
            MAX_PARALLEL_K8S_JOBS=${CPUS_AVAILABLE_FOR_DOCKER}
            echo
            echo "${COLOR_YELLOW}Maximum parallel k8s jobs set to number of CPUs available for Docker = ${MAX_PARALLEL_K8S_JOBS}${COLOR_RESET}"
            echo
        fi
    fi
    export MAX_PARALLEL_K8S_JOBS
}

# Launches parallel building of images. Redirects output to log set the right directories
function run_kubernetes_test() {
    local kubernetes_version=$1
    local python_version=$2
    local single_job_filename=$3
    local job="Cluster-${kubernetes_version}-python-${python_version}"

    mkdir -p "${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}/${job}"
    export JOB_LOG="${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}/${job}/stdout"
    export PARALLEL_JOB_STATUS="${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}/${job}/status"
    echo "Starting helm tests for kubernetes version ${kubernetes_version}, python version: ${python_version}"
    parallel --ungroup --bg --semaphore --semaphorename "${SEMAPHORE_NAME}" \
        --jobs "${MAX_PARALLEL_K8S_JOBS}" \
            "$(dirname "${BASH_SOURCE[0]}")/${single_job_filename}" \
                "${kubernetes_version}" "${python_version}"
}

function run_k8s_tests_in_parallel() {
    parallel::cleanup_runner
    start_end::group_start "Monitoring helm tests"
    parallel::initialize_monitoring
    parallel::monitor_progress
    local single_job_filename=$1
    # In case there are more kubernetes versions than strings, we can reuse python versions so we add it twice here
    local repeated_python_versions
    # shellcheck disable=SC2206
    repeated_python_versions=(${CURRENT_PYTHON_MAJOR_MINOR_VERSIONS_AS_STRING} ${CURRENT_PYTHON_MAJOR_MINOR_VERSIONS_AS_STRING})
    local index=0
    for kubernetes_version in ${CURRENT_KUBERNETES_VERSIONS_AS_STRING}
    do
        index=$((index + 1))
        python_version=${repeated_python_versions[${index}]}
        FORWARDED_PORT_NUMBER=$((38080 + index))
        export FORWARDED_PORT_NUMBER
        API_SERVER_PORT=$((19090 + index))
        export API_SERVER_PORT
        run_kubernetes_test "${kubernetes_version}" "${python_version}" "${single_job_filename}" "${@}"
    done
    set +e
    parallel --semaphore --semaphorename "${SEMAPHORE_NAME}" --wait
    parallel::kill_monitor
    set -e
    start_end::group_end
}


get_maximum_parallel_k8s_jobs

run_k8s_tests_in_parallel "ci_setup_cluster_and_run_kubernetes_tests_single_job.sh" "${@}"
