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

initialization::set_output_color_variables

parallel::make_sure_gnu_parallel_is_installed

if [[ -z "${CURRENT_PYTHON_MAJOR_MINOR_VERSIONS_AS_STRING=}" ]]; then
    echo
    echo "${COLOR_RED}Missing CURRENT_PYTHON_MAJOR_MINOR_VERSIONS_AS_STRING variable containing list of python versions${COLOR_RESET}"
    echo
    exit 1
fi

echo
echo "Waiting for all CI images to appear: ${CURRENT_PYTHON_MAJOR_MINOR_VERSIONS_AS_STRING}"
echo

docker_engine_resources::get_available_cpus_in_docker
MAX_PARALLEL_BUILD_JOBS=$((CPUS_AVAILABLE_FOR_DOCKER + 2))
export MAX_PARALLEL_BUILD_JOBS

parallel::initialize_monitoring

parallel::monitor_progress

# shellcheck disable=SC2086
parallel --results "${PARALLEL_MONITORED_DIR}" --joblog "${PARALLEL_JOB_LOG}" --line-buffer\
    --jobs "${MAX_PARALLEL_BUILD_JOBS}" \
    "$( dirname "${BASH_SOURCE[0]}" )/ci_wait_for_and_verify_ci_image.sh" ::: \
    ${CURRENT_PYTHON_MAJOR_MINOR_VERSIONS_AS_STRING}

parallel --wait

parallel::print_job_summary_and_return_status_code
