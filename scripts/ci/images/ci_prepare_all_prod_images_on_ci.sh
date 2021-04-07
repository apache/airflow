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

# shellcheck source=scripts/ci/libraries/_all_libs.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_all_libs.sh"
initialization::set_output_color_variables
export SEMAPHORE_NAME="prepare_prod_images"

echo
echo "${COLOR_BLUE}Prepare all PROD images on CI${COLOR_RESET}"
echo

parallel::make_sure_gnu_parallel_is_installed
parallel::make_sure_python_versions_are_specified

parallel::initialize_monitoring
parallel::kill_stale_semaphore_locks

# Get as many parallel jobs as many python versions we have to work on
# Docker operations are not CPU bound
parallel::max_parallel_from_variable "${CURRENT_PYTHON_MAJOR_MINOR_VERSIONS_AS_STRING}"

start_end::group_start "Building PROD images in parallel"
parallel::monitor_progress

# shellcheck disable=SC2086
for python_version in ${CURRENT_PYTHON_MAJOR_MINOR_VERSIONS_AS_STRING}
do
    mkdir -p "${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}/${python_version}"
    export JOB_LOG="${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}/${python_version}/stdout"
    parallel --ungroup --bg --semaphore --semaphorename "${SEMAPHORE_NAME}" \
        --jobs "${MAX_PARALLEL_FROM_VARIABLE}" \
            "$(dirname "${BASH_SOURCE[0]}")/ci_prepare_prod_image_on_ci.sh" "${python_version}" >"${JOB_LOG}" 2>&1
done

parallel --semaphore --semaphorename "${SEMAPHORE_NAME}" --wait

start_end::group_end

parallel::print_job_summary_and_return_status_code
