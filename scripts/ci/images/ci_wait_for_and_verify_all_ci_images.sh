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

python3 "$( dirname "${BASH_SOURCE[0]}" )/ci_run_docker_tests.py" "--initialize"

initialization::set_output_color_variables

export PARALLEL_TAIL_LENGTH=5

parallel::make_sure_gnu_parallel_is_installed

parallel::make_sure_python_versions_are_specified

echo
echo "${COLOR_BLUE}Waiting for all CI images to appear${COLOR_RESET}"
echo

parallel::initialize_monitoring

parallel::monitor_progress

build_images::login_to_docker_registry

# shellcheck disable=SC2086
parallel --results "${PARALLEL_MONITORED_DIR}" \
    "$( dirname "${BASH_SOURCE[0]}" )/ci_wait_for_and_verify_ci_image.sh" ::: \
    ${CURRENT_PYTHON_MAJOR_MINOR_VERSIONS_AS_STRING}
