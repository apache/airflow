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

set -eo pipefail

if [[ $(uname -s | tr '[:upper:]' '[:lower:]') != "darwin" ]]; then
    # do not fail with undefined variable on MacOS. The old Bash which is default on Mac OS
    # fails with undefined variable when you are passing an empty variable and this causes
    # problems for example when you try to pass empty list of arguments "${@}"
    set -u
fi

export AIRFLOW_SOURCES="${AIRFLOW_SOURCES:=$( cd "$( dirname "${BASH_SOURCE[0]}" )/../../.." && pwd )}"
readonly AIRFLOW_SOURCES

# shellcheck source=scripts/ci/libraries/_all_libs.sh
. "${AIRFLOW_SOURCES}/scripts/ci/libraries/_all_libs.sh"

initialization::create_directories

initialization::initialize_common_environment

initialization::check_docker_version

sanity_checks::basic_sanity_checks

start_end::script_start

initialization::get_docker_cache_image_names

initialization::get_environment_for_builds_on_ci

initialization::make_constants_read_only

# Work around occasional unexplained failure on CI. Clear file flags on
# STDOUT (which is connected to a tmp file by GitHub Runner).
# The one error I did see: BlockingIOError: [Errno 11] write could not complete without blocking
[[ "$CI" == "true" ]] && python3 -c "import fcntl; fcntl.fcntl(1, fcntl.F_SETFL, 0)"

traps::add_trap start_end::script_end EXIT HUP INT TERM
