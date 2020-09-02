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

export AIRFLOW_SOURCES="${AIRFLOW_SOURCES:=$( cd "$( dirname "${BASH_SOURCE[0]}" )/../../.." && pwd )}"
readonly AIRFLOW_SOURCES

# shellcheck source=scripts/ci/libraries/_all_libs.sh
. "${AIRFLOW_SOURCES}/scripts/ci/libraries/_all_libs.sh"

initialize_common_environment

basic_sanity_checks

script_start

determine_docker_cache_strategy

get_environment_for_builds_on_ci

get_docker_image_names

make_constants_read_only

add_trap script_end EXIT HUP INT TERM
