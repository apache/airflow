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

#
# Pulls and rebuilds the full CI image used for testing
#
set -euo pipefail

MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

AIRFLOW_SOURCES="$(cd "${MY_DIR}"/../../ && pwd )"
export AIRFLOW_SOURCES

# shellcheck source=scripts/ci/utils/_include_all.sh
. "${MY_DIR}/utils/_include_all.sh"

script_start

export FORCE_PULL_IMAGES="true"
export FORCE_DOCKER_BUILD="true"
export PULL_BASE_IMAGES="true"

rebuild_all_images_if_needed_and_confirmed

script_end
