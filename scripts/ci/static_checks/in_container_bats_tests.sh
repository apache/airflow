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

function run_in_container_bats_tests() {
    if [[ "${#@}" == "0" ]]; then
        docker_v run "${EXTRA_DOCKER_FLAGS[@]}" \
        --entrypoint "/opt/bats/bin/bats"  \
        "-v" "$(pwd):/airflow" \
        "${AIRFLOW_CI_IMAGE_WITH_TAG}" \
        --tap  "tests/bats/in_container/"
    else
        docker_v run "${EXTRA_DOCKER_FLAGS[@]}" \
        --entrypoint "/opt/bats/bin/bats"  \
        "-v" "$(pwd):/airflow" \
        "${AIRFLOW_CI_IMAGE_WITH_TAG}" \
        --tap "${@}"
    fi
}

build_images::prepare_ci_build

build_images::rebuild_ci_image_if_needed

run_in_container_bats_tests "$@"
