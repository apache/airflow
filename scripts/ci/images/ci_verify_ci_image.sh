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
. "$(dirname "${BASH_SOURCE[0]}")/../libraries/_script_init.sh"

function verify_ci_image_dependencies() {
    start_end::group_start "Checking if Airflow dependencies are non-conflicting in ${AIRFLOW_CI_IMAGE} image."
    set +e
    docker run --rm --entrypoint /bin/bash "${AIRFLOW_CI_IMAGE}" -c 'pip check'
    local res=$?
    if [[ ${res} != "0" ]]; then
        echo  "${COLOR_RED}ERROR: ^^^ Some dependencies are conflicting. See instructions below on how to deal with it.  ${COLOR_RESET}"
        echo
        build_images::inform_about_pip_check ""
    else
        echo
        echo  "${COLOR_GREEN}OK. The ${AIRFLOW_PROD_IMAGE} image dependencies are consistent.  ${COLOR_RESET}"
        echo
    fi
    set -e
    start_end::group_end
    exit ${res}
}

function pull_ci_image() {
    local image_name_with_tag="${GITHUB_REGISTRY_AIRFLOW_CI_IMAGE}:${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
    start_end::group_start "Pulling ${image_name_with_tag} image"

    push_pull_remove_images::pull_image_github_dockerhub "${AIRFLOW_CI_IMAGE}" "${image_name_with_tag}"
    start_end::group_end

}


build_images::prepare_ci_build

pull_ci_image

verify_ci_image_dependencies
