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

function verify_prod_image_has_airflow_and_providers() {
    start_end::group_start "Verify prod image: ${AIRFLOW_PROD_IMAGE}"
    echo
    echo "Checking if Providers are installed"
    echo

    all_providers_installed_in_image=$(
        docker run --rm --entrypoint /bin/bash "${AIRFLOW_PROD_IMAGE}" \
            -c "airflow providers list --output table"
    )

    echo
    echo "Installed providers:"
    echo
    echo "${all_providers_installed_in_image}"
    echo
    local error="false"
    for provider in "${INSTALLED_PROVIDERS[@]}"; do
        echo -n "Verifying if provider ${provider} installed: "
        if [[ ${all_providers_installed_in_image} == *"| apache-airflow-providers-${provider//./-}"* ]]; then
            echo "${COLOR_GREEN}OK${COLOR_RESET}"
        else
            echo "${COLOR_RED}NOK${COLOR_RESET}"
            error="true"
        fi
    done
    if [[ ${error} == "true" ]]; then
        echo
        echo "${COLOR_RED_ERROR} Some expected providers are not installed!${COLOR_RESET}"
        echo
    fi
    start_end::group_end
}

function verify_prod_image_dependencies() {
    start_end::group_start "Checking if Airflow dependencies are non-conflicting in ${AIRFLOW_PROD_IMAGE} image."

    set +e
    docker run --rm --entrypoint /bin/bash "${AIRFLOW_PROD_IMAGE}" -c 'pip check'
    local res=$?
    if [[ ${res} != "0" ]]; then
        echo "${COLOR_RED_ERROR} ^^^ Some dependencies are conflicting. See instructions below on how to deal with it.  ${COLOR_RESET}"
        echo
        build_images::inform_about_pip_check "--production "
        # TODO(potiuk) - enable the comment once https://github.com/apache/airflow/pull/12188 is merged
        # exit ${res}
    else
        echo
        echo "${COLOR_GREEN_OK} The ${AIRFLOW_PROD_IMAGE} image dependencies are consistent.  ${COLOR_RESET}"
        echo
    fi
    set -e
    start_end::group_end
}

function pull_prod_image() {
    local image_name_with_tag="${GITHUB_REGISTRY_AIRFLOW_PROD_IMAGE}:${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
    start_end::group_start "Pulling the ${image_name_with_tag} image."

    push_pull_remove_images::pull_image_github_dockerhub "${AIRFLOW_PROD_IMAGE}" "${image_name_with_tag}"
    start_end::group_end
}

build_images::prepare_prod_build

pull_prod_image

verify_prod_image_has_airflow_and_providers

verify_prod_image_dependencies
