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

# Builds or waits for the PROD image in the CI environment
# Depending on the "USE_GITHUB_REGISTRY" and "GITHUB_REGISTRY_WAIT_FOR_IMAGE" setting
function build_prod_images_on_ci() {
    build_images::prepare_prod_build

    rm -rf "${BUILD_CACHE_DIR}"
    mkdir -pv "${BUILD_CACHE_DIR}"

    if [[ ${USE_GITHUB_REGISTRY} == "true" && ${GITHUB_REGISTRY_WAIT_FOR_IMAGE} == "true" ]]; then

        # Tries to wait for the image indefinitely
        # skips further image checks - since we already have the target image

        build_images::wait_for_image_tag "${GITHUB_REGISTRY_AIRFLOW_PROD_IMAGE}" \
            ":${GITHUB_REGISTRY_PULL_IMAGE_TAG}" "${AIRFLOW_PROD_IMAGE}"

        build_images::wait_for_image_tag "${GITHUB_REGISTRY_AIRFLOW_PROD_BUILD_IMAGE}" \
            ":${GITHUB_REGISTRY_PULL_IMAGE_TAG}" "${AIRFLOW_PROD_BUILD_IMAGE}"
    else
        build_images::build_prod_images
    fi


    # Disable force pulling forced above this is needed for the subsequent scripts so that
    # They do not try to pull/build images again
    unset FORCE_PULL_IMAGES
    unset FORCE_BUILD
}



function verify_prod_image_has_airflow_and_providers {
    echo
    echo "Airflow folders installed in the image:"
    echo

    AIRFLOW_INSTALLATION_LOCATION="/home/airflow/.local"

    docker run --rm --entrypoint /bin/bash "${AIRFLOW_PROD_IMAGE}" -c \
        'find '"${AIRFLOW_INSTALLATION_LOCATION}"'/lib/python*/site-packages/airflow/ -type d'

    EXPECTED_MIN_AIRFLOW_DIRS_COUNT="60"
    readonly EXPECTED_MIN_AIRFLOW_DIRS_COUNT

    COUNT_AIRFLOW_DIRS=$(docker run --rm --entrypoint /bin/bash "${AIRFLOW_PROD_IMAGE}" -c \
         'find '"${AIRFLOW_INSTALLATION_LOCATION}"'/lib/python*/site-packages/airflow/ -type d | grep -c -v "/airflow/providers"')

    echo
    echo "Number of airflow dirs: ${COUNT_AIRFLOW_DIRS}"
    echo

    if [[ "${COUNT_AIRFLOW_DIRS}" -lt "${EXPECTED_MIN_AIRFLOW_DIRS_COUNT}" ]]; then
        >&2 echo
        >&2 echo Number of airflow folders installed is less than ${EXPECTED_MIN_AIRFLOW_DIRS_COUNT}
        >&2 echo This is unexpected. Please investigate, looking at the output above!
        >&2 echo
        exit 1
    else
        echo
        echo "OK. Airflow seems to be installed!"
        echo
    fi

    EXPECTED_MIN_PROVIDERS_DIRS_COUNT="240"
    readonly EXPECTED_MIN_PROVIDERS_DIRS_COUNT

    COUNT_AIRFLOW_PROVIDERS_DIRS=$(docker run --rm --entrypoint /bin/bash "${AIRFLOW_PROD_IMAGE}" -c \
         'find '"${AIRFLOW_INSTALLATION_LOCATION}"'/lib/python*/site-packages/airflow/providers -type d | grep -c "" | xargs')

    echo
    echo "Number of providers dirs: ${COUNT_AIRFLOW_PROVIDERS_DIRS}"
    echo

    if [ "${COUNT_AIRFLOW_PROVIDERS_DIRS}" -lt "${EXPECTED_MIN_PROVIDERS_DIRS_COUNT}" ]; then
        >&2 echo
        >&2 echo Number of providers folders installed is less than ${EXPECTED_MIN_PROVIDERS_DIRS_COUNT}
        >&2 echo This is unexpected. Please investigate, looking at the output above!
        >&2 echo
        exit 1
    else
        echo
        echo "OK. Airflow Providers seems to be installed!"
        echo
    fi
}

build_prod_images_on_ci
verify_prod_image_has_airflow_and_providers
