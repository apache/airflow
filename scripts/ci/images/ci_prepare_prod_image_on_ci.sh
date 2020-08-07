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

# In case GITHUB_REGISTRY_PULL_IMAGE_TAG is different than latest, tries to pull the image indefinitely
# skips further image checks - assuming that this is the right image
function wait_for_prod_images_tag {
    PROD_IMAGE_TO_WAIT_FOR="${GITHUB_REGISTRY_AIRFLOW_PROD_IMAGE}:${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
    PROD_BUILD_IMAGE_TO_WAIT_FOR="${GITHUB_REGISTRY_AIRFLOW_PROD_BUILD_IMAGE}:${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
    echo
    echo "Waiting for image ${PROD_IMAGE_TO_WAIT_FOR}"
    echo
    while true; do
        docker pull "${PROD_IMAGE_TO_WAIT_FOR}" || true
        if [[ "$(docker images -q "${PROD_IMAGE_TO_WAIT_FOR}" 2> /dev/null)" == "" ]]; then
            echo
            echo "The image ${PROD_IMAGE_TO_WAIT_FOR} is not yet available. Waiting"
            echo
            sleep 10
        else
            echo
            echo "The image ${PROD_IMAGE_TO_WAIT_FOR} downloaded."
            echo "Tagging ${PROD_IMAGE_TO_WAIT_FOR} as ${GITHUB_REGISTRY_AIRFLOW_PROD_IMAGE}."
            docker tag  "${PROD_IMAGE_TO_WAIT_FOR}" "${GITHUB_REGISTRY_AIRFLOW_PROD_IMAGE}"
            echo "Tagging ${PROD_IMAGE_TO_WAIT_FOR} as ${AIRFLOW_PROD_IMAGE}."
            docker tag  "${PROD_IMAGE_TO_WAIT_FOR}" "${AIRFLOW_PROD_IMAGE}"
            echo
            break
        fi
    done
    echo
    echo "Waiting for image ${PROD_BUILD_IMAGE_TO_WAIT_FOR}"
    echo
    while true; do
        docker pull "${PROD_BUILD_IMAGE_TO_WAIT_FOR}" || true
        if [[ "$(docker images -q "${PROD_BUILD_IMAGE_TO_WAIT_FOR}" 2> /dev/null)" == "" ]]; then
            echo
            echo "The image ${PROD_BUILD_IMAGE_TO_WAIT_FOR} is not yet available. Waiting"
            echo
            sleep 10
        else
            echo
            echo "The image ${PROD_BUILD_IMAGE_TO_WAIT_FOR} downloaded."
            echo "Tagging ${PROD_BUILD_IMAGE_TO_WAIT_FOR} as ${GITHUB_REGISTRY_AIRFLOW_PROD_BUILD_IMAGE}."
            docker tag  "${PROD_BUILD_IMAGE_TO_WAIT_FOR}" "${GITHUB_REGISTRY_AIRFLOW_PROD_BUILD_IMAGE}"
            echo "Tagging ${PROD_BUILD_IMAGE_TO_WAIT_FOR} as ${AIRFLOW_PROD_BUILD_IMAGE}."
            docker tag  "${PROD_BUILD_IMAGE_TO_WAIT_FOR}" "${AIRFLOW_PROD_BUILD_IMAGE}"
            echo
            break
        fi
    done
}


# Builds the prod image in the CI environment.
# Depending on the type of build (push/pr/scheduled) it will either build it incrementally or
# from the scratch without cache (the latter for scheduled builds only)
function build_prod_images_on_ci() {
    get_environment_for_builds_on_ci
    prepare_prod_build

    rm -rf "${BUILD_CACHE_DIR}"
    mkdir -pv "${BUILD_CACHE_DIR}"

    if [[ ${GITHUB_REGISTRY_WAIT_FOR_IMAGE:="false"} == "true" ]]; then
        wait_for_prod_images_tag
    fi

    build_prod_images

    # Disable force pulling forced above this is needed for the subsequent scripts so that
    # They do not try to pull/build images again
    unset FORCE_PULL_IMAGES
    unset FORCE_BUILD
}


build_prod_images_on_ci
