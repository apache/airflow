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

# Pulls image in case it is needed (either has never been pulled or pulling was forced
# Should be run with set +e
# Parameters:
#   $1 -> image to pull
function pull_image_if_needed() {
    local IMAGE_TO_PULL="${1}"
    local IMAGE_HASH
    IMAGE_HASH=$(docker images -q "${IMAGE_TO_PULL}" 2> /dev/null || true)
    local PULL_IMAGE=${FORCE_PULL_IMAGES}

    if [[ "${IMAGE_HASH}" == "" ]]; then
        PULL_IMAGE="true"
    fi
    if [[ "${PULL_IMAGE}" == "true" ]]; then
        echo
        echo "Pulling the image ${IMAGE_TO_PULL}"
        echo
        verbose_docker pull "${IMAGE_TO_PULL}" | tee -a "${OUTPUT_LOG}"
        EXIT_VALUE="$?"
        echo
        return ${EXIT_VALUE}
    fi
}

# Pulls image if needed but tries to pull it from cache (for example GitHub registry) before
# It attempts to pull it from the main repository. This is used to speed up the builds
# In GitHub Actions.
# Parameters:
#   $1 -> image to pull
#   $2 -> cache image to pull first
function pull_image_possibly_from_cache() {
    local IMAGE="${1}"
    local CACHED_IMAGE="${2}"
    local IMAGE_PULL_RETURN_VALUE=-1

    set +e
    if [[ ${CACHED_IMAGE:=} != "" ]]; then
        pull_image_if_needed "${CACHED_IMAGE}"
        IMAGE_PULL_RETURN_VALUE="$?"
        if [[ ${IMAGE_PULL_RETURN_VALUE} == "0" ]]; then
            # Tag the image to be the target one
            verbose_docker tag "${CACHED_IMAGE}" "${IMAGE}"
        fi
    fi
    if [[ ${IMAGE_PULL_RETURN_VALUE} != "0" ]]; then
        pull_image_if_needed "${IMAGE}"
    fi
    set -e
}

# Pulls CI image in case caching strategy is "pulled" and the image needs to be pulled
function pull_ci_image_if_needed() {
    # Whether to force pull images to populate cache
    export FORCE_PULL_IMAGES=${FORCE_PULL_IMAGES:="false"}

    if [[ "${DOCKER_CACHE}" == "pulled" ]]; then
        if [[ "${FORCE_PULL_IMAGES}" == "true" ]]; then
            echo
            echo "Force pull base image ${PYTHON_BASE_IMAGE}"
            echo
            if [[ -n ${DETECTED_TERMINAL:=""} ]]; then
                echo -n "
Docker pulling ${PYTHON_BASE_IMAGE}.
                    " > "${DETECTED_TERMINAL}"
            fi
            if [[ ${PULL_PYTHON_BASE_IMAGES_FROM_CACHE:="true"} == "true" ]]; then
                pull_image_possibly_from_cache "${PYTHON_BASE_IMAGE}" "${CACHED_PYTHON_BASE_IMAGE}"
            else
                verbose_docker pull "${PYTHON_BASE_IMAGE}" | tee -a "${OUTPUT_LOG}"
            fi
            echo
        fi
        pull_image_possibly_from_cache "${AIRFLOW_CI_IMAGE}" "${CACHED_AIRFLOW_CI_IMAGE}"
    fi
}


# Pulls PROD image in case caching strategy is "pulled" and the image needs to be pulled
function pull_prod_images_if_needed() {
    # Whether to force pull images to populate cache
    export FORCE_PULL_IMAGES=${FORCE_PULL_IMAGES:="false"}

    if [[ "${DOCKER_CACHE}" == "pulled" ]]; then
        if [[ "${FORCE_PULL_IMAGES}" == "true" ]]; then
            echo
            echo "Force pull base image ${PYTHON_BASE_IMAGE}"
            echo
            if [[ ${PULL_PYTHON_BASE_IMAGES_FROM_CACHE:="true"} == "true" ]]; then
                pull_image_possibly_from_cache "${PYTHON_BASE_IMAGE}" "${CACHED_PYTHON_BASE_IMAGE}"
            else
                verbose_docker pull "${PYTHON_BASE_IMAGE}" | tee -a "${OUTPUT_LOG}"
            fi
            echo
        fi
        # "Build" segment of production image
        pull_image_possibly_from_cache "${AIRFLOW_PROD_BUILD_IMAGE}" "${CACHED_AIRFLOW_PROD_BUILD_IMAGE}"
        # Main segment of production image
        pull_image_possibly_from_cache "${AIRFLOW_PROD_IMAGE}" "${CACHED_AIRFLOW_PROD_IMAGE}"
    fi
}
