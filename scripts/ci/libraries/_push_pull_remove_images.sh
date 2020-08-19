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
        # need eto be explicitly verbose in order to have pre-commit spinner working
        # No aliases in pre-commit non-interactive shells :(
        verbose_docker pull "${IMAGE_TO_PULL}"
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
            docker tag "${CACHED_IMAGE}" "${IMAGE}"
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
                # need eto be explicitly verbose in order to have pre-commit spinner working
                # No aliases in pre-commit non-interactive shells :(
                verbose_docker pull "${PYTHON_BASE_IMAGE}"
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
                # need eto be explicitly verbose in order to have pre-commit spinner working
                # No aliases in pre-commit non-interactive shells :(
                verbose_docker pull "${PYTHON_BASE_IMAGE}"
            fi
            echo
        fi
        # "Build" segment of production image
        pull_image_possibly_from_cache "${AIRFLOW_PROD_BUILD_IMAGE}" "${CACHED_AIRFLOW_PROD_BUILD_IMAGE}"
        # we never pull the main segment of production image - we always build it locally = this is
        # usually very fast this way and it is much nicer for rebuilds and development
    fi
}

# Pushes Ci image and it's manifest to the registry. In case the image was taken from cache registry
# it is pushed to the cache, not to the main registry. Manifest is only pushed to the main registry
function push_ci_image() {
    if [[ ${CACHED_AIRFLOW_CI_IMAGE:=} != "" ]]; then
        docker tag "${AIRFLOW_CI_IMAGE}" "${CACHED_AIRFLOW_CI_IMAGE}"
        IMAGE_TO_PUSH="${CACHED_AIRFLOW_CI_IMAGE}"
    else
        IMAGE_TO_PUSH="${AIRFLOW_CI_IMAGE}"
    fi
    docker push "${IMAGE_TO_PUSH}"
    if [[ ${CACHED_AIRFLOW_CI_IMAGE} == "" ]]; then
        # Only push manifest image for builds that are not using CI cache
        docker tag "${AIRFLOW_CI_LOCAL_MANIFEST_IMAGE}" "${AIRFLOW_CI_REMOTE_MANIFEST_IMAGE}"
        docker push "${AIRFLOW_CI_REMOTE_MANIFEST_IMAGE}"
        if [[ -n ${DEFAULT_IMAGE:=""} ]]; then
            docker push "${DEFAULT_IMAGE}"
        fi
    fi
    if [[ ${CACHED_PYTHON_BASE_IMAGE} != "" ]]; then
        docker tag "${PYTHON_BASE_IMAGE}" "${CACHED_PYTHON_BASE_IMAGE}"
        docker push "${CACHED_PYTHON_BASE_IMAGE}"
    fi

}

# Pushes PROD image to the registry. In case the image was taken from cache registry
# it is also pushed to the cache, not to the main registry
function push_prod_images() {
    if [[ ${CACHED_AIRFLOW_PROD_IMAGE:=} != "" ]]; then
        docker tag "${AIRFLOW_PROD_IMAGE}" "${CACHED_AIRFLOW_PROD_IMAGE}"
        IMAGE_TO_PUSH="${CACHED_AIRFLOW_PROD_IMAGE}"
    else
        IMAGE_TO_PUSH="${AIRFLOW_PROD_IMAGE}"
    fi
    if [[ ${CACHED_AIRFLOW_PROD_BUILD_IMAGE:=} != "" ]]; then
        docker tag "${AIRFLOW_PROD_BUILD_IMAGE}" "${CACHED_AIRFLOW_PROD_BUILD_IMAGE}"
        IMAGE_TO_PUSH_BUILD="${CACHED_AIRFLOW_PROD_BUILD_IMAGE}"
    else
        IMAGE_TO_PUSH_BUILD="${AIRFLOW_PROD_BUILD_IMAGE}"
    fi
    docker push "${IMAGE_TO_PUSH}"
    docker push "${IMAGE_TO_PUSH_BUILD}"
    if [[ -n ${DEFAULT_IMAGE:=""} && ${CACHED_AIRFLOW_PROD_IMAGE} == "" ]]; then
        docker push "${DEFAULT_IMAGE}"
    fi
    # we do not need to push PYTHON base image here - they are already pushed in the CI push
}

# Removes airflow CI and base images
function remove_all_images() {
    echo
    "${AIRFLOW_SOURCES}/confirm" "Removing all local images ."
    echo
    docker rmi "${PYTHON_BASE_IMAGE}" || true
    docker rmi "${AIRFLOW_CI_IMAGE}" || true
    echo
    echo "###################################################################"
    echo "NOTE!! Removed Airflow images for Python version ${PYTHON_MAJOR_MINOR_VERSION}."
    echo "       But the disk space in docker will be reclaimed only after"
    echo "       running 'docker system prune' command."
    echo "###################################################################"
    echo
}
