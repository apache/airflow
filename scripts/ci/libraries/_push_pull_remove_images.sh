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


# Tries to push the image several times in case we receive an intermittent error on push
# $1 - tag to push
function push_pull_remove_images::push_image_with_retries() {
    for try_num in 1 2 3 4
    do
        set +e
        echo
        echo "Trying to push the image ${1}. Number of try: ${try_num}"
        docker push "${1}"
        local res=$?
        set -e
        if [[ ${res} != "0" ]]; then
            >&2 echo
            >&2 echo "Error ${res} when pushing image on ${try_num} try"
            >&2 echo
            continue
        else
            return 0
        fi
    done
    >&2 echo
    >&2 echo "Error ${res} when pushing image on ${try_num} try. Giving up!"
    >&2 echo
    return 1
}


# Pulls image in case it is needed (either has never been pulled or pulling was forced
# Should be run with set +e
# Parameters:
#   $1 -> image to pull
function push_pull_remove_images::pull_image_if_not_present_or_forced() {
    local IMAGE_TO_PULL="${1}"
    local IMAGE_HASH
    IMAGE_HASH=$(docker images -q "${IMAGE_TO_PULL}" 2> /dev/null || true)
    local PULL_IMAGE=${FORCE_PULL_IMAGES}

    if [[ -z "${IMAGE_HASH=}" ]]; then
        PULL_IMAGE="true"
    fi
    if [[ "${PULL_IMAGE}" == "true" ]]; then
        echo
        echo "Pulling the image ${IMAGE_TO_PULL}"
        echo
        docker pull "${IMAGE_TO_PULL}"
        EXIT_VALUE="$?"
        if [[ ${EXIT_VALUE} != "0" && ${FAIL_ON_GITHUB_DOCKER_PULL_ERROR} == "true" ]]; then
            >&2 echo
            >&2 echo "ERROR! Exiting on docker pull error"
            >&2 echo
            >&2 echo "If you have authorisation problems, you might want to run:"
            >&2 echo
            >&2 echo "docker login ${IMAGE_TO_PULL%%\/*}"
            >&2 echo
            >&2 echo "You need to use generate token as the password, not your personal password."
            >&2 echo "You can generate one at https://github.com/settings/tokens"
            >&2 echo "Make sure to choose 'read:packages' scope".
            >&2 echo
            exit ${EXIT_VALUE}
        fi
        echo
        return ${EXIT_VALUE}
    fi
}

# Pulls image if needed but tries to pull it from GitHub registry before
# It attempts to pull it from the DockerHub registry. This is used to speed up the builds
# In GitHub Actions and to pull appropriately tagged builds.
# Parameters:
#   $1 -> DockerHub image to pull
#   $2 -> GitHub image to try to pull first
function push_pull_remove_images::pull_image_github_dockerhub() {
    local DOCKERHUB_IMAGE="${1}"
    local GITHUB_IMAGE="${2}"

    set +e
    if push_pull_remove_images::pull_image_if_not_present_or_forced "${GITHUB_IMAGE}"; then
        # Tag the image to be the DockerHub one
        docker tag "${GITHUB_IMAGE}" "${DOCKERHUB_IMAGE}"
    else
        push_pull_remove_images::pull_image_if_not_present_or_forced "${DOCKERHUB_IMAGE}"
    fi
    set -e
}

# Pulls CI image in case caching strategy is "pulled" and the image needs to be pulled
function push_pull_remove_images::pull_ci_images_if_needed() {
    if [[ "${DOCKER_CACHE}" == "pulled" ]]; then
        if [[ "${FORCE_PULL_IMAGES}" == "true" ]]; then
            echo
            echo "Force pull base image ${PYTHON_BASE_IMAGE}"
            echo
            if [[ -n ${DETECTED_TERMINAL=} ]]; then
                echo -n "
Docker pulling ${PYTHON_BASE_IMAGE}.
                    " > "${DETECTED_TERMINAL}"
            fi
            if [[ ${USE_GITHUB_REGISTRY} == "true" ]]; then
                PYTHON_TAG_SUFFIX=""
                if [[ ${GITHUB_REGISTRY_PULL_IMAGE_TAG} != "latest" ]]; then
                    PYTHON_TAG_SUFFIX="-${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
                fi
                push_pull_remove_images::pull_image_github_dockerhub "${PYTHON_BASE_IMAGE}" "${GITHUB_REGISTRY_PYTHON_BASE_IMAGE}${PYTHON_TAG_SUFFIX}"
            else
                docker pull "${AIRFLOW_CI_PYTHON_IMAGE}"
            fi
            echo
        fi
        if [[ ${USE_GITHUB_REGISTRY} == "true" ]]; then
            push_pull_remove_images::pull_image_github_dockerhub "${AIRFLOW_CI_IMAGE}" "${GITHUB_REGISTRY_AIRFLOW_CI_IMAGE}:${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
        else
            push_pull_remove_images::pull_image_if_not_present_or_forced "${AIRFLOW_CI_IMAGE}"
        fi
    fi
}


# Pulls PROD image in case caching strategy is "pulled" and the image needs to be pulled
function push_pull_remove_images::pull_prod_images_if_needed() {
    if [[ "${DOCKER_CACHE}" == "pulled" ]]; then
        if [[ "${FORCE_PULL_IMAGES}" == "true" ]]; then
            echo
            echo "Force pull base image ${PYTHON_BASE_IMAGE}"
            echo
            if [[ ${USE_GITHUB_REGISTRY} == "true" ]]; then
                PYTHON_TAG_SUFFIX=""
                if [[ ${GITHUB_REGISTRY_PULL_IMAGE_TAG} != "latest" ]]; then
                    PYTHON_TAG_SUFFIX="-${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
                fi
                push_pull_remove_images::pull_image_github_dockerhub "${PYTHON_BASE_IMAGE}" "${GITHUB_REGISTRY_PYTHON_BASE_IMAGE}${PYTHON_TAG_SUFFIX}"
            else
                docker pull "${PYTHON_BASE_IMAGE}"
            fi
            echo
        fi
        if [[ ${USE_GITHUB_REGISTRY} == "true" ]]; then
            # "Build" segment of production image
            push_pull_remove_images::pull_image_github_dockerhub "${AIRFLOW_PROD_BUILD_IMAGE}" "${GITHUB_REGISTRY_AIRFLOW_PROD_BUILD_IMAGE}:${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
            # "Main" segment of production image
            push_pull_remove_images::pull_image_github_dockerhub "${AIRFLOW_PROD_IMAGE}" "${GITHUB_REGISTRY_AIRFLOW_PROD_IMAGE}:${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
        else
            push_pull_remove_images::pull_image_if_not_present_or_forced "${AIRFLOW_PROD_BUILD_IMAGE}"
            push_pull_remove_images::pull_image_if_not_present_or_forced "${AIRFLOW_PROD_IMAGE}"
        fi
    fi
}

# Pushes Ci images and the manifest to the registry in DockerHub.
function push_pull_remove_images::push_ci_images_to_dockerhub() {
    push_pull_remove_images::push_image_with_retries "${AIRFLOW_CI_IMAGE}"
    docker tag "${AIRFLOW_CI_LOCAL_MANIFEST_IMAGE}" "${AIRFLOW_CI_REMOTE_MANIFEST_IMAGE}"
    push_pull_remove_images::push_image_with_retries "${AIRFLOW_CI_REMOTE_MANIFEST_IMAGE}"
    if [[ -n ${DEFAULT_CI_IMAGE=} ]]; then
        # Only push default image to DockerHub registry if it is defined
        push_pull_remove_images::push_image_with_retries "${DEFAULT_CI_IMAGE}"
    fi
    # Also push python image so that we use the same image as the CI image it was built with
    docker tag "${PYTHON_BASE_IMAGE}" "${AIRFLOW_CI_PYTHON_IMAGE}"
    push_pull_remove_images::push_image_with_retries "${AIRFLOW_CI_PYTHON_IMAGE}"
}

# Pushes Ci images and their tags to registry in GitHub
function push_pull_remove_images::push_ci_images_to_github() {
    # Push image to GitHub registry with chosen push tag
    # the PUSH tag might be:
    #     "${GITHUB_RUN_ID}" - in case of pull-request triggered 'workflow_run' builds
    #     "latest"           - in case of push builds
    AIRFLOW_CI_TAGGED_IMAGE="${GITHUB_REGISTRY_AIRFLOW_CI_IMAGE}:${GITHUB_REGISTRY_PUSH_IMAGE_TAG}"
    docker tag "${AIRFLOW_CI_IMAGE}" "${AIRFLOW_CI_TAGGED_IMAGE}"
    push_pull_remove_images::push_image_with_retries "${AIRFLOW_CI_TAGGED_IMAGE}"
    if [[ -n ${GITHUB_SHA=} ]]; then
        # Also push image to GitHub registry with commit SHA
        AIRFLOW_CI_SHA_IMAGE="${GITHUB_REGISTRY_AIRFLOW_CI_IMAGE}:${COMMIT_SHA}"
        docker tag "${AIRFLOW_CI_IMAGE}" "${AIRFLOW_CI_SHA_IMAGE}"
        push_pull_remove_images::push_image_with_retries "${AIRFLOW_CI_SHA_IMAGE}"
    fi
    PYTHON_TAG_SUFFIX=""
    if [[ ${GITHUB_REGISTRY_PUSH_IMAGE_TAG} != "latest" ]]; then
        PYTHON_TAG_SUFFIX="-${GITHUB_REGISTRY_PUSH_IMAGE_TAG}"
    fi
    docker tag "${PYTHON_BASE_IMAGE}" "${GITHUB_REGISTRY_PYTHON_BASE_IMAGE}${PYTHON_TAG_SUFFIX}"
    push_pull_remove_images::push_image_with_retries "${GITHUB_REGISTRY_PYTHON_BASE_IMAGE}${PYTHON_TAG_SUFFIX}"
}


# Pushes Ci image and it's manifest to the registry.
function push_pull_remove_images::push_ci_images() {
    if [[ ${USE_GITHUB_REGISTRY} == "true" ]]; then
        push_pull_remove_images::push_ci_images_to_github
    else
        push_pull_remove_images::push_ci_images_to_dockerhub
    fi
}

# Pushes PROD image to registry in DockerHub
function push_pull_remove_images::push_prod_images_to_dockerhub () {
    # Prod image
    push_pull_remove_images::push_image_with_retries "${AIRFLOW_PROD_IMAGE}"
    if [[ -n ${DEFAULT_PROD_IMAGE=} ]]; then
        push_pull_remove_images::push_image_with_retries "${DEFAULT_PROD_IMAGE}"
    fi
    # Prod build image
    push_pull_remove_images::push_image_with_retries "${AIRFLOW_PROD_BUILD_IMAGE}"

}

# Pushes PROD image to and their tags to registry in GitHub
function push_pull_remove_images::push_prod_images_to_github () {
    # Push image to GitHub registry with chosen push tag
    # the PUSH tag might be:
    #     "${GITHUB_RUN_ID}" - in case of pull-request triggered 'workflow_run' builds
    #     "latest"           - in case of push builds
    AIRFLOW_PROD_TAGGED_IMAGE="${GITHUB_REGISTRY_AIRFLOW_PROD_IMAGE}:${GITHUB_REGISTRY_PUSH_IMAGE_TAG}"
    docker tag "${AIRFLOW_PROD_IMAGE}" "${AIRFLOW_PROD_TAGGED_IMAGE}"
    push_pull_remove_images::push_image_with_retries "${GITHUB_REGISTRY_AIRFLOW_PROD_IMAGE}:${GITHUB_REGISTRY_PUSH_IMAGE_TAG}"
    if [[ -n ${COMMIT_SHA=} ]]; then
        # Also push image to GitHub registry with commit SHA
        AIRFLOW_PROD_SHA_IMAGE="${GITHUB_REGISTRY_AIRFLOW_PROD_IMAGE}:${COMMIT_SHA}"
        docker tag "${AIRFLOW_PROD_IMAGE}" "${AIRFLOW_PROD_SHA_IMAGE}"
        push_pull_remove_images::push_image_with_retries "${AIRFLOW_PROD_SHA_IMAGE}"
    fi
    # Also push prod build image
    AIRFLOW_PROD_BUILD_TAGGED_IMAGE="${GITHUB_REGISTRY_AIRFLOW_PROD_BUILD_IMAGE}:${GITHUB_REGISTRY_PUSH_IMAGE_TAG}"
    docker tag "${AIRFLOW_PROD_BUILD_IMAGE}" "${AIRFLOW_PROD_BUILD_TAGGED_IMAGE}"
    push_pull_remove_images::push_image_with_retries "${AIRFLOW_PROD_BUILD_TAGGED_IMAGE}"
}


# Pushes PROD image to the registry. In case the image was taken from cache registry
# it is also pushed to the cache, not to the main registry
function push_pull_remove_images::push_prod_images() {
    if [[ ${USE_GITHUB_REGISTRY} == "true" ]]; then
        push_pull_remove_images::push_prod_images_to_github
    else
        push_pull_remove_images::push_prod_images_to_dockerhub
    fi
}

# waits for an image to be available in the GitHub registry
function push_pull_remove_images::wait_for_github_registry_image() {
    github_repository_lowercase="$(echo "${GITHUB_REPOSITORY}" |tr '[:upper:]' '[:lower:]')"
    GITHUB_API_ENDPOINT="https://${GITHUB_REGISTRY}/v2/${github_repository_lowercase}"
    IMAGE_NAME="${1}"
    IMAGE_TAG=${2}
    echo "Waiting for ${IMAGE_NAME}:${IMAGE_TAG} image"

    GITHUB_API_CALL="${GITHUB_API_ENDPOINT}/${IMAGE_NAME}/manifests/${IMAGE_TAG}"
    while true; do
        curl -X GET "${GITHUB_API_CALL}" -u "${GITHUB_USERNAME}:${GITHUB_TOKEN}" 2>/dev/null > "${OUTPUT_LOG}"
        local digest
        digest=$(jq '.config.digest' < "${OUTPUT_LOG}")
        echo -n "."
        if [[ ${digest} != "null" ]]; then
            echo -e " \e[32mOK.\e[0m"
            break
        fi
        sleep 10
    done
    verbosity::print_info "Found ${IMAGE_NAME}:${IMAGE_TAG} image"
    verbosity::print_info "Digest: '${digest}'"
}
