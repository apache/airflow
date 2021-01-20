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
            echo
            echo  "${COLOR_YELLOW}WARNING: Error ${res} when pushing image on ${try_num} try  ${COLOR_RESET}"
            echo
            continue
        else
            return 0
        fi
    done
    echo
    echo  "${COLOR_RED}ERROR: Error ${res} when pushing image on ${try_num} try. Giving up!  ${COLOR_RESET}"
    echo
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
            echo
            echo """
${COLOR_RED}ERROR: Exiting on docker pull error

If you have authorisation problems, you might want to run:

docker login ${IMAGE_TO_PULL%%\/*}

You need to use generate token as the password, not your personal password.
You can generate one at https://github.com/settings/tokens
Make sure to choose 'read:packages' scope.
${COLOR_RESET}
"""
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
        local python_image_hash
        python_image_hash=$(docker images -q "${AIRFLOW_CI_PYTHON_IMAGE}" 2> /dev/null || true)
        if [[ -z "${python_image_hash=}" ]]; then
            FORCE_PULL_IMAGES="true"
        fi
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
                docker tag "${AIRFLOW_CI_PYTHON_IMAGE}" "${PYTHON_BASE_IMAGE}"
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
                docker pull "${AIRFLOW_CI_PYTHON_IMAGE}"
                docker tag "${AIRFLOW_CI_PYTHON_IMAGE}" "${PYTHON_BASE_IMAGE}"
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
    local github_repository_lowercase
    github_repository_lowercase="$(echo "${GITHUB_REPOSITORY}" |tr '[:upper:]' '[:lower:]')"
    local github_api_endpoint
    github_api_endpoint="https://${GITHUB_REGISTRY}/v2/${github_repository_lowercase}"
    local image_name_in_github_registry="${1}"
    local image_tag_in_github_registry=${2}

    echo
    echo "Waiting for ${GITHUB_REPOSITORY}/${image_name_in_github_registry}:${image_tag_in_github_registry} image"
    echo

    GITHUB_API_CALL="${github_api_endpoint}/${image_name_in_github_registry}/manifests/${image_tag_in_github_registry}"
    while true; do
        http_status=$(curl --silent --output "${OUTPUT_LOG}" --write-out "%{http_code}" \
            --connect-timeout 60  --max-time 60 \
            -X GET "${GITHUB_API_CALL}" -u "${GITHUB_USERNAME}:${GITHUB_TOKEN}")
        if [[ ${http_status} == "200" ]]; then
            echo  "${COLOR_GREEN}OK.  ${COLOR_RESET}"
            break
        else
            echo "${COLOR_YELLOW}Still waiting - status code ${http_status}!${COLOR_RESET}"
            cat "${OUTPUT_LOG}"
        fi
        sleep 60
    done
    verbosity::print_info "Found ${image_name_in_github_registry}:${image_tag_in_github_registry} image"
}

function push_pull_remove_images::check_if_github_registry_wait_for_image_enabled() {
    if [[ ${USE_GITHUB_REGISTRY} != "true" ||  ${GITHUB_REGISTRY_WAIT_FOR_IMAGE} != "true" ]]; then
        echo
        echo "This script should not be called"
        echo "It need both USE_GITHUB_REGISTRY and GITHUB_REGISTRY_WAIT_FOR_IMAGE to true!"
        echo
        echo "USE_GITHUB_REGISTRY = ${USE_GITHUB_REGISTRY}"
        echo "GITHUB_REGISTRY_WAIT_FOR_IMAGE =${GITHUB_REGISTRY_WAIT_FOR_IMAGE}"
        echo
        exit 1
    fi
}
