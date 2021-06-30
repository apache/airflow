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
        docker_v push "${1}"
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
    local image_to_pull="${1}"
    local image_hash
    image_hash=$(docker images -q "${image_to_pull}" 2> /dev/null || true)
    local pull_image=${FORCE_PULL_IMAGES}

    if [[ -z "${image_hash=}" ]]; then
        pull_image="true"
    fi
    if [[ "${pull_image}" == "true" ]]; then
        echo
        echo "Pulling the image ${image_to_pull}"
        echo
        docker_v pull "${image_to_pull}"
        local exit_value="$?"
        if [[ ${exit_value} != "0" && ${FAIL_ON_GITHUB_DOCKER_PULL_ERROR} == "true" ]]; then
            echo
            echo """
${COLOR_RED}ERROR: Exiting on docker pull error

If you have authorisation problems, you might want to run:

docker login ${image_to_pull%%\/*}

You need to use generate token as the password, not your personal password.
You can generate one at https://github.com/settings/tokens
Make sure to choose 'read:packages' scope.
${COLOR_RESET}
"""
            exit ${exit_value}
        fi
        echo
        return ${exit_value}
    fi
}

# Pulls image if needed but tries to pull it from GitHub registry before
# It attempts to pull it from the DockerHub registry. This is used to speed up the builds
# In GitHub Actions and to pull appropriately tagged builds.
# Parameters:
#   $1 -> DockerHub image to pull
#   $2 -> GitHub image to try to pull first
function push_pull_remove_images::pull_image_github_dockerhub() {
    local dockerhub_image="${1}"
    local github_image="${2}"

    set +e
    if push_pull_remove_images::pull_image_if_not_present_or_forced "${github_image}"; then
        # Tag the image to be the DockerHub one
        docker_v tag "${github_image}" "${dockerhub_image}"
    else
        push_pull_remove_images::pull_image_if_not_present_or_forced "${dockerhub_image}"
    fi
    set -e
}

# Rebuilds python base image from the latest available Python version
function push_pull_remove_images::rebuild_python_base_image() {
   echo
   echo "Rebuilding ${AIRFLOW_PYTHON_BASE_IMAGE} from latest ${PYTHON_BASE_IMAGE}"
   echo
   docker_v pull "${PYTHON_BASE_IMAGE}"
   echo "FROM ${PYTHON_BASE_IMAGE}" | \
        docker_v build \
            --label "org.opencontainers.image.source=https://github.com/${GITHUB_REPOSITORY}" \
            -t "${AIRFLOW_PYTHON_BASE_IMAGE}" -
}

# Pulls the base Python image. This image is used as base for CI and PROD images, depending on the parameters used:
#
# * if FORCE_PULL_BASE_PYTHON_IMAGE != false, then it rebuild the image using latest Python image available
#     and adds `org.opencontainers.image.source` label to it, so that it is linked to Airflow
#     repository when we push it to GHCR registry
# * Otherwise it pulls the Python base image from either GitHub registry or from DockerHub
#     depending on USE_GITHUB_REGISTRY variable. In case we pull specific build image (via suffix)
#     it will pull the right image using the specified suffix
function push_pull_remove_images::pull_base_python_image() {
    if [[ ${FORCE_PULL_BASE_PYTHON_IMAGE} == "true" ]] ; then
        push_pull_remove_images::rebuild_python_base_image
        return
    fi
    echo
    echo "Docker pulling base python image. Upgrade to newer deps: ${UPGRADE_TO_NEWER_DEPENDENCIES}"
    echo
    if [[ -n ${DETECTED_TERMINAL=} ]]; then
        echo -n "Docker pulling base python image. Upgrade to newer deps: ${UPGRADE_TO_NEWER_DEPENDENCIES}
" > "${DETECTED_TERMINAL}"
    fi
    if [[ ${USE_GITHUB_REGISTRY} == "true" ]]; then
        local python_tag_suffix=""
        if [[ ${GITHUB_REGISTRY_PULL_IMAGE_TAG} != "latest" ]]; then
            python_tag_suffix="-${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
        fi
        push_pull_remove_images::pull_image_github_dockerhub "${AIRFLOW_PYTHON_BASE_IMAGE}" \
            "${GITHUB_REGISTRY_PYTHON_BASE_IMAGE}${python_tag_suffix}"
    else
        docker_v pull "${AIRFLOW_PYTHON_BASE_IMAGE}" || true
    fi
}

# Pulls CI image in case caching strategy is "pulled" and the image needs to be pulled
function push_pull_remove_images::pull_ci_images_if_needed() {
    local python_image_hash
    python_image_hash=$(docker images -q "${AIRFLOW_PYTHON_BASE_IMAGE}" 2> /dev/null || true)
    if [[ -z "${python_image_hash=}" || "${FORCE_PULL_IMAGES}" == "true" || \
            ${FORCE_PULL_BASE_PYTHON_IMAGE} == "true" ]]; then
        push_pull_remove_images::pull_base_python_image
    fi
    if [[ "${DOCKER_CACHE}" == "pulled" ]]; then
        if [[ ${USE_GITHUB_REGISTRY} == "true" ]]; then
            push_pull_remove_images::pull_image_github_dockerhub "${AIRFLOW_CI_IMAGE}" \
                "${GITHUB_REGISTRY_AIRFLOW_CI_IMAGE}:${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
        else
            push_pull_remove_images::pull_image_if_not_present_or_forced "${AIRFLOW_CI_IMAGE}" || true
        fi
    fi
}


# Pulls PROD image in case caching strategy is "pulled" and the image needs to be pulled
function push_pull_remove_images::pull_prod_images_if_needed() {
    local python_image_hash
    python_image_hash=$(docker images -q "${AIRFLOW_PYTHON_BASE_IMAGE}" 2> /dev/null || true)
    if [[ -z "${python_image_hash=}" || "${FORCE_PULL_IMAGES}" == "true"  || \
            ${FORCE_PULL_BASE_PYTHON_IMAGE} == "true" ]]; then
        push_pull_remove_images::pull_base_python_image
    fi
    if [[ "${DOCKER_CACHE}" == "pulled" ]]; then
        if [[ ${USE_GITHUB_REGISTRY} == "true" ]]; then
            # "Build" segment of production image
            push_pull_remove_images::pull_image_github_dockerhub "${AIRFLOW_PROD_BUILD_IMAGE}" \
                "${GITHUB_REGISTRY_AIRFLOW_PROD_BUILD_IMAGE}:${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
            # "Main" segment of production image
            push_pull_remove_images::pull_image_github_dockerhub "${AIRFLOW_PROD_IMAGE}" \
                "${GITHUB_REGISTRY_AIRFLOW_PROD_IMAGE}:${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
        else
            push_pull_remove_images::pull_image_if_not_present_or_forced "${AIRFLOW_PROD_BUILD_IMAGE}"
            push_pull_remove_images::pull_image_if_not_present_or_forced "${AIRFLOW_PROD_IMAGE}"
        fi
    fi
}

# Pushes Ci images and the manifest to the registry in DockerHub.
function push_pull_remove_images::push_ci_images_to_dockerhub() {
    push_pull_remove_images::push_image_with_retries "${AIRFLOW_PYTHON_BASE_IMAGE}"
    push_pull_remove_images::push_image_with_retries "${AIRFLOW_CI_IMAGE}"
    docker_v tag "${AIRFLOW_CI_LOCAL_MANIFEST_IMAGE}" "${AIRFLOW_CI_REMOTE_MANIFEST_IMAGE}"
    push_pull_remove_images::push_image_with_retries "${AIRFLOW_CI_REMOTE_MANIFEST_IMAGE}"
    if [[ -n ${DEFAULT_CI_IMAGE=} ]]; then
        # Only push default image to DockerHub registry if it is defined
        push_pull_remove_images::push_image_with_retries "${DEFAULT_CI_IMAGE}"
    fi
}


# Push image to GitHub registry with the push tag:
#     "${COMMIT_SHA}" - in case of pull-request triggered 'workflow_run' builds
#     "latest"        - in case of push builds
# Push python image to GitHub registry with the push tag:
#     X.Y-slim-buster-"${COMMIT_SHA}" - in case of pull-request triggered 'workflow_run' builds
#     X.Y-slim-buster                 - in case of push builds
function push_pull_remove_images::push_python_image_to_github() {
    local python_tag_suffix=""
    if [[ ${GITHUB_REGISTRY_PUSH_IMAGE_TAG} != "latest" ]]; then
        python_tag_suffix="-${GITHUB_REGISTRY_PUSH_IMAGE_TAG}"
    fi
    docker_v tag "${AIRFLOW_PYTHON_BASE_IMAGE}" \
        "${GITHUB_REGISTRY_PYTHON_BASE_IMAGE}${python_tag_suffix}"
    push_pull_remove_images::push_image_with_retries \
        "${GITHUB_REGISTRY_PYTHON_BASE_IMAGE}${python_tag_suffix}"
}

# Pushes Ci images and their tags to registry in GitHub
function push_pull_remove_images::push_ci_images_to_github() {
    if [[ "${PUSH_PYTHON_BASE_IMAGE=}" != "false" ]]; then
        push_pull_remove_images::push_python_image_to_github
    fi
    local airflow_ci_tagged_image="${GITHUB_REGISTRY_AIRFLOW_CI_IMAGE}:${GITHUB_REGISTRY_PUSH_IMAGE_TAG}"
    docker_v tag "${AIRFLOW_CI_IMAGE}" "${airflow_ci_tagged_image}"
    push_pull_remove_images::push_image_with_retries "${airflow_ci_tagged_image}"
    if [[ -n ${GITHUB_SHA=} ]]; then
        # Also push image to GitHub registry with commit SHA
        local airflow_ci_sha_image="${GITHUB_REGISTRY_AIRFLOW_CI_IMAGE}:${COMMIT_SHA}"
        docker_v tag "${AIRFLOW_CI_IMAGE}" "${airflow_ci_sha_image}"
        push_pull_remove_images::push_image_with_retries "${airflow_ci_sha_image}"
    fi
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
    push_pull_remove_images::push_image_with_retries "${AIRFLOW_PYTHON_BASE_IMAGE}"
    # Prod image
    push_pull_remove_images::push_image_with_retries "${AIRFLOW_PROD_IMAGE}"
    if [[ -n ${DEFAULT_PROD_IMAGE=} ]]; then
        push_pull_remove_images::push_image_with_retries "${DEFAULT_PROD_IMAGE}"
    fi
    # Prod build image
    push_pull_remove_images::push_image_with_retries "${AIRFLOW_PROD_BUILD_IMAGE}"

}

# Pushes PROD image to and their tags to registry in GitHub
# Push image to GitHub registry with chosen push tag
# the PUSH tag might be:
#     "${COMMIT_SHA}" - in case of pull-request triggered 'workflow_run' builds
#     "latest"        - in case of push builds
function push_pull_remove_images::push_prod_images_to_github () {
    local airflow_prod_tagged_image="${GITHUB_REGISTRY_AIRFLOW_PROD_IMAGE}:${GITHUB_REGISTRY_PUSH_IMAGE_TAG}"
    docker_v tag "${AIRFLOW_PROD_IMAGE}" "${airflow_prod_tagged_image}"
    push_pull_remove_images::push_image_with_retries "${GITHUB_REGISTRY_AIRFLOW_PROD_IMAGE}:${GITHUB_REGISTRY_PUSH_IMAGE_TAG}"
    if [[ -n ${COMMIT_SHA=} ]]; then
        # Also push image to GitHub registry with commit SHA
        local airflow_prod_sha_image="${GITHUB_REGISTRY_AIRFLOW_PROD_IMAGE}:${COMMIT_SHA}"
        docker_v tag "${AIRFLOW_PROD_IMAGE}" "${airflow_prod_sha_image}"
        push_pull_remove_images::push_image_with_retries "${airflow_prod_sha_image}"
    fi
    # Also push prod build image
    local airflow_prod_build_tagged_image="${GITHUB_REGISTRY_AIRFLOW_PROD_BUILD_IMAGE}:${GITHUB_REGISTRY_PUSH_IMAGE_TAG}"
    docker_v tag "${AIRFLOW_PROD_BUILD_IMAGE}" "${airflow_prod_build_tagged_image}"
    push_pull_remove_images::push_image_with_retries "${airflow_prod_build_tagged_image}"
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

# waits for an image to be available in GitHub Packages. Should be run with `set +e`
# the build automatically determines which registry to use based one the images available
function push_pull_remove_images::check_for_image_in_github_packages() {
    local github_repository_lowercase
    github_repository_lowercase="$(echo "${GITHUB_REPOSITORY}" |tr '[:upper:]' '[:lower:]')"
    local github_api_endpoint
    github_api_endpoint="https://docker.pkg.github.com/v2/${github_repository_lowercase}"
    local image_name_in_github_registry="${1}"
    local image_tag_in_github_registry=${2}
    local image_to_wait_for=${GITHUB_REPOSITORY}/${image_name_in_github_registry}:${image_tag_in_github_registry}
    local github_api_call
    github_api_call="${github_api_endpoint}/${image_name_in_github_registry}/manifests/${image_tag_in_github_registry}"
    echo "GitHub Packages: checking for ${image_to_wait_for} via ${github_api_call}!"
    http_status=$(curl --silent --output "${OUTPUT_LOG}" --write-out "%{http_code}" \
        --connect-timeout 60  --max-time 60 \
        -X GET "${github_api_call}" -u "${GITHUB_USERNAME}:${GITHUB_TOKEN}")
    if [[ ${http_status} == "200" ]]; then
        echo  "Image: ${image_to_wait_for} found in GitHub Packages: ${COLOR_GREEN}OK.  ${COLOR_RESET}"
        echo "::set-output name=githubRegistry::docker.pkg.github.com"
        echo
        echo "Setting githubRegistry output to docker.pkg.github.com"
        echo
        return 0
    else
        cat "${OUTPUT_LOG}"
        echo "${COLOR_YELLOW}Still waiting. Status code ${http_status}!${COLOR_RESET}"
        return 1
    fi
}

# waits for an image to be available in GitHub Container Registry. Should be run with `set +e`
function push_pull_remove_images::check_for_image_in_github_container_registry() {
    local image_name_in_github_registry="${1}"
    local image_tag_in_github_registry=${2}

    local image_to_wait_for="ghcr.io/${GITHUB_REPOSITORY}-${image_name_in_github_registry}:${image_tag_in_github_registry}"
    echo "GitHub Container Registry: checking for ${image_to_wait_for} via docker manifest inspect!"
    docker_v manifest inspect "${image_to_wait_for}"
    local res=$?
    if [[ ${res} == "0" ]]; then
        echo  "Image: ${image_to_wait_for} found in Container Registry: ${COLOR_GREEN}OK.${COLOR_RESET}"
        echo
        echo "Setting githubRegistry output to ghcr.io"
        echo
        echo "::set-output name=githubRegistry::ghcr.io"
        return 0
    else
        echo "${COLOR_YELLOW}Still waiting. Not found!${COLOR_RESET}"
        return 1
    fi
}

# waits for an image to be available in the GitHub registry
function push_pull_remove_images::wait_for_github_registry_image() {
    set +e
    echo " Waiting for github registry image: " "${@}"
    while true
    do
        if push_pull_remove_images::check_for_image_in_github_container_registry "${@}"; then
            break
        fi
        if push_pull_remove_images::check_for_image_in_github_packages "${@}"; then
            break
        fi
        sleep 30
    done
    set -e
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
