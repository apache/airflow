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

# Rebuilds python base image from the latest available Python version if it has been updated
function push_pull_remove_images::check_and_rebuild_python_base_image_if_needed() {
   docker_v pull "${PYTHON_BASE_IMAGE}"
   local dockerhub_python_version
   dockerhub_python_version=$(docker run "${PYTHON_BASE_IMAGE}" python -c 'import sys; print(sys.version)')
   local local_python_version
   local_python_version=$(docker run "${AIRFLOW_PYTHON_BASE_IMAGE}" python -c 'import sys; print(sys.version)')
   if [[ ${local_python_version} != "${dockerhub_python_version}" ]]; then
       echo
       echo "There is a new Python Base image updated!"
       echo "The version used in Airflow: ${local_python_version}"
       echo "The version available in DockerHub: ${dockerhub_python_version}"
       echo "Rebuilding ${AIRFLOW_PYTHON_BASE_IMAGE} from the latest ${PYTHON_BASE_IMAGE}"
       echo
       echo "FROM ${PYTHON_BASE_IMAGE}" | \
            docker_v build \
                --label "org.opencontainers.image.source=https://github.com/${GITHUB_REPOSITORY}" \
                -t "${AIRFLOW_PYTHON_BASE_IMAGE}" -
  fi
}

# Pulls the base Python image. This image is used as base for CI and PROD images, depending on the parameters used:
#
# * if CHECK_IF_BASE_PYTHON_IMAGE_UPDATED == "true", then it checks if new image of Python has been released
#     in DockerHub and it will rebuild the base python image and add the `org.opencontainers.image.source`
#     label to it, so that it is linked to Airflow repository when we push it to the
#     Github Container registry
# * Otherwise it pulls the Python base image from GitHub Container Registry registry.
#     In case we pull specific build image (via suffix)
#     it will pull the right image using the specified suffix
function push_pull_remove_images::pull_base_python_image() {
    echo
    echo "Docker pulling base python image. Upgrade to newer deps: ${UPGRADE_TO_NEWER_DEPENDENCIES}"
    echo
    if [[ -n ${DETECTED_TERMINAL=} ]]; then
        echo -n "Docker pulling base python image. Upgrade to newer deps: ${UPGRADE_TO_NEWER_DEPENDENCIES}
" > "${DETECTED_TERMINAL}"
    fi
    if [[ ${GITHUB_REGISTRY_PULL_IMAGE_TAG} != "latest" ]]; then
        push_pull_remove_images::pull_image_if_not_present_or_forced \
            "${AIRFLOW_PYTHON_BASE_IMAGE}${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
        if [[ ${CHECK_IF_BASE_PYTHON_IMAGE_UPDATED} == "true" ]] ; then
            echo
            echo  "${COLOR_RED}ERROR: You cannot check for base python image if you pull specific tag: ${GITHUB_REGISTRY_PULL_IMAGE_TAG}.${COLOR_RESET}"
            echo
            return 1
        fi
    else
        push_pull_remove_images::pull_image_if_not_present_or_forced "${AIRFLOW_PYTHON_BASE_IMAGE}"
        if [[ ${CHECK_IF_BASE_PYTHON_IMAGE_UPDATED} == "true" ]] ; then
            push_pull_remove_images::check_and_rebuild_python_base_image_if_needed
        fi
    fi
}

# Pulls CI image in case caching strategy is "pulled" and the image needs to be pulled
function push_pull_remove_images::pull_ci_images_if_needed() {
    local python_image_hash
    python_image_hash=$(docker images -q "${AIRFLOW_PYTHON_BASE_IMAGE}" 2> /dev/null || true)
    if [[ -z "${python_image_hash=}" || "${FORCE_PULL_IMAGES}" == "true" || \
            ${CHECK_IF_BASE_PYTHON_IMAGE_UPDATED} == "true" ]]; then
        if [[ ${GITHUB_REGISTRY_PULL_IMAGE_TAG} == "latest" ]]; then
            # Pull base python image when building latest image
            push_pull_remove_images::pull_base_python_image
        fi
    fi
    if [[ "${DOCKER_CACHE}" == "pulled" ]]; then
        push_pull_remove_images::pull_image_if_not_present_or_forced \
            "${AIRFLOW_CI_IMAGE}:${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
    fi
}


# Pulls PROD image in case caching strategy is "pulled" and the image needs to be pulled
function push_pull_remove_images::pull_prod_images_if_needed() {
    local python_image_hash
    python_image_hash=$(docker images -q "${AIRFLOW_PYTHON_BASE_IMAGE}" 2> /dev/null || true)
    if [[ -z "${python_image_hash=}" || "${FORCE_PULL_IMAGES}" == "true"  || \
            ${CHECK_IF_BASE_PYTHON_IMAGE_UPDATED} == "true" ]]; then
        if [[ ${GITHUB_REGISTRY_PULL_IMAGE_TAG} == "latest" ]]; then
            # Pull base python image when building latest image
            push_pull_remove_images::pull_base_python_image
        fi
    fi
    if [[ "${DOCKER_CACHE}" == "pulled" ]]; then
        # "Build" segment of production image
        push_pull_remove_images::pull_image_if_not_present_or_forced \
            "${AIRFLOW_PROD_BUILD_IMAGE}:${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
        # "Main" segment of production image
        push_pull_remove_images::pull_image_if_not_present_or_forced \
            "${AIRFLOW_PROD_IMAGE}:${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
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
        "${AIRFLOW_PYTHON_BASE_IMAGE}${python_tag_suffix}"
    push_pull_remove_images::push_image_with_retries \
        "${AIRFLOW_PYTHON_BASE_IMAGE}${python_tag_suffix}"
}

# Pushes Ci images and their tags to registry in GitHub
function push_pull_remove_images::push_ci_images_to_github() {
    if [[ "${PUSH_PYTHON_BASE_IMAGE=}" != "false" ]]; then
        push_pull_remove_images::push_python_image_to_github
    fi
    local airflow_ci_tagged_image="${AIRFLOW_CI_IMAGE}:${GITHUB_REGISTRY_PUSH_IMAGE_TAG}"
    docker_v tag "${AIRFLOW_CI_IMAGE}" "${airflow_ci_tagged_image}"
    push_pull_remove_images::push_image_with_retries "${airflow_ci_tagged_image}"
    if [[ ${GITHUB_REGISTRY_PUSH_IMAGE_TAG} == "latest" ]]; then
        local airflow_ci_manifest_tagged_image="${AIRFLOW_CI_REMOTE_MANIFEST_IMAGE}:${GITHUB_REGISTRY_PUSH_IMAGE_TAG}"
        docker_v tag "${AIRFLOW_CI_LOCAL_MANIFEST_IMAGE}" "${airflow_ci_manifest_tagged_image}"
        push_pull_remove_images::push_image_with_retries "${airflow_ci_manifest_tagged_image}"
    fi
    if [[ -n ${GITHUB_SHA=} ]]; then
        # Also push image to GitHub registry with commit SHA
        local airflow_ci_sha_image="${AIRFLOW_CI_IMAGE}:${COMMIT_SHA}"
        docker_v tag "${AIRFLOW_CI_IMAGE}" "${airflow_ci_sha_image}"
        push_pull_remove_images::push_image_with_retries "${airflow_ci_sha_image}"
    fi
}

# Pushes PROD image to registry in GitHub
# Push image to GitHub registry with chosen push tag
# the PUSH tag might be:
#     "${COMMIT_SHA}" - in case of pull-request triggered 'workflow_run' builds
#     "latest"        - in case of push builds
function push_pull_remove_images::push_prod_images_to_github () {
    local airflow_prod_tagged_image="${AIRFLOW_PROD_IMAGE}:${GITHUB_REGISTRY_PUSH_IMAGE_TAG}"
    docker_v tag "${AIRFLOW_PROD_IMAGE}" "${airflow_prod_tagged_image}"
    push_pull_remove_images::push_image_with_retries "${airflow_prod_tagged_image}"
    if [[ -n ${COMMIT_SHA=} ]]; then
        # Also push image to GitHub registry with commit SHA
        local airflow_prod_sha_image="${AIRFLOW_PROD_IMAGE}:${COMMIT_SHA}"
        docker_v tag "${AIRFLOW_PROD_IMAGE}" "${airflow_prod_sha_image}"
        push_pull_remove_images::push_image_with_retries "${airflow_prod_sha_image}"
    fi
    # Also push prod build image
    local airflow_prod_build_tagged_image="${AIRFLOW_PROD_BUILD_IMAGE}:${GITHUB_REGISTRY_PUSH_IMAGE_TAG}"
    docker_v tag "${AIRFLOW_PROD_BUILD_IMAGE}" "${airflow_prod_build_tagged_image}"
    push_pull_remove_images::push_image_with_retries "${airflow_prod_build_tagged_image}"
}

# waits for an image to be available in GitHub Container Registry. Should be run with `set +e`
function push_pull_remove_images::check_image_manifest() {
    local image_to_wait_for="${1}"
    echo "GitHub Container Registry: checking for ${image_to_wait_for} via docker manifest inspect!"
    docker_v manifest inspect "${image_to_wait_for}"
    local res=$?
    if [[ ${res} == "0" ]]; then
        echo  "Image: ${image_to_wait_for} found in Container Registry: ${COLOR_GREEN}OK.${COLOR_RESET}"
        return 0
    else
        echo "${COLOR_YELLOW}Still waiting. Not found!${COLOR_RESET}"
        return 1
    fi
}

# waits for an image to be available in the GitHub registry
function push_pull_remove_images::wait_for_image() {
    set +e
    echo " Waiting for github registry image: " "$1"
    while true
    do
        if push_pull_remove_images::check_image_manifest "$1"; then
            break
        fi
        sleep 30
    done
    set -e
}
