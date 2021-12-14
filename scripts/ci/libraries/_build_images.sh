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

# For remote installation of airflow (from GitHub or PyPI) when building the image, you need to
# pass build flags depending on the version and method of the installation (for example to
# get proper requirement constraint files)
function build_images::add_build_args_for_remote_install() {
    # entrypoint is used as AIRFLOW_SOURCES_FROM/TO in order to avoid costly copying of all sources of
    # Airflow - those are not needed for remote install at all. Entrypoint is later overwritten by
    EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
        "--build-arg" "AIRFLOW_SOURCES_FROM=empty"
        "--build-arg" "AIRFLOW_SOURCES_TO=/empty"
    )
    if [[ ${CI} == "true" ]]; then
        EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
            "--build-arg" "PIP_PROGRESS_BAR=off"
        )
    fi
    if [[ -n "${AIRFLOW_CONSTRAINTS_REFERENCE}" ]]; then
        EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
            "--build-arg" "AIRFLOW_CONSTRAINTS_REFERENCE=${AIRFLOW_CONSTRAINTS_REFERENCE}"
        )
    else
        if  [[ ${AIRFLOW_VERSION} =~ v?2.* ]]; then
            EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
                # For specified minor version of 2.0 or v2 branch use specific reference constraints
                "--build-arg" "AIRFLOW_CONSTRAINTS_REFERENCE=constraints-${AIRFLOW_VERSION}"
            )
        else
            # For all other we just get the default constraint branch coming from the _initialization.sh
            EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
                "--build-arg" "AIRFLOW_CONSTRAINTS_REFERENCE=${DEFAULT_CONSTRAINTS_BRANCH}"
            )
        fi
    fi
    if [[ -n "${AIRFLOW_CONSTRAINTS_LOCATION}" ]]; then
        EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
            "--build-arg" "AIRFLOW_CONSTRAINTS_LOCATION=${AIRFLOW_CONSTRAINTS_LOCATION}"
        )
    fi
    # Depending on the version built, we choose the right branch for preloading the packages from
    # For v2-*-test we choose v2-*-test
    # all other builds when you choose a specific version (1.0, 2.0, 2.1. series) should choose stable branch
    # to preload. For all other builds we use the default branch defined in _initialization.sh
    # TODO: Generalize me
    if [[ ${AIRFLOW_VERSION} == 'v2-0-test' ]]; then
        AIRFLOW_BRANCH_FOR_PYPI_PRELOADING="v2-0-test"
    elif [[ ${AIRFLOW_VERSION} == 'v2-1-test' ]]; then
        AIRFLOW_BRANCH_FOR_PYPI_PRELOADING="v2-1-test"
    elif [[ ${AIRFLOW_VERSION} =~ v?2\.0* ]]; then
        AIRFLOW_BRANCH_FOR_PYPI_PRELOADING="v2-0-stable"
    elif [[ ${AIRFLOW_VERSION} =~ v?2\.1* ]]; then
        AIRFLOW_BRANCH_FOR_PYPI_PRELOADING="v2-1-stable"
    else
        AIRFLOW_BRANCH_FOR_PYPI_PRELOADING=${DEFAULT_BRANCH}
    fi
}

# Retrieves version of airflow stored in the production image (used to display the actual
# Version we use if it was build from PyPI or GitHub
function build_images::get_airflow_version_from_production_image() {
    docker run --entrypoint /bin/bash "${AIRFLOW_PROD_IMAGE}" -c 'echo "${AIRFLOW_VERSION}"'
}

# Removes the "Forced answer" (yes/no/quit) given previously, unless you specifically want to remember it.
#
# This is the default behaviour of all rebuild scripts to ask independently whether you want to
# rebuild the image or not. Sometimes however we want to remember answer previously given. For
# example if you answered "no" to rebuild the image, the assumption is that you do not
# want to rebuild image also for other rebuilds in the same pre-commit execution.
#
# All the pre-commit checks therefore have `export REMEMBER_LAST_ANSWER="true"` set
# So that in case they are run in a sequence of commits they will not rebuild. Similarly if your most
# recent answer was "no" and you run `pre-commit run mypy` (for example) it will also reuse the
# "no" answer given previously. This happens until you run any of the breeze commands or run all
# pre-commits `pre-commit run` - then the "LAST_FORCE_ANSWER_FILE" will be removed and you will
# be asked again.
function build_images::forget_last_answer() {
    if [[ ${REMEMBER_LAST_ANSWER:="false"} != "true" ]]; then
        verbosity::print_info
        verbosity::print_info "Forgetting last answer from ${LAST_FORCE_ANSWER_FILE}:"
        verbosity::print_info
        rm -f "${LAST_FORCE_ANSWER_FILE}"
    else
        if [[ -f "${LAST_FORCE_ANSWER_FILE}" ]]; then
            verbosity::print_info
            verbosity::print_info "Still remember last answer from ${LAST_FORCE_ANSWER_FILE}:"
            verbosity::print_info "$(cat "${LAST_FORCE_ANSWER_FILE}")"
            verbosity::print_info
        fi
    fi
}

function build_images::confirm_via_terminal() {
    echo >"${DETECTED_TERMINAL}"
    echo >"${DETECTED_TERMINAL}"
    set +u
    if [[ ${#MODIFIED_FILES[@]} != "" ]]; then
        echo "${COLOR_YELLOW}The CI image for Python ${PYTHON_BASE_IMAGE} image likely needs to be rebuild${COLOR_RESET}" >"${DETECTED_TERMINAL}"
        echo "${COLOR_YELLOW}The files were modified since last build: ${MODIFIED_FILES[*]}${COLOR_RESET}" >"${DETECTED_TERMINAL}"
    fi
    if [[ ${ACTION} == "pull and rebuild" ]]; then
        echo "${COLOR_YELLOW}This build involves pull and it might take some time and network to pull the base image first!${COLOR_RESET}" >"${DETECTED_TERMINAL}"
    fi
    set -u
    echo >"${DETECTED_TERMINAL}"
    echo "${COLOR_YELLOW}WARNING!!!!:Make sure that you rebased to latest upstream before rebuilding or the rebuild might take a lot of time!${COLOR_RESET}" >"${DETECTED_TERMINAL}"
    echo >"${DETECTED_TERMINAL}"
    # Make sure to use output of tty rather than stdin/stdout when available - this way confirm
    # will works also in case of pre-commits (git does not pass stdin/stdout to pre-commit hooks)
    # shellcheck disable=SC2094
    "${AIRFLOW_SOURCES}/confirm" "${ACTION} image ${THE_IMAGE_TYPE}-python${PYTHON_MAJOR_MINOR_VERSION}" \
        <"${DETECTED_TERMINAL}" >"${DETECTED_TERMINAL}"
    RES=$?
}

# Confirms if the image should be rebuild and interactively checks it with the user.
# In case iit needs to be rebuild. It only ask the user if it determines that the rebuild
# is needed and that the rebuild is not already forced. It asks the user using available terminals
# So that the script works also from within pre-commit run via git hooks - where stdin is not
# available - it tries to find usable terminal and ask the user via this terminal.
function build_images::confirm_image_rebuild() {
    ACTION="rebuild"
    if [[ ${FORCE_PULL_IMAGES:=} == "true" ]]; then
        ACTION="pull and rebuild"
    fi
    if [[ -f "${LAST_FORCE_ANSWER_FILE}" ]]; then
        # set variable from last answered response given in the same pre-commit run - so that it can be
        # answered in the first pre-commit check (build) and then used in another (mypy/flake8 etc).
        # shellcheck disable=SC1090
        source "${LAST_FORCE_ANSWER_FILE}"
    fi
    set +e
    local RES
    if [[ ${CI:="false"} == "true" ]]; then
        verbosity::print_info
        verbosity::print_info "CI environment - forcing rebuild for image ${THE_IMAGE_TYPE}."
        verbosity::print_info
        RES="0"
    elif [[ -n "${FORCE_ANSWER_TO_QUESTIONS=}" ]]; then
        verbosity::print_info
        verbosity::print_info "Forcing answer '${FORCE_ANSWER_TO_QUESTIONS}'"
        verbosity::print_info
        case "${FORCE_ANSWER_TO_QUESTIONS}" in
        [yY][eE][sS] | [yY])
            RES="0"
            ;;
        [qQ][uU][iI][tT] | [qQ])
            RES="2"
            ;;
        *)
            RES="1"
            ;;
        esac
    elif [[ -t 0 ]]; then
        # Check if this script is run interactively with stdin open and terminal attached
        echo
        set +u
        if [[ ${#MODIFIED_FILES[@]} != "" ]]; then
            echo "${COLOR_YELLOW}The CI image for Python ${PYTHON_BASE_IMAGE} image likely needs to be rebuild${COLOR_RESET}"
            echo "${COLOR_YELLOW}The files were modified since last build: ${MODIFIED_FILES[*]}${COLOR_RESET}"
        fi
        echo
        echo "${COLOR_YELLOW}WARNING!!!!:Make sure that you rebased to latest upstream before rebuilding or the rebuild might take a lot of time!${COLOR_RESET}"
        echo
        set -u
        "${AIRFLOW_SOURCES}/confirm" "${ACTION} image ${THE_IMAGE_TYPE}-python${PYTHON_MAJOR_MINOR_VERSION}"
        RES=$?
    elif [[ ${DETECTED_TERMINAL:=$(tty)} != "not a tty" ]]; then
        export DETECTED_TERMINAL
        build_images::confirm_via_terminal
    elif [[ -c /dev/tty ]]; then
        export DETECTED_TERMINAL=/dev/tty
        build_images::confirm_via_terminal
    else
        verbosity::print_info
        verbosity::print_info "No terminal, no stdin - quitting"
        verbosity::print_info
        # No terminal, no stdin, no force answer - quitting!
        RES="2"
    fi
    set -e
    if [[ ${RES} == "1" ]]; then
        verbosity::print_info
        verbosity::print_info "Skipping rebuilding the image ${THE_IMAGE_TYPE}-python${PYTHON_MAJOR_MINOR_VERSION}"
        verbosity::print_info
        export SKIP_REBUILD="true"
        # Force "no" also to subsequent questions so that if you answer it once, you are not asked
        # For all other pre-commits and you will continue using the images you already have
        export FORCE_ANSWER_TO_QUESTIONS="no"
        echo 'export FORCE_ANSWER_TO_QUESTIONS="no"' >"${LAST_FORCE_ANSWER_FILE}"
    elif [[ ${RES} == "2" ]]; then
        echo
        echo  "${COLOR_RED}ERROR: The ${THE_IMAGE_TYPE} needs to be rebuilt - it is outdated.   ${COLOR_RESET}"
        echo """

   Make sure you build the images by running:

      ./breeze --python ${PYTHON_MAJOR_MINOR_VERSION} build-image

   If you run it via pre-commit as individual hook, you can run 'pre-commit run build'.

"""
        exit 1
    else
        # Force "yes" also to subsequent questions
        export FORCE_ANSWER_TO_QUESTIONS="yes"
    fi
}

function build_images::check_for_docker_context_files() {
    local num_docker_context_files
    local docker_context_files_dir="${AIRFLOW_SOURCES}/docker-context-files/"
    num_docker_context_files=$(find "${docker_context_files_dir}" -type f | grep -c -v "README.md" || true)
    if [[ ${num_docker_context_files} == "0" ]]; then
        if [[ ${INSTALL_FROM_DOCKER_CONTEXT_FILES} != "false" ]]; then
            echo
            echo "${COLOR_YELLOW}ERROR! You want to install packages from docker-context-files${COLOR_RESET}"
            echo "${COLOR_YELLOW}       but there are no packages to install in this folder.${COLOR_RESET}"
            echo
            exit 1
        fi
    else
        if [[ ${INSTALL_FROM_DOCKER_CONTEXT_FILES} == "false" ]]; then
            echo
            echo "${COLOR_YELLOW}ERROR! There are some extra files in docker-context-files except README.md${COLOR_RESET}"
            echo "${COLOR_YELLOW}       And you did not choose --install-from-docker-context-files flag${COLOR_RESET}"
            echo "${COLOR_YELLOW}       This might result in unnecessary cache invalidation and long build times${COLOR_RESET}"
            echo "${COLOR_YELLOW}       Exiting now - please restart the command with --cleanup-docker-context-files switch${COLOR_RESET}"
            echo
            exit 2
        fi
    fi
}

# Builds local image manifest. It contains only one random file generated during Docker.ci build
function build_images::build_ci_image_manifest() {
    docker_v build \
        --tag="${AIRFLOW_CI_LOCAL_MANIFEST_IMAGE}" \
        -f- . <<EOF
FROM scratch
COPY "manifests/local-build-cache-hash-${PYTHON_MAJOR_MINOR_VERSION}" /build-cache-hash
LABEL org.opencontainers.image.source="https://github.com/${GITHUB_REPOSITORY}"
CMD ""
EOF
}

#
# Retrieves information about build cache hash random file from the local image
# The random file is generated during the build and is best indicator whether your local CI image
# has been built using the same pulled image as the remote one
#
function build_images::get_local_build_cache_hash() {
    set +e
    local local_image_build_cache_file
    local_image_build_cache_file="${AIRFLOW_SOURCES}/manifests/local-build-cache-hash-${PYTHON_MAJOR_MINOR_VERSION}"
    # Remove the container just in case
    docker_v rm --force "local-airflow-ci-container" 2>/dev/null >/dev/null
    if ! docker_v inspect "${AIRFLOW_CI_IMAGE_WITH_TAG}" 2>/dev/null >/dev/null; then
        verbosity::print_info
        verbosity::print_info "Local airflow CI image not available"
        verbosity::print_info
        LOCAL_MANIFEST_IMAGE_UNAVAILABLE="true"
        export LOCAL_MANIFEST_IMAGE_UNAVAILABLE
        touch "${local_image_build_cache_file}"
        set -e
        return

    fi
    docker_v create --name "local-airflow-ci-container" "${AIRFLOW_CI_IMAGE_WITH_TAG}" 2>/dev/null >/dev/null
    docker_v cp "local-airflow-ci-container:/build-cache-hash" \
        "${local_image_build_cache_file}" 2>/dev/null ||
        touch "${local_image_build_cache_file}"
    set -e
    verbosity::print_info
    verbosity::print_info "Local build cache hash: '$(cat "${local_image_build_cache_file}")'"
    verbosity::print_info
}

# Retrieves information about the build cache hash random file from the remote image.
# We use manifest image for that, which is a really, really small image to pull!
# The image is a specially prepared manifest image which is built together with the main image and
# pushed with it. This special manifest image is prepared during building of the CI image and contains
# single file which is generated with random content during the docker
# build in the right step of the image build (right after installing all dependencies of Apache Airflow
# for the first time).
# When this random file gets regenerated it means that either base image has changed before that step
# or some of the earlier layers was modified - which means that it is usually faster to pull
# that image first and then rebuild it.
function build_images::get_remote_image_build_cache_hash() {
    set +e
    local remote_image_container_id_file
    remote_image_container_id_file="$(mktemp)"
    local remote_image_build_cache_file
    remote_image_build_cache_file=$(mktemp)
    local target_remote_cache_file="${AIRFLOW_SOURCES}/manifests/remote-build-cache-hash-${PYTHON_MAJOR_MINOR_VERSION}"
    # Pull remote manifest image
    if ! docker_v pull "${AIRFLOW_CI_REMOTE_MANIFEST_IMAGE}" 2>/dev/null >/dev/null; then
        verbosity::print_info
        verbosity::print_info "Remote docker registry unreachable"
        verbosity::print_info
        REMOTE_DOCKER_REGISTRY_UNREACHABLE="true"
        export REMOTE_DOCKER_REGISTRY_UNREACHABLE
        touch "${remote_image_build_cache_file}"
        set -e
        return
    fi
    set -e
    rm -f "${remote_image_container_id_file}"
    # Create container dump out of the manifest image without actually running it
    docker_v create --cidfile "${remote_image_container_id_file}" "${AIRFLOW_CI_REMOTE_MANIFEST_IMAGE}"
    # Extract manifest and store it in local file
    docker_v cp "$(cat "${remote_image_container_id_file}"):/build-cache-hash" \
        "${remote_image_build_cache_file}"
    # The `mv` is an atomic operation so even if we run it in parallel (for example in flake) it will
    # never be empty (happened in the past)
    mv "${remote_image_build_cache_file}" "${target_remote_cache_file}"
    docker_v rm --force "$(cat "${remote_image_container_id_file}")"
    rm -f "${remote_image_container_id_file}"
    verbosity::print_info
    verbosity::print_info "Remote build cache hash: '$(cat "${target_remote_cache_file}")'"
    verbosity::print_info
}

# Compares layers from both remote and local image and set FORCE_PULL_IMAGES to true in case
# The random has in remote image is different than that in the local image
# indicating that it is likely faster to pull the image from cache rather than let the
# image rebuild fully locally
function build_images::compare_local_and_remote_build_cache_hash() {
    set +e
    local local_image_build_cache_file
    local_image_build_cache_file="${AIRFLOW_SOURCES}/manifests/local-build-cache-hash-${PYTHON_MAJOR_MINOR_VERSION}"
    local remote_image_build_cache_file
    remote_image_build_cache_file="${AIRFLOW_SOURCES}/manifests/remote-build-cache-hash-${PYTHON_MAJOR_MINOR_VERSION}"
    local remote_hash
    remote_hash=$(cat "${remote_image_build_cache_file}")
    local local_hash
    local_hash=$(cat "${local_image_build_cache_file}")

    if [[ ${remote_hash} != "${local_hash}" || -z ${local_hash} ]]; then
        echo
        echo
        echo "Your image and the dockerhub have different or missing build cache hashes."
        echo "Local hash: '${local_hash}'. Remote hash: '${remote_hash}'."
        echo
        echo "Forcing pulling the images. It will be faster than rebuilding usually."
        echo "You can avoid it by setting SKIP_CHECK_REMOTE_IMAGE to true"
        echo
        export FORCE_PULL_IMAGES="true"
    else
        echo
        echo "No need to pull the image. Yours and remote cache hashes are the same!"
        echo
    fi
    set -e
}

# Prints summary of the build parameters
function build_images::print_build_info() {
    verbosity::print_info
    verbosity::print_info "Airflow ${AIRFLOW_VERSION} Python: ${PYTHON_MAJOR_MINOR_VERSION}. Image description: ${IMAGE_DESCRIPTION}"
    verbosity::print_info
}

# Retrieves GitHub Container Registry image prefix from repository name
# GitHub Container Registry stores all images at the organization level, they are just
# linked to the repository via docker label - however we assume a convention where we will
# add repository name to organisation separated by '-' and convert everything to lowercase
# this is because in order for it to work for internal PR for users or other organisation's
# repositories, the other organisations and repositories can be uppercase
# container registry image name has to be lowercase
function build_images::get_github_container_registry_image_prefix() {
    echo "${GITHUB_REPOSITORY}" | tr '[:upper:]' '[:lower:]'
}

function build_images::get_docker_cache_image_names() {
    # Python base image to use
    export PYTHON_BASE_IMAGE="python:${PYTHON_MAJOR_MINOR_VERSION}-slim-buster"

    local image_name
    image_name="ghcr.io/$(build_images::get_github_container_registry_image_prefix)"

    # Example:
    #  ghcr.io/apache/airflow/main/python:3.8-slim-buster
    export AIRFLOW_PYTHON_BASE_IMAGE="${image_name}/${BRANCH_NAME}/python:${PYTHON_MAJOR_MINOR_VERSION}-slim-buster"

    # Example:
    #  ghcr.io/apache/airflow/main/ci/python3.8
    export AIRFLOW_CI_IMAGE="${image_name}/${BRANCH_NAME}/ci/python${PYTHON_MAJOR_MINOR_VERSION}"

    # Example:
    #  ghcr.io/apache/airflow/main/ci/python3.8:latest
    #  ghcr.io/apache/airflow/main/ci/python3.8:<COMMIT_SHA>
    export AIRFLOW_CI_IMAGE_WITH_TAG="${image_name}/${BRANCH_NAME}/ci/python${PYTHON_MAJOR_MINOR_VERSION}:${GITHUB_REGISTRY_PULL_IMAGE_TAG}"

    # Example:
    #  local-airflow-ci-manifest/main/python3.8
    export AIRFLOW_CI_LOCAL_MANIFEST_IMAGE="local-airflow-ci-manifest/${BRANCH_NAME}/python${PYTHON_MAJOR_MINOR_VERSION}"

    # Example:
    #  ghcr.io/apache/airflow/main/ci-manifest/python3.8
    export AIRFLOW_CI_REMOTE_MANIFEST_IMAGE="${image_name}/${BRANCH_NAME}/ci-manifest/python${PYTHON_MAJOR_MINOR_VERSION}"

    # File that is touched when the CI image is built for the first time locally
    export BUILT_CI_IMAGE_FLAG_FILE="${BUILD_CACHE_DIR}/${BRANCH_NAME}/.built_${PYTHON_MAJOR_MINOR_VERSION}"

    # Example:
    #  ghcr.io/apache/airflow/main/prod/python3.8
    export AIRFLOW_PROD_IMAGE="${image_name}/${BRANCH_NAME}/prod/python${PYTHON_MAJOR_MINOR_VERSION}"

    # Example:
    #   ghcr.io/apache/airflow/main/prod-build/python3.8
    export AIRFLOW_PROD_BUILD_IMAGE="${image_name}/${BRANCH_NAME}/prod-build/python${PYTHON_MAJOR_MINOR_VERSION}"

    # Kubernetes image to build
    #  ghcr.io/apache/airflow/main/kubernetes/python3.8
    export AIRFLOW_IMAGE_KUBERNETES="${image_name}/${BRANCH_NAME}/kubernetes/python${PYTHON_MAJOR_MINOR_VERSION}"



}

# If GitHub Registry is used, login to the registry using GITHUB_USERNAME and
# GITHUB_TOKEN. We only need to login to docker registry on CI and only when we push
# images. All other images we pull from docker registry are public and we do not need
# to login there.
function build_images::login_to_docker_registry() {
    if [[ "${CI}" == "true" ]]; then
        start_end::group_start "Configure Docker Registry"
        local token="${GITHUB_TOKEN}"
        if [[ -z "${token}" ]]; then
            verbosity::print_info
            verbosity::print_info "Skip logging in to GitHub Registry. No Token available!"
            verbosity::print_info
        elif [[ ${AIRFLOW_LOGIN_TO_GITHUB_REGISTRY=} != "true" ]]; then
            verbosity::print_info
            verbosity::print_info "Skip logging in to GitHub Registry. AIRFLOW_LOGIN_TO_GITHUB_REGISTRY != true"
            verbosity::print_info
        elif [[ -n "${token}" ]]; then
            # logout from the repository first - so that we do not keep us logged in if the token
            # already expired (which can happen if we have a long build running)
            docker_v logout "ghcr.io"
            # The login might succeed or not - in some cases, when we pull public images in forked
            # repos it might fail, but the pulls will continue to work
            echo "${token}" | docker_v login \
                --username "${GITHUB_USERNAME:-apache}" \
                --password-stdin \
                "ghcr.io" || true
        else
            verbosity::print_info "Skip Login to GitHub Container Registry as token is missing"
        fi
        start_end::group_end
    fi
}


# Prepares all variables needed by the CI build. Depending on the configuration used (python version
# DockerHub user etc. the variables are set so that other functions can use those variables.
function build_images::prepare_ci_build() {
    export THE_IMAGE_TYPE="CI"
    export IMAGE_DESCRIPTION="Airflow CI"

    # Those constants depend on the type of image run so they are only made constants here
    export AIRFLOW_EXTRAS="${AIRFLOW_EXTRAS:="${DEFAULT_CI_EXTRAS}"}"
    readonly AIRFLOW_EXTRAS

    sanity_checks::go_to_airflow_sources
    permissions::fix_group_permissions
}

# Only rebuilds CI image if needed. It checks if the docker image build is needed
# because any of the important source files (from scripts/ci/libraries/_initialization.sh) has
# changed or in any of the edge cases (docker image removed, .build cache removed etc.
# In case rebuild is needed, it determines (by comparing layers in local and remote image)
# Whether pull is needed before rebuild.
function build_images::rebuild_ci_image_if_needed() {
    verbosity::print_info
    verbosity::print_info "Checking if pull or just build for ${THE_IMAGE_TYPE} is needed."
    verbosity::print_info
    if [[ -f "${BUILT_CI_IMAGE_FLAG_FILE}" ]]; then
        verbosity::print_info
        verbosity::print_info "${THE_IMAGE_TYPE} image already built locally."
        verbosity::print_info
    else
        verbosity::print_info
        verbosity::print_info "${THE_IMAGE_TYPE} image not built locally: pulling and building"
        verbosity::print_info
        export FORCE_PULL_IMAGES="true"
        export FORCE_BUILD_IMAGES="true"
    fi

    if [[ ${CHECK_IMAGE_FOR_REBUILD} == "false" ]]; then
        verbosity::print_info
        verbosity::print_info "Skip checking for rebuilds of the CI image but checking if it needs to be pulled"
        verbosity::print_info
        push_pull_remove_images::pull_ci_images_if_needed
        return
    fi
    local needs_docker_build="false"
    md5sum::check_if_docker_build_is_needed
    build_images::get_local_build_cache_hash
    if [[ ${needs_docker_build} == "true" ]]; then
        md5sum::check_if_pull_is_needed
        SKIP_REBUILD="false"
        if [[ ${CI:=} != "true" && "${FORCE_BUILD:=}" != "true" ]]; then
            build_images::confirm_image_rebuild
        fi
        if [[ ${SKIP_REBUILD} != "true" ]]; then
            local system
            system=$(uname -s)
            if [[ ${system} != "Darwin" ]]; then
                local root_files_count
                root_files_count=$(find "airflow" "tests" -user root | wc -l | xargs)
                if [[ ${root_files_count} != "0" ]]; then
                    ./scripts/ci/tools/fix_ownership.sh || true
                fi
            fi
            verbosity::print_info
            verbosity::print_info "Build start: ${THE_IMAGE_TYPE} image."
            verbosity::print_info
            build_images::build_ci_image
            build_images::get_local_build_cache_hash
            md5sum::update_all_md5
            build_images::build_ci_image_manifest
            verbosity::print_info
            verbosity::print_info "Build completed: ${THE_IMAGE_TYPE} image."
            verbosity::print_info
        fi
    else
        echo
        echo "${COLOR_GREEN}No need to rebuild the image: none of the important files changed${COLOR_RESET}"
        echo
    fi
}

function build_images::rebuild_ci_image_if_needed_with_group() {
    start_end::group_start "Check if CI image build is needed"
    build_images::rebuild_ci_image_if_needed
    start_end::group_end
}


# Interactive version of confirming the ci image that is used in pre-commits
# it displays additional information - what the user should do in order to bring the local images
# back to state that pre-commit will be happy with
function build_images::rebuild_ci_image_if_needed_and_confirmed() {
    local needs_docker_build="false"
    THE_IMAGE_TYPE="CI"

    md5sum::check_if_docker_build_is_needed

    if [[ ${needs_docker_build} == "true" ]]; then
        md5sum::check_if_pull_is_needed
        verbosity::print_info
        verbosity::print_info "Docker image build is needed!"
        verbosity::print_info
    else
        verbosity::print_info
        verbosity::print_info "Docker image build is not needed!"
        verbosity::print_info
    fi

    if [[ "${needs_docker_build}" == "true" ]]; then
        SKIP_REBUILD="false"
        build_images::confirm_image_rebuild

        if [[ ${SKIP_REBUILD} != "true" ]]; then
            build_images::rebuild_ci_image_if_needed
        fi
    fi
}

# Builds CI image - depending on the caching strategy (pulled, local, disabled) it
# passes the necessary docker build flags via DOCKER_CACHE_CI_DIRECTIVE array
# it also passes the right Build args depending on the configuration of the build
# selected by Breeze flags or environment variables.
function build_images::build_ci_image() {
    local spin_pid
    build_images::print_build_info
    if [[ -n ${DETECTED_TERMINAL=} ]]; then
        echo -n "Preparing ${AIRFLOW_CI_IMAGE}.
        " >"${DETECTED_TERMINAL}"
        spinner::spin "${OUTPUT_LOG}" &
        spin_pid=$!
        # shellcheck disable=SC2064,SC2016
        traps::add_trap '$(kill '${spin_pid}' || true)' EXIT HUP INT TERM
    fi
    push_pull_remove_images::pull_ci_images_if_needed
    if [[ "${DOCKER_CACHE}" == "disabled" ]]; then
        export DOCKER_CACHE_CI_DIRECTIVE=("--no-cache")
    elif [[ "${DOCKER_CACHE}" == "local" ]]; then
        export DOCKER_CACHE_CI_DIRECTIVE=()
    elif [[ "${DOCKER_CACHE}" == "pulled" ]]; then
        export DOCKER_CACHE_CI_DIRECTIVE=(
            "--cache-from" "${AIRFLOW_CI_IMAGE}"
        )
    else
        echo
        echo  "${COLOR_RED}ERROR: The ${DOCKER_CACHE} cache is unknown!  ${COLOR_RESET}"
        echo
        exit 1
    fi
    EXTRA_DOCKER_CI_BUILD_FLAGS=(
    )
    if [[ ${CI} == "true" ]]; then
        EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
            "--build-arg" "PIP_PROGRESS_BAR=off"
        )
    fi
    if [[ -n "${AIRFLOW_CONSTRAINTS_LOCATION}" ]]; then
        EXTRA_DOCKER_CI_BUILD_FLAGS+=(
            "--build-arg" "AIRFLOW_CONSTRAINTS_LOCATION=${AIRFLOW_CONSTRAINTS_LOCATION}"
        )
    fi

    if [[ -n ${spin_pid=} ]]; then
        kill -HUP "${spin_pid}" || true
        wait "${spin_pid}" || true
        echo >"${DETECTED_TERMINAL}"
    fi
    if [[ -n ${DETECTED_TERMINAL=} ]]; then
        echo -n "Preparing ${AIRFLOW_CI_IMAGE}.
        " >"${DETECTED_TERMINAL}"
        spinner::spin "${OUTPUT_LOG}" &
        spin_pid=$!
        # shellcheck disable=SC2064,SC2016
        traps::add_trap '$(kill '${spin_pid}' || true)' EXIT HUP INT TERM
    fi
    if [[ -n ${DETECTED_TERMINAL=} ]]; then
        echo -n "
Docker building ${AIRFLOW_CI_IMAGE}.
" >"${DETECTED_TERMINAL}"
    fi
    set +u

    local additional_dev_args=()
    if [[ -n "${DEV_APT_DEPS}" ]]; then
        additional_dev_args+=("--build-arg" "DEV_APT_DEPS=\"${DEV_APT_DEPS}\"")
    fi
    if [[ -n "${DEV_APT_COMMAND}" ]]; then
        additional_dev_args+=("--build-arg" "DEV_APT_COMMAND=\"${DEV_APT_COMMAND}\"")
    fi

    local additional_runtime_args=()
    if [[ -n "${RUNTIME_APT_DEPS}" ]]; then
        additional_runtime_args+=("--build-arg" "RUNTIME_APT_DEPS=\"${RUNTIME_APT_DEPS}\"")
    fi
    if [[ -n "${RUNTIME_APT_COMMAND}" ]]; then
        additional_runtime_args+=("--build-arg" "RUNTIME_APT_COMMAND=\"${RUNTIME_APT_COMMAND}\"")
    fi
    docker_v build \
        "${EXTRA_DOCKER_CI_BUILD_FLAGS[@]}" \
        --build-arg PYTHON_BASE_IMAGE="${AIRFLOW_PYTHON_BASE_IMAGE}" \
        --build-arg AIRFLOW_VERSION="${AIRFLOW_VERSION}" \
        --build-arg AIRFLOW_BRANCH="${BRANCH_NAME}" \
        --build-arg AIRFLOW_EXTRAS="${AIRFLOW_EXTRAS}" \
        --build-arg AIRFLOW_PRE_CACHED_PIP_PACKAGES="${AIRFLOW_PRE_CACHED_PIP_PACKAGES}" \
        --build-arg ADDITIONAL_AIRFLOW_EXTRAS="${ADDITIONAL_AIRFLOW_EXTRAS}" \
        --build-arg ADDITIONAL_PYTHON_DEPS="${ADDITIONAL_PYTHON_DEPS}" \
        --build-arg ADDITIONAL_DEV_APT_COMMAND="${ADDITIONAL_DEV_APT_COMMAND}" \
        --build-arg ADDITIONAL_DEV_APT_DEPS="${ADDITIONAL_DEV_APT_DEPS}" \
        --build-arg ADDITIONAL_DEV_APT_ENV="${ADDITIONAL_DEV_APT_ENV}" \
        --build-arg ADDITIONAL_RUNTIME_APT_COMMAND="${ADDITIONAL_RUNTIME_APT_COMMAND}" \
        --build-arg ADDITIONAL_RUNTIME_APT_DEPS="${ADDITIONAL_RUNTIME_APT_DEPS}" \
        --build-arg ADDITIONAL_RUNTIME_APT_ENV="${ADDITIONAL_RUNTIME_APT_ENV}" \
        --build-arg UPGRADE_TO_NEWER_DEPENDENCIES="${UPGRADE_TO_NEWER_DEPENDENCIES}" \
        --build-arg CONSTRAINTS_GITHUB_REPOSITORY="${CONSTRAINTS_GITHUB_REPOSITORY}" \
        --build-arg AIRFLOW_CONSTRAINTS_REFERENCE="${DEFAULT_CONSTRAINTS_BRANCH}" \
        --build-arg AIRFLOW_CONSTRAINTS="${AIRFLOW_CONSTRAINTS}" \
        --build-arg AIRFLOW_IMAGE_REPOSITORY="https://github.com/${GITHUB_REPOSITORY}" \
        --build-arg AIRFLOW_IMAGE_DATE_CREATED="$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
        --build-arg BUILD_ID="${CI_BUILD_ID}" \
        --build-arg COMMIT_SHA="${COMMIT_SHA}" \
        "${additional_dev_args[@]}" \
        "${additional_runtime_args[@]}" \
        "${DOCKER_CACHE_CI_DIRECTIVE[@]}" \
        -t "${AIRFLOW_CI_IMAGE}" \
        --target "main" \
        . -f Dockerfile.ci
    set -u
    if [[ -n "${IMAGE_TAG=}" ]]; then
        echo "Tagging additionally image ${AIRFLOW_CI_IMAGE} with ${IMAGE_TAG}"
        docker_v tag "${AIRFLOW_CI_IMAGE}" "${IMAGE_TAG}"
    fi
    if [[ -n ${spin_pid=} ]]; then
        kill -HUP "${spin_pid}" || true
        wait "${spin_pid}" || true
        echo >"${DETECTED_TERMINAL}"
    fi
}

# Prepares all variables needed by the CI build. Depending on the configuration used (python version
# DockerHub user etc. the variables are set so that other functions can use those variables.
function build_images::prepare_prod_build() {
    if [[ -n "${INSTALL_AIRFLOW_REFERENCE=}" ]]; then
        # When --install-airflow-reference is used then the image is build from GitHub tag
        EXTRA_DOCKER_PROD_BUILD_FLAGS=(
            "--build-arg" "AIRFLOW_INSTALLATION_METHOD=https://github.com/apache/airflow/archive/${INSTALL_AIRFLOW_REFERENCE}.tar.gz#egg=apache-airflow"
        )
        export AIRFLOW_VERSION="${INSTALL_AIRFLOW_REFERENCE}"
        build_images::add_build_args_for_remote_install
    elif [[ -n "${INSTALL_AIRFLOW_VERSION=}" ]]; then
        # When --install-airflow-version is used then the image is build using released PIP package
        # For PROD image only numeric versions are allowed and RC candidates
        if [[ ! ${INSTALL_AIRFLOW_VERSION} =~ ^[0-9\.]+((a|b|rc|alpha|beta|pre)[0-9]+)?$ ]]; then
            echo
            echo  "${COLOR_RED}ERROR: Bad value for install-airflow-version: '${INSTALL_AIRFLOW_VERSION}'. Only numerical versions allowed for PROD image here !${COLOR_RESET}"
            echo
            exit 1
        fi
        EXTRA_DOCKER_PROD_BUILD_FLAGS=(
            "--build-arg" "AIRFLOW_INSTALLATION_METHOD=apache-airflow"
            "--build-arg" "AIRFLOW_VERSION_SPECIFICATION===${INSTALL_AIRFLOW_VERSION}"
            "--build-arg" "AIRFLOW_VERSION=${INSTALL_AIRFLOW_VERSION}"
        )
        export AIRFLOW_VERSION="${INSTALL_AIRFLOW_VERSION}"
        export INSTALL_PROVIDERS_FROM_SOURCES="false"
        build_images::add_build_args_for_remote_install
    else
        # When no airflow version/reference is specified, production image is built either from the
        # local sources (in Breeze) or from PyPI (in the ci_scripts)
        # Default values for the variables are set in breeze (breeze defaults) and _initialization.sh (CI ones)
        EXTRA_DOCKER_PROD_BUILD_FLAGS=(
            "--build-arg" "AIRFLOW_SOURCES_FROM=${AIRFLOW_SOURCES_FROM}"
            "--build-arg" "AIRFLOW_SOURCES_TO=${AIRFLOW_SOURCES_TO}"
            "--build-arg" "AIRFLOW_INSTALLATION_METHOD=${AIRFLOW_INSTALLATION_METHOD}"
            "--build-arg" "AIRFLOW_CONSTRAINTS_REFERENCE=${DEFAULT_CONSTRAINTS_BRANCH}"
        )
    fi
    export THE_IMAGE_TYPE="PROD"
    export IMAGE_DESCRIPTION="Airflow production"

    # Those constants depend on the type of image run so they are only made constants here
    export AIRFLOW_EXTRAS="${AIRFLOW_EXTRAS:="${DEFAULT_PROD_EXTRAS}"}"
    readonly AIRFLOW_EXTRAS

    AIRFLOW_BRANCH_FOR_PYPI_PRELOADING="${BRANCH_NAME}"
    sanity_checks::go_to_airflow_sources
}

# Builds PROD image - depending on the caching strategy (pulled, local, disabled) it
# passes the necessary docker build flags via DOCKER_CACHE_PROD_DIRECTIVE and
# DOCKER_CACHE_PROD_BUILD_DIRECTIVE (separate caching options are needed for "build" segment of the image)
# it also passes the right Build args depending on the configuration of the build
# selected by Breeze flags or environment variables.
function build_images::build_prod_images() {
    build_images::print_build_info

    if [[ ${SKIP_BUILDING_PROD_IMAGE} == "true" ]]; then
        echo
        echo "${COLOR_YELLOW}Skip building production image. Assume the one we have is good!${COLOR_RESET}"
        echo "${COLOR_YELLOW}You must run './breeze build-image --production-image before for all python versions!${COLOR_RESET}"
        echo
        return
    fi

    push_pull_remove_images::pull_prod_images_if_needed

    if [[ "${DOCKER_CACHE}" == "disabled" ]]; then
        export DOCKER_CACHE_PROD_DIRECTIVE=("--cache-from" "${AIRFLOW_PROD_BUILD_IMAGE}")
        export DOCKER_CACHE_PROD_BUILD_DIRECTIVE=("--no-cache")
    elif [[ "${DOCKER_CACHE}" == "local" ]]; then
        export DOCKER_CACHE_PROD_DIRECTIVE=()
        export DOCKER_CACHE_PROD_BUILD_DIRECTIVE=()
    elif [[ "${DOCKER_CACHE}" == "pulled" ]]; then
        export DOCKER_CACHE_PROD_DIRECTIVE=(
            "--cache-from" "${AIRFLOW_PROD_BUILD_IMAGE}"
            "--cache-from" "${AIRFLOW_PROD_IMAGE}"
        )
        export DOCKER_CACHE_PROD_BUILD_DIRECTIVE=(
            "--cache-from" "${AIRFLOW_PROD_BUILD_IMAGE}"
        )
    else
        echo
        echo  "${COLOR_RED}ERROR: The ${DOCKER_CACHE} cache is unknown  ${COLOR_RESET}"
        echo
        echo
        exit 1
    fi
    set +u
    local additional_dev_args=()
    if [[ -n "${DEV_APT_DEPS}" ]]; then
        additional_dev_args+=("--build-arg" "DEV_APT_DEPS=\"${DEV_APT_DEPS}\"")
    fi
    if [[ -n "${DEV_APT_COMMAND}" ]]; then
        additional_dev_args+=("--build-arg" "DEV_APT_COMMAND=\"${DEV_APT_COMMAND}\"")
    fi
    docker_v build \
        "${EXTRA_DOCKER_PROD_BUILD_FLAGS[@]}" \
        --build-arg PYTHON_BASE_IMAGE="${AIRFLOW_PYTHON_BASE_IMAGE}" \
        --build-arg INSTALL_MYSQL_CLIENT="${INSTALL_MYSQL_CLIENT}" \
        --build-arg INSTALL_MSSQL_CLIENT="${INSTALL_MSSQL_CLIENT}" \
        --build-arg AIRFLOW_VERSION="${AIRFLOW_VERSION}" \
        --build-arg AIRFLOW_BRANCH="${AIRFLOW_BRANCH_FOR_PYPI_PRELOADING}" \
        --build-arg AIRFLOW_EXTRAS="${AIRFLOW_EXTRAS}" \
        --build-arg ADDITIONAL_AIRFLOW_EXTRAS="${ADDITIONAL_AIRFLOW_EXTRAS}" \
        --build-arg ADDITIONAL_PYTHON_DEPS="${ADDITIONAL_PYTHON_DEPS}" \
        "${additional_dev_args[@]}" \
        --build-arg INSTALL_PROVIDERS_FROM_SOURCES="${INSTALL_PROVIDERS_FROM_SOURCES}" \
        --build-arg ADDITIONAL_DEV_APT_COMMAND="${ADDITIONAL_DEV_APT_COMMAND}" \
        --build-arg ADDITIONAL_DEV_APT_DEPS="${ADDITIONAL_DEV_APT_DEPS}" \
        --build-arg ADDITIONAL_DEV_APT_ENV="${ADDITIONAL_DEV_APT_ENV}" \
        --build-arg AIRFLOW_PRE_CACHED_PIP_PACKAGES="${AIRFLOW_PRE_CACHED_PIP_PACKAGES}" \
        --build-arg INSTALL_FROM_PYPI="${INSTALL_FROM_PYPI}" \
        --build-arg INSTALL_FROM_DOCKER_CONTEXT_FILES="${INSTALL_FROM_DOCKER_CONTEXT_FILES}" \
        --build-arg UPGRADE_TO_NEWER_DEPENDENCIES="${UPGRADE_TO_NEWER_DEPENDENCIES}" \
        --build-arg BUILD_ID="${CI_BUILD_ID}" \
        --build-arg COMMIT_SHA="${COMMIT_SHA}" \
        --build-arg CONSTRAINTS_GITHUB_REPOSITORY="${CONSTRAINTS_GITHUB_REPOSITORY}" \
        --build-arg AIRFLOW_CONSTRAINTS="${AIRFLOW_CONSTRAINTS}" \
        --build-arg AIRFLOW_IMAGE_REPOSITORY="https://github.com/${GITHUB_REPOSITORY}" \
        --build-arg AIRFLOW_IMAGE_DATE_CREATED="$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
        "${DOCKER_CACHE_PROD_BUILD_DIRECTIVE[@]}" \
        -t "${AIRFLOW_PROD_BUILD_IMAGE}" \
        --target "airflow-build-image" \
        . -f Dockerfile
    local additional_runtime_args=()
    if [[ -n "${RUNTIME_APT_DEPS}" ]]; then
        additional_runtime_args+=("--build-arg" "RUNTIME_APT_DEPS=\"${RUNTIME_APT_DEPS}\"")
    fi
    if [[ -n "${RUNTIME_APT_COMMAND}" ]]; then
        additional_runtime_args+=("--build-arg" "RUNTIME_APT_COMMAND=\"${RUNTIME_APT_COMMAND}\"")
    fi
    docker_v build \
        "${EXTRA_DOCKER_PROD_BUILD_FLAGS[@]}" \
        --build-arg PYTHON_BASE_IMAGE="${AIRFLOW_PYTHON_BASE_IMAGE}" \
        --build-arg INSTALL_MYSQL_CLIENT="${INSTALL_MYSQL_CLIENT}" \
        --build-arg INSTALL_MSSQL_CLIENT="${INSTALL_MSSQL_CLIENT}" \
        --build-arg ADDITIONAL_AIRFLOW_EXTRAS="${ADDITIONAL_AIRFLOW_EXTRAS}" \
        --build-arg ADDITIONAL_PYTHON_DEPS="${ADDITIONAL_PYTHON_DEPS}" \
        --build-arg INSTALL_PROVIDERS_FROM_SOURCES="${INSTALL_PROVIDERS_FROM_SOURCES}" \
        --build-arg ADDITIONAL_DEV_APT_COMMAND="${ADDITIONAL_DEV_APT_COMMAND}" \
        --build-arg ADDITIONAL_DEV_APT_DEPS="${ADDITIONAL_DEV_APT_DEPS}" \
        --build-arg ADDITIONAL_DEV_APT_ENV="${ADDITIONAL_DEV_APT_ENV}" \
        --build-arg ADDITIONAL_RUNTIME_APT_COMMAND="${ADDITIONAL_RUNTIME_APT_COMMAND}" \
        --build-arg ADDITIONAL_RUNTIME_APT_DEPS="${ADDITIONAL_RUNTIME_APT_DEPS}" \
        --build-arg ADDITIONAL_RUNTIME_APT_ENV="${ADDITIONAL_RUNTIME_APT_ENV}" \
        --build-arg AIRFLOW_PRE_CACHED_PIP_PACKAGES="${AIRFLOW_PRE_CACHED_PIP_PACKAGES}" \
        --build-arg INSTALL_FROM_PYPI="${INSTALL_FROM_PYPI}" \
        --build-arg INSTALL_FROM_DOCKER_CONTEXT_FILES="${INSTALL_FROM_DOCKER_CONTEXT_FILES}" \
        --build-arg UPGRADE_TO_NEWER_DEPENDENCIES="${UPGRADE_TO_NEWER_DEPENDENCIES}" \
        --build-arg AIRFLOW_VERSION="${AIRFLOW_VERSION}" \
        --build-arg AIRFLOW_BRANCH="${AIRFLOW_BRANCH_FOR_PYPI_PRELOADING}" \
        --build-arg AIRFLOW_EXTRAS="${AIRFLOW_EXTRAS}" \
        --build-arg BUILD_ID="${CI_BUILD_ID}" \
        --build-arg COMMIT_SHA="${COMMIT_SHA}" \
        --build-arg CONSTRAINTS_GITHUB_REPOSITORY="${CONSTRAINTS_GITHUB_REPOSITORY}" \
        --build-arg AIRFLOW_CONSTRAINTS="${AIRFLOW_CONSTRAINTS}" \
        --build-arg AIRFLOW_IMAGE_REPOSITORY="https://github.com/${GITHUB_REPOSITORY}" \
        --build-arg AIRFLOW_IMAGE_DATE_CREATED="$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
        "${additional_dev_args[@]}" \
        "${additional_runtime_args[@]}" \
        "${DOCKER_CACHE_PROD_DIRECTIVE[@]}" \
        -t "${AIRFLOW_PROD_IMAGE}" \
        --target "main" \
        . -f Dockerfile
    set -u
    if [[ -n "${IMAGE_TAG=}" ]]; then
        echo "Tagging additionally image ${AIRFLOW_PROD_IMAGE} with ${IMAGE_TAG}"
        docker_v tag "${AIRFLOW_PROD_IMAGE}" "${IMAGE_TAG}"
    fi
}

# Tags source image with names provided
# $1 source image
# $2, $3 - target image names
function build_images::tag_image() {
    local source_image_name="$1"
    shift
    local target_image_name
    for target_image_name in "${@}"; do
        echo
        echo "Tagging ${source_image_name} as ${target_image_name}."
        echo
        docker_v tag "${source_image_name}" "${target_image_name}"
    done
}

# We use pulled docker image cache by default for CI images to speed up the builds
# and local to speed up iteration on kerberos tests
function build_images::determine_docker_cache_strategy() {
    if [[ -z "${DOCKER_CACHE=}" ]]; then
        if [[ "${PRODUCTION_IMAGE}" == "true" ]]; then
            export DOCKER_CACHE="local"
        else
            export DOCKER_CACHE="pulled"
        fi
    fi
    verbosity::print_info
    verbosity::print_info "Using ${DOCKER_CACHE} cache strategy for the build."
    verbosity::print_info
}


function build_images::assert_variable() {
    local variable_name="${1}"
    local expected_value="${2}"
    local variable_value=${!variable_name}
    if [[ ${variable_value} != "${expected_value}" ]]; then
        echo
        echo  "${COLOR_RED}ERROR: Variable ${variable_name}: expected_value: '${expected_value}' but was '${variable_value}'!${COLOR_RESET}"
        echo
        exit 1
    fi
}

function build_images::cleanup_dist() {
    mkdir -pv "${AIRFLOW_SOURCES}/dist"
    rm -f "${AIRFLOW_SOURCES}/dist/"*.{whl,tar.gz}
}


function build_images::cleanup_docker_context_files() {
    mkdir -pv "${AIRFLOW_SOURCES}/docker-context-files"
    rm -f "${AIRFLOW_SOURCES}/docker-context-files/"*.{whl,tar.gz}
}

function build_images::build_prod_images_from_locally_built_airflow_packages() {
    # We do not install from PyPI
    build_images::assert_variable INSTALL_FROM_PYPI "false"
    # But then we reinstall airflow and providers from prepared packages in the docker context files
    build_images::assert_variable INSTALL_FROM_DOCKER_CONTEXT_FILES "true"
    # But we install everything from scratch to make a "clean" installation in case any dependencies got removed
    build_images::assert_variable AIRFLOW_PRE_CACHED_PIP_PACKAGES "false"

    build_images::cleanup_dist
    build_images::cleanup_docker_context_files

    # Build necessary provider packages
    IFS=$'\n' read -d '' -r -a installed_providers < "${AIRFLOW_SOURCES}/scripts/ci/installed_providers.txt" || true
    runs::run_prepare_provider_packages "${installed_providers[@]}"
    mv "${AIRFLOW_SOURCES}/dist/"* "${AIRFLOW_SOURCES}/docker-context-files/"

    # Build apache airflow packages
    runs::run_prepare_airflow_packages
    mv "${AIRFLOW_SOURCES}/dist/"* "${AIRFLOW_SOURCES}/docker-context-files/"

    build_images::build_prod_images
}

# Useful information for people who stumble upon a pip check failure
function build_images::inform_about_pip_check() {
    echo """
${COLOR_BLUE}***** Beginning of the instructions ****${COLOR_RESET}

The image did not pass 'pip check' verification. This means that there are some conflicting dependencies
in the image.

It can mean one of those:

1) The main is currently broken (other PRs will fail with the same error)
2) You changed some dependencies in setup.py or setup.cfg and they are conflicting.



In case 1) - apologies for the trouble.Please let committers know and they will fix it. You might
be asked to rebase to the latest main after the problem is fixed.

In case 2) - Follow the steps below:

* try to build CI and then PROD image locally with breeze, adding --upgrade-to-newer-dependencies flag
  (repeat it for all python versions)

CI image:

${COLOR_BLUE}
     ./breeze build-image --upgrade-to-newer-dependencies --python 3.6
${COLOR_RESET}

Production image:

${COLOR_BLUE}
     ./breeze build-image --production-image --upgrade-to-newer-dependencies --python 3.6
${COLOR_RESET}

* You will see error messages there telling which requirements are conflicting and which packages caused the
  conflict. Add the limitation that caused the conflict to EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS
  variable in Dockerfile.ci. Note that the limitations might be different for Dockerfile.ci and Dockerfile
  because not all packages are installed by default in the PROD Dockerfile. So you might find that you
  only need to add the limitation to the Dockerfile.ci

${COLOR_BLUE}***** End of the instructions ****${COLOR_RESET}

"""
}
