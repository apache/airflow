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

# Needs to be declared outside of function for MacOS

BUILD_COMMAND=()

# For remote installation of airflow (from GitHub or PyPI) when building the image, you need to
# pass build flags depending on the version and method of the installation (for example to
# get proper requirement constraint files)
function build_images::add_build_args_for_remote_install() {
    # entrypoint is used as AIRFLOW_SOURCES_(WWW)_FROM/TO in order to avoid costly copying of all sources of
    # Airflow - those are not needed for remote install at all. Entrypoint is later overwritten by
    EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
        "--build-arg" "AIRFLOW_SOURCES_WWW_FROM=empty"
        "--build-arg" "AIRFLOW_SOURCES_WWW_TO=/empty"
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


function build_images::reconfirm_rebuilding_if_not_rebased() {
    local latest_main_commit_sha
    latest_main_commit_sha=$(curl -s -H "Accept: application/vnd.github.VERSION.sha" \
        "https://api.github.com/repos/${GITHUB_REPOSITORY}/commits/${DEFAULT_BRANCH}")
    if [[ "$(git log --format=format:%H | grep -c "${latest_main_commit_sha}")" == "0" ]]; then
         echo
         echo "${COLOR_YELLOW}WARNING!!!!:You are not rebased on top of the latest ${DEFAULT_BRANCH} branch of the airflow repo.${COLOR_RESET}"
         echo "${COLOR_YELLOW}The rebuild might take a lot of time and you might need to do it again${COLOR_RESET}"
         echo
         echo "${COLOR_YELLOW}It is STRONGLY RECOMMENDED that you rebase your code first!${COLOR_RESET}"
         echo
         "${AIRFLOW_SOURCES}/confirm" "You are really sure you want to rebuild ${THE_IMAGE_TYPE}-python${PYTHON_MAJOR_MINOR_VERSION}"
         RES=$?
    fi
}

function build_images::print_modified_files() {
    echo "${MODIFIED_FILES[@]}" | xargs -n 1 echo " * "
}

function build_images::encourage_rebuilding_on_modified_files() {
    echo
    set +u
    if [[ ${#MODIFIED_FILES[@]} != "" ]]; then
        echo
        echo "${COLOR_YELLOW}The CI image for Python ${PYTHON_MAJOR_MINOR_VERSION} image might be outdated${COLOR_RESET}"
        echo
        echo "${COLOR_BLUE}Please run this command at earliest convenience: ${COLOR_RESET}"
        echo
        echo "${COLOR_YELLOW}./breeze build-image --python ${PYTHON_MAJOR_MINOR_VERSION}${COLOR_RESET}"
        echo
    fi
}

function build_images::confirm_rebuilding_on_modified_files() {
    echo
    set +u
    if [[ ${#MODIFIED_FILES[@]} != "" ]]; then
        echo "${COLOR_BLUE}The CI image for Python ${PYTHON_MAJOR_MINOR_VERSION} image likely needs to be rebuild${COLOR_RESET}"
        echo "${COLOR_BLUE}The files were modified since last build:${COLOR_RESET}"
        echo
        echo "${COLOR_BLUE}$(build_images::print_modified_files)${COLOR_RESET}"
        echo
    fi
    set -u
    # Make sure to use output of tty rather than stdin/stdout when available - this way confirm
    # will works also in case of pre-commits (git does not pass stdin/stdout to pre-commit hooks)
    # shellcheck disable=SC2094
    "${AIRFLOW_SOURCES}/confirm" "PULL & BUILD the image ${THE_IMAGE_TYPE}-python${PYTHON_MAJOR_MINOR_VERSION}"
    RES=$?
    if [[ ${RES} == "0" ]]; then
        build_images::reconfirm_rebuilding_if_not_rebased
    fi
}

# Confirms if the image should be rebuilt and interactively checks it with the user.
# In case iit needs to be rebuilt. It only ask the user if it determines that the rebuild
# is needed and that the rebuild is not already forced. It asks the user using available terminals
# So that the script works also from within pre-commit run via git hooks - where stdin is not
# available - it tries to find usable terminal and ask the user via this terminal.
function build_images::confirm_image_rebuild() {
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
        verbosity::print_info "CI environment - forcing pull and rebuild for image ${THE_IMAGE_TYPE}."
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
         build_images::confirm_rebuilding_on_modified_files
    elif [[ ${DETECTED_TERMINAL:=$(tty)} != "not a tty" ]]; then
        export DETECTED_TERMINAL
        # shellcheck disable=SC2094
        build_images::encourage_rebuilding_on_modified_files >"${DETECTED_TERMINAL}" <"${DETECTED_TERMINAL}"
        RES=1
    elif [[ -c /dev/tty ]]; then
        export DETECTED_TERMINAL=/dev/tty
        # shellcheck disable=SC2094
        build_images::encourage_rebuilding_on_modified_files >"${DETECTED_TERMINAL}" <"${DETECTED_TERMINAL}"
        RES=1
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
    #  ghcr.io/apache/airflow/main/ci/python3.8
    export AIRFLOW_CI_IMAGE="${image_name}/${BRANCH_NAME}/ci/python${PYTHON_MAJOR_MINOR_VERSION}"

    # Example:
    #  ghcr.io/apache/airflow/main/ci/python3.8:latest
    #  ghcr.io/apache/airflow/main/ci/python3.8:<COMMIT_SHA>
    export AIRFLOW_CI_IMAGE_WITH_TAG="${image_name}/${BRANCH_NAME}/ci/python${PYTHON_MAJOR_MINOR_VERSION}:${GITHUB_REGISTRY_PULL_IMAGE_TAG}"

    # File that is touched when the CI image is built for the first time locally
    export BUILT_CI_IMAGE_FLAG_FILE="${BUILD_CACHE_DIR}/${BRANCH_NAME}/.built_${PYTHON_MAJOR_MINOR_VERSION}"

    # Example:
    #  ghcr.io/apache/airflow/main/prod/python3.8
    export AIRFLOW_PROD_IMAGE="${image_name}/${BRANCH_NAME}/prod/python${PYTHON_MAJOR_MINOR_VERSION}"

    # Kubernetes image to build
    #  ghcr.io/apache/airflow/main/kubernetes/python3.8
    export AIRFLOW_IMAGE_KUBERNETES="${image_name}/${BRANCH_NAME}/kubernetes/python${PYTHON_MAJOR_MINOR_VERSION}"
}

function build_images::check_if_buildx_plugin_available() {
    local buildx_version
    buildx_version=$(docker buildx version 2>/dev/null || true)
    if [[ ${buildx_version} != "" ]]; then
        if [[ ${PREPARE_BUILDX_CACHE} == "true" ]]; then
            BUILD_COMMAND+=("buildx" "build" "--builder" "airflow_cache" "--progress=tty")
            docker_v buildx inspect airflow_cache || docker_v buildx create --name airflow_cache
        else
            BUILD_COMMAND+=("buildx" "build" "--builder" "default" "--progress=tty")
        fi
    else
        if [[ ${PREPARE_BUILDX_CACHE} == "true" ]]; then
            echo
            echo "${COLOR_RED}Buildx cli plugin is not available and you need it to prepare buildx cache.${COLOR_RESET}"
            echo "${COLOR_RED}Please install it following https://docs.docker.com/buildx/working-with-buildx/${COLOR_RESET}"
            echo
            exit 1
        fi
        BUILD_COMMAND+=("build")
    fi
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
    if [[ -f "${BUILT_CI_IMAGE_FLAG_FILE}" ]]; then
        verbosity::print_info
        verbosity::print_info "CI image already built locally."
        verbosity::print_info
    else
        verbosity::print_info
        verbosity::print_info "CI image not built locally: force pulling and building"
        verbosity::print_info
        export FORCE_BUILD="true"
    fi
    local needs_docker_build="false"
    md5sum::check_if_docker_build_is_needed
    if [[ ${needs_docker_build} == "true" ]]; then
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
            md5sum::update_all_md5
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

# Builds CI image - depending on the caching strategy (pulled, local, disabled) it
# passes the necessary docker build flags via docker_ci_cache_directive array
# it also passes the right Build args depending on the configuration of the build
# selected by Breeze flags or environment variables.
function build_images::build_ci_image() {
    build_images::check_if_buildx_plugin_available
    build_images::print_build_info
    local docker_ci_cache_directive
    if [[ "${DOCKER_CACHE}" == "disabled" ]]; then
        docker_ci_cache_directive=("--no-cache")
    elif [[ "${DOCKER_CACHE}" == "local" ]]; then
        docker_ci_cache_directive=()
    elif [[ "${DOCKER_CACHE}" == "pulled" ]]; then
        docker_ci_cache_directive=(
            "--cache-from=${AIRFLOW_CI_IMAGE}:cache"
        )
    else
        echo
        echo  "${COLOR_RED}ERROR: The ${DOCKER_CACHE} cache is unknown!  ${COLOR_RESET}"
        echo
        exit 1
    fi
    if [[ ${PREPARE_BUILDX_CACHE} == "true" ]]; then
        docker_ci_cache_directive+=(
            "--cache-to=type=registry,ref=${AIRFLOW_CI_IMAGE}:cache"
            "--load"
        )
    fi
    local extra_docker_ci_flags=()
    if [[ ${CI} == "true" ]]; then
        EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
            "--build-arg" "PIP_PROGRESS_BAR=off"
        )
    fi
    if [[ -n "${AIRFLOW_CONSTRAINTS_LOCATION}" ]]; then
        extra_docker_ci_flags+=(
            "--build-arg" "AIRFLOW_CONSTRAINTS_LOCATION=${AIRFLOW_CONSTRAINTS_LOCATION}"
        )
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
    docker_v "${BUILD_COMMAND[@]}" \
        "${extra_docker_ci_flags[@]}" \
        --pull \
        --build-arg PYTHON_BASE_IMAGE="${PYTHON_BASE_IMAGE}" \
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
        "${docker_ci_cache_directive[@]}" \
        -t "${AIRFLOW_CI_IMAGE}" \
        --target "main" \
        . -f Dockerfile.ci
    set -u
    if [[ -n "${IMAGE_TAG=}" ]]; then
        echo "Tagging additionally image ${AIRFLOW_CI_IMAGE} with ${IMAGE_TAG}"
        docker_v tag "${AIRFLOW_CI_IMAGE}" "${IMAGE_TAG}"
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
            "--build-arg" "AIRFLOW_SOURCES_WWW_FROM=${AIRFLOW_SOURCES_WWW_FROM}"
            "--build-arg" "AIRFLOW_SOURCES_WWW_TO=${AIRFLOW_SOURCES_WWW_TO}"
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
# docker_cache_prod_build_directive (separate caching options are needed for "build" segment of the image)
# it also passes the right Build args depending on the configuration of the build
# selected by Breeze flags or environment variables.
function build_images::build_prod_images() {
    build_images::check_if_buildx_plugin_available
    build_images::print_build_info

    if [[ ${SKIP_BUILDING_PROD_IMAGE} == "true" ]]; then
        echo
        echo "${COLOR_YELLOW}Skip building production image. Assume the one we have is good!${COLOR_RESET}"
        echo "${COLOR_YELLOW}You must run './breeze build-image --production-image before for all python versions!${COLOR_RESET}"
        echo
        return
    fi
    local docker_cache_prod_directive
    if [[ "${DOCKER_CACHE}" == "disabled" ]]; then
        docker_cache_prod_directive=("--no-cache")
    elif [[ "${DOCKER_CACHE}" == "local" ]]; then
        docker_cache_prod_directive=()
    elif [[ "${DOCKER_CACHE}" == "pulled" ]]; then
        docker_cache_prod_directive=(
            "--cache-from=${AIRFLOW_PROD_IMAGE}:cache"
        )
    else
        echo
        echo  "${COLOR_RED}ERROR: The ${DOCKER_CACHE} cache is unknown  ${COLOR_RESET}"
        echo
        echo
        exit 1
    fi
    if [[ ${PREPARE_BUILDX_CACHE} == "true" ]]; then
        # Cache for prod image contains also build stage for buildx when mode=max specified!
        docker_cache_prod_directive+=(
            "--cache-to=type=registry,ref=${AIRFLOW_PROD_IMAGE}:cache,mode=max"
            "--load"
        )
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
    docker_v "${BUILD_COMMAND[@]}" \
        "${EXTRA_DOCKER_PROD_BUILD_FLAGS[@]}" \
        --pull \
        --build-arg PYTHON_BASE_IMAGE="${PYTHON_BASE_IMAGE}" \
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
        "${docker_cache_prod_directive[@]}" \
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
        export DOCKER_CACHE="pulled"
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
