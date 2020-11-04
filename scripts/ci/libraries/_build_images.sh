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

# For remote installation of airflow (from GitHub or Pypi) when building the image, you need to
# pass build flags depending on the version and method of the installation (for example to
# get proper requirement constraint files)
function build_images::add_build_args_for_remote_install() {
    # entrypoint is used as AIRFLOW_SOURCES_FROM/TO in order to avoid costly copying of all sources of
    # Airflow - those are not needed for remote install at all. Entrypoint is later overwritten by
    EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
        "--build-arg" "AIRFLOW_SOURCES_FROM=empty"
        "--build-arg" "AIRFLOW_SOURCES_TO=/empty"
    )
    if [[ ${AIRFLOW_CONSTRAINTS_REFERENCE} != "" ]]; then
        EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
            "--build-arg" "AIRFLOW_CONSTRAINTS_REFERENCE=${AIRFLOW_CONSTRAINTS_REFERENCE}"
        )
    fi
    if [[ "${AIRFLOW_CONSTRAINTS_LOCATION}" != "" ]]; then
        EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
            "--build-arg" "AIRFLOW_CONSTRAINTS_LOCATION=${AIRFLOW_CONSTRAINTS_LOCATION}"
        )
    fi
    if [[ ${AIRFLOW_VERSION} =~ [^0-9]*1[^0-9]*10[^0-9]([0-9]*) ]]; then
        # All types of references/versions match this regexp for 1.10 series
        # for example v1_10_test, 1.10.10, 1.10.9 etc. ${BASH_REMATCH[1]} matches last
        # minor digit of version and it's length is 0 for v1_10_test, 1 for 1.10.9 and 2 for 1.10.10+
        AIRFLOW_MINOR_VERSION_NUMBER=${BASH_REMATCH[1]}
        if [[ ${#AIRFLOW_MINOR_VERSION_NUMBER} == "0" ]]; then
            # For v1_10_* branches use constraints-1-10 branch
            EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
                "--build-arg" "AIRFLOW_CONSTRAINTS_REFERENCE=constraints-1-10"
            )
        else
            EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
                # For specified minor version of 1.10 use specific reference constraints
                "--build-arg" "AIRFLOW_CONSTRAINTS_REFERENCE=constraints-${AIRFLOW_VERSION}"
            )
        fi
        AIRFLOW_BRANCH_FOR_PYPI_PRELOADING="v1-10-test"
    else
        # For all other (master, 2.0+) we just get the default constraint branch
        EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
            "--build-arg" "AIRFLOW_CONSTRAINTS_REFERENCE=${DEFAULT_CONSTRAINTS_BRANCH}"
        )
        AIRFLOW_BRANCH_FOR_PYPI_PRELOADING=${DEFAULT_BRANCH}
    fi
}

# Retrieves version of airflow stored in the production image (used to display the actual
# Version we use if it was build from PyPI or GitHub
function build_images::get_airflow_version_from_production_image() {
    VERBOSE="false" docker run --entrypoint /bin/bash "${AIRFLOW_PROD_IMAGE}" -c 'echo "${AIRFLOW_VERSION}"'
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

# Confirms if hte image should be rebuild and interactively checks it with the user.
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
        # answered in teh first pre-commit check (build) and then used in another (pylint/mypy/flake8 etc).
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
        "${AIRFLOW_SOURCES}/confirm" "${ACTION} image ${THE_IMAGE_TYPE}-python${PYTHON_MAJOR_MINOR_VERSION}"
        RES=$?
    elif [[ ${DETECTED_TERMINAL:=$(tty)} != "not a tty" ]]; then
        # Make sure to use output of tty rather than stdin/stdout when available - this way confirm
        # will works also in case of pre-commits (git does not pass stdin/stdout to pre-commit hooks)
        # shellcheck disable=SC2094
        "${AIRFLOW_SOURCES}/confirm" "${ACTION} image ${THE_IMAGE_TYPE}-python${PYTHON_MAJOR_MINOR_VERSION}" \
            <"${DETECTED_TERMINAL}" >"${DETECTED_TERMINAL}"
        RES=$?
        export DETECTED_TERMINAL
    elif [[ -c /dev/tty ]]; then
        export DETECTED_TERMINAL=/dev/tty
        # Make sure to use /dev/tty first rather than stdin/stdout when available - this way confirm
        # will works also in case of pre-commits (git does not pass stdin/stdout to pre-commit hooks)
        # shellcheck disable=SC2094
        "${AIRFLOW_SOURCES}/confirm" "${ACTION} image ${THE_IMAGE_TYPE}-python${PYTHON_MAJOR_MINOR_VERSION}" \
            <"${DETECTED_TERMINAL}" >"${DETECTED_TERMINAL}"
        RES=$?
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
        echo >&2
        echo >&2 "ERROR: The ${THE_IMAGE_TYPE} needs to be rebuilt - it is outdated. "
        echo >&2 "   Make sure you build the images bu running"
        echo >&2
        echo >&2 "      ./breeze --python ${PYTHON_MAJOR_MINOR_VERSION}" build-image
        echo >&2
        echo >&2 "   If you run it via pre-commit as individual hook, you can run 'pre-commit run build'."
        echo >&2
        exit 1
    else
        # Force "yes" also to subsequent questions
        export FORCE_ANSWER_TO_QUESTIONS="yes"
    fi
}

# Builds local image manifest
# It contains only one .json file - result of docker inspect - describing the image
# We cannot use docker registry APIs as they are available only with authorisation
# But this image can be pulled without authentication
function build_images::build_ci_image_manifest() {
    docker inspect "${AIRFLOW_CI_IMAGE}" >"manifests/${AIRFLOW_CI_BASE_TAG}.json"
    docker build \
        --build-arg AIRFLOW_CI_BASE_TAG="${AIRFLOW_CI_BASE_TAG}" \
        --tag="${AIRFLOW_CI_LOCAL_MANIFEST_IMAGE}" \
        -f- . <<EOF
ARG AIRFLOW_CI_BASE_TAG
FROM scratch

COPY "manifests/${AIRFLOW_CI_BASE_TAG}.json" .

CMD ""
EOF
}

#
# Retrieves information about layers in the local IMAGE
# it stores list of SHAs of image layers in the file pointed at by TMP_MANIFEST_LOCAL_SHA
#
function build_images::get_local_image_info() {
    TMP_MANIFEST_LOCAL_JSON=$(mktemp)
    TMP_MANIFEST_LOCAL_SHA=$(mktemp)
    set +e
    # Remove the container just in case
    docker rm --force "local-airflow-manifest" 2>/dev/null >/dev/null
    # Create manifest from the local manifest image
    if ! docker create --name "local-airflow-manifest" "${AIRFLOW_CI_LOCAL_MANIFEST_IMAGE}" 2>/dev/null; then
        echo
        echo "Local manifest image not available"
        echo
        LOCAL_MANIFEST_IMAGE_UNAVAILABLE="true"
        return
    fi
    # Create manifest from the local manifest image
    docker cp "local-airflow-manifest:${AIRFLOW_CI_BASE_TAG}.json" "${TMP_MANIFEST_LOCAL_JSON}"
    sed 's/ *//g' "${TMP_MANIFEST_LOCAL_JSON}" | grep '^"sha256:' >"${TMP_MANIFEST_LOCAL_SHA}"
    docker rm --force "local-airflow-manifest" 2>/dev/null >/dev/null
    set -e
}

#
# Retrieves information about layers in the remote IMAGE
# it stores list of SHAs of image layers in the file pointed at by TMP_MANIFEST_REMOTE_SHA
# This cannot be done easily with existing APIs of Dockerhub because they require additional authentication
# even for public images. Therefore instead we are downloading a specially prepared manifest image
# which is built together with the main image. This special manifest image is prepared during
# building of the main image and contains single JSON file being result of docker inspect on that image
# This image is from scratch so it is very tiny
function build_images::get_remote_image_info() {
    set +e
    # Pull remote manifest image
    if ! docker pull "${AIRFLOW_CI_REMOTE_MANIFEST_IMAGE}" 2>/dev/null >/dev/null; then
        echo >&2
        echo >&2 "Remote docker registry unreachable"
        echo >&2
        REMOTE_DOCKER_REGISTRY_UNREACHABLE="true"
        return
    fi
    set -e

    # Docker needs the file passed to --cidfile to not exist, so we can't use mktemp
    TMP_CONTAINER_DIR="$(mktemp -d)"
    TMP_CONTAINER_ID="${TMP_CONTAINER_DIR}/remote-airflow-manifest-$$.container_id"
    FILES_TO_CLEANUP_ON_EXIT+=("$TMP_CONTAINER_ID")

    TMP_MANIFEST_REMOTE_JSON=$(mktemp)
    TMP_MANIFEST_REMOTE_SHA=$(mktemp)
    # Create container out of the manifest image without running it
    docker create --cidfile "${TMP_CONTAINER_ID}" "${AIRFLOW_CI_REMOTE_MANIFEST_IMAGE}" 2>/dev/null >/dev/null
    # Extract manifest and store it in local file
    docker cp "$(cat "${TMP_CONTAINER_ID}"):${AIRFLOW_CI_BASE_TAG}.json" "${TMP_MANIFEST_REMOTE_JSON}"
    # Filter everything except SHAs of image layers
    sed 's/ *//g' "${TMP_MANIFEST_REMOTE_JSON}" | grep '^"sha256:' >"${TMP_MANIFEST_REMOTE_SHA}"
    docker rm --force "$(cat "${TMP_CONTAINER_ID}")" 2>/dev/null >/dev/null
}

# The Number determines the cut-off between local building time and pull + build time.
# It is a bit experimental and it will have to be kept
# updated as we keep on changing layers. The cut-off point is at the moment when we do first
# pip install "https://github.com/apache/airflow/archive/${AIRFLOW_BRANCH}.tar...
# you can get it via this command:
# docker history --no-trunc  apache/airflow:master-python3.6-ci | \
#      grep ^sha256 | grep -n "pip uninstall" | awk 'BEGIN {FS=":"} {print $1 }'
#
# This command returns the number of layer in docker history where pip uninstall is called. This is the
# line that will take a lot of time to run and at this point it's worth to pull the image from repo
# if there are at least NN changed layers in your docker file, you should pull the image.
#
# Note that this only matters if you have any of the important files changed since the last build
# of your image such as Dockerfile.ci, setup.py etc.
#
MAGIC_CUT_OFF_NUMBER_OF_LAYERS=36

# Compares layers from both remote and local image and set FORCE_PULL_IMAGES to true in case
# More than the last NN layers are different.
function build_images::compare_layers() {
    NUM_DIFF=$(diff -y --suppress-common-lines "${TMP_MANIFEST_REMOTE_SHA}" "${TMP_MANIFEST_LOCAL_SHA}" |
        wc -l || true)
    rm -f "${TMP_MANIFEST_REMOTE_JSON}" "${TMP_MANIFEST_REMOTE_SHA}" "${TMP_MANIFEST_LOCAL_JSON}" "${TMP_MANIFEST_LOCAL_SHA}"
    echo
    echo "Number of layers different between the local and remote image: ${NUM_DIFF}"
    echo
    # This is where setup py is rebuilt - it will usually take a looooot of time to build it, so it is
    # Better to pull here
    if ((NUM_DIFF >= MAGIC_CUT_OFF_NUMBER_OF_LAYERS)); then
        echo
        echo
        echo "WARNING! Your image and the dockerhub image differ significantly"
        echo
        echo "Forcing pulling the images. It will be faster than rebuilding usually."
        echo "You can avoid it by setting SKIP_CHECK_REMOTE_IMAGE to true"
        echo
        export FORCE_PULL_IMAGES="true"
    else
        echo
        echo "No need to pull the image. Local rebuild will be faster"
        echo
    fi
}

# Prints summary of the build parameters
function build_images::print_build_info() {
    verbosity::print_info
    verbosity::print_info "Airflow ${AIRFLOW_VERSION} Python: ${PYTHON_MAJOR_MINOR_VERSION}. Image description: ${IMAGE_DESCRIPTION}"
    verbosity::print_info
}

function build_images::get_docker_image_names() {
    # python image version to use
    export PYTHON_BASE_IMAGE_VERSION=${PYTHON_BASE_IMAGE_VERSION:=${PYTHON_MAJOR_MINOR_VERSION}}

    # Python base image to use
    export PYTHON_BASE_IMAGE="python:${PYTHON_BASE_IMAGE_VERSION}-slim-buster"

    # CI image base tag
    export AIRFLOW_CI_BASE_TAG="${DOCKER_TAG=${BRANCH_NAME}-python${PYTHON_MAJOR_MINOR_VERSION}-ci}"

    # CI image to build
    export AIRFLOW_CI_IMAGE="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${AIRFLOW_CI_BASE_TAG}"

    # Default CI image
    export AIRFLOW_CI_IMAGE_DEFAULT="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${BRANCH_NAME}-ci"

    # Base production image tag - used to build kubernetes tag as well
    export AIRFLOW_PROD_BASE_TAG="${DOCKER_TAG=${BRANCH_NAME}-python${PYTHON_MAJOR_MINOR_VERSION}}"

    # PROD image to build
    export AIRFLOW_PROD_IMAGE="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${AIRFLOW_PROD_BASE_TAG}"

    # PROD build segment
    export AIRFLOW_PROD_BUILD_IMAGE="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${AIRFLOW_PROD_BASE_TAG}-build"

    # PROD Kubernetes image to build
    export AIRFLOW_PROD_IMAGE_KUBERNETES="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${AIRFLOW_PROD_BASE_TAG}-kubernetes"

    # PROD default image
    export AIRFLOW_PROD_IMAGE_DEFAULT="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${BRANCH_NAME}"

    # File that is touched when the CI image is built for the first time locally
    export BUILT_CI_IMAGE_FLAG_FILE="${BUILD_CACHE_DIR}/${BRANCH_NAME}/.built_${PYTHON_MAJOR_MINOR_VERSION}"
}

# Prepares all variables needed by the CI build. Depending on the configuration used (python version
# DockerHub user etc. the variables are set so that other functions can use those variables.
function build_images::prepare_ci_build() {
    export AIRFLOW_CI_LOCAL_MANIFEST_IMAGE="local/${DOCKERHUB_REPO}:${AIRFLOW_CI_BASE_TAG}-manifest"
    export AIRFLOW_CI_REMOTE_MANIFEST_IMAGE="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${AIRFLOW_CI_BASE_TAG}-manifest"
    if [[ ${USE_GITHUB_REGISTRY} == "true" ]]; then
        if [[ -n ${GITHUB_TOKEN=} ]]; then
            echo "${GITHUB_TOKEN}" | docker login \
                --username "${GITHUB_USERNAME:-apache}" \
                --password-stdin \
                "${GITHUB_REGISTRY}"
        fi
        # GitHub Registry names must be lowercase :(
        github_repository_lowercase="$(echo "${GITHUB_REPOSITORY}" |tr '[:upper:]' '[:lower:]')"
        export GITHUB_REGISTRY_AIRFLOW_CI_IMAGE="${GITHUB_REGISTRY}/${github_repository_lowercase}/${AIRFLOW_CI_BASE_TAG}"
        export GITHUB_REGISTRY_PYTHON_BASE_IMAGE="${GITHUB_REGISTRY}/${github_repository_lowercase}/python:${PYTHON_BASE_IMAGE_VERSION}-slim-buster"
    fi
    if [[ "${DEFAULT_PYTHON_MAJOR_MINOR_VERSION}" == "${PYTHON_MAJOR_MINOR_VERSION}" ]]; then
        export DEFAULT_PROD_IMAGE="${AIRFLOW_CI_IMAGE_DEFAULT}"
    else
        export DEFAULT_PROD_IMAGE=""
    fi
    export THE_IMAGE_TYPE="CI"
    export IMAGE_DESCRIPTION="Airflow CI"

    # Those constants depend on the type of image run so they are only made constants here
    export AIRFLOW_EXTRAS="${AIRFLOW_EXTRAS:="${DEFAULT_CI_EXTRAS}"}"
    readonly AIRFLOW_EXTRAS

    export AIRFLOW_IMAGE="${AIRFLOW_CI_IMAGE}"
    readonly AIRFLOW_IMAGE

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
    if [[ ${needs_docker_build} == "true" ]]; then
        if [[ ${SKIP_CHECK_REMOTE_IMAGE:=} != "true" && ${DOCKER_CACHE} == "pulled" ]]; then
            # Check if remote image is different enough to force pull
            # This is an optimisation pull vs. build time. When there
            # are enough changes (specifically after setup.py changes) it is faster to pull
            # and build the image rather than just build it
            echo
            echo "Checking if the remote image needs to be pulled"
            echo
            build_images::get_remote_image_info
            if [[ ${REMOTE_DOCKER_REGISTRY_UNREACHABLE:=} != "true" ]]; then
                build_images::get_local_image_info
                if [[ ${LOCAL_MANIFEST_IMAGE_UNAVAILABLE:=} != "true" ]]; then
                    build_images::compare_layers
                else
                    FORCE_PULL_IMAGES="true"
                fi
            fi
        fi
        SKIP_REBUILD="false"
        if [[ ${CI:=} != "true" && "${FORCE_BUILD:=}" != "true" ]]; then
            build_images::confirm_image_rebuild
        fi
        if [[ ${SKIP_REBUILD} != "true" ]]; then
            SYSTEM=$(uname -s)
            if [[ ${SYSTEM} != "Darwin" ]]; then
                ROOT_FILES_COUNT=$(find "airflow" "tests" -user root | wc -l | xargs)
                if [[ ${ROOT_FILES_COUNT} != "0" ]]; then
                    ./scripts/ci/tools/ci_fix_ownership.sh
                fi
            fi
            verbosity::print_info
            verbosity::print_info "Build start: ${THE_IMAGE_TYPE} image."
            verbosity::print_info
            build_images::build_ci_image
            md5sum::update_all_md5
            build_images::build_ci_image_manifest
            verbosity::print_info
            verbosity::print_info "Build completed: ${THE_IMAGE_TYPE} image."
            verbosity::print_info
        fi
    else
        verbosity::print_info
        verbosity::print_info "No need to build - none of the important files changed: ${FILES_FOR_REBUILD_CHECK[*]}"
        verbosity::print_info
    fi
}

# Interactive version of confirming the ci image that is used in pre-commits
# it displays additional information - what the user should do in order to bring the local images
# back to state that pre-commit will be happy with
function build_images::rebuild_ci_image_if_needed_and_confirmed() {
    local needs_docker_build="false"
    THE_IMAGE_TYPE="CI"

    md5sum::check_if_docker_build_is_needed

    if [[ ${needs_docker_build} == "true" ]]; then
        verbosity::print_info
        verbosity::print_info "Docker image build is needed!"
        verbosity::print_info
    else
        verbosity::print_info
        verbosity::print_info "Docker image build is not needed!"
        verbosity::print_info
    fi

    if [[ "${needs_docker_build}" == "true" ]]; then
        echo
        echo "Some of your images need to be rebuild because important files (like package list) has changed."
        echo
        echo "You have those options:"
        echo "   * Rebuild the images now by answering 'y' (this might take some time!)"
        echo "   * Skip rebuilding the images and hope changes are not big (you will be asked again)"
        echo "   * Quit and manually rebuild the images using one of the following commands"
        echo "        * ./breeze build-image"
        echo "        * ./breeze build-image --force-pull-images"
        echo
        echo "   The first command works incrementally from your last local build."
        echo "   The second command you use if you want to completely refresh your images from dockerhub."
        echo
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
    build_images::print_build_info
    if [[ -n ${DETECTED_TERMINAL=} ]]; then
        echo -n "Preparing ${AIRFLOW_CI_IMAGE}.
        " >"${DETECTED_TERMINAL}"
        spinner::spin "${OUTPUT_LOG}" &
        SPIN_PID=$!
        # shellcheck disable=SC2064,SC2016
        traps::add_trap '$(kill '${SPIN_PID}' || true)' EXIT HUP INT TERM
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
        echo >&2
        echo >&2 "Error - thee ${DOCKER_CACHE} cache is unknown!"
        echo >&2
        exit 1
    fi
    EXTRA_DOCKER_CI_BUILD_FLAGS=(
        "--build-arg" "AIRFLOW_CONSTRAINTS_REFERENCE=${DEFAULT_CONSTRAINTS_BRANCH}"
    )

    if [[ "${AIRFLOW_CONSTRAINTS_LOCATION}" != "" ]]; then
        EXTRA_DOCKER_CI_BUILD_FLAGS+=(
            "--build-arg" "AIRFLOW_CONSTRAINTS_LOCATION=${AIRFLOW_CONSTRAINTS_LOCATION}"
        )
    fi

    if [[ -n ${SPIN_PID=} ]]; then
        kill -HUP "${SPIN_PID}" || true
        wait "${SPIN_PID}" || true
        echo >"${DETECTED_TERMINAL}"
    fi
    if [[ -n ${DETECTED_TERMINAL=} ]]; then
        echo -n "Preparing ${AIRFLOW_CI_IMAGE}.
        " >"${DETECTED_TERMINAL}"
        spinner::spin "${OUTPUT_LOG}" &
        SPIN_PID=$!
        # shellcheck disable=SC2064,SC2016
        traps::add_trap '$(kill '${SPIN_PID}' || true)' EXIT HUP INT TERM
    fi
    if [[ -n ${DETECTED_TERMINAL=} ]]; then
        echo -n "
Docker building ${AIRFLOW_CI_IMAGE}.
" >"${DETECTED_TERMINAL}"
    fi
    set +u

    local additional_dev_args=()
    if [[ ${DEV_APT_DEPS} != "" ]]; then
        additional_dev_args+=("--build-arg" "DEV_APT_DEPS=\"${DEV_APT_DEPS}\"")
    fi
    if [[ ${DEV_APT_COMMAND} != "" ]]; then
        additional_dev_args+=("--build-arg" "DEV_APT_COMMAND=\"${DEV_APT_COMMAND}\"")
    fi

    local additional_runtime_args=()
    if [[ ${RUNTIME_APT_DEPS} != "" ]]; then
        additional_runtime_args+=("--build-arg" "RUNTIME_APT_DEPS=\"${RUNTIME_APT_DEPS}\"")
    fi
    if [[ ${RUNTIME_APT_COMMAND} != "" ]]; then
        additional_runtime_args+=("--build-arg" "RUNTIME_APT_COMMAND=\"${RUNTIME_APT_COMMAND}\"")
    fi

    docker build \
        "${EXTRA_DOCKER_CI_BUILD_FLAGS[@]}" \
        --build-arg PYTHON_BASE_IMAGE="${PYTHON_BASE_IMAGE}" \
        --build-arg PYTHON_MAJOR_MINOR_VERSION="${PYTHON_MAJOR_MINOR_VERSION}" \
        --build-arg AIRFLOW_VERSION="${AIRFLOW_VERSION}" \
        --build-arg AIRFLOW_BRANCH="${BRANCH_NAME}" \
        --build-arg AIRFLOW_EXTRAS="${AIRFLOW_EXTRAS}" \
        --build-arg AIRFLOW_PRE_CACHED_PIP_PACKAGES="${AIRFLOW_PRE_CACHED_PIP_PACKAGES}" \
        --build-arg INSTALL_PROVIDERS_FROM_SOURCES="${INSTALL_PROVIDERS_FROM_SOURCES}" \
        --build-arg ADDITIONAL_AIRFLOW_EXTRAS="${ADDITIONAL_AIRFLOW_EXTRAS}" \
        --build-arg ADDITIONAL_PYTHON_DEPS="${ADDITIONAL_PYTHON_DEPS}" \
        --build-arg ADDITIONAL_DEV_APT_COMMAND="${ADDITIONAL_DEV_APT_COMMAND}" \
        --build-arg ADDITIONAL_DEV_APT_DEPS="${ADDITIONAL_DEV_APT_DEPS}" \
        --build-arg ADDITIONAL_DEV_APT_ENV="${ADDITIONAL_DEV_APT_ENV}" \
        --build-arg ADDITIONAL_RUNTIME_APT_COMMAND="${ADDITIONAL_RUNTIME_APT_COMMAND}" \
        --build-arg ADDITIONAL_RUNTIME_APT_DEPS="${ADDITIONAL_RUNTIME_APT_DEPS}" \
        --build-arg ADDITIONAL_RUNTIME_APT_ENV="${ADDITIONAL_RUNTIME_APT_ENV}" \
        --build-arg INSTALL_AIRFLOW_VIA_PIP="${INSTALL_AIRFLOW_VIA_PIP}" \
        --build-arg AIRFLOW_LOCAL_PIP_WHEELS="${AIRFLOW_LOCAL_PIP_WHEELS}" \
        --build-arg UPGRADE_TO_LATEST_CONSTRAINTS="${UPGRADE_TO_LATEST_CONSTRAINTS}" \
        --build-arg BUILD_ID="${CI_BUILD_ID}" \
        --build-arg COMMIT_SHA="${COMMIT_SHA}" \
        "${additional_dev_args[@]}" \
        "${additional_runtime_args[@]}" \
        "${DOCKER_CACHE_CI_DIRECTIVE[@]}" \
        -t "${AIRFLOW_CI_IMAGE}" \
        --target "main" \
        . -f Dockerfile.ci
    set -u
    if [[ -n "${DEFAULT_CI_IMAGE=}" ]]; then
        echo "Tagging additionally image ${AIRFLOW_CI_IMAGE} with ${DEFAULT_CI_IMAGE}"
        docker tag "${AIRFLOW_CI_IMAGE}" "${DEFAULT_CI_IMAGE}"
    fi
    if [[ -n "${IMAGE_TAG=}" ]]; then
        echo "Tagging additionally image ${AIRFLOW_CI_IMAGE} with ${IMAGE_TAG}"
        docker tag "${AIRFLOW_CI_IMAGE}" "${IMAGE_TAG}"
    fi
    if [[ -n ${SPIN_PID=} ]]; then
        kill -HUP "${SPIN_PID}" || true
        wait "${SPIN_PID}" || true
        echo >"${DETECTED_TERMINAL}"
    fi
}

# Prepares all variables needed by the CI build. Depending on the configuration used (python version
# DockerHub user etc. the variables are set so that other functions can use those variables.
function build_images::prepare_prod_build() {
    if [[ -n "${INSTALL_AIRFLOW_REFERENCE=}" ]]; then
        # When --install-airflow-reference is used then the image is build from GitHub tag
        EXTRA_DOCKER_PROD_BUILD_FLAGS=(
            "--build-arg" "AIRFLOW_INSTALL_SOURCES=https://github.com/apache/airflow/archive/${INSTALL_AIRFLOW_REFERENCE}.tar.gz#egg=apache-airflow"
        )
        export AIRFLOW_VERSION="${INSTALL_AIRFLOW_REFERENCE}"
        build_images::add_build_args_for_remote_install
    elif [[ -n "${INSTALL_AIRFLOW_VERSION=}" ]]; then
        # When --install-airflow-version is used then the image is build from PIP package
        EXTRA_DOCKER_PROD_BUILD_FLAGS=(
            "--build-arg" "AIRFLOW_INSTALL_SOURCES=apache-airflow"
            "--build-arg" "AIRFLOW_INSTALL_VERSION===${INSTALL_AIRFLOW_VERSION}"
        )
        export AIRFLOW_VERSION="${INSTALL_AIRFLOW_VERSION}"
        if [[ ${AIRFLOW_VERSION} == "1.10.2" || ${AIRFLOW_VERSION} == "1.10.1" ]]; then
            EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
                "--build-arg" "SLUGIFY_USES_TEXT_UNIDECODE=yes"
            )
        fi
        build_images::add_build_args_for_remote_install
    else
        # When no airflow version/reference is specified, production image is built from local sources
        EXTRA_DOCKER_PROD_BUILD_FLAGS=(
            "--build-arg" "AIRFLOW_CONSTRAINTS_REFERENCE=${DEFAULT_CONSTRAINTS_BRANCH}"
        )
    fi
    if [[ "${DEFAULT_PYTHON_MAJOR_MINOR_VERSION}" == "${PYTHON_MAJOR_MINOR_VERSION}" ]]; then
        export DEFAULT_CI_IMAGE="${AIRFLOW_PROD_IMAGE_DEFAULT}"
    else
        export DEFAULT_CI_IMAGE=""
    fi
    export THE_IMAGE_TYPE="PROD"
    export IMAGE_DESCRIPTION="Airflow production"

    # Those constants depend on the type of image run so they are only made constants here
    export AIRFLOW_EXTRAS="${AIRFLOW_EXTRAS:="${DEFAULT_PROD_EXTRAS}"}"
    readonly AIRFLOW_EXTRAS

    export AIRFLOW_IMAGE="${AIRFLOW_PROD_IMAGE}"
    readonly AIRFLOW_IMAGE

    if [[ ${USE_GITHUB_REGISTRY="false"} == "true" ]]; then
        if [[ -n ${GITHUB_TOKEN=} ]]; then
            echo "${GITHUB_TOKEN}" | docker login \
                --username "${GITHUB_USERNAME}" \
                --password-stdin \
                "${GITHUB_REGISTRY}"
        fi
        # GitHub Registry names must be lowercase :(
        github_repository_lowercase="$(echo "${GITHUB_REPOSITORY}" |tr '[:upper:]' '[:lower:]')"
        export GITHUB_REGISTRY_AIRFLOW_PROD_IMAGE="${GITHUB_REGISTRY}/${github_repository_lowercase}/${AIRFLOW_PROD_BASE_TAG}"
        export GITHUB_REGISTRY_AIRFLOW_PROD_BUILD_IMAGE="${GITHUB_REGISTRY}/${github_repository_lowercase}/${AIRFLOW_PROD_BASE_TAG}-build"
        export GITHUB_REGISTRY_PYTHON_BASE_IMAGE="${GITHUB_REGISTRY}/${github_repository_lowercase}/python:${PYTHON_BASE_IMAGE_VERSION}-slim-buster"
    fi

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
        verbosity::print_info
        verbosity::print_info "Skip building production image. Assume the one we have is good!"
        verbosity::print_info
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
        echo >&2
        echo >&2 "Error - thee ${DOCKER_CACHE} cache is unknown!"
        echo >&2
        exit 1
    fi
    set +u
    local additional_dev_args=()
    if [[ ${DEV_APT_DEPS} != "" ]]; then
        additional_dev_args+=("--build-arg" "DEV_APT_DEPS=\"${DEV_APT_DEPS}\"")
    fi
    if [[ ${DEV_APT_COMMAND} != "" ]]; then
        additional_dev_args+=("--build-arg" "DEV_APT_COMMAND=\"${DEV_APT_COMMAND}\"")
    fi
    docker build \
        "${EXTRA_DOCKER_PROD_BUILD_FLAGS[@]}" \
        --build-arg PYTHON_BASE_IMAGE="${PYTHON_BASE_IMAGE}" \
        --build-arg PYTHON_MAJOR_MINOR_VERSION="${PYTHON_MAJOR_MINOR_VERSION}" \
        --build-arg INSTALL_MYSQL_CLIENT="${INSTALL_MYSQL_CLIENT}" \
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
        --build-arg INSTALL_AIRFLOW_VIA_PIP="${INSTALL_AIRFLOW_VIA_PIP}" \
        --build-arg AIRFLOW_LOCAL_PIP_WHEELS="${AIRFLOW_LOCAL_PIP_WHEELS}" \
        --build-arg BUILD_ID="${CI_BUILD_ID}" \
        --build-arg COMMIT_SHA="${COMMIT_SHA}" \
        "${DOCKER_CACHE_PROD_BUILD_DIRECTIVE[@]}" \
        -t "${AIRFLOW_PROD_BUILD_IMAGE}" \
        --target "airflow-build-image" \
        . -f Dockerfile
    local additional_runtime_args=()
    if [[ ${RUNTIME_APT_DEPS} != "" ]]; then
        additional_runtime_args+=("--build-arg" "RUNTIME_APT_DEPS=\"${RUNTIME_APT_DEPS}\"")
    fi
    if [[ ${RUNTIME_APT_COMMAND} != "" ]]; then
        additional_runtime_args+=("--build-arg" "RUNTIME_APT_COMMAND=\"${RUNTIME_APT_COMMAND}\"")
    fi
    docker build \
        "${EXTRA_DOCKER_PROD_BUILD_FLAGS[@]}" \
        --build-arg PYTHON_BASE_IMAGE="${PYTHON_BASE_IMAGE}" \
        --build-arg PYTHON_MAJOR_MINOR_VERSION="${PYTHON_MAJOR_MINOR_VERSION}" \
        --build-arg INSTALL_MYSQL_CLIENT="${INSTALL_MYSQL_CLIENT}" \
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
        --build-arg INSTALL_AIRFLOW_VIA_PIP="${INSTALL_AIRFLOW_VIA_PIP}" \
        --build-arg AIRFLOW_LOCAL_PIP_WHEELS="${AIRFLOW_LOCAL_PIP_WHEELS}" \
        --build-arg AIRFLOW_VERSION="${AIRFLOW_VERSION}" \
        --build-arg AIRFLOW_BRANCH="${AIRFLOW_BRANCH_FOR_PYPI_PRELOADING}" \
        --build-arg AIRFLOW_EXTRAS="${AIRFLOW_EXTRAS}" \
        --build-arg BUILD_ID="${CI_BUILD_ID}" \
        --build-arg COMMIT_SHA="${COMMIT_SHA}" \
        "${additional_dev_args[@]}" \
        "${additional_runtime_args[@]}" \
        "${DOCKER_CACHE_PROD_DIRECTIVE[@]}" \
        -t "${AIRFLOW_PROD_IMAGE}" \
        --target "main" \
        . -f Dockerfile
    set -u
    if [[ -n "${DEFAULT_PROD_IMAGE:=}" ]]; then
        echo "Tagging additionally image ${AIRFLOW_PROD_IMAGE} with ${DEFAULT_PROD_IMAGE}"
        docker tag "${AIRFLOW_PROD_IMAGE}" "${DEFAULT_PROD_IMAGE}"
    fi
    if [[ -n "${IMAGE_TAG=}" ]]; then
        echo "Tagging additionally image ${AIRFLOW_PROD_IMAGE} with ${IMAGE_TAG}"
        docker tag "${AIRFLOW_PROD_IMAGE}" "${IMAGE_TAG}"
    fi
}

# Waits for image tag to appear in GitHub Registry, pulls it and tags with the target tag
# Parameters:
#  $1 - image name to wait for
#  $2 - suffix of the image to wait for
#  $3, $4, ... - target tags to tag the image with
function build_images::wait_for_image_tag() {
    IMAGE_NAME="${1}"
    IMAGE_SUFFIX=${2}
    shift 2

    IMAGE_TO_WAIT_FOR="${IMAGE_NAME}${IMAGE_SUFFIX}"
    echo
    echo "Waiting for image ${IMAGE_TO_WAIT_FOR}"
    echo
    while true; do
        set +e
        docker pull "${IMAGE_TO_WAIT_FOR}" 2>/dev/null >/dev/null
        set -e
        if [[ -z "$(docker images -q "${IMAGE_TO_WAIT_FOR}" 2>/dev/null || true)" ]]; then
            echo
            echo "The image ${IMAGE_TO_WAIT_FOR} is not yet available. Waiting"
            echo
            sleep 10
        else
            echo
            echo "The image ${IMAGE_TO_WAIT_FOR} with '${IMAGE_NAME}' tag"
            echo
            echo
            echo "Tagging ${IMAGE_TO_WAIT_FOR} as ${IMAGE_NAME}."
            echo
            docker tag "${IMAGE_TO_WAIT_FOR}" "${IMAGE_NAME}"
            for TARGET_TAG in "${@}"; do
                echo
                echo "Tagging ${IMAGE_TO_WAIT_FOR} as ${TARGET_TAG}."
                echo
                docker tag "${IMAGE_TO_WAIT_FOR}" "${TARGET_TAG}"
            done
            break
        fi
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
    readonly DOCKER_CACHE
    verbosity::print_info
    verbosity::print_info "Using ${DOCKER_CACHE} cache strategy for the build."
    verbosity::print_info
}
