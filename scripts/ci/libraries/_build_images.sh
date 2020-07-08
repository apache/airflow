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
function add_build_args_for_remote_install() {
    # entrypoint is used as AIRFLOW_SOURCES_FROM/TO in order to avoid costly copying of all sources of
    # Airflow - those are not needed for remote install at all. Entrypoint is later overwritten by
    EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
        "--build-arg" "AIRFLOW_SOURCES_FROM=empty"
        "--build-arg" "AIRFLOW_SOURCES_TO=/empty"
    )
    if [[ ${AIRFLOW_VERSION} =~ [^0-9]*1[^0-9]*10[^0-9]([0-9]*) ]]; then
        # All types of references/versions match this regexp for 1.10 series
        # for example v1_10_test, 1.10.10, 1.10.9 etc. ${BASH_REMATCH[1]} is the () group matches last
        # minor digit of version and it's length is 0 for v1_10_test, 1 for 1.10.9 and 2 for 1.10.10+
        if [[ ${#BASH_REMATCH[1]} == "1" ]]; then
            # This is only for 1.10.0 - 1.10.9
            EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
                "--build-arg" "CONSTRAINT_REQUIREMENTS=https://raw.githubusercontent.com/apache/airflow/1.10.10/requirements/requirements-python${PYTHON_MAJOR_MINOR_VERSION}.txt"
            )
        else
            EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
                # For 1.10.10+ and v1-10-test it's ok to use AIRFLOW_VERSION as reference
                "--build-arg" "CONSTRAINT_REQUIREMENTS=https://raw.githubusercontent.com/apache/airflow/${AIRFLOW_VERSION}/requirements/requirements-python${PYTHON_MAJOR_MINOR_VERSION}.txt"
            )
        fi
        AIRFLOW_BRANCH_FOR_PYPI_PRELOADING="v1-10-test"
    else
        # For all other (master, 2.0+) we just match ${AIRFLOW_VERSION}
        EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
            "--build-arg" "CONSTRAINT_REQUIREMENTS=https://raw.githubusercontent.com/apache/airflow/${AIRFLOW_VERSION}/requirements/requirements-python${PYTHON_MAJOR_MINOR_VERSION}.txt"
        )
        AIRFLOW_BRANCH_FOR_PYPI_PRELOADING="master"
    fi
}

# Retrieves version of airflow stored in the production image (used to display the actual
# Version we use if it was build from PyPI or GitHub
function get_airflow_version_from_production_image() {
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
# precommits `pre-commit run` - then the "LAST_FORCE_ANSWER_FILE" will be removed and you will
# be asked again.
function forget_last_answer() {
    if [[ ${REMEMBER_LAST_ANSWER:="false"} != "true" ]]; then
        print_info
        print_info "Forgetting last answer from ${LAST_FORCE_ANSWER_FILE}:"
        print_info
        rm -f "${LAST_FORCE_ANSWER_FILE}"
    else
        if [[ -f "${LAST_FORCE_ANSWER_FILE}" ]]; then
            print_info
            print_info "Still remember last answer from ${LAST_FORCE_ANSWER_FILE}:"
            print_info "$(cat "${LAST_FORCE_ANSWER_FILE}")"
            print_info
        fi
    fi
}

# Confirms if hte image should be rebuild and interactively checks it with the user.
# In case iit needs to be rebuild. It only ask the user if it determines that the rebuild
# is needed and that the rebuild is not already forced. It asks the user using available terminals
# So that the script works also from within pre-commit run via git hooks - where stdin is not
# available - it tries to find usable terminal and ask the user via this terminal.
function confirm_image_rebuild() {
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
        print_info
        print_info "CI environment - forcing rebuild for image ${THE_IMAGE_TYPE}."
        print_info
        RES="0"
    elif [[ -n "${FORCE_ANSWER_TO_QUESTIONS:=""}" ]]; then
        print_info
        print_info "Forcing answer '${FORCE_ANSWER_TO_QUESTIONS}'"
        print_info
        case "${FORCE_ANSWER_TO_QUESTIONS}" in
            [yY][eE][sS]|[yY])
                RES="0" ;;
            [qQ][uU][iI][tT]|[qQ])
                RES="2" ;;
            *)
                RES="1" ;;
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
        print_info
        print_info "No terminal, no stdin - quitting"
        print_info
        # No terminal, no stdin, no force answer - quitting!
        RES="2"
    fi
    set -e
    if [[ ${RES} == "1" ]]; then
        print_info
        print_info "Skipping rebuilding the image ${THE_IMAGE_TYPE}-python${PYTHON_MAJOR_MINOR_VERSION}"
        print_info
        export SKIP_REBUILD="true"
        # Force "no" also to subsequent questions so that if you answer it once, you are not asked
        # For all other pre-commits and you will continue using the images you already have
        export FORCE_ANSWER_TO_QUESTIONS="no"
        echo 'export FORCE_ANSWER_TO_QUESTIONS="no"' > "${LAST_FORCE_ANSWER_FILE}"
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
function build_ci_image_manifest() {
    verbose_docker inspect "${AIRFLOW_CI_IMAGE}" > "manifests/${AIRFLOW_CI_BASE_TAG}.json"
    verbose_docker build \
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
function get_local_image_info() {
    TMP_MANIFEST_LOCAL_JSON=$(mktemp)
    TMP_MANIFEST_LOCAL_SHA=$(mktemp)
    set +e
    # Remove the container just in case
    verbose_docker_hide_output_on_success rm --force "local-airflow-manifest"
    # Create manifest from the local manifest image
    if ! verbose_docker_hide_output_on_success create --name "local-airflow-manifest" \
        "${AIRFLOW_CI_LOCAL_MANIFEST_IMAGE}"  >>"${OUTPUT_LOG}" 2>&1 ; then
        echo
        echo "Local manifest image not available"
        echo
        LOCAL_MANIFEST_IMAGE_UNAVAILABLE="true"
        return
    fi
    set -e
     # Create manifest from the local manifest image
    verbose_docker_hide_output_on_success cp "local-airflow-manifest:${AIRFLOW_CI_BASE_TAG}.json" \
        "${TMP_MANIFEST_LOCAL_JSON}" >>"${OUTPUT_LOG}" 2>&1
    sed 's/ *//g' "${TMP_MANIFEST_LOCAL_JSON}" | grep '^"sha256:' >"${TMP_MANIFEST_LOCAL_SHA}"
    verbose_docker_hide_output_on_success rm --force "local-airflow-manifest" >>"${OUTPUT_LOG}" 2>&1
}

#
# Retrieves information about layers in the remote IMAGE
# it stores list of SHAs of image layers in the file pointed at by TMP_MANIFEST_REMOTE_SHA
# This cannot be done easily with existing APIs of Dockerhub because they require additional authentication
# even for public images. Therefore instead we are downloading a specially prepared manifest image
# which is built together with the main image. This special manifest image is prepared during
# building of the main image and contains single JSON file being result of docker inspect on that image
# This image is from scratch so it is very tiny
function get_remote_image_info() {
    set +e
    # Pull remote manifest image
    if ! verbose_docker_hide_output_on_success pull "${AIRFLOW_CI_REMOTE_MANIFEST_IMAGE}";  then
        echo
        echo "Remote docker registry unreachable"
        echo
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
    verbose_docker_hide_output_on_success create --cidfile "${TMP_CONTAINER_ID}" \
        "${AIRFLOW_CI_REMOTE_MANIFEST_IMAGE}"
    # Extract manifest and store it in local file
    verbose_docker_hide_output_on_success cp "$(cat "${TMP_CONTAINER_ID}"):${AIRFLOW_CI_BASE_TAG}.json" \
        "${TMP_MANIFEST_REMOTE_JSON}"
    # Filter everything except SHAs of image layers
    sed 's/ *//g' "${TMP_MANIFEST_REMOTE_JSON}" | grep '^"sha256:' >"${TMP_MANIFEST_REMOTE_SHA}"
    verbose_docker_hide_output_on_success rm --force "$( cat "${TMP_CONTAINER_ID}")"
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
function compare_layers() {
    NUM_DIFF=$(diff  -y --suppress-common-lines "${TMP_MANIFEST_REMOTE_SHA}" "${TMP_MANIFEST_LOCAL_SHA}" | \
        wc -l || true)
    rm -f "${TMP_MANIFEST_REMOTE_JSON}" "${TMP_MANIFEST_REMOTE_SHA}" "${TMP_MANIFEST_LOCAL_JSON}" "${TMP_MANIFEST_LOCAL_SHA}"
    echo
    echo "Number of layers different between the local and remote image: ${NUM_DIFF}"
    echo
    # This is where setup py is rebuilt - it will usually take a looooot of time to build it, so it is
    # Better to pull here
    if (( NUM_DIFF >= MAGIC_CUT_OFF_NUMBER_OF_LAYERS )); then
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
function print_build_info() {
    print_info
    print_info "Airflow ${AIRFLOW_VERSION} Python: ${PYTHON_MAJOR_MINOR_VERSION}. Image description: ${IMAGE_DESCRIPTION}"
    print_info
}



# Prepares all variables needed by the CI build. Depending on the configuration used (python version
# DockerHub user etc. the variables are set so that other functions can use those variables.
function prepare_ci_build() {
    # We use pulled docker image cache by default for CI images to  speed up the builds
    export DOCKER_CACHE=${DOCKER_CACHE:="pulled"}
    echo
    echo "Using ${DOCKER_CACHE} cache strategy for the build."
    echo
    export AIRFLOW_CI_BASE_TAG="${BRANCH_NAME}-python${PYTHON_MAJOR_MINOR_VERSION}-ci"
    export AIRFLOW_CI_LOCAL_MANIFEST_IMAGE="local/${DOCKERHUB_REPO}:${AIRFLOW_CI_BASE_TAG}-manifest"
    export AIRFLOW_CI_REMOTE_MANIFEST_IMAGE="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${AIRFLOW_CI_BASE_TAG}-manifest"
    export AIRFLOW_CI_IMAGE="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${AIRFLOW_CI_BASE_TAG}"
    if [[ ${USE_GITHUB_REGISTRY="false"} == "true" ]]; then
        if [[ ${CACHE_REGISTRY_PASSWORD:=} != "" ]]; then
            echo "${CACHE_REGISTRY_PASSWORD}" | docker login \
                --username "${CACHE_REGISTRY_USERNAME}" \
                --password-stdin \
                "${CACHE_REGISTRY}"
        fi
        export CACHE_IMAGE_PREFIX=${CACHE_IMAGE_PREFX:=${GITHUB_ORGANISATION}/${GITHUB_REPO}}
        export CACHED_AIRFLOW_CI_IMAGE="${CACHE_REGISTRY}/${CACHE_IMAGE_PREFIX}/${AIRFLOW_CI_BASE_TAG}"
        export CACHED_PYTHON_BASE_IMAGE="${CACHE_REGISTRY}/${CACHE_IMAGE_PREFIX}/python:${PYTHON_MAJOR_MINOR_VERSION}-slim-buster"
    else
        export CACHED_AIRFLOW_CI_IMAGE=""
        export CACHED_PYTHON_BASE_IMAGE=""
    fi
    export AIRFLOW_BUILD_CI_IMAGE="${DOCKERHUB_USER}/${DOCKERHUB_REPO}/${AIRFLOW_CI_BASE_TAG}"
    export AIRFLOW_CI_IMAGE_DEFAULT="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${BRANCH_NAME}-ci"
    export PYTHON_BASE_IMAGE="python:${PYTHON_MAJOR_MINOR_VERSION}-slim-buster"
    export BUILT_IMAGE_FLAG_FILE="${BUILD_CACHE_DIR}/${BRANCH_NAME}/.built_${PYTHON_MAJOR_MINOR_VERSION}"
    if [[ "${DEFAULT_PYTHON_MAJOR_MINOR_VERSION}" == "${PYTHON_MAJOR_MINOR_VERSION}" ]]; then
        export DEFAULT_IMAGE="${AIRFLOW_CI_IMAGE_DEFAULT}"
    else
        export DEFAULT_IMAGE=""
    fi
    export THE_IMAGE_TYPE="CI"
    export IMAGE_DESCRIPTION="Airflow CI"
    export AIRFLOW_EXTRAS="${AIRFLOW_EXTRAS:="${DEFAULT_CI_EXTRAS}"}"
    export ADDITIONAL_AIRFLOW_EXTRAS="${ADDITIONAL_AIRFLOW_EXTRAS:=""}"
    export ADDITIONAL_PYTHON_DEPS="${ADDITIONAL_PYTHON_DEPS:=""}"
    export ADDITIONAL_DEV_DEPS="${ADDITIONAL_DEV_DEPS:=""}"
    export ADDITIONAL_RUNTIME_DEPS="${ADDITIONAL_RUNTIME_DEPS:=""}"
    export AIRFLOW_IMAGE="${AIRFLOW_CI_IMAGE}"
    go_to_airflow_sources
    fix_group_permissions
}


# Only rebuilds CI image if needed. It checks if the docker image build is needed
# because any of the important source files (from common/_files_for_rebuild_check.sh) has
# changed or in any of the edge cases (docker image removed, .build cache removed etc.
# In case rebuild is needed, it determines (by comparing layers in local and remote image)
# Whether pull is needed before rebuild.
function rebuild_ci_image_if_needed() {
    if [[ ${SKIP_CI_IMAGE_CHECK:="false"} == "true" ]]; then
        echo
        echo "Skip checking CI image"
        echo
        return
    fi
    if [[ -f "${BUILT_IMAGE_FLAG_FILE}" ]]; then
        print_info
        print_info "${THE_IMAGE_TYPE} image already built locally."
        print_info
    else
        print_info
        print_info "${THE_IMAGE_TYPE} image not built locally: pulling and building"
        print_info
        export FORCE_PULL_IMAGES="true"
        export FORCE_BUILD_IMAGES="true"
    fi

    NEEDS_DOCKER_BUILD="false"
    check_if_docker_build_is_needed
    if [[ ${NEEDS_DOCKER_BUILD} == "true" ]]; then
        if [[ ${SKIP_CHECK_REMOTE_IMAGE:=} != "true" && ${DOCKER_CACHE} == "pulled" ]]; then
            # Check if remote image is different enough to force pull
            # This is an optimisation pull vs. build time. When there
            # are enough changes (specifically after setup.py changes) it is faster to pull
            # and build the image rather than just build it
            echo
            echo "Checking if the remote image needs to be pulled"
            echo
            get_remote_image_info
            if [[ ${REMOTE_DOCKER_REGISTRY_UNREACHABLE:=} != "true" ]]; then
                get_local_image_info
                if [[ ${LOCAL_MANIFEST_IMAGE_UNAVAILABLE:=} != "true" ]]; then
                    compare_layers
                else
                    FORCE_PULL_IMAGES="true"
                fi
            fi
        fi
        SKIP_REBUILD="false"
        if [[ ${CI:=} != "true" && "${FORCE_BUILD:=}" != "true" ]]; then
            confirm_image_rebuild
        fi
        if [[ ${SKIP_REBUILD} != "true" ]]; then
            SYSTEM=$(uname -s)
            if [[ ${SYSTEM} != "Darwin" ]]; then
                ROOT_FILES_COUNT=$(find "airflow" "tests" -user root | wc -l | xargs)
                if [[ ${ROOT_FILES_COUNT} != "0" ]]; then
                    ./scripts/ci/ci_fix_ownership.sh
                fi
            fi
            print_info
            print_info "Build start: ${THE_IMAGE_TYPE} image."
            print_info
            build_ci_image
            update_all_md5_files
            build_ci_image_manifest
            print_info
            print_info "Build completed: ${THE_IMAGE_TYPE} image."
            print_info
        fi
    else
        print_info
        print_info "No need to build - none of the important files changed: ${FILES_FOR_REBUILD_CHECK[*]}"
        print_info
    fi
}


# Interactive version of confirming the ci image that is used in pre-commits
# it displays additional information - what the user should do in order to bring the local images
# back to state that pre-commit will be happy with
function rebuild_ci_image_if_needed_and_confirmed() {
    NEEDS_DOCKER_BUILD="false"
    THE_IMAGE_TYPE="CI"

    check_if_docker_build_is_needed

    if [[ ${NEEDS_DOCKER_BUILD} == "true" ]]; then
        print_info
        print_info "Docker image build is needed!"
        print_info
    else
        print_info
        print_info "Docker image build is not needed!"
        print_info
    fi

    if [[ "${NEEDS_DOCKER_BUILD}" == "true" ]]; then
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
        confirm_image_rebuild

        if [[ ${SKIP_REBUILD} != "true" ]]; then
            rebuild_ci_image_if_needed
        fi
    fi
}

# Determines the strategy to be used for caching based on the type of CI job run.
# In case of CRON jobs we run builds without cache and upgrade to latest requirements
function determine_cache_strategy() {
    if [[ "${CI_EVENT_TYPE:=}" == "schedule" ]]; then
        echo
        echo "Disabling cache for scheduled jobs"
        echo
        export DOCKER_CACHE="disabled"
        echo
        echo "Requirements are upgraded to latest for scheduled CI build"
        echo
        export UPGRADE_TO_LATEST_REQUIREMENTS="true"
    else
        echo
        echo "Pull cache used for regular CI builds"
        echo
        export DOCKER_CACHE="pulled"
        echo
        echo "Requirements are not upgraded to latest ones for regular CI builds"
        echo
        export UPGRADE_TO_LATEST_REQUIREMENTS="false"
    fi
}


# Builds the CI image in the CI environment.
# Depending on the type of build (push/pr/scheduled) it will either build it incrementally or
# from the scratch without cache (the latter for scheduled builds only)
function build_ci_image_on_ci() {
    export SKIP_CI_IMAGE_CHECK="false"

    get_environment_for_builds_on_ci
    determine_cache_strategy
    prepare_ci_build

    rm -rf "${BUILD_CACHE_DIR}"
    mkdir -pv "${BUILD_CACHE_DIR}"

    rebuild_ci_image_if_needed

    # Disable force pulling forced above this is needed for the subsequent scripts so that
    # They do not try to pull/build images again. Also skip the image check entirely for
    # the rest of the script
    unset FORCE_PULL_IMAGES
    unset FORCE_BUILD
    export SKIP_CI_IMAGE_CHECK="true"
}

# Builds CI image - depending on the caching strategy (pulled, local, disabled) it
# passes the necessary docker build flags via DOCKER_CACHE_CI_DIRECTIVE array
# it also passes the right Build args depending on the configuration of the build
# selected by Breeze flags or environment variables.
function build_ci_image() {
    print_build_info
    if [[ -n ${DETECTED_TERMINAL:=""} ]]; then
        echo -n "Preparing ${AIRFLOW_CI_IMAGE}.
        " > "${DETECTED_TERMINAL}"
        spin "${OUTPUT_LOG}" &
        SPIN_PID=$!
        # shellcheck disable=SC2064
        trap "kill ${SPIN_PID}" SIGINT SIGTERM
    fi
    pull_ci_image_if_needed

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
    if [[ -n ${DETECTED_TERMINAL:=""} ]]; then
        echo -n "
Docker building ${AIRFLOW_CI_IMAGE}.
" > "${DETECTED_TERMINAL}"
    fi
    set +u
    verbose_docker build \
        --build-arg PYTHON_BASE_IMAGE="${PYTHON_BASE_IMAGE}" \
        --build-arg PYTHON_MAJOR_MINOR_VERSION="${PYTHON_MAJOR_MINOR_VERSION}" \
            --build-arg AIRFLOW_VERSION="${AIRFLOW_VERSION}" \
        --build-arg AIRFLOW_BRANCH="${BRANCH_NAME}" \
        --build-arg AIRFLOW_EXTRAS="${AIRFLOW_EXTRAS}" \
        --build-arg ADDITIONAL_AIRFLOW_EXTRAS="${ADDITIONAL_AIRFLOW_EXTRAS}" \
        --build-arg ADDITIONAL_PYTHON_DEPS="${ADDITIONAL_PYTHON_DEPS}" \
        --build-arg ADDITIONAL_DEV_DEPS="${ADDITIONAL_DEV_DEPS}" \
        --build-arg ADDITIONAL_RUNTIME_DEPS="${ADDITIONAL_RUNTIME_DEPS}" \
        --build-arg UPGRADE_TO_LATEST_REQUIREMENTS="${UPGRADE_TO_LATEST_REQUIREMENTS}" \
        "${DOCKER_CACHE_CI_DIRECTIVE[@]}" \
        -t "${AIRFLOW_CI_IMAGE}" \
        --target "main" \
        . -f Dockerfile.ci | tee -a "${OUTPUT_LOG}"
    set -u
    if [[ -n "${DEFAULT_IMAGE:=}" ]]; then
        verbose_docker tag "${AIRFLOW_CI_IMAGE}" "${DEFAULT_IMAGE}" | tee -a "${OUTPUT_LOG}"
    fi
    if [[ -n ${SPIN_PID:=""} ]]; then
        kill "${SPIN_PID}" || true
        wait "${SPIN_PID}" || true
        echo > "${DETECTED_TERMINAL}"
    fi
}

# Prepares all variables needed by the CI build. Depending on the configuration used (python version
# DockerHub user etc. the variables are set so that other functions can use those variables.
function prepare_prod_build() {
    # We use local docker image cache by default for Production images
    export DOCKER_CACHE=${DOCKER_CACHE:="local"}
    echo
    echo "Using ${DOCKER_CACHE} cache strategy for the build."
    echo
    if [[ "${INSTALL_AIRFLOW_REFERENCE:=}" != "" ]]; then
        # When --install-airflow-reference is used then the image is build from github tag
        EXTRA_DOCKER_PROD_BUILD_FLAGS=(
            "--build-arg" "AIRFLOW_INSTALL_SOURCES=https://github.com/apache/airflow/archive/${INSTALL_AIRFLOW_REFERENCE}.tar.gz#egg=apache-airflow"
        )
        export AIRFLOW_VERSION="${INSTALL_AIRFLOW_REFERENCE}"
        add_build_args_for_remote_install
    elif [[ "${INSTALL_AIRFLOW_VERSION:=}" != "" ]]; then
        # When --install-airflow-version is used then the image is build from PIP package
        EXTRA_DOCKER_PROD_BUILD_FLAGS=(
            "--build-arg" "AIRFLOW_INSTALL_SOURCES=apache-airflow"
            "--build-arg" "AIRFLOW_INSTALL_VERSION===${INSTALL_AIRFLOW_VERSION}"
        )
        export AIRFLOW_VERSION="${INSTALL_AIRFLOW_VERSION}"
        add_build_args_for_remote_install
    else
        # When no airflow version/reference is specified, production image is built from local sources
        EXTRA_DOCKER_PROD_BUILD_FLAGS=(
        )
    fi

    export AIRFLOW_PROD_BASE_TAG="${BRANCH_NAME}-python${PYTHON_MAJOR_MINOR_VERSION}"
    export AIRFLOW_PROD_BUILD_IMAGE="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${AIRFLOW_PROD_BASE_TAG}-build"
    export AIRFLOW_PROD_IMAGE="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${AIRFLOW_PROD_BASE_TAG}"
    export AIRFLOW_PROD_IMAGE_DEFAULT="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${BRANCH_NAME}"
    export PYTHON_BASE_IMAGE="python:${PYTHON_MAJOR_MINOR_VERSION}-slim-buster"
    if [[ "${DEFAULT_PYTHON_MAJOR_MINOR_VERSION}" == "${PYTHON_MAJOR_MINOR_VERSION}" ]]; then
        export DEFAULT_IMAGE="${AIRFLOW_PROD_IMAGE_DEFAULT}"
    else
        export DEFAULT_IMAGE=""
    fi
    export THE_IMAGE_TYPE="PROD"
    export IMAGE_DESCRIPTION="Airflow production"
    export AIRFLOW_EXTRAS="${AIRFLOW_EXTRAS:="${DEFAULT_PROD_EXTRAS}"}"
    export ADDITIONAL_AIRFLOW_EXTRAS="${ADDITIONAL_AIRFLOW_EXTRAS:=""}"
    export ADDITIONAL_PYTHON_DEPS="${ADDITIONAL_PYTHON_DEPS:=""}"
    export ADDITIONAL_DEV_DEPS="${ADDITIONAL_DEV_DEPS:=""}"
    export ADDITIONAL_RUNTIME_DEPS="${ADDITIONAL_RUNTIME_DEPS:=""}"
    export AIRFLOW_IMAGE="${AIRFLOW_PROD_IMAGE}"

    if [[ ${USE_GITHUB_REGISTRY="false"} == "true" ]]; then
        if [[ ${CACHE_REGISTRY_PASSWORD:=} != "" ]]; then
            echo "${CACHE_REGISTRY_PASSWORD}" | docker login \
                --username "${CACHE_REGISTRY_USERNAME}" \
                --password-stdin \
                "${CACHE_REGISTRY}"
        fi
        export CACHE_IMAGE_PREFIX=${CACHE_IMAGE_PREFX:=${GITHUB_ORGANISATION}/${GITHUB_REPO}}
        export CACHED_AIRFLOW_PROD_IMAGE="${CACHE_REGISTRY}/${CACHE_IMAGE_PREFIX}/${AIRFLOW_PROD_BASE_TAG}"
        export CACHED_AIRFLOW_PROD_BUILD_IMAGE="${CACHE_REGISTRY}/${CACHE_IMAGE_PREFIX}/${AIRFLOW_PROD_BASE_TAG}-build"
        export CACHED_PYTHON_BASE_IMAGE="${CACHE_REGISTRY}/${CACHE_IMAGE_PREFIX}/python:${PYTHON_MAJOR_MINOR_VERSION}-slim-buster"
    else
        export CACHED_AIRFLOW_PROD_IMAGE=""
        export CACHED_AIRFLOW_PROD_BUILD_IMAGE=""
        export CACHED_PYTHON_BASE_IMAGE=""
    fi

    AIRFLOW_BRANCH_FOR_PYPI_PRELOADING="${BRANCH_NAME}"
    go_to_airflow_sources
}


# Builds the prod image in the CI environment.
# Depending on the type of build (push/pr/scheduled) it will either build it incrementally or
# from the scratch without cache (the latter for scheduled builds only)
function build_prod_image_on_ci() {
    get_environment_for_builds_on_ci

    determine_cache_strategy

    prepare_prod_build

    rm -rf "${BUILD_CACHE_DIR}"
    mkdir -pv "${BUILD_CACHE_DIR}"

    build_prod_image

    # Disable force pulling forced above this is needed for the subsequent scripts so that
    # They do not try to pull/build images again
    unset FORCE_PULL_IMAGES
    unset FORCE_BUILD
}


# Builds PROD image - depending on the caching strategy (pulled, local, disabled) it
# passes the necessary docker build flags via DOCKER_CACHE_PROD_DIRECTIVE and
# DOCKER_CACHE_PROD_BUILD_DIRECTIVE (separate caching options are needed for "build" segment of the image)
# it also passes the right Build args depending on the configuration of the build
# selected by Breeze flags or environment variables.
function build_prod_image() {
    print_build_info
    pull_prod_images_if_needed

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
    verbose_docker build \
        "${EXTRA_DOCKER_PROD_BUILD_FLAGS[@]}" \
        --build-arg PYTHON_BASE_IMAGE="${PYTHON_BASE_IMAGE}" \
        --build-arg PYTHON_MAJOR_MINOR_VERSION="${PYTHON_MAJOR_MINOR_VERSION}" \
        --build-arg AIRFLOW_VERSION="${AIRFLOW_VERSION}" \
        --build-arg AIRFLOW_BRANCH="${AIRFLOW_BRANCH_FOR_PYPI_PRELOADING}" \
        --build-arg AIRFLOW_EXTRAS="${AIRFLOW_EXTRAS}" \
        --build-arg ADDITIONAL_AIRFLOW_EXTRAS="${ADDITIONAL_AIRFLOW_EXTRAS}" \
        --build-arg ADDITIONAL_PYTHON_DEPS="${ADDITIONAL_PYTHON_DEPS}" \
        --build-arg ADDITIONAL_DEV_DEPS="${ADDITIONAL_DEV_DEPS}" \
        "${DOCKER_CACHE_PROD_BUILD_DIRECTIVE[@]}" \
        -t "${AIRFLOW_PROD_BUILD_IMAGE}" \
        --target "airflow-build-image" \
        . -f Dockerfile | tee -a "${OUTPUT_LOG}"
    verbose_docker build \
        "${EXTRA_DOCKER_PROD_BUILD_FLAGS[@]}" \
        --build-arg PYTHON_BASE_IMAGE="${PYTHON_BASE_IMAGE}" \
        --build-arg PYTHON_MAJOR_MINOR_VERSION="${PYTHON_MAJOR_MINOR_VERSION}" \
        --build-arg ADDITIONAL_AIRFLOW_EXTRAS="${ADDITIONAL_AIRFLOW_EXTRAS}" \
        --build-arg ADDITIONAL_PYTHON_DEPS="${ADDITIONAL_PYTHON_DEPS}" \
        --build-arg ADDITIONAL_DEV_DEPS="${ADDITIONAL_DEV_DEPS}" \
        --build-arg ADDITIONAL_RUNTIME_DEPS="${ADDITIONAL_RUNTIME_DEPS}" \
        --build-arg AIRFLOW_VERSION="${AIRFLOW_VERSION}" \
        --build-arg AIRFLOW_BRANCH="${AIRFLOW_BRANCH_FOR_PYPI_PRELOADING}" \
        --build-arg AIRFLOW_EXTRAS="${AIRFLOW_EXTRAS}" \
        --build-arg EMBEDDED_DAGS="${EMBEDDED_DAGS}" \
        "${DOCKER_CACHE_PROD_DIRECTIVE[@]}" \
        -t "${AIRFLOW_PROD_IMAGE}" \
        --target "main" \
        . -f Dockerfile | tee -a "${OUTPUT_LOG}"
    set -u
    if [[ -n "${DEFAULT_IMAGE:=}" ]]; then
        verbose_docker tag "${AIRFLOW_PROD_IMAGE}" "${DEFAULT_IMAGE}" | tee -a "${OUTPUT_LOG}"
    fi
}
