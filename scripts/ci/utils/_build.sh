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

# Assume AIRFLOW_SOURCES are set to point to sources of Airflow

#
# Sets file name for last forced answer. This is where last forced answer is stored. It is usually
# removed by "forget_last_answer" method just before question is asked. The only exceptions
# are pre-commit checks which (except the first - build - step) do not remove the last forced
# answer so that the first answer is used for all builds within this pre-commit run.
#
function _set_last_force_answer_file() {
    LAST_FORCE_ANSWER_FILE="${BUILD_CACHE_DIR}/last_force_answer.sh"
    export LAST_FORCE_ANSWER_FILE
}

function _match_files_regexp() {
    FILE_MATCHES="false"
    REGEXP=${1}
    while (($#))
    do
        REGEXP=${1}
        for FILE in ${CHANGED_FILE_NAMES}
        do
          if  [[ ${FILE} =~ ${REGEXP} ]]; then
             FILE_MATCHES="true"
          fi
        done
        shift
    done
    export FILE_MATCHES
}

# Removes the "Force answer" (yes/no/quit) given previously
#
# This is the default behaviour of all rebuild scripts to ask independently whether you want to
# rebuild the image or not. Sometimes however we want to remember answer previously given.
# This is for example if you answered "no" to rebuild the image, the assumption is that you do not
# want to rebuild image for other rebuilds in the same pre-commit execution.
#
# All the pre-commit checks therefore have `export REMEMBER_LAST_ANSWER="true"` set
# So that in case they are run in a sequence of commits they will not rebuild. Similarly if your most
# recent answer was "no" and you run `pre-commit run mypy` (for example) it will also reuse the
# "no" answer given previously.
#
# This happens until you run any of the breeze commands or run build stepo of pre-commit.
#
function forget_last_answer() {
    if [[ ${REMEMBER_LAST_ANSWER:=""} != "true" ]]; then
        print_info
        print_info "Removing last answer from ${LAST_FORCE_ANSWER_FILE}"
        print_info
        rm -f "${LAST_FORCE_ANSWER_FILE}"
    else
        if [[ -f "${LAST_FORCE_ANSWER_FILE}" ]]; then
            print_info
            print_info "Retaining last answer from ${LAST_FORCE_ANSWER_FILE}"
            print_info "$(cat "${LAST_FORCE_ANSWER_FILE}")"
            print_info
        fi
    fi
}

#
# Sets mounting of host volumes to container for static checks
# unless MOUNT_HOST_VOLUMES is not true
#
# Output: EXTRA_DOCKER_FLAGS
function _set_extra_container_docker_flags() {
    MOUNT_HOST_VOLUMES=${MOUNT_HOST_VOLUMES:="true"}
    export MOUNT_HOST_VOLUMES

    if [[ ${MOUNT_HOST_VOLUMES} == "true" ]]; then
        print_info
        print_info "Mounting host volumes to Docker"
        print_info
        if [[ -d "${AIRFLOW_SOURCES}/.bash_history" ]]; then
            rm -rf "${AIRFLOW_SOURCES}/.bash_history"
        fi
        touch "${AIRFLOW_SOURCES}/.bash_history"
        EXTRA_DOCKER_FLAGS=( \
          "-v" "${AIRFLOW_SOURCES}/airflow:/opt/airflow/airflow:cached" \
          "-v" "${AIRFLOW_SOURCES}/common:/opt/airflow/common:cached" \
          "-v" "${AIRFLOW_SOURCES}/.mypy_cache:/opt/airflow/.mypy_cache:cached" \
          "-v" "${AIRFLOW_SOURCES}/dev:/opt/airflow/dev:cached" \
          "-v" "${AIRFLOW_SOURCES}/docs:/opt/airflow/docs:cached" \
          "-v" "${AIRFLOW_SOURCES}/scripts:/opt/airflow/scripts:cached" \
          "-v" "${AIRFLOW_SOURCES}/.kube:/root/.kube:cached" \
          "-v" "${AIRFLOW_SOURCES}/.bash_history:/root/.bash_history:cached" \
          "-v" "${AIRFLOW_SOURCES}/.bash_aliases:/root/.bash_aliases:cached" \
          "-v" "${AIRFLOW_SOURCES}/.inputrc:/root/.inputrc:cached" \
          "-v" "${AIRFLOW_SOURCES}/.bash_completion.d:/root/.bash_completion.d:cached" \
          "-v" "${AIRFLOW_SOURCES}/tmp:/opt/airflow/tmp:cached" \
          "-v" "${AIRFLOW_SOURCES}/tests:/opt/airflow/tests:cached" \
          "-v" "${AIRFLOW_SOURCES}/.flake8:/opt/airflow/.flake8:cached" \
          "-v" "${AIRFLOW_SOURCES}/pylintrc:/opt/airflow/pylintrc:cached" \
          "-v" "${AIRFLOW_SOURCES}/setup.cfg:/opt/airflow/setup.cfg:cached" \
          "-v" "${AIRFLOW_SOURCES}/setup.py:/opt/airflow/setup.py:cached" \
          "-v" "${AIRFLOW_SOURCES}/.rat-excludes:/opt/airflow/.rat-excludes:cached" \
          "-v" "${AIRFLOW_SOURCES}/logs:/opt/airflow/logs:cached" \
          "-v" "${AIRFLOW_SOURCES}/logs:/root/logs:cached" \
          "-v" "${AIRFLOW_SOURCES}/files:/files:cached" \
          "-v" "${AIRFLOW_SOURCES}/tmp:/opt/airflow/tmp:cached" \
          "--env" "PYTHONDONTWRITEBYTECODE" \
        )
    else
        print_info
        print_info "Skip mounting host volumes to Docker"
        print_info
        EXTRA_DOCKER_FLAGS=( \
            "--env" "PYTHONDONTWRITEBYTECODE" \
        )
    fi
    export EXTRA_DOCKER_FLAGS
}

#
# Verifies if stored md5sum of the file changed since the last tme ot was checked
# The md5sum files are stored in .build directory - you can delete this directory
# If you want to rebuild everything from the scratch
# Returns 0 if md5sum is OK
function _check_file_md5sum {
    local FILE="${1}"
    local MD5SUM
    local MD5SUM_CACHE_DIR="${BUILD_CACHE_DIR}/${DEFAULT_BRANCH}/${THE_IMAGE_TYPE}"
    mkdir -pv "${MD5SUM_CACHE_DIR}"
    MD5SUM=$(md5sum "${FILE}")
    local MD5SUM_FILE
    MD5SUM_FILE="${MD5SUM_CACHE_DIR}"/$(basename "${FILE}").md5sum
    local MD5SUM_FILE_NEW
    MD5SUM_FILE_NEW=${CACHE_TMP_FILE_DIR}/$(basename "${FILE}").md5sum.new
    echo "${MD5SUM}" > "${MD5SUM_FILE_NEW}"
    local RET_CODE=0
    if [[ ! -f "${MD5SUM_FILE}" ]]; then
        print_info "Missing md5sum for ${FILE#${AIRFLOW_SOURCES}} (${MD5SUM_FILE#${AIRFLOW_SOURCES}})"
        RET_CODE=1
    else
        diff "${MD5SUM_FILE_NEW}" "${MD5SUM_FILE}" >/dev/null
        RES=$?
        if [[ "${RES}" != "0" ]]; then
            print_info "The md5sum changed for ${FILE}"
            RET_CODE=1
        fi
    fi
    return ${RET_CODE}
}

#
# Moves md5sum file from it's temporary location in CACHE_TMP_FILE_DIR to
# BUILD_CACHE_DIR - thus updating stored MD5 sum fo the file
#
function _move_file_md5sum {
    local FILE="${1}"
    local MD5SUM_FILE
    local MD5SUM_CACHE_DIR="${BUILD_CACHE_DIR}/${DEFAULT_BRANCH}/${THE_IMAGE_TYPE}"
    mkdir -pv "${MD5SUM_CACHE_DIR}"
    MD5SUM_FILE="${MD5SUM_CACHE_DIR}"/$(basename "${FILE}").md5sum
    local MD5SUM_FILE_NEW
    MD5SUM_FILE_NEW=${CACHE_TMP_FILE_DIR}/$(basename "${FILE}").md5sum.new
    if [[ -f "${MD5SUM_FILE_NEW}" ]]; then
        mv "${MD5SUM_FILE_NEW}" "${MD5SUM_FILE}"
        print_info "Updated md5sum file ${MD5SUM_FILE} for ${FILE}."
    fi
}

#
# Stores md5sum files for all important files and
# records that we built the images locally so that next time we use
# it from the local docker cache rather than pull (unless forced)
#
function _update_all_md5_files() {
    print_info
    print_info "Updating md5sum files"
    print_info
    local FILE
    for FILE in "${FILES_FOR_REBUILD_CHECK[@]}"
    do
        _move_file_md5sum "${AIRFLOW_SOURCES}/${FILE}"
    done
    local SUFFIX=""
    if [[ -n ${PYTHON_VERSION:=""} ]]; then
        SUFFIX="_${PYTHON_VERSION}"
    fi
    mkdir -pv "${BUILD_CACHE_DIR}/${DEFAULT_BRANCH}"
    touch "${BUILD_CACHE_DIR}/${DEFAULT_BRANCH}/.built_${THE_IMAGE_TYPE}${SUFFIX}"
}

#
# Checks md5sum of all important files in order to optimise speed of running various operations
# That mount sources of Airflow to container and require docker image built with latest dependencies.
# the Docker image will only be marked for rebuilding only in case any of the important files change:
#
# This is needed because we want to skip rebuilding of the image when only airflow sources change but
# Trigger rebuild in case we need to change dependencies (setup.py, setup.cfg, change version of Airflow
# or the Dockerfile itself changes.
#
# As result of this check - most of the static checks will start pretty much immediately.
#
# Output:  DOCKER_BUILD_NEEDED
#
function _check_if_docker_build_is_needed() {
    print_info "Checking if build is needed for ${THE_IMAGE_TYPE} image python version: ${PYTHON_VERSION}"
    local IMAGE_BUILD_NEEDED="false"
    local FILE
    if [[ ${FORCE_DOCKER_BUILD:=""} == "true" ]]; then
        print_info "Docker image build is forced for ${THE_IMAGE_TYPE} image"
        set +e
        for FILE in "${FILES_FOR_REBUILD_CHECK[@]}"
        do
            # Just store md5sum for all files in md5sum.new - do not check if it is different
            _check_file_md5sum "${AIRFLOW_SOURCES}/${FILE}"
        done
        set -e
        IMAGES_TO_REBUILD+=("${THE_IMAGE_TYPE}")
        export DOCKER_BUILD_NEEDED="true"
    else
        set +e
        for FILE in "${FILES_FOR_REBUILD_CHECK[@]}"
        do
            if ! _check_file_md5sum "${AIRFLOW_SOURCES}/${FILE}"; then
                export DOCKER_BUILD_NEEDED="true"
                IMAGE_BUILD_NEEDED=true
            fi
        done
        set -e
        if [[ ${IMAGE_BUILD_NEEDED} == "true" ]]; then
            IMAGES_TO_REBUILD+=("${THE_IMAGE_TYPE}")
            export DOCKER_BUILD_NEEDED="true"
            print_info "Docker image build is needed for ${THE_IMAGE_TYPE} image!"
        else
            print_info "Docker image build is not needed for ${THE_IMAGE_TYPE} image!"
        fi
    fi
    print_info
}

#
# Confirms that the image should be rebuilt
# Input LAST_FORCE_ANSWER_FILE - file containing FORCE_ANSWER_TO_QUESTIONS variable set
# Output: FORCE_ANSWER_TO_QUESTIONS (yes/no)
#         LAST_FORCE_ANSWER_FILE contains `export FORCE_ANSWER_TO_QUESTIONS=yes/no`
# Exits when q is selected.
function _confirm_image_rebuild() {
    if [[ -f "${LAST_FORCE_ANSWER_FILE}" ]]; then
        # set variable from last answered response given in the same pre-commit run - so that it can be
        # set in one pre-commit check (build) and then used in another (pylint/mypy/flake8 etc).
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
        "${AIRFLOW_SOURCES}/confirm" "Rebuild image ${THE_IMAGE_TYPE} (might take some time)"
        RES=$?
    elif [[ ${DETECTED_TERMINAL:=$(tty)} != "not a tty" ]]; then
        # Make sure to use output of tty rather than stdin/stdout when available - this way confirm
        # will works also in case of pre-commits (git does not pass stdin/stdout to pre-commit hooks)
        # shellcheck disable=SC2094
        "${AIRFLOW_SOURCES}/confirm" "Rebuild image ${THE_IMAGE_TYPE} (might take some time)" \
            <"${DETECTED_TERMINAL}" >"${DETECTED_TERMINAL}"
        RES=$?
        export DETECTED_TERMINAL
    elif [[ -c /dev/tty ]]; then
        export DETECTED_TERMINAL=/dev/tty
        # Make sure to use /dev/tty first rather than stdin/stdout when available - this way confirm
        # will works also in case of pre-commits (git does not pass stdin/stdout to pre-commit hooks)
        # shellcheck disable=SC2094
        "${AIRFLOW_SOURCES}/confirm" "Rebuild image ${THE_IMAGE_TYPE} (might take some time)" \
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
        print_info "Skipping rebuild for image ${THE_IMAGE_TYPE}"
        print_info
        SKIP_REBUILD="true"
        # Force "no" also to subsequent questions so that if you answer it once, you are not asked
        # For all other pre-commits and you will continue using the images you already have
        export FORCE_ANSWER_TO_QUESTIONS="no"
        echo 'export FORCE_ANSWER_TO_QUESTIONS="no"' > "${LAST_FORCE_ANSWER_FILE}"
    elif [[ ${RES} == "2" ]]; then
        echo >&2
        echo >&2 "ERROR: The ${THE_IMAGE_TYPE} needs to be rebuilt - it is outdated. "
        echo >&2 "   Make sure you build the images bu running run one of:"
        echo >&2 "         * PYTHON_VERSION=${PYTHON_VERSION} ./scripts/ci/local_ci_build*.sh"
        echo >&2 "         * PYTHON_VERSION=${PYTHON_VERSION} ./scripts/ci/local_ci_pull_and_build*.sh"
        echo >&2
        echo >&2 "   If you run it via pre-commit as individual hook, you can run 'pre-commit run build'."
        echo >&2
        exit 1
    else
        # Force "yes" also to subsequent questions
        export FORCE_ANSWER_TO_QUESTIONS="yes"
    fi
}

function pull_base_python_image() {
    echo
    verbose_docker pull "${PYTHON_BASE_IMAGE}"
    echo
}

function _pull_image_if_needed() {
    # Whether to force pull images to populate cache
    export FORCE_PULL_IMAGES=${FORCE_PULL_IMAGES:="false"}
    # In CI environment we skip pulling latest python image
    export PULL_BASE_IMAGES=${PULL_BASE_IMAGES:=${NON_CI}}

    if [[ "${USE_DOCKER_CACHE}" == "true" ]]; then
        if [[ "${FORCE_PULL_IMAGES}" == "true" ]]; then
            if [[ ${PULL_BASE_IMAGES} == "false" ]]; then
                echo
                echo "Skip force-pulling the ${PYTHON_BASE_IMAGE} image."
                echo
            else
                echo
                echo "Force pull base image ${PYTHON_BASE_IMAGE}"
                echo
                verbose_docker pull "${PYTHON_BASE_IMAGE}"
                echo
            fi
        fi
        IMAGES="${AIRFLOW_IMAGE}"
        if [[ ${AIRFLOW_IMAGE} == "${AIRFLOW_PROD_IMAGE}" ]]; then
            # If building PROD image we also need CI image to be pulled and used (until BUILDKIT works)
            IMAGES="${IMAGES} ${AIRFLOW_CI_IMAGE}"
        fi
        for IMAGE in ${IMAGES}
        do
            local PULL_IMAGE=${FORCE_PULL_IMAGES}
            local IMAGE_HASH
            IMAGE_HASH=$(docker images -q "${IMAGE}" 2> /dev/null)
            if [[ "${IMAGE_HASH}" == "" ]]; then
                PULL_IMAGE="true"
            fi
            if [[ "${PULL_IMAGE}" == "true" ]]; then
                echo
                echo "Pulling the image ${IMAGE}"
                echo
                verbose_docker pull "${IMAGE}" || true
                echo
            fi
        done
    fi
}

#
# Builds the image
#
function _build_image() {
    _print_build_info
    echo
    echo Building image "${IMAGE_DESCRIPTION}"
    echo
    _pull_image_if_needed

    if [[ "${USE_LOCAL_DOCKER_CACHE}" == "true" ]]; then
        export DOCKER_CACHE_CI_DIRECTIVE=()
        export DOCKER_CACHE_PROD_DIRECTIVE=()
    elif [[ "${USE_DOCKER_CACHE}" == "false" ]]; then
        export DOCKER_CACHE_CI_DIRECTIVE=("--no-cache")
        export DOCKER_CACHE_PROD_DIRECTIVE=("--no-cache")
    else
        export DOCKER_CACHE_CI_DIRECTIVE=(
            "--cache-from" "${AIRFLOW_CI_IMAGE}"
        )
        export DOCKER_CACHE_PROD_DIRECTIVE=(
            "--cache-from" "${AIRFLOW_CI_IMAGE}"
            "--cache-from" "${AIRFLOW_PROD_IMAGE}"
        )
    fi
    VERBOSE=${VERBOSE:="false"}
    if [[ -n ${DETECTED_TERMINAL:=""} ]]; then
        echo -n "Building ${THE_IMAGE_TYPE}.
        " > "${DETECTED_TERMINAL}"
        spin "${OUTPUT_LOG}" &
        SPIN_PID=$!
        # shellcheck disable=SC2064
        trap "kill ${SPIN_PID}" SIGINT SIGTERM
    fi
    if [[ ${THE_IMAGE_TYPE} == "CI" ]]; then
        DEBUG_FIXING_PERMISSIONS=${DEBUG_FIXING_PERMISSIONS:="false"}
        set +u
        verbose_docker build \
            --build-arg PYTHON_BASE_IMAGE="${PYTHON_BASE_IMAGE}" \
            --build-arg AIRFLOW_VERSION="${AIRFLOW_VERSION}" \
            --build-arg AIRFLOW_BRANCH="${BRANCH_NAME}" \
            --build-arg DEBUG_FIXING_PERMISSIONS="${DEBUG_FIXING_PERMISSIONS}" \
            "${DOCKER_CACHE_CI_DIRECTIVE[@]}" \
            -t "${AIRFLOW_CI_IMAGE}" \
            --target "${TARGET_IMAGE}" \
            . | tee -a "${OUTPUT_LOG}"
        set -u
   elif [[ ${THE_IMAGE_TYPE} == "PROD" ]]; then
        set +u
        verbose_docker build \
            --build-arg PYTHON_BASE_IMAGE="${PYTHON_BASE_IMAGE}" \
            --build-arg AIRFLOW_VERSION="${AIRFLOW_VERSION}" \
            --build-arg AIRFLOW_BRANCH="${BRANCH_NAME}" \
            "${DOCKER_CACHE_PROD_DIRECTIVE[@]}" \
            -t "${AIRFLOW_IMAGE}" \
            --target "${TARGET_IMAGE}" \
            . | tee -a "${OUTPUT_LOG}"
        set -u
    fi
    if [[ -n "${DEFAULT_IMAGE:=}" ]]; then
        verbose_docker tag "${AIRFLOW_IMAGE}" "${DEFAULT_IMAGE}" | tee -a "${OUTPUT_LOG}"
    fi
    if [[ -n ${SPIN_PID:=""} ]]; then
        kill "${SPIN_PID}" || true
        wait "${SPIN_PID}" || true
        echo > "${DETECTED_TERMINAL}"
    fi
}

function remove_all_images() {
    echo
    "${AIRFLOW_SOURCES}/confirm" "Removing all local images ."
    echo
    verbose_docker rmi "${PYTHON_BASE_IMAGE}" || true
    verbose_docker rmi "${AIRFLOW_PROD_IMAGE}" || true
    verbose_docker rmi "${AIRFLOW_CI_IMAGE}" || true
    echo
    echo "###################################################################"
    echo "NOTE!! Removed Airflow images for Python version ${PYTHON_VERSION}."
    echo "       But the disk space in docker will be reclaimed only after"
    echo "       running 'docker system prune' command."
    echo "###################################################################"
    echo
}

#
# Rebuilds an image if needed
#
function _rebuild_image_if_needed() {
    set_image_variables

    if [[ -f "${BUILD_CACHE_DIR}/${DEFAULT_BRANCH}/.built_${THE_IMAGE_TYPE}_${PYTHON_VERSION}" ]]; then
        print_info
        print_info "${THE_IMAGE_TYPE} image already built locally."
        print_info
    else
        print_info
        print_info "${THE_IMAGE_TYPE} image not built locally: force building"
        print_info
        export FORCE_DOCKER_BUILD="true"
    fi

    DOCKER_BUILD_NEEDED="false"
    IMAGES_TO_REBUILD=()
    _check_if_docker_build_is_needed
    if [[ "${DOCKER_BUILD_NEEDED}" == "true" ]]; then
        SKIP_REBUILD="false"
        if [[ ${CI:=} != "true" && "${FORCE_BUILD:=}" != "true" ]]; then
            _confirm_image_rebuild
        fi
        if [[ ${SKIP_REBUILD} != "true" ]]; then
            _fix_group_permissions
            print_info
            print_info "Rebuilding started: ${THE_IMAGE_TYPE} image."
            print_info
            _build_image
            _update_all_md5_files
            print_info
            print_info "Rebuilding completed: ${THE_IMAGE_TYPE} image."
            print_info
        fi
    else
        print_info
        print_info "No need to rebuild - none of the important files changed: ${FILES_FOR_REBUILD_CHECK[*]}"
        print_info
    fi
}

_cleanup_image() {
    set_image_variables
    verbose_docker rmi "${AIRFLOW_IMAGE}" || true
}

_push_image() {
    set_image_variables
    verbose_docker push "${AIRFLOW_IMAGE}"
    if [[ -n ${DEFAULT_IMAGE:=""} ]]; then
        verbose_docker push "${DEFAULT_IMAGE}"
    fi
}

function rebuild_ci_image_if_needed() {
    export THE_IMAGE_TYPE="CI"
    export IMAGE_DESCRIPTION="Airflow CI"
    export TARGET_IMAGE="airflow-ci"
    _rebuild_image_if_needed
    if [[ ${START_KUBERNETES_CLUSTER} == "true" || ! -f "${AIRFLOW_CI_KUBERNETES_IMAGE_ARCHIVE}" ]]; then
        _build_and_save_kubernetes_image
    fi
}

function cleanup_ci_image() {
    export THE_IMAGE_TYPE="CI"
    _cleanup_image
}

function push_ci_image() {
    export THE_IMAGE_TYPE="CI"
    _push_image
}

function rebuild_prod_image_if_needed() {
    export THE_IMAGE_TYPE="PROD"
    export IMAGE_DESCRIPTION="Airflow PROD"
    export TARGET_IMAGE="airflow-prod"
    _rebuild_image_if_needed
}

function cleanup_prod_image() {
    export THE_IMAGE_TYPE="PROD"
    _cleanup_image
}

function push_prod_image() {
    export THE_IMAGE_TYPE="PROD"
    _push_image
}

function _go_to_airflow_sources {
    print_info
    pushd "${AIRFLOW_SOURCES}" &>/dev/null || exit 1
    print_info
    print_info "Running in host in $(pwd)"
    print_info
}

function rebuild_all_images_if_needed_and_confirmed() {
    DOCKER_BUILD_NEEDED="false"
    IMAGES_TO_REBUILD=()
    for THE_IMAGE_TYPE in "${LOCALLY_BUILT_IMAGES[@]}"
    do
        _check_if_docker_build_is_needed
    done

    if [[ ${DOCKER_BUILD_NEEDED} == "true" ]]; then
        print_info
        print_info "Docker image build is needed for ${IMAGES_TO_REBUILD[*]}!"
        print_info
    else
        print_info
        print_info "Docker image build is not needed for any of the image types!"
        print_info
    fi

    if [[ "${DOCKER_BUILD_NEEDED}" == "true" ]]; then
        echo
        echo "Some of your images need to be rebuild because important files (like package list) has changed."
        echo
        echo "You have those options:"
        echo "   * Rebuild the images now by answering 'y' (this might take some time!)"
        echo "   * Skip rebuilding the images and hope changes are not big (you will be asked again)"
        echo "   * Quit and manually rebuild the images using"
        echo "        * PYTHON_VERSION=${PYTHON_VERSION} scripts/local_ci_build.sh or"
        echo "        * PYTHON_VERSION=${PYTHON_VERSION} scripts/local_ci_pull_and_build.sh or"
        echo
        export ACTION="rebuild"
        export THE_IMAGE_TYPE="${IMAGES_TO_REBUILD[*]}"

        SKIP_REBUILD="false"
        _confirm_image_rebuild

        if [[ ${SKIP_REBUILD} != "true" ]]; then
            rebuild_ci_image_if_needed
        fi
    fi
}

function build_image_on_ci() {
    if [[ "${CI:=}" != "true" ]]; then
        print_info
        print_info "Cleaning up docker installation!!!!!!"
        print_info
        "${AIRFLOW_SOURCES}/confirm" "Cleaning docker data and rebuilding"
    fi

    export FORCE_PULL_IMAGES="true"
    export FORCE_BUILD="true"
    export VERBOSE="${VERBOSE:="false"}"

    # Cleanup docker installation. It should be empty in CI but let's not risk
    verbose_docker system prune --all --force | tee -a "${OUTPUT_LOG}"
    rm -rf "${BUILD_CACHE_DIR}"
    mkdir -pv "${BUILD_CACHE_DIR}"

    echo
    echo "Finding changed file names ${TRAVIS_BRANCH}...HEAD"
    echo
    git config remote.origin.fetch "+refs/heads/*:refs/remotes/origin/*"
    git fetch origin "${TRAVIS_BRANCH}"
    CHANGED_FILE_NAMES=$(git diff --name-only "${TRAVIS_BRANCH}...HEAD")
    echo
    echo "Changed file names in this commit"
    echo "${CHANGED_FILE_NAMES}"
    echo

    if  [[ ${TRAVIS_JOB_NAME} == "Build PROD"* ]]; then
        echo "Skipping building image before PROD build"
    elif [[ ${TRAVIS_JOB_NAME} == "Tests"*"kubernetes"* ]]; then
        _match_files_regexp 'airflow/kubernetes/.*\.py' 'tests/kubernetes/.*\.py' \
            'airflow/www/.*\.py' 'airflow/www/.*\.js' 'airflow/www/.*\.html' \
            'scripts/ci/.*'
        if [[ ${FILE_MATCHES} == "true" || ${TRAVIS_PULL_REQUEST:=} == "false" ]]; then
            rebuild_ci_image_if_needed
        else
            touch "${BUILD_CACHE_DIR}"/.skip_tests
        fi
    elif [[ ${TRAVIS_JOB_NAME:=""} == "Tests"* ]]; then
        _match_files_regexp '.*\.py' 'airflow/www/.*\.py' 'airflow/www/.*\.js' 'airflow/www/.*\.html' \
            'scripts/ci/.*'
        if [[ ${FILE_MATCHES} == "true" || ${TRAVIS_PULL_REQUEST:=} == "false" ]]; then
            rebuild_ci_image_if_needed
        else
            touch "${BUILD_CACHE_DIR}"/.skip_tests
        fi
    elif [[ ${TRAVIS_JOB_NAME} == "Pylint"* ]]; then
        _match_files_regexp '.*\.py'
        if [[ ${FILE_MATCHES} == "true" || ${TRAVIS_PULL_REQUEST:=} == "false" ]]; then
            rebuild_ci_image_if_needed
        else
            touch "${BUILD_CACHE_DIR}"/.skip_tests
        fi
    else
        rebuild_ci_image_if_needed
    fi

    if [[ -f "${BUILD_CACHE_DIR}/.skip_tests" ]]; then
        echo
        echo "Skip running tests !!!!"
        echo
    fi

    # Disable force pulling forced above
    unset FORCE_PULL_IMAGES
    unset FORCE_BUILD
}

function _print_build_info() {
    print_info
    print_info "Airflow ${AIRFLOW_VERSION} Python: ${PYTHON_VERSION}."
    print_info
}

# Clears the status of fixing permissions so that this is done only once per script execution
function _clear_fix_group_permissions() {
    unset PERMISSIONS_FIXED
}
#
# Fixing permissions for all important files that are going to be added to Docker context
# This is necessary, because there are different default umask settings on different *NIX
# In case of some systems (especially in the CI environments) there is default +w group permission
# set automatically via UMASK when git checkout is performed.
#    https://unix.stackexchange.com/questions/315121/why-is-the-default-umask-002-or-022-in-many-unix-systems-seems-insecure-by-defa
# Unfortunately default setting in git is to use UMASK by default:
#    https://git-scm.com/docs/git-config/1.6.3.1#git-config-coresharedRepository
# This messes around with Docker context invalidation because the same files have different permissions
# and effectively different hash used for context validation calculation.
#
# We fix it by removing write permissions for other/group for important files that are checked during
# building docker images
#
function _fix_group_permissions() {
    if [[ ${PERMISSIONS_FIXED:=} == "true" ]]; then
        echo
        echo "Permissions already fixed"
        echo
        return
    fi
    echo
    echo "Fixing group permissions"
    # Fix permissions for all files (and directories of those files) that are not git-ignored.
    # Create HTML dir for dangling symlink. Using git ls-files is fast way of getting
    # all the relevant files and removing group permissions. Then we use the files to also
    # walk up the path and fix all the directories. This way we avoid costly walking through
    # all directories in the folder including node_modules and other git-ignored folders generated
    # in static
    pushd "${AIRFLOW_SOURCES}" >/dev/null
    FILES=$(git ls-files)
    FILE_NAME="files"
    while [[ "${FILES}" != "." ]];
    do
        echo "${FILES}" | xargs chmod og-rw
        echo "Fixing $(echo "${FILES}" | wc -l) ${FILE_NAME}"
        FILES=$(echo "${FILES}" | xargs -n 1 dirname | sort | uniq)
        FILE_NAME="directories"
    done
    echo
    popd >/dev/null
    echo "Fixed group permissions"
    echo
    export PERMISSIONS_FIXED="true"
}

function set_image_variables() {
    export PYTHON_BASE_IMAGE="python:${PYTHON_VERSION}-slim-buster"

    if [[ ${THE_IMAGE_TYPE:=} == "CI" ]]; then
        export AIRFLOW_IMAGE="${AIRFLOW_CI_IMAGE}"
        export AIRFLOW_IMAGE_DEFAULT="${AIRFLOW_CI_IMAGE_DEFAULT}"
    elif [[ ${THE_IMAGE_TYPE} == "PROD" ]]; then
        export AIRFLOW_IMAGE="${AIRFLOW_PROD_IMAGE}"
        export AIRFLOW_IMAGE_DEFAULT="${AIRFLOW_PROD_IMAGE_DEFAULT}"
    else
        export AIRFLOW_IMAGE=""
        export AIRFLOW_IMAGE_DEFAULT=""
    fi

    if [[ "${PYTHON_VERSION_FOR_DEFAULT_IMAGE}" == "${PYTHON_VERSION}" ]]; then
        export DEFAULT_IMAGE="${AIRFLOW_IMAGE_DEFAULT}"
    else
        export DEFAULT_IMAGE=""
    fi
}

function _get_image_variables {
    export AIRFLOW_CI_IMAGE="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${DEFAULT_BRANCH}-python${PYTHON_VERSION}-ci"
    export AIRFLOW_CI_SAVED_IMAGE_DIR="${BUILD_CACHE_DIR}/${DEFAULT_BRANCH}-python${PYTHON_VERSION}-ci-image"
    export AIRFLOW_CI_IMAGE_ID_FILE="${BUILD_CACHE_DIR}/${DEFAULT_BRANCH}-python${PYTHON_VERSION}-ci-image.sha256"

    export AIRFLOW_PROD_IMAGE="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${DEFAULT_BRANCH}-python${PYTHON_VERSION}"
    export AIRFLOW_PROD_IMAGE_DEFAULT="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${DEFAULT_BRANCH}"

    echo
    echo "Using CI image: ${AIRFLOW_CI_IMAGE} for docker compose runs"
    echo "Saved docker image will be created here: ${AIRFLOW_CI_SAVED_IMAGE_DIR}"
    echo "Id of the saved docker image will be here: ${AIRFLOW_CI_IMAGE_ID_FILE}"
    echo
    echo
    echo "Using PROD image: ${AIRFLOW_PROD_IMAGE} for production builds"
    echo

}

# Builds and saves Kubernetes image to a .tar file so that it can be mounted inside the container and
# Loaded by kind inside the docker
function _build_and_save_kubernetes_image() {

    SAVED_AIRFLOW_IMAGE_ID=$(cat "${AIRFLOW_CI_IMAGE_ID_FILE}" 2>/dev/null || true)
    export SAVED_AIRFLOW_IMAGE_ID

    if [[ ${SAVED_AIRFLOW_IMAGE_ID} == "" ]]; then
        echo
        echo "This is the first time the image is saved"
        echo
    fi
    AIRFLOW_IMAGE_ID=$(docker inspect --format='{{index .Id}}' "${AIRFLOW_CI_IMAGE}")
    if [[ "${AIRFLOW_IMAGE_ID}" == "${SAVED_AIRFLOW_IMAGE_ID}" ]]; then
        echo
        echo "The image ${AIRFLOW_CI_IMAGE} has not changed since the last time, skiping saving."
        echo
    else
        TMPDIR=$(mktemp -d)
        echo
        echo "Saving ${AIRFLOW_CI_IMAGE} image to ${AIRFLOW_CI_SAVED_IMAGE_DIR}"
        echo
        docker save "${AIRFLOW_CI_IMAGE}" | tar -xC "${TMPDIR}"
        mkdir -pv "${AIRFLOW_CI_SAVED_IMAGE_DIR}"
        rsync --recursive --checksum --whole-file --delete --stats --human-readable \
            "${TMPDIR}/" "${AIRFLOW_CI_SAVED_IMAGE_DIR}"
        rm -rf "${TMPDIR}"
        echo
        echo "Image ${AIRFLOW_CI_IMAGE} saved to ${AIRFLOW_CI_SAVED_IMAGE_DIR}"
        echo
        echo "${AIRFLOW_IMAGE_ID}" >"${AIRFLOW_CI_IMAGE_ID_FILE}"
    fi
}

#
# Performs basic sanity checks common for most of the scripts in this directory
#
function prepare_build() {
    _set_last_force_answer_file
    _get_image_variables
    forget_last_answer
    _go_to_airflow_sources
    _set_extra_container_docker_flags
    _clear_fix_group_permissions
}
