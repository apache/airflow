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

# Assume all the scripts are sourcing the _utils.sh from the scripts/ci directory
# and MY_DIR variable is set to this directory. It can be overridden however

if [[ ${VERBOSE:=} == "true" ]]; then
    set -x
else
    set +x
fi

AIRFLOW_SOURCES=${AIRFLOW_SOURCES:=$(cd "${MY_DIR}/../../" && pwd)}
export AIRFLOW_SOURCES

BUILD_CACHE_DIR="${AIRFLOW_SOURCES}/.build"
export BUILD_CACHE_DIR

LAST_FORCE_ANSWER_FILE="${BUILD_CACHE_DIR}/last_force_answer.sh"

IMAGES_TO_CHECK=("CI")
export IMAGES_TO_CHECK

mkdir -p "${AIRFLOW_SOURCES}/.mypy_cache"
mkdir -p "${AIRFLOW_SOURCES}/logs"
mkdir -p "${AIRFLOW_SOURCES}/tmp"

# shellcheck source=common/_autodetect_variables.sh
. "${AIRFLOW_SOURCES}/common/_autodetect_variables.sh"
# shellcheck source=common/_files_for_rebuild_check.sh
. "${AIRFLOW_SOURCES}/common/_files_for_rebuild_check.sh"

# Default branch name for triggered builds is the one configured in default branch
export AIRFLOW_CONTAINER_BRANCH_NAME=${AIRFLOW_CONTAINER_BRANCH_NAME:=${DEFAULT_BRANCH}}

# Default port numbers for forwarded ports
export WEBSERVER_HOST_PORT=${WEBSERVER_HOST_PORT:="28080"}
export POSTGRES_HOST_PORT=${POSTGRES_HOST_PORT:="25433"}
export MYSQL_HOST_PORT=${MYSQL_HOST_PORT:="23306"}

# Do not push images from here by default (push them directly from the build script on Dockerhub)
export AIRFLOW_CONTAINER_PUSH_IMAGES=${AIRFLOW_CONTAINER_PUSH_IMAGES:="false"}

# Disable writing .pyc files - slightly slower imports but not messing around when switching
# Python version and avoids problems with root-owned .pyc files in host
export PYTHONDONTWRITEBYTECODE=${PYTHONDONTWRITEBYTECODE:="true"}

#
# Sets mounting of host volumes to container for static checks
# unless AIRFLOW_MOUNT_HOST_VOLUMES_FOR_STATIC_CHECKS is not true
#
# Note that this cannot be function because we need the AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS array variable
#
AIRFLOW_MOUNT_HOST_VOLUMES_FOR_STATIC_CHECKS=${AIRFLOW_MOUNT_HOST_VOLUMES_FOR_STATIC_CHECKS:="true"}

# If this variable is set, we mount the whole sources directory to the host rather than
# selected volumes
AIRFLOW_MOUNT_SOURCE_DIR_FOR_STATIC_CHECKS=${AIRFLOW_MOUNT_SOURCE_DIR_FOR_STATIC_CHECKS="false"}

function print_info() {
    if [[ ${AIRFLOW_CI_SILENT:="false"} != "true" || ${VERBOSE:="false"} == "true" ]]; then
        echo "$@"
    fi
}

declare -a AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS
if [[ ${AIRFLOW_MOUNT_SOURCE_DIR_FOR_STATIC_CHECKS} == "true" ]]; then
    print_info
    print_info "Mount whole airflow source directory for static checks (make sure all files are in container)"
    print_info
    AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS=( \
      "-v" "${AIRFLOW_SOURCES}:/opt/airflow" \
      "--env" "PYTHONDONTWRITEBYTECODE" \
    )
elif [[ ${AIRFLOW_MOUNT_HOST_VOLUMES_FOR_STATIC_CHECKS} == "true" ]]; then
    print_info
    print_info "Mounting necessary host volumes to Docker"
    print_info
    AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS=( \
      "-v" "${AIRFLOW_SOURCES}/airflow:/opt/airflow/airflow:cached" \
      "-v" "${AIRFLOW_SOURCES}/.mypy_cache:/opt/airflow/.mypy_cache:cached" \
      "-v" "${AIRFLOW_SOURCES}/dev:/opt/airflow/dev:cached" \
      "-v" "${AIRFLOW_SOURCES}/docs:/opt/airflow/docs:cached" \
      "-v" "${AIRFLOW_SOURCES}/scripts:/opt/airflow/scripts:cached" \
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
    AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS=( \
        "--env" "PYTHONDONTWRITEBYTECODE" \
    )
fi

export AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS

#
# Creates cache directory where we will keep temporary files needed for the build
#
# This directory will be automatically deleted when the script is killed or exists (via trap)
# Unless SKIP_CACHE_DELETION variable is set. You can set this variable and then see
# the output/files generated by the scripts in this directory.
#
# Most useful is out.log file in this directory storing verbose output of the scripts.
#
function create_cache_directory() {
    CACHE_TMP_FILE_DIR=$(mktemp -d)
    export CACHE_TMP_FILE_DIR

    if [[ ${SKIP_CACHE_DELETION:=} != "true" ]]; then
        trap 'rm -rf -- "${CACHE_TMP_FILE_DIR}"' INT TERM HUP
    fi

    OUTPUT_LOG="${CACHE_TMP_FILE_DIR}/out.log"
    export OUTPUT_LOG
}

function remove_cache_directory() {
    if [[ -z "${CACHE_TMP_FILE_DIR}" ]]; then
        rm -rf -- "${CACHE_TMP_FILE_DIR}"
    fi
}

#
# Verifies if stored md5sum of the file changed since the last tme ot was checked
# The md5sum files are stored in .build directory - you can delete this directory
# If you want to rebuild everything from the scratch
#
function check_file_md5sum {
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
function move_file_md5sum {
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
function update_all_md5_files() {
    print_info
    print_info "Updating md5sum files"
    print_info
    for FILE in "${FILES_FOR_REBUILD_CHECK[@]}"
    do
        move_file_md5sum "${AIRFLOW_SOURCES}/${FILE}"
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
# * setup.py
# * setup.cfg
# * Dockerfile
# * airflow/version.py
#
# This is needed because we want to skip rebuilding of the image when only airflow sources change but
# Trigger rebuild in case we need to change dependencies (setup.py, setup.cfg, change version of Airflow
# or the Dockerfile itself changes.
#
# Another reason to skip rebuilding Docker is thar currently it takes a bit longer time than simple Docker
# We need to fix group permissions of files in Docker because different linux build services have
# different default umask and Docker uses group permissions in checking for cache invalidation.
#
# As result of this check - most of the static checks will start pretty much immediately.
#
function check_if_docker_build_is_needed() {
    print_info
    print_info "Checking if docker image build is needed for ${THE_IMAGE_TYPE} image."
    print_info
    local IMAGE_BUILD_NEEDED="false"
    if [[ ${AIRFLOW_CONTAINER_FORCE_DOCKER_BUILD:=""} == "true" ]]; then
        print_info "Docker image build is forced for ${THE_IMAGE_TYPE} image"
        set +e
        for FILE in "${FILES_FOR_REBUILD_CHECK[@]}"
        do
            # Just store md5sum for all files in md5sum.new - do not check if it is different
            check_file_md5sum "${AIRFLOW_SOURCES}/${FILE}"
        done
        set -e
        IMAGES_TO_REBUILD+=("${THE_IMAGE_TYPE}")
        export AIRFLOW_CONTAINER_DOCKER_BUILD_NEEDED="true"
    else
        set +e
        for FILE in "${FILES_FOR_REBUILD_CHECK[@]}"
        do
            if ! check_file_md5sum "${AIRFLOW_SOURCES}/${FILE}"; then
                export AIRFLOW_CONTAINER_DOCKER_BUILD_NEEDED="true"
                IMAGE_BUILD_NEEDED=true
            fi
        done
        set -e
        if [[ ${IMAGE_BUILD_NEEDED} == "true" ]]; then
            IMAGES_TO_REBUILD+=("${THE_IMAGE_TYPE}")
            export AIRFLOW_CONTAINER_DOCKER_BUILD_NEEDED="true"
            print_info "Docker image build is needed for ${THE_IMAGE_TYPE} image!"
        else
            print_info "Docker image build is not needed for ${THE_IMAGE_TYPE} image!"
        fi
    fi
    print_info
}

#
# Checks if core utils required in the host system are installed and explain what needs to be done if not
#
function check_if_coreutils_installed() {
    set +e
    getopt -T >/dev/null
    GETOPT_RETVAL=$?

    if [[ $(uname -s) == 'Darwin' ]] ; then
        command -v gstat >/dev/null
        STAT_PRESENT=$?
    else
        command -v stat >/dev/null
        STAT_PRESENT=$?
    fi

    command -v md5sum >/dev/null
    MD5SUM_PRESENT=$?

    set -e

    CMDNAME="$(basename -- "$0")"

    ####################  Parsing options/arguments
    if [[ ${GETOPT_RETVAL} != 4 || "${STAT_PRESENT}" != "0" || "${MD5SUM_PRESENT}" != "0" ]]; then
        print_info
        if [[ $(uname -s) == 'Darwin' ]] ; then
            echo >&2 "You are running ${CMDNAME} in OSX environment"
            echo >&2 "And you need to install gnu commands"
            echo >&2
            echo >&2 "Run 'brew install gnu-getopt coreutils'"
            echo >&2
            echo >&2 "Then link the gnu-getopt to become default as suggested by brew."
            echo >&2
            echo >&2 "If you use bash, you should run this command:"
            echo >&2
            echo >&2 "echo 'export PATH=\"/usr/local/opt/gnu-getopt/bin:\$PATH\"' >> ~/.bash_profile"
            echo >&2 ". ~/.bash_profile"
            echo >&2
            echo >&2 "If you use zsh, you should run this command:"
            echo >&2
            echo >&2 "echo 'export PATH=\"/usr/local/opt/gnu-getopt/bin:\$PATH\"' >> ~/.zprofile"
            echo >&2 ". ~/.zprofile"
            echo >&2
            echo >&2 "Login and logout afterwards !!"
            echo >&2
            echo >&2 "After re-login, your PATH variable should start with \"/usr/local/opt/gnu-getopt/bin\""
            echo >&2 "Your current path is ${PATH}"
            echo >&2
        else
            echo >&2 "You do not have necessary tools in your path (getopt, stat, md5sum)."
            echo >&2 "Please install latest/GNU version of getopt and coreutils."
            echo >&2 "This can usually be done with 'apt install util-linux coreutils'"
        fi
        print_info
        exit 1
    fi
}

#
# Asserts that we are not inside of the container
#
function assert_not_in_container() {
    if [[ -f /.dockerenv ]]; then
        echo >&2
        echo >&2 "You are inside the Airflow docker container!"
        echo >&2 "You should only run this script from the host."
        echo >&2 "Learn more about how we develop and test airflow in:"
        echo >&2 "https://github.com/apache/airflow/blob/master/CONTRIBUTING.md"
        echo >&2
        exit 1
    fi
}

function forget_last_answer() {
    # Removes the "Forced answer" (yes/no/quit) given previously, unles you specifically want to remember it.
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


function confirm_image_rebuild() {
    if [[ -f "${LAST_FORCE_ANSWER_FILE}" ]]; then
        # set variable from last answered response given in the same pre-commit run - so that it can be
        # set in one pre-commit check (build) and then used in another (pylint/mypy/flake8 etc).
        # shellcheck disable=SC1090
        source "${LAST_FORCE_ANSWER_FILE}"
    fi
    set +e
    if [[ ${CI:="false"} == "true" ]]; then
        print_info
        print_info "CI environment - forcing ${ACTION} for ${THE_IMAGE_TYPE} image."
        print_info
        RES="0"
    elif [[ -c /dev/tty ]]; then
        # Make sure to use /dev/tty first rather than stdin/stdout when available - this way confirm
        # will works also in case of pre-commits (git does not pass stdin/stdout to pre-commit hooks)
        "${AIRFLOW_SOURCES}/confirm" "${ACTION} ${THE_IMAGE_TYPE}" </dev/tty >/dev/tty
        RES=$?
    elif [[ -t 0 ]]; then
        # Check if this script is run interactively with stdin open and terminal attached
        "${AIRFLOW_SOURCES}/confirm" "${ACTION} ${THE_IMAGE_TYPE}"
        RES=$?
    else
        # No terminal, no stdin - quitting!
        RES="2"
    fi
    set -e
    if [[ ${RES} == "1" ]]; then
        print_info
        print_info "Skipping ${ACTION} for ${THE_IMAGE_TYPE}"
        print_info
        SKIP_REBUILD="true"
        # Force "no" also to subsequent questions so that if you answer it once, you are not asked
        # For all other pre-commits and you will continue using the images you already have
        export FORCE_ANSWER_TO_QUESTIONS="no"
        echo 'export FORCE_ANSWER_TO_QUESTIONS="no"' > "${LAST_FORCE_ANSWER_FILE}"
    elif [[ ${RES} == "2" ]]; then
        echo >&2
        echo >&2 "ERROR: The image needs ${ACTION} for ${THE_IMAGE_TYPE} - it is outdated. "
        echo >&2 "   Make sure you build the images bu running run one of:"
        echo >&2 "         * ./scripts/ci/local_ci_build.sh"
        echo >&2 "         * ./scripts/ci/local_ci_pull_and_build.sh"
        echo >&2
        echo >&2 "   If you run it via pre-commit separately, run 'pre-commit run build' first."
        echo >&2
        exit 1
    else
        # Force "yes" also to subsequent questions
        export FORCE_ANSWER_TO_QUESTIONS="yes"
    fi
}

function rebuild_image_if_needed() {
    AIRFLOW_VERSION=$(cat airflow/version.py - << EOF | python
print(version.replace("+",""))
EOF
    )
    export AIRFLOW_VERSION

    if [[ ${AIRFLOW_CONTAINER_CLEANUP_IMAGES:="false"} == "true" ]]; then
        print_info
        print_info "Clean up ${THE_IMAGE_TYPE} image. Just cleanup no pull of images happen."
        print_info
        export AIRFLOW_CONTAINER_FORCE_PULL_IMAGES="false"
        export AIRFLOW_CONTAINER_FORCE_DOCKER_BUILD="true"
    elif [[ -f "${BUILD_CACHE_DIR}/${DEFAULT_BRANCH}/.built_${THE_IMAGE_TYPE}_${PYTHON_VERSION}" ]]; then
        print_info
        print_info "${THE_IMAGE_TYPE} image already built locally."
        print_info
    else
        print_info
        print_info "${THE_IMAGE_TYPE} image not built locally: pulling and building"
        print_info
        export AIRFLOW_CONTAINER_FORCE_PULL_IMAGES="true"
        export AIRFLOW_CONTAINER_FORCE_DOCKER_BUILD="true"
    fi

    AIRFLOW_CONTAINER_DOCKER_BUILD_NEEDED="false"
    IMAGES_TO_REBUILD=()
    check_if_docker_build_is_needed
    if [[ "${AIRFLOW_CONTAINER_DOCKER_BUILD_NEEDED}" == "true" ]]; then
        SKIP_REBUILD="false"
        if [[ ${AIRFLOW_CONTAINER_CLEANUP_IMAGES} == "true" ]]; then
            export ACTION="clean"
        else
            export ACTION="rebuild"
        fi
        if [[ ${CI:=} != "true" && "${FORCE_BUILD:=}" != "true" ]]; then
            confirm_image_rebuild
        fi
        if [[ ${SKIP_REBUILD} != "true" ]]; then
            print_info
            print_info "${ACTION} start: ${THE_IMAGE_TYPE} image."
            print_info
            # shellcheck source=hooks/build
            ./hooks/build | tee -a "${OUTPUT_LOG}"
            update_all_md5_files
            print_info
            print_info "${ACTION} completed: ${THE_IMAGE_TYPE} image."
            print_info
        fi
    else
        print_info
        print_info "No need to rebuild - none of the important files changed: ${FILES_FOR_REBUILD_CHECK[*]}"
        print_info
    fi
}

#
# Rebuilds the image for tests if needed.
#
function rebuild_ci_image_if_needed() {
    export AIRFLOW_CONTAINER_SKIP_CI_IMAGE="false"

    export THE_IMAGE_TYPE="CI"

    rebuild_image_if_needed

    export AIRFLOW_CI_IMAGE="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${DEFAULT_BRANCH}-python${PYTHON_VERSION}-ci"
}


#
# Cleans up the CI image
#
function cleanup_ci_image() {
    export AIRFLOW_CONTAINER_SKIP_CI_IMAGE="false"
    export AIRFLOW_CONTAINER_CLEANUP_IMAGES="true"

    export THE_IMAGE_TYPE="CI"

    rebuild_image_if_needed
}

#
# Starts the script/ If VERBOSE variable is set to true, it enables verbose output of commands executed
# Also prints some useful diagnostics information at start of the script
#
function script_start {
    print_info
    print_info "Running $(basename "$0")"
    print_info
    print_info "Log is redirected to ${OUTPUT_LOG}"
    print_info
    if [[ ${VERBOSE:=} == "true" ]]; then
        print_info
        print_info "Variable VERBOSE Set to \"true\""
        print_info "You will see a lot of output"
        print_info
        set -x
    else
        print_info "You can increase verbosity by running 'export VERBOSE=\"true\""
        if [[ ${SKIP_CACHE_DELETION:=} != "true" ]]; then
            print_info "And skip deleting the output file with 'export SKIP_CACHE_DELETION=\"true\""
        fi
        print_info
        set +x
    fi
    START_SCRIPT_TIME=$(date +%s)
}

#
#
# Disables verbosity in the script
#
function script_end {
    if [[ ${VERBOSE:=} == "true" ]]; then
        set +x
    fi
    END_SCRIPT_TIME=$(date +%s)
    RUN_SCRIPT_TIME=$((END_SCRIPT_TIME-START_SCRIPT_TIME))
    print_info
    print_info "Finished the script $(basename "$0")"
    print_info "It took ${RUN_SCRIPT_TIME} seconds"
    print_info
    remove_cache_directory
}

function go_to_airflow_sources {
    print_info
    pushd "${AIRFLOW_SOURCES}" &>/dev/null || exit 1
    print_info
    print_info "Running in host in $(pwd)"
    print_info
}

#
# Performs basic sanity checks common for most of the scripts in this directory
#
function basic_sanity_checks() {
    assert_not_in_container
    go_to_airflow_sources
    check_if_coreutils_installed
    create_cache_directory
    forget_last_answer
}


function run_flake8() {
    FILES=("$@")

    if [[ "${#FILES[@]}" == "0" ]]; then
        docker run "${AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS[@]}" \
            --entrypoint "/usr/local/bin/dumb-init"  \
            --env PYTHONDONTWRITEBYTECODE \
            --env AIRFLOW_CI_VERBOSE="${VERBOSE}" \
            --env AIRFLOW_CI_SILENT \
            --env HOST_USER_ID="$(id -ur)" \
            --env HOST_GROUP_ID="$(id -gr)" \
            --rm \
            "${AIRFLOW_CI_IMAGE}" \
            "--" "/opt/airflow/scripts/ci/in_container/run_flake8.sh" \
            | tee -a "${OUTPUT_LOG}"
    else
        docker run "${AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS[@]}" \
            --entrypoint "/usr/local/bin/dumb-init"  \
            --env PYTHONDONTWRITEBYTECODE \
            --env AIRFLOW_CI_VERBOSE="${VERBOSE}" \
            --env AIRFLOW_CI_SILENT \
            --env HOST_USER_ID="$(id -ur)" \
            --env HOST_GROUP_ID="$(id -gr)" \
            --rm \
            "${AIRFLOW_CI_IMAGE}" \
            "--" "/opt/airflow/scripts/ci/in_container/run_flake8.sh" "${FILES[@]}" \
            | tee -a "${OUTPUT_LOG}"
    fi
}

function run_docs() {
    docker run "${AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS[@]}" -t \
            --entrypoint "/usr/local/bin/dumb-init"  \
            --env PYTHONDONTWRITEBYTECODE \
            --env AIRFLOW_CI_VERBOSE="${VERBOSE}" \
            --env AIRFLOW_CI_SILENT \
            --env HOST_USER_ID="$(id -ur)" \
            --env HOST_GROUP_ID="$(id -gr)" \
            --rm \
            "${AIRFLOW_CI_IMAGE}" \
            "--" "/opt/airflow/docs/build.sh" \
            | tee -a "${OUTPUT_LOG}"
}

function run_check_license() {
    docker run "${AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS[@]}" -t \
            --entrypoint "/usr/local/bin/dumb-init"  \
            --env PYTHONDONTWRITEBYTECODE \
            --env AIRFLOW_CI_VERBOSE="${VERBOSE}" \
            --env AIRFLOW_CI_SILENT \
            --env HOST_USER_ID="$(id -ur)" \
            --env HOST_GROUP_ID="$(id -gr)" \
            --rm \
            "${AIRFLOW_CI_IMAGE}" \
            "--" "/opt/airflow/scripts/ci/in_container/run_check_licence.sh" \
            | tee -a "${OUTPUT_LOG}"
}

function run_mypy() {
    FILES=("$@")
    if [[ "${#FILES[@]}" == "0" ]]; then
        docker run "${AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS[@]}" \
            --entrypoint "/usr/local/bin/dumb-init"  \
            --env PYTHONDONTWRITEBYTECODE \
            --env AIRFLOW_CI_VERBOSE="${VERBOSE}" \
            --env AIRFLOW_CI_SILENT \
            --env HOST_USER_ID="$(id -ur)" \
            --env HOST_GROUP_ID="$(id -gr)" \
            --rm \
            "${AIRFLOW_CI_IMAGE}" \
            "--" "/opt/airflow/scripts/ci/in_container/run_mypy.sh" "airflow" "tests" "docs" \
            | tee -a "${OUTPUT_LOG}"
    else
        docker run "${AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS[@]}" \
            --entrypoint "/usr/local/bin/dumb-init" \
            --env PYTHONDONTWRITEBYTECODE \
            --env AIRFLOW_CI_VERBOSE="${VERBOSE}" \
            --env AIRFLOW_CI_SILENT \
            --env HOST_USER_ID="$(id -ur)" \
            --env HOST_GROUP_ID="$(id -gr)" \
            --rm \
            "${AIRFLOW_CI_IMAGE}" \
            "--" "/opt/airflow/scripts/ci/in_container/run_mypy.sh" "${FILES[@]}" \
            | tee -a "${OUTPUT_LOG}"
    fi
}

function run_pylint_main() {
    FILES=("$@")
    if [[ "${#FILES[@]}" == "0" ]]; then
        docker run "${AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS[@]}" \
            --entrypoint "/usr/local/bin/dumb-init"  \
            --env PYTHONDONTWRITEBYTECODE \
            --env AIRFLOW_CI_VERBOSE="${VERBOSE}" \
            --env AIRFLOW_CI_SILENT \
            --env HOST_USER_ID="$(id -ur)" \
            --env HOST_GROUP_ID="$(id -gr)" \
            --rm \
            "${AIRFLOW_CI_IMAGE}" \
            "--" "/opt/airflow/scripts/ci/in_container/run_pylint_main.sh" \
            | tee -a "${OUTPUT_LOG}"
    else
        docker run "${AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS[@]}" \
            --entrypoint "/usr/local/bin/dumb-init" \
            --env PYTHONDONTWRITEBYTECODE \
            --env AIRFLOW_CI_VERBOSE="${VERBOSE}" \
            --env AIRFLOW_CI_SILENT \
            --env HOST_USER_ID="$(id -ur)" \
            --env HOST_GROUP_ID="$(id -gr)" \
            --rm \
            "${AIRFLOW_CI_IMAGE}" \
            "--" "/opt/airflow/scripts/ci/in_container/run_pylint_main.sh" "${FILES[@]}" \
            | tee -a "${OUTPUT_LOG}"
    fi
}


function run_pylint_tests() {
    FILES=("$@")
    if [[ "${#FILES[@]}" == "0" ]]; then
        docker run "${AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS[@]}" \
            --entrypoint "/usr/local/bin/dumb-init"  \
            --env PYTHONDONTWRITEBYTECODE \
            --env AIRFLOW_CI_VERBOSE="${VERBOSE}" \
            --env AIRFLOW_CI_SILENT \
            --env HOST_USER_ID="$(id -ur)" \
            --env HOST_GROUP_ID="$(id -gr)" \
            --rm \
            "${AIRFLOW_CI_IMAGE}" \
            "--" "/opt/airflow/scripts/ci/in_container/run_pylint_tests.sh" \
            | tee -a "${OUTPUT_LOG}"
    else
        docker run "${AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS[@]}" \
            --entrypoint "/usr/local/bin/dumb-init"  \
            --env PYTHONDONTWRITEBYTECODE \
            --env AIRFLOW_CI_VERBOSE="${VERBOSE}" \
            --env AIRFLOW_CI_SILENT \
            --env HOST_USER_ID="$(id -ur)" \
            --env HOST_GROUP_ID="$(id -gr)" \
            --rm \
            "${AIRFLOW_CI_IMAGE}" \
            "--" "/opt/airflow/scripts/ci/in_container/run_pylint_tests.sh" "${FILES[@]}" \
            | tee -a "${OUTPUT_LOG}"
    fi
}

function run_docker_lint() {
    FILES=("$@")
    if [[ "${#FILES[@]}" == "0" ]]; then
        echo
        echo "Running docker lint for all Dockerfiles"
        echo
        docker run \
            -v "$(pwd):/root" \
            -w /root \
            --rm \
            hadolint/hadolint /bin/hadolint Dockerfile*
        echo
        echo "Docker pylint completed with no errors"
        echo
    else
        echo
        echo "Running docker lint for $*"
        echo
        docker run \
            -v "$(pwd):/root" \
            -w /root \
            --rm \
            hadolint/hadolint /bin/hadolint "$@"
        echo
        echo "Docker pylint completed with no errors"
        echo
    fi
}

function filter_out_files_from_pylint_todo_list() {
  FILTERED_FILES=()
  set +e
  for FILE in "$@"
  do
      if [[ ${FILE} == "airflow/migrations/versions/"* ]]; then
          # Skip all generated migration scripts
          continue
      fi
      if ! grep -x "./${FILE}" <"${AIRFLOW_SOURCES}/scripts/ci/pylint_todo.txt" >/dev/null; then
          FILTERED_FILES+=("${FILE}")
      fi
  done
  set -e
  export FILTERED_FILES
}

function refresh_pylint_todo() {
    docker run "${AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS[@]}" \
        --entrypoint /opt/airflow/scripts/ci/in_container/refresh_pylint_todo.sh \
        --env PYTHONDONTWRITEBYTECODE \
        --env AIRFLOW_CI_VERBOSE="${VERBOSE}" \
        --env AIRFLOW_CI_SILENT \
        --env HOST_USER_ID="$(id -ur)" \
        --env HOST_GROUP_ID="$(id -gr)" \
        "${AIRFLOW_CI_IMAGE}" | tee -a "${OUTPUT_LOG}"
}

function rebuild_all_images_if_needed_and_confirmed() {
    AIRFLOW_CONTAINER_DOCKER_BUILD_NEEDED="false"
    IMAGES_TO_REBUILD=()

    for THE_IMAGE_TYPE in "${IMAGES_TO_CHECK[@]}"
    do
        check_if_docker_build_is_needed
    done

    if [[ ${AIRFLOW_CONTAINER_DOCKER_BUILD_NEEDED} == "true" ]]; then
        print_info
        print_info "Docker image build is needed for ${IMAGES_TO_REBUILD[*]}!"
        print_info
    else
        print_info
        print_info "Docker image build is not needed for any of the image types!"
        print_info
    fi

    if [[ "${AIRFLOW_CONTAINER_DOCKER_BUILD_NEEDED}" == "true" ]]; then
        echo
        echo "Some of your images need to be rebuild because important files (like package list) has changed."
        echo
        echo "You have those options:"
        echo "   * Rebuild the images now by answering 'y' (this might take some time!)"
        echo "   * Skip rebuilding the images and hope changes are not big (you will be asked again)"
        echo "   * Quit and manually rebuild the images using"
        echo "        * scripts/local_ci_build.sh or"
        echo "        * scripts/local_ci_pull_and_build.sh or"
        echo
        export ACTION="rebuild"
        export THE_IMAGE_TYPE="${IMAGES_TO_REBUILD[*]}"

        SKIP_REBUILD="false"
        confirm_image_rebuild

        if [[ ${SKIP_REBUILD} != "true" ]]; then
            rebuild_ci_image_if_needed
        fi
    fi
}

function match_files_regexp() {
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

function build_image_on_ci() {
    if [[ "${CI:=}" != "true" ]]; then
        print_info
        print_info "Cleaning up docker installation!!!!!!"
        print_info
        "${AIRFLOW_SOURCES}/confirm" "Cleaning docker data and rebuilding"
    fi

    export AIRFLOW_CONTAINER_FORCE_PULL_IMAGES="true"
    export FORCE_BUILD="true"
    export VERBOSE="${VERBOSE:="false"}"

    # Cleanup docker installation. It should be empty in CI but let's not risk
    docker system prune --all --force
    rm -rf "${BUILD_CACHE_DIR}"
    mkdir -pv "${BUILD_CACHE_DIR}"

    echo
    echo "Finding changed file names ${TRAVIS_BRANCH}...HEAD"
    echo

    git config remote.origin.fetch "+refs/heads/*:refs/remotes/origin/*"
    git fetch origin "${TRAVIS_BRANCH}"
    CHANGED_FILE_NAMES=$(git diff --name-only "remotes/origin/${TRAVIS_BRANCH}...HEAD")
    echo
    echo "Changed file names in this commit"
    echo "${CHANGED_FILE_NAMES}"
    echo

    if [[ ${TRAVIS_JOB_NAME:=""} == "Tests"*"kubernetes"* ]]; then
        match_files_regexp 'airflow/kubernetes/.*\.py' 'tests/kubernetes/.*\.py' \
            'airflow/www/.*\.py' 'airflow/www/.*\.js' 'airflow/www/.*\.html'
        if [[ ${FILE_MATCHES} == "true" || ${TRAVIS_PULL_REQUEST:=} == "false" ]]; then
            rebuild_ci_image_if_needed
        else
            touch "${BUILD_CACHE_DIR}"/.skip_tests
        fi
    elif [[ ${TRAVIS_JOB_NAME:=""} == "Tests"* ]]; then
        match_files_regexp '.*\.py' 'airflow/www/.*\.py' 'airflow/www/.*\.js' 'airflow/www/.*\.html'
        if [[ ${FILE_MATCHES} == "true" || ${TRAVIS_PULL_REQUEST:=} == "false" ]]; then
            rebuild_ci_image_if_needed
        else
            touch "${BUILD_CACHE_DIR}"/.skip_tests
        fi
    elif [[ ${TRAVIS_JOB_NAME} == "Static"* ]]; then
        rebuild_ci_image_if_needed
    elif [[ ${TRAVIS_JOB_NAME} == "Pylint"* ]]; then
        match_files_regexp '.*\.py'
        if [[ ${FILE_MATCHES} == "true" || ${TRAVIS_PULL_REQUEST:=} == "false" ]]; then
            rebuild_ci_image_if_needed
        else
            touch "${BUILD_CACHE_DIR}"/.skip_tests
        fi
    elif [[ ${TRAVIS_JOB_NAME} == "Build documentation"* ]]; then
        rebuild_ci_image_if_needed
    else
        echo
        echo "Error! Unexpected Travis job name: ${TRAVIS_JOB_NAME}"
        echo
        exit 1
    fi

    if [[ -f "${BUILD_CACHE_DIR}/.skip_tests" ]]; then
        echo
        echo "Skip running tests !!!!"
        echo
    fi

    # Disable force pulling forced above
    unset AIRFLOW_CONTAINER_FORCE_PULL_IMAGES
    unset FORCE_BUILD
}
