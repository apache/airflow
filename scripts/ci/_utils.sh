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

AIRFLOW_SOURCES=${AIRFLOW_SOURCES:=$(cd "${MY_DIR}/../../" && pwd)}
export AIRFLOW_SOURCES

BUILD_CACHE_DIR="${AIRFLOW_SOURCES}/.build"
export BUILD_CACHE_DIR

FILES_FOR_REBUILD_CHECK="\
setup.py \
setup.cfg \
Dockerfile \
.dockerignore \
airflow/version.py
"

mkdir -p "${AIRFLOW_SOURCES}/.mypy_cache"
mkdir -p "${AIRFLOW_SOURCES}/logs"
mkdir -p "${AIRFLOW_SOURCES}/tmp"

# Dockerhub user and repo where the images are stored.
export DOCKERHUB_USER=${DOCKERHUB_USER:="apache"}
export DOCKERHUB_REPO=${DOCKERHUB_REPO:="airflow"}
# Port on which webserver is exposed in host environment
export WEBSERVER_HOST_PORT=${WEBSERVER_HOST_PORT:="8080"}

# Do not push images from here by default (push them directly from the build script on Dockerhub)
export AIRFLOW_CONTAINER_PUSH_IMAGES=${AIRFLOW_CONTAINER_PUSH_IMAGES:="false"}

# Disable writing .pyc files - slightly slower imports but not messing around when switching
# Python version and avoids problems with root-owned .pyc files in host
export PYTHONDONTWRITEBYTECODE=${PYTHONDONTWRITEBYTECODE:="true"}

# Read default branch name
# shellcheck source=hooks/_default_branch.sh
. "${AIRFLOW_SOURCES}/hooks/_default_branch.sh"

# Default branch name for triggered builds is the one configured in hooks/_default_branch.sh
export AIRFLOW_CONTAINER_BRANCH_NAME=${AIRFLOW_CONTAINER_BRANCH_NAME:=${DEFAULT_BRANCH}}

PYTHON_VERSION=${PYTHON_VERSION:=$(python -c \
    'import sys; print("%s.%s" % (sys.version_info.major, sys.version_info.minor))')}
export PYTHON_VERSION

if [[ ${PYTHON_VERSION} == 2.* ]]; then
    echo 2>&1
    echo 2>&1 " You have python 2.7 on your path but python 2 is not supported any more."
    echo 2>&1 " Switching to python 3.5"
    echo 2>&1
    PYTHON_VERSION=3.5
    export PYTHON_VERSION
fi

export PYTHON_BINARY=${PYTHON_BINARY:=python${PYTHON_VERSION}}

#
# Sets mounting of host volumes to container for static checks
# unless AIRFLOW_MOUNT_HOST_VOLUMES_FOR_STATIC_CHECKS is not true
#
# Note that this cannot be function because we need the AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS array variable
#
AIRFLOW_MOUNT_HOST_VOLUMES_FOR_STATIC_CHECKS=${AIRFLOW_MOUNT_HOST_VOLUMES_FOR_STATIC_CHECKS:="true"}

function print_info() {
    if [[ ${AIRFLOW_CI_SILENT:="false"} != "true" || ${AIRFLOW_CI_VERBOSE:="false"} == "true" ]]; then
        echo "$@"
    fi
}

if [[ ${REBUILD:=false} ==  "true" ]]; then
    print_info
    print_info "Rebuilding is enabled. Assuming yes to all questions"
    print_info
    export ASSUME_YES_TO_ALL_QUESTIONS="true"
fi

declare -a AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS
if [[ ${AIRFLOW_MOUNT_HOST_VOLUMES_FOR_STATIC_CHECKS} == "true" ]]; then
    print_info
    print_info "Mounting host volumes to Docker"
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
    mkdir -p "${BUILD_CACHE_DIR}/cache/"

    CACHE_TMP_FILE_DIR=$(mktemp -d "${BUILD_CACHE_DIR}/cache/XXXXXXXXXX")
    export CACHE_TMP_FILE_DIR

    if [[ ${SKIP_CACHE_DELETION:=} != "true" ]]; then
        trap 'rm -rf -- "${CACHE_TMP_FILE_DIR}"' INT TERM HUP EXIT
    fi

    OUTPUT_LOG="${CACHE_TMP_FILE_DIR}/out.log"
    export OUTPUT_LOG
}

#
# Verifies if stored md5sum of the file changed since the last tme ot was checked
# The md5sum files are stored in .build directory - you can delete this directory
# If you want to rebuild everything from the scratch
#
function check_file_md5sum {
    local FILE="${1}"
    local MD5SUM
    mkdir -pv "${BUILD_CACHE_DIR}/${THE_IMAGE_TYPE}"
    MD5SUM=$(md5sum "${FILE}")
    local MD5SUM_FILE
    MD5SUM_FILE=${BUILD_CACHE_DIR}/${THE_IMAGE_TYPE}/$(basename "${FILE}").md5sum
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
    mkdir -pv "${BUILD_CACHE_DIR}/${THE_IMAGE_TYPE}"
    MD5SUM_FILE=${BUILD_CACHE_DIR}/${THE_IMAGE_TYPE}/$(basename "${FILE}").md5sum
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
    for FILE in ${FILES_FOR_REBUILD_CHECK}
    do
        move_file_md5sum "${AIRFLOW_SOURCES}/${FILE}"
    done
    local SUFFIX=""
    if [[ -n ${PYTHON_VERSION:=""} ]]; then
        SUFFIX="_${PYTHON_VERSION}"
    fi
    touch "${BUILD_CACHE_DIR}/.built_${THE_IMAGE_TYPE}${SUFFIX}"
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
# files. There are the following, problems with the current Dockerfiles that need longer build times:
# 1) We need to fix group permissions of files in Docker because different linux build services have
#    different default umask and Docker uses group permissions in checking for cache invalidation.
# 2) we use multi-stage build and in case of slim image we needlessly build a full CI image because
#    support for this only comes with the upcoming buildkit: https://github.com/docker/cli/issues/1134
#
# As result of this check - most of the static checks will start pretty much immediately.
#
function check_if_docker_build_is_needed() {
    set +e
    for FILE in ${FILES_FOR_REBUILD_CHECK}
    do
        if ! check_file_md5sum "${AIRFLOW_SOURCES}/${FILE}"; then
            export AIRFLOW_CONTAINER_DOCKER_BUILD_NEEDED="true"
        fi
    done
    set -e
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

#
# Forces Python version to 3.5 (for static checks)
#
function force_python_3_5() {
    # Set python version variable to force it in the container scripts
    PYTHON_VERSION=3.5
    export PYTHON_VERSION
}

function confirm_image_rebuild() {
    set +e
    "${AIRFLOW_SOURCES}/confirm" "${ACTION} the image ${THE_IMAGE_TYPE}."
    RES=$?
    set -e
    if [[ ${RES} == "1" ]]; then
        SKIP_REBUILD="true"
        # Assume No also to subsequent questions
        export ASSUME_NO_TO_ALL_QUESTIONS="true"
    elif [[ ${RES} == "2" ]]; then
        echo >&2
        echo >&2 "#############################################"
        echo >&2 "  ERROR:  ${ACTION} the image stopped. "
        echo >&2 "#############################################"
        echo >&2
        echo >&2 "  You should re-run your command with REBUILD=true environment variable set"
        echo >&2
        echo >&2 "  * 'REBUILD=true git commit'"
        echo >&2 "  * 'REBUILD=true git push'"
        echo >&2
        echo >&2 "  In case you do not want to rebuild, You can always commit the code "
        echo >&2 "  with --no-verify switch. This skips pre-commit checks. CI will run the tests anyway."
        echo >&2
        echo >&2 "  You can also rebuild the image:        './scripts/ci/local_ci_build.sh'"
        echo >&2 "  Or pull&build the image from registry: './scripts/ci/local_ci_pull_and_build.sh'"
        echo >&2
        exit 1
    else
        # Assume Yes also to subsequent questions
        export ASSUME_YES_TO_ALL_QUESTIONS="true"
    fi
}

function rebuild_image_if_needed() {
    PYTHON_VERSION=${PYTHON_VERSION:=$(python -c \
        'import sys; print("%s.%s" % (sys.version_info.major, sys.version_info.minor))')}
    export PYTHON_VERSION
    if [[ ${PYTHON_VERSION} == 2.* ]]; then
        echo 2>&1
        echo 2>&1 " You have python 2.7 on your path but python 2 is not supported any more."
        echo 2>&1 " Switching to python 3.5"
        echo 2>&1
        PYTHON_VERSION=3.5
        export PYTHON_VERSION
    fi

    AIRFLOW_VERSION=$(cat airflow/version.py - << EOF | python
print(version.replace("+",""))
EOF
    )
    export AIRFLOW_VERSION

    if [[ ${AIRFLOW_CONTAINER_CLEANUP_IMAGES:="false"} == "true" ]]; then
        print_info
        print_info "Clean up ${THE_IMAGE_TYPE}"
        print_info
        export AIRFLOW_CONTAINER_FORCE_PULL_IMAGES="false"
        export AIRFLOW_CONTAINER_DOCKER_BUILD_NEEDED="true"
    elif [[ -f "${BUILD_CACHE_DIR}/.built_${THE_IMAGE_TYPE}_${PYTHON_VERSION}" ]]; then
        print_info
        print_info "Image ${THE_IMAGE_TYPE} built locally - skip force-pulling"
        print_info
    else
        print_info
        print_info "Image ${THE_IMAGE_TYPE} not built locally - force pulling"
        print_info
        export AIRFLOW_CONTAINER_FORCE_PULL_IMAGES="true"
        export AIRFLOW_CONTAINER_DOCKER_BUILD_NEEDED="true"
    fi

    AIRFLOW_CONTAINER_DOCKER_BUILD_NEEDED=${AIRFLOW_CONTAINER_DOCKER_BUILD_NEEDED:="false"}
    check_if_docker_build_is_needed
    if [[ "${AIRFLOW_CONTAINER_DOCKER_BUILD_NEEDED}" == "true" ]]; then
        SKIP_REBUILD="false"
        if [[ ${AIRFLOW_CONTAINER_CLEANUP_IMAGES} == "true" ]]; then
            export ACTION="Cleaning"
        else
            export ACTION="Rebuilding"
        fi
        if [[ ${CI:=} != "true" ]]; then
            confirm_image_rebuild
        fi
        if [[ ${SKIP_REBUILD} != "true" ]]; then
            print_info
            print_info "${ACTION} image: ${THE_IMAGE_TYPE}"
            print_info
            # shellcheck source=hooks/build
            ./hooks/build | tee -a "${OUTPUT_LOG}"
            update_all_md5_files
            print_info
            print_info "${ACTION} image completed: ${THE_IMAGE_TYPE}"
            print_info
        fi
    else
        print_info
        print_info "No need to rebuild the image as none of the sensitive files changed: ${FILES_FOR_REBUILD_CHECK}"
        print_info
    fi
}

#
# Rebuilds the slim image for static checks if needed. In order to speed it up, it's built without NPM
#
function rebuild_ci_slim_image_if_needed() {
    export AIRFLOW_CONTAINER_SKIP_SLIM_CI_IMAGE="false"
    export AIRFLOW_CONTAINER_SKIP_CI_IMAGE="true"
    export AIRFLOW_CONTAINER_SKIP_CHECKLICENCE_IMAGE="true"

    export PYTHON_VERSION=3.5  # Always use python version 3.5 for static checks

    export THE_IMAGE_TYPE="SLIM_CI"

    rebuild_image_if_needed

    AIRFLOW_SLIM_CI_IMAGE=$(cat "${BUILD_CACHE_DIR}/.AIRFLOW_SLIM_CI_IMAGE") || true 2>/dev/null
    export AIRFLOW_SLIM_CI_IMAGE
}

#
# Cleans up the CI slim image
#
function cleanup_ci_slim_image() {
    export AIRFLOW_CONTAINER_SKIP_SLIM_CI_IMAGE="false"
    export AIRFLOW_CONTAINER_SKIP_CI_IMAGE="true"
    export AIRFLOW_CONTAINER_SKIP_CHECKLICENCE_IMAGE="true"
    export AIRFLOW_CONTAINER_CLEANUP_IMAGES="true"

    export PYTHON_VERSION=3.5  # Always use python version 3.5 for static checks

    export THE_IMAGE_TYPE="SLIM_CI"

    rebuild_image_if_needed
}

#
# Rebuilds the image for tests if needed.
#
function rebuild_ci_image_if_needed() {
    export AIRFLOW_CONTAINER_SKIP_SLIM_CI_IMAGE="true"
    export AIRFLOW_CONTAINER_SKIP_CHECKLICENCE_IMAGE="true"
    export AIRFLOW_CONTAINER_SKIP_CI_IMAGE="false"

    export THE_IMAGE_TYPE="CI"

    rebuild_image_if_needed

    AIRFLOW_CI_IMAGE=$(cat "${BUILD_CACHE_DIR}/.AIRFLOW_CI_IMAGE") || true 2>/dev/null
    export AIRFLOW_CI_IMAGE
}


#
# Cleans up the CI slim image
#
function cleanup_ci_image() {
    export AIRFLOW_CONTAINER_SKIP_SLIM_CI_IMAGE="true"
    export AIRFLOW_CONTAINER_SKIP_CI_IMAGE="false"
    export AIRFLOW_CONTAINER_SKIP_CHECKLICENCE_IMAGE="true"
    export AIRFLOW_CONTAINER_CLEANUP_IMAGES="true"

    export THE_IMAGE_TYPE="CI"

    rebuild_image_if_needed
}

#
# Rebuilds the image for licence checks if needed.
#
function rebuild_checklicence_image_if_needed() {
    export AIRFLOW_CONTAINER_SKIP_SLIM_CI_IMAGE="true"
    export AIRFLOW_CONTAINER_SKIP_CHECKLICENCE_IMAGE="false"
    export AIRFLOW_CONTAINER_SKIP_CI_IMAGE="true"

    export THE_IMAGE_TYPE="CHECKLICENCE"

    rebuild_image_if_needed

    AIRFLOW_CHECKLICENCE_IMAGE=$(cat "${BUILD_CACHE_DIR}/.AIRFLOW_CHECKLICENCE_IMAGE") || true 2>/dev/null
    export AIRFLOW_CHECKLICENCE_IMAGE
}

#
# Cleans up the CI slim image
#
function cleanup_checklicence_image() {
    export AIRFLOW_CONTAINER_SKIP_SLIM_CI_IMAGE="true"
    export AIRFLOW_CONTAINER_SKIP_CI_IMAGE="true"
    export AIRFLOW_CONTAINER_SKIP_CHECKLICENCE_IMAGE="false"
    export AIRFLOW_CONTAINER_CLEANUP_IMAGES="true"

    export THE_IMAGE_TYPE="CHECKLICENCE"

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
    fi
    START_SCRIPT_TIME=$(date +%s)
}

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
}


function run_flake8() {
    FILES=("$@")

    if [[ "${#FILES[@]}" == "0" ]]; then
        docker run "${AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS[@]}" \
            --entrypoint /opt/airflow/scripts/ci/in_container/run_flake8.sh \
            --env PYTHONDONTWRITEBYTECODE \
            --env AIRFLOW_CI_VERBOSE="${VERBOSE}" \
            --env AIRFLOW_CI_SILENT \
            --env HOST_USER_ID="$(id -ur)" \
            --env HOST_GROUP_ID="$(id -gr)" \
            --rm \
            "${AIRFLOW_SLIM_CI_IMAGE}" | tee -a "${OUTPUT_LOG}"
    else
        docker run "${AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS[@]}" \
            --entrypoint /opt/airflow/scripts/ci/in_container/run_flake8.sh \
            --env PYTHONDONTWRITEBYTECODE \
            --env AIRFLOW_CI_VERBOSE="${VERBOSE}" \
            --env AIRFLOW_CI_SILENT \
            --env HOST_USER_ID="$(id -ur)" \
            --env HOST_GROUP_ID="$(id -gr)" \
            --rm \
            "${AIRFLOW_SLIM_CI_IMAGE}" \
            "${FILES[@]}" | tee -a "${OUTPUT_LOG}"
    fi
}

function run_docs() {
    docker run "${AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS[@]}" -t \
            --entrypoint /opt/airflow/docs/build.sh \
            --env PYTHONDONTWRITEBYTECODE \
            --env AIRFLOW_CI_VERBOSE="${VERBOSE}" \
            --env AIRFLOW_CI_SILENT \
            --env HOST_USER_ID="$(id -ur)" \
            --env HOST_GROUP_ID="$(id -gr)" \
            --rm \
            "${AIRFLOW_SLIM_CI_IMAGE}"
}

function run_check_license() {
    docker run "${AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS[@]}" -t \
            --entrypoint /opt/airflow/scripts/ci/in_container/run_check_licence.sh \
            --env PYTHONDONTWRITEBYTECODE \
            --env AIRFLOW_CI_VERBOSE="${VERBOSE}" \
            --env AIRFLOW_CI_SILENT \
            --env HOST_USER_ID="$(id -ur)" \
            --env HOST_GROUP_ID="$(id -gr)" \
            --rm \
            "${AIRFLOW_CHECKLICENCE_IMAGE}"
}

function run_mypy() {
    FILES=("$@")
    if [[ "${#FILES[@]}" == "0" ]]; then
        docker run "${AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS[@]}" \
            --entrypoint /opt/airflow/scripts/ci/in_container/run_mypy.sh \
            --env PYTHONDONTWRITEBYTECODE \
            --env AIRFLOW_CI_VERBOSE="${VERBOSE}" \
            --env AIRFLOW_CI_SILENT \
            --env HOST_USER_ID="$(id -ur)" \
            --env HOST_GROUP_ID="$(id -gr)" \
            --rm \
            "${AIRFLOW_SLIM_CI_IMAGE}" \
            "airflow" "tests" "docs" | tee -a "${OUTPUT_LOG}"
    else
        docker run "${AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS[@]}" \
            --entrypoint /opt/airflow/scripts/ci/in_container/run_mypy.sh \
            --env PYTHONDONTWRITEBYTECODE \
            --env AIRFLOW_CI_VERBOSE="${VERBOSE}" \
            --env AIRFLOW_CI_SILENT \
            --env HOST_USER_ID="$(id -ur)" \
            --env HOST_GROUP_ID="$(id -gr)" \
            --rm \
            "${AIRFLOW_SLIM_CI_IMAGE}" \
            "${FILES[@]}" | tee -a "${OUTPUT_LOG}"
    fi
}

function run_pylint_main() {
    FILES=("$@")
    if [[ "${#FILES[@]}" == "0" ]]; then
        docker run "${AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS[@]}" \
            --entrypoint /opt/airflow/scripts/ci/in_container/run_pylint_main.sh \
            --env PYTHONDONTWRITEBYTECODE \
            --env AIRFLOW_CI_VERBOSE="${VERBOSE}" \
            --env AIRFLOW_CI_SILENT \
            --env HOST_USER_ID="$(id -ur)" \
            --env HOST_GROUP_ID="$(id -gr)" \
            --rm \
            "${AIRFLOW_SLIM_CI_IMAGE}" | tee -a "${OUTPUT_LOG}"
    else
        docker run "${AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS[@]}" \
            --entrypoint /opt/airflow/scripts/ci/in_container/run_pylint_main.sh \
            --env PYTHONDONTWRITEBYTECODE \
            --env AIRFLOW_CI_VERBOSE="${VERBOSE}" \
            --env AIRFLOW_CI_SILENT \
            --env HOST_USER_ID="$(id -ur)" \
            --env HOST_GROUP_ID="$(id -gr)" \
            --rm \
            "${AIRFLOW_SLIM_CI_IMAGE}" \
            "${FILES[@]}" | tee -a "${OUTPUT_LOG}"
    fi
}


function run_pylint_tests() {
    FILES=("$@")
    if [[ "${#FILES[@]}" == "0" ]]; then
        docker run "${AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS[@]}" \
            --entrypoint /opt/airflow/scripts/ci/in_container/run_pylint_tests.sh \
            --env PYTHONDONTWRITEBYTECODE \
            --env AIRFLOW_CI_VERBOSE="${VERBOSE}" \
            --env AIRFLOW_CI_SILENT \
            --env HOST_USER_ID="$(id -ur)" \
            --env HOST_GROUP_ID="$(id -gr)" \
            --rm \
            "${AIRFLOW_SLIM_CI_IMAGE}" | tee -a "${OUTPUT_LOG}"
    else
        docker run "${AIRFLOW_CONTAINER_EXTRA_DOCKER_FLAGS[@]}" \
            --entrypoint /opt/airflow/scripts/ci/in_container/run_pylint_tests.sh \
            --env PYTHONDONTWRITEBYTECODE \
            --env AIRFLOW_CI_VERBOSE="${VERBOSE}" \
            --env AIRFLOW_CI_SILENT \
            --env HOST_USER_ID="$(id -ur)" \
            --env HOST_GROUP_ID="$(id -gr)" \
            --rm \
            "${AIRFLOW_SLIM_CI_IMAGE}" \
            "${FILES[@]}" | tee -a "${OUTPUT_LOG}"
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
      if ! grep -x "./${FILE}" <"${AIRFLOW_SOURCES}/scripts/ci/pylint_todo.txt" >/dev/null; then
          FILTERED_FILES+=("${FILE}")
      fi
  done
  set -e
  export FILTERED_FILES
}
