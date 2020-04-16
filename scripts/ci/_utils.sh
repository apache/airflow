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

declare -a EXTRA_DOCKER_FLAGS
declare -a EXTRA_DOCKER_PROD_BUILD_FLAGS

function check_verbose_setup {
    if [[ ${VERBOSE_COMMANDS:="false"} == "true" ]]; then
        set -x
    else
        set +x
    fi
}


function verbose_docker {
    if [[ ${VERBOSE:="false"} == "true" ]]; then
        echo "docker" "${@}"
    fi
    docker "${@}"
}

# Common environment that is initialized by both Breeze and CI scripts
function initialize_common_environment {
    # default python version
    PYTHON_MAJOR_MINOR_VERSION=${PYTHON_MAJOR_MINOR_VERSION:="3.5"}

    AIRFLOW_SOURCES=${AIRFLOW_SOURCES:=$(cd "${MY_DIR}/../../" && pwd)}
    export AIRFLOW_SOURCES

    BUILD_CACHE_DIR="${AIRFLOW_SOURCES}/.build"
    export BUILD_CACHE_DIR

    LAST_FORCE_ANSWER_FILE="${BUILD_CACHE_DIR}/last_force_answer.sh"

    # Create directories if needed
    mkdir -p "${AIRFLOW_SOURCES}/.mypy_cache"
    mkdir -p "${AIRFLOW_SOURCES}/logs"
    mkdir -p "${AIRFLOW_SOURCES}/tmp"
    mkdir -p "${AIRFLOW_SOURCES}/files"
    mkdir -p "${AIRFLOW_SOURCES}/dist"

    # shellcheck source=common/_image_variables.sh
    . "${AIRFLOW_SOURCES}/common/_image_variables.sh"
    # shellcheck source=common/_files_for_rebuild_check.sh
    . "${AIRFLOW_SOURCES}/common/_files_for_rebuild_check.sh"

    # Default branch name for triggered builds is the one configured in default branch
    export BRANCH_NAME=${BRANCH_NAME:=${DEFAULT_BRANCH}}

    # Default port numbers for forwarded ports
    export WEBSERVER_HOST_PORT=${WEBSERVER_HOST_PORT:="28080"}
    export POSTGRES_HOST_PORT=${POSTGRES_HOST_PORT:="25433"}
    export MYSQL_HOST_PORT=${MYSQL_HOST_PORT:="23306"}

    # Default MySQL/Postgres versions
    export POSTGRES_VERSION=${POSTGRES_VERSION:="9.6"}
    export MYSQL_VERSION=${MYSQL_VERSION:="5.6"}

    # Do not push images from here by default (push them directly from the build script on Dockerhub)
    export PUSH_IMAGES=${PUSH_IMAGES:="false"}

    # Disable writing .pyc files - slightly slower imports but not messing around when switching
    # Python version and avoids problems with root-owned .pyc files in host
    export PYTHONDONTWRITEBYTECODE=${PYTHONDONTWRITEBYTECODE:="true"}

    # By default we assume the kubernetes cluster is not being started
    export ENABLE_KIND_CLUSTER=${ENABLE_KIND_CLUSTER:="false"}
    #
    # Sets mounting of host volumes to container for static checks
    # unless MOUNT_HOST_AIRFLOW_VOLUME is not true
    #
    # Note that this cannot be function because we need the EXTRA_DOCKER_FLAGS array variable
    #
    MOUNT_HOST_AIRFLOW_VOLUME=${MOUNT_HOST_AIRFLOW_VOLUME:="true"}
    export MOUNT_HOST_AIRFLOW_VOLUME

    # If this variable is set, we mount the whole sources directory to the host rather than
    # selected volumes
    AIRFLOW_MOUNT_SOURCE_DIR_FOR_STATIC_CHECKS=${AIRFLOW_MOUNT_SOURCE_DIR_FOR_STATIC_CHECKS="false"}
    export AIRFLOW_MOUNT_SOURCE_DIR_FOR_STATIC_CHECKS

    # Set host user id to current user
    HOST_USER_ID="$(id -ur)"
    export HOST_USER_ID

    # Set host group id to current group
    HOST_GROUP_ID="$(id -gr)"
    export HOST_GROUP_ID

    if [[ ${AIRFLOW_MOUNT_SOURCE_DIR_FOR_STATIC_CHECKS} == "true" ]]; then
        print_info
        print_info "Mount whole airflow source directory for static checks (make sure all files are in container)"
        print_info
        EXTRA_DOCKER_FLAGS=( \
          "-v" "${AIRFLOW_SOURCES}:/opt/airflow" \
          "--env" "PYTHONDONTWRITEBYTECODE" \
        )
    elif [[ ${MOUNT_HOST_AIRFLOW_VOLUME} == "true" ]]; then
        print_info
        print_info "Mounting necessary host volumes to Docker"
        print_info

        EXTRA_DOCKER_FLAGS=()

        while IFS= read -r LINE; do
            EXTRA_DOCKER_FLAGS+=( "${LINE}")
        done < <(convert_local_mounts_to_docker_params)
    else
        print_info
        print_info "Skip mounting host volumes to Docker"
        print_info
        EXTRA_DOCKER_FLAGS=( \
            "--env" "PYTHONDONTWRITEBYTECODE" \
        )
    fi

    export EXTRA_DOCKER_FLAGS

    EXTRA_DOCKER_PROD_BUILD_FLAGS=()
    export EXTRA_DOCKER_PROD_BUILD_FLAGS

    # We use pulled docker image cache by default to speed up the builds
    export DOCKER_CACHE=${DOCKER_CACHE:="pulled"}

    # By default we are not upgrading to latest requirements
    # This will only be done in cron jobs and when run with breeze
    export UPGRADE_TO_LATEST_REQUIREMENTS=${UPGRADE_TO_LATEST_REQUIREMENTS:="false"}

    STAT_BIN=stat
    if [[ "${OSTYPE}" == "darwin"* ]]; then
        STAT_BIN=gstat
    fi

    AIRFLOW_VERSION=$(grep version "airflow/version.py" | awk '{print $3}' | sed "s/['+]//g")
    export AIRFLOW_VERSION

    # default version for dockerhub images
    export DEFAULT_PYTHON_MAJOR_MINOR_VERSION=3.6


    if [[ ${CI:="false"} == "true" ]]; then
        export LOCAL_RUN="false"
    else
        export LOCAL_RUN="true"
    fi

    # upgrade while generating requirements should only happen in localy run
    # pre-commits or in cron job
    if [[ ${LOCAL_RUN} == "true" || "${TRAVIS_EVENT_TYPE:=}" != "cron" ]]; then
        export UPGRADE_WHILE_GENERATING_REQUIREMENTS="true"
    else
        export UPGRADE_WHILE_GENERATING_REQUIREMENTS="false"
    fi

    export SHOW_GENERATE_REQUIREMENTS_INSTRUCTIONS=${SHOW_GENERATE_REQUIREMENTS_INSTRUCTIONS:="false"}

    export DEFAULT_CI_EXTRAS="devel_ci"


    DEFAULT_PROD_EXTRAS=$(grep "ARG AIRFLOW_EXTRAS=" "${AIRFLOW_SOURCES}/Dockerfile"|
            awk 'BEGIN { FS="=" } { print $2 }' | tr -d '"')

    # By default we build CI images  but when we specify --producton-image we switch to production image
    export PRODUCTION_IMAGE="false"

    export SQLITE_VERSION="sqlite:////root/airflow/airflow.db"

    export INSTALL_AIRFLOW_REFERENCE=""
}


function print_info() {
    if [[ ${VERBOSE:="false"} == "true" ]]; then
        echo "$@"
    fi
}

LOCAL_MOUNTS="
.bash_aliases /root/
.bash_history /root/
.coveragerc /opt/airflow/
.dockerignore /opt/airflow/
.flake8 /opt/airflow/
.github /opt/airflow/
.inputrc /root/
.kube /root/
.rat-excludes /opt/airflow/
CHANGELOG.txt /opt/airflow/
Dockerfile.ci /opt/airflow/
LICENSE /opt/airflow/
MANIFEST.in /opt/airflow/
NOTICE /opt/airflow/
airflow /opt/airflow/
common /opt/airflow/
dags /opt/airflow/
dev /opt/airflow/
docs /opt/airflow/
files /
dist /
hooks /opt/airflow/
logs /root/airflow/
pytest.ini /opt/airflow/
requirements /opt/airflow/
scripts /opt/airflow/
scripts/ci/in_container/entrypoint_ci.sh /
setup.cfg /opt/airflow/
setup.py /opt/airflow/
tests /opt/airflow/
tmp /opt/airflow/
"

# parse docker-compose-local.yaml file to convert volumes entries
# from airflow section to "-v" "volume mapping" series of options
function convert_local_mounts_to_docker_params() {
    echo "${LOCAL_MOUNTS}" |sed '/^$/d' | awk -v AIRFLOW_SOURCES="${AIRFLOW_SOURCES}" \
    '
    function basename(file) {
        sub(".*/", "", file)
        return file
    }
    { print "-v"; print AIRFLOW_SOURCES "/" $1 ":" $2 basename($1) ":cached" }'
}

function sanitize_file() {
    if [[ -d "${1}" ]]; then
        rm -rf "${1}"
    fi
    touch "${1}"

}
function sanitize_mounted_files() {
    sanitize_file "${AIRFLOW_SOURCES}/.bash_history"
    sanitize_file "${AIRFLOW_SOURCES}/.bash_aliases"
    sanitize_file "${AIRFLOW_SOURCES}/.inputrc"
}

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
    if [[ -d ${CACHE_TMP_FILE_DIR} ]]; then
        rm -rf "${CACHE_TMP_FILE_DIR}"
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
    local MD5SUM_CACHE_DIR="${BUILD_CACHE_DIR}/${DEFAULT_BRANCH}/${PYTHON_MAJOR_MINOR_VERSION}/${THE_IMAGE_TYPE}"
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
    local MD5SUM_CACHE_DIR="${BUILD_CACHE_DIR}/${DEFAULT_BRANCH}/${PYTHON_MAJOR_MINOR_VERSION}/${THE_IMAGE_TYPE}"
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
    mkdir -pv "${BUILD_CACHE_DIR}/${DEFAULT_BRANCH}"
    touch "${BUILT_IMAGE_FLAG_FILE}"
}

#
# Checks md5sum of all important files in order to optimise speed of running various operations
# That mount sources of Airflow to container and require docker image built with latest dependencies.
# the Docker image will only be marked for rebuilding only in case any of the important files change:
# * setup.py
# * setup.cfg
# * Dockerfile.ci
# * airflow/version.py
#
# This is needed because we want to skip rebuilding of the image when only airflow sources change but
# Trigger rebuild in case we need to change dependencies (setup.py, setup.cfg, change version of Airflow
# or the Dockerfile.ci itself changes.
#
# Another reason to skip rebuilding Docker is thar currently it takes a bit longer time than simple Docker
# We need to fix group permissions of files in Docker because different linux build services have
# different default umask and Docker uses group permissions in checking for cache invalidation.
#
# As result of this check - most of the static checks will start pretty much immediately.
#
function check_if_docker_build_is_needed() {
    print_info
    print_info "Checking if image build is needed for ${THE_IMAGE_TYPE} image."
    print_info
    if [[ ${FORCE_BUILD_IMAGES:=""} == "true" ]]; then
        echo "Docker image build is forced for ${THE_IMAGE_TYPE} image"
        set +e
        for FILE in "${FILES_FOR_REBUILD_CHECK[@]}"
        do
            # Just store md5sum for all files in md5sum.new - do not check if it is different
            check_file_md5sum "${AIRFLOW_SOURCES}/${FILE}"
        done
        set -e
        export NEEDS_DOCKER_BUILD="true"
    else
        set +e
        for FILE in "${FILES_FOR_REBUILD_CHECK[@]}"
        do
            if ! check_file_md5sum "${AIRFLOW_SOURCES}/${FILE}"; then
                export NEEDS_DOCKER_BUILD="true"
            fi
        done
        set -e
        if [[ ${NEEDS_DOCKER_BUILD} == "true" ]]; then
            echo "Docker image build is needed for ${THE_IMAGE_TYPE} image!"
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
    if [[ ${SKIP_IN_CONTAINER_CHECK:=} == "true" ]]; then
        return
    fi
    if [[ -f /.dockerenv ]]; then
        echo >&2
        echo >&2 "You are inside the Airflow docker container!"
        echo >&2 "You should only run this script from the host."
        echo >&2 "Learn more about how we develop and test airflow in:"
        echo >&2 "https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst"
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
    ACTION="rebuild"
    if [[ ${FORCE_PULL_IMAGES:=} == "true" ]]; then
        ACTION="pull and rebuild"
    fi
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
        SKIP_REBUILD="true"
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
# It contiains .json file - result of docker inspect - decscribing the image
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
# Retrieve information about layers in the local IMAGE
# it stores list of SHAS of image layers in the file pointed at by TMP_MANIFEST_LOCAL_SHA
#
function get_local_image_info() {
    TMP_MANIFEST_LOCAL_JSON=$(mktemp)
    TMP_MANIFEST_LOCAL_SHA=$(mktemp)
    set +e
    # Remove the container just in case
    verbose_docker rm --force "local-airflow-manifest"  >/dev/null 2>&1
    # Create manifest from the local manifest image
    if ! verbose_docker create --name "local-airflow-manifest" \
        "${AIRFLOW_CI_LOCAL_MANIFEST_IMAGE}"  >/dev/null 2>&1 ; then
        echo
        echo "Local manifest image not available"
        echo
        LOCAL_MANIFEST_IMAGE_UNAVAILABLE="true"
        return
    fi
    set -e
     # Create manifest from the local manifest image
    verbose_docker cp "local-airflow-manifest:${AIRFLOW_CI_BASE_TAG}.json" "${TMP_MANIFEST_LOCAL_JSON}" >/dev/null 2>&1
    sed 's/ *//g' "${TMP_MANIFEST_LOCAL_JSON}" | grep '^"sha256:' >"${TMP_MANIFEST_LOCAL_SHA}"
    verbose_docker rm --force "local-airflow-manifest" >/dev/null 2>&1
}

#
# Retrieve information about layers in the remote IMAGE
# it stores list of SHAS of image layers in the file pointed at by TMP_MANIFEST_REMTOE_SHA
# This cannot be done easily with existing APIs of Dockerhub because they require additional authentication
# even for public images. Therefore instead we are downloading a specially prepared manifest image
# which is built together with the main image. This special manifest image is prepared during
# building of the main image and contains single JSON file being result of docker inspect on that image
# This image is from scratch so it is very tiny
function get_remote_image_info() {
    set +e
    # Pull remote manifest image
    if ! verbose_docker pull "${AIRFLOW_CI_REMOTE_MANIFEST_IMAGE}" >/dev/null 2>&1 ; then
        echo
        echo "Remote docker registry unreachable"
        echo
        REMOTE_DOCKER_REGISTRY_UNREACHABLE="true"
        return
    fi
    TMP_MANIFEST_REMOTE_JSON=$(mktemp)
    TMP_MANIFEST_REMOTE_SHA=$(mktemp)
    # delete container just in case
    verbose_docker rm --force "remote-airflow-manifest" >/dev/null 2>&1
    set -e
    # Create container out of the manifest image without runnning it
    verbose_docker create --name "remote-airflow-manifest" "${AIRFLOW_CI_REMOTE_MANIFEST_IMAGE}" >/dev/null 2>&1
    # Extract manifest and store it in local file
    verbose_docker cp "remote-airflow-manifest:${AIRFLOW_CI_BASE_TAG}.json" "${TMP_MANIFEST_REMOTE_JSON}" >/dev/null 2>&1
    # Filter everything except SHAs of image layers
    sed 's/ *//g' "${TMP_MANIFEST_REMOTE_JSON}" | grep '^"sha256:' >"${TMP_MANIFEST_REMOTE_SHA}"
    verbose_docker rm --force "remote-airflow-manifest" >/dev/null 2>&1
}

# The Number is determines the cut-off between local building time and pull + build time.
# It is a bit experimental and it will have to be kept
# updated as we keep on changing layers. The cut-off point is at the moment when we do first
# pip install "https://github.com/apache/airflow/archive/${AIRFLOW_BRANCH}.tar...
# you can get it via this command:
# docker history --no-trunc  apache/airflow:master-python3.6-ci | \
#      grep ^sha256 | grep -n "pip uninstall" | awk 'BEGIN {FS=":"} {print $1 }'
#
# This command returns the number of layer in docker history where pip uninstall is called. This is the
# line that will take a lot of time to run and at this point it's worth to pull the image from repo
# if there are at least NN chaanged layers in your docker file, you should pull the image.
#
# Note that this only matters if you have any of the important files changed since the last build
# of your image such as Dockerfile.ci, setup.py etc.
#
MAGIC_CUT_OFF_NUMBER_OF_LAYERS=34

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
        echo "WARNING! Your image and the dockerhub image differ signifcantly"
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

function rebuild_ci_image_if_needed() {
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


#
# Starts the script/ If VERBOSE_COMMANDS variable is set to true, it enables verbose output of commands executed
# Also prints some useful diagnostics information at start of the script if VERBOSE is set to true
#
function script_start {
    print_info
    print_info "Running $(basename "$0")"
    print_info
    print_info "Log is redirected to ${OUTPUT_LOG}"
    print_info
    if [[ ${VERBOSE_COMMANDS:="false"} == "true" ]]; then
        print_info
        print_info "Variable VERBOSE_COMMANDS Set to \"true\""
        print_info "You will see a lot of output"
        print_info
        set -x
    else
        print_info "You can increase verbosity by running 'export VERBOSE_COMMANDS=\"true\""
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
    #shellcheck disable=2181
    EXIT_CODE=$?
    if [[ ${EXIT_CODE} != 0 ]]; then
        print_info "###########################################################################################"
        print_info "                   EXITING WITH STATUS CODE ${EXIT_CODE}"
        print_info "###########################################################################################"
    fi
    if [[ ${VERBOSE_COMMANDS:="false"} == "true" ]]; then
        set +x
    fi
    END_SCRIPT_TIME=$(date +%s)
    RUN_SCRIPT_TIME=$((END_SCRIPT_TIME-START_SCRIPT_TIME))
    if [[ ${BREEZE:=} != "true" ]]; then
        print_info
        print_info "Finished the script $(basename "$0")"
        print_info "Elapsed time spent in the script: ${RUN_SCRIPT_TIME} seconds"
        print_info "Exit code ${EXIT_CODE}"
        print_info
    fi
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
    sanitize_mounted_files
}


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
        echo "   * Quit and manually rebuild the images using one of the following commmands"
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

function build_ci_image_on_ci() {
    if [[ "${CI:=}" != "true" ]]; then
        print_info
        print_info "Cleaning up docker installation!!!!!!"
        print_info
        "${AIRFLOW_SOURCES}/confirm" "Cleaning docker data and rebuilding"
    fi

    prepare_ci_build

    # Cleanup docker installation. It should be empty in CI but let's not risk
    verbose_docker system prune --all --force
    rm -rf "${BUILD_CACHE_DIR}"
    mkdir -pv "${BUILD_CACHE_DIR}"

    if [[ "${TRAVIS_PULL_REQUEST:=}" == "false" ]]; then
        # If we are building a tag or a branch build, then we don't want to skip any tests
        rebuild_ci_image_if_needed
        return
    else
        # Don't try and find changed files for non-PR builds (tags, branch pushes etc.)
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
    fi

    export FORCE_PULL_IMAGES="true"
    export FORCE_BUILD="true"
    export VERBOSE="${VERBOSE:="false"}"

    if [[ ${TRAVIS_JOB_NAME:=""} == "Tests"*"Kubernetes"* ]]; then
        match_files_regexp 'airflow/kubernetes/.*\.py' 'tests/runtime/kubernetes/.*\.py' \
            'airflow/www/.*\.py' 'airflow/www/.*\.js' 'airflow/www/.*\.html' \
            'scripts/ci/.*' 'airflow/example_dags/.*'
        if [[ ${FILE_MATCHES} == "true" ]]; then
            rebuild_ci_image_if_needed
        else
            touch "${BUILD_CACHE_DIR}"/.skip_tests
        fi
    elif [[ ${TRAVIS_JOB_NAME:=""} == "Tests"* ]]; then
        match_files_regexp '.*\.py' 'airflow/www/.*\.py' 'airflow/www/.*\.js' \
            'airflow/www/.*\.html' 'scripts/ci/.*' 'airflow/example_dags/.*'
        if [[ ${FILE_MATCHES} == "true" ]]; then
            rebuild_ci_image_if_needed
        else
            touch "${BUILD_CACHE_DIR}"/.skip_tests
        fi
    elif [[ ${TRAVIS_JOB_NAME} == "Static"* ]]; then
        rebuild_ci_image_if_needed
    elif [[ ${TRAVIS_JOB_NAME} == *"documentation"* ]]; then
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
    unset FORCE_PULL_IMAGES
    unset FORCE_BUILD
}

function read_from_file {
    cat "${BUILD_CACHE_DIR}/.$1" 2>/dev/null || true
}

function save_to_file {
    # shellcheck disable=SC2005
    echo "$(eval echo "\$$1")" > "${BUILD_CACHE_DIR}/.$1"
}

function check_and_save_allowed_param {
    _VARIABLE_NAME="${1}"
    _VARIABLE_DESCRIPTIVE_NAME="${2}"
    _FLAG="${3}"
    _ALLOWED_VALUES_ENV_NAME="_BREEZE_ALLOWED_${_VARIABLE_NAME}S"
    _ALLOWED_VALUES=" ${!_ALLOWED_VALUES_ENV_NAME//$'\n'/ } "
    _VALUE=${!_VARIABLE_NAME}
    if [[ ${_ALLOWED_VALUES:=} != *" ${_VALUE} "* ]]; then
        echo >&2
        echo >&2 "ERROR:  Allowed ${_VARIABLE_DESCRIPTIVE_NAME}: [${_ALLOWED_VALUES}]. Is: '${!_VARIABLE_NAME}'."
        echo >&2
        echo >&2 "Switch to supported value with ${_FLAG} flag."

        if [[ -n ${!_VARIABLE_NAME} && \
            -f "${BUILD_CACHE_DIR}/.${_VARIABLE_NAME}" && \
            ${!_VARIABLE_NAME} == $(cat "${BUILD_CACHE_DIR}/.${_VARIABLE_NAME}" ) ]]; then
            echo >&2
            echo >&2 "Removing ${BUILD_CACHE_DIR}/.${_VARIABLE_NAME}. Next time you run it, it should be OK."
            echo >&2
            rm -f "${BUILD_CACHE_DIR}/.${_VARIABLE_NAME}"
        fi
        exit 1
    fi
    save_to_file "${_VARIABLE_NAME}"
}

function run_docs() {
    verbose_docker run "${EXTRA_DOCKER_FLAGS[@]}" -t \
            --entrypoint "/usr/local/bin/dumb-init"  \
            --env PYTHONDONTWRITEBYTECODE \
            --env VERBOSE \
            --env VERBOSE_COMMANDS \
            --env HOST_USER_ID="$(id -ur)" \
            --env HOST_GROUP_ID="$(id -gr)" \
            --rm \
            "${AIRFLOW_CI_IMAGE}" \
            "--" "/opt/airflow/docs/build.sh" \
            | tee -a "${OUTPUT_LOG}"
}

function pull_ci_image_if_needed() {
    # Whether to force pull images to populate cache
    export FORCE_PULL_IMAGES=${FORCE_PULL_IMAGES:="false"}
    # In CI environment we skip pulling latest python image
    export PULL_BASE_IMAGES=${PULL_BASE_IMAGES:="false"}

    if [[ "${DOCKER_CACHE}" == "pulled" ]]; then
        if [[ "${FORCE_PULL_IMAGES}" == "true" ]]; then
            if [[ ${PULL_BASE_IMAGES} == "false" ]]; then
                echo
                echo "Skip force-pulling the ${PYTHON_BASE_IMAGE} image."
                echo
            else
                echo
                echo "Force pull base image ${PYTHON_BASE_IMAGE}"
                echo
                if [[ -n ${DETECTED_TERMINAL:=""} ]]; then
                    echo -n "
Docker pulling ${PYTHON_BASE_IMAGE}.
                    " > "${DETECTED_TERMINAL}"
                fi
                verbose_docker pull "${PYTHON_BASE_IMAGE}" | tee -a "${OUTPUT_LOG}"
                echo
            fi
        fi
        local PULL_IMAGE=${FORCE_PULL_IMAGES}
        local IMAGE_HASH
        IMAGE_HASH=$(docker images -q "${AIRFLOW_CI_IMAGE}" 2> /dev/null)
        if [[ "${IMAGE_HASH}" == "" ]]; then
            PULL_IMAGE="true"
        fi
        if [[ "${PULL_IMAGE}" == "true" ]]; then
            echo
            echo "Pulling the image ${AIRFLOW_CI_IMAGE}"
            echo
            if [[ -n ${DETECTED_TERMINAL:=""} ]]; then
                echo -n "
Docker pulling ${IMAGE}.
" > "${DETECTED_TERMINAL}"
            fi
            verbose_docker pull "${AIRFLOW_CI_IMAGE}" | tee -a "${OUTPUT_LOG}" || true
            echo
        fi
    fi
}

function pull_prod_images_if_needed() {
    # Whether to force pull images to populate cache
    export FORCE_PULL_IMAGES=${FORCE_PULL_IMAGES:="false"}
    # In CI environment we skip pulling latest python image
    export PULL_BASE_IMAGES=${PULL_BASE_IMAGES:="false"}

    if [[ "${DOCKER_CACHE}" == "pulled" ]]; then
        if [[ "${FORCE_PULL_IMAGES}" == "true" ]]; then
            if [[ ${PULL_BASE_IMAGES} == "false" ]]; then
                echo
                echo "Skip force-pulling the ${PYTHON_BASE_IMAGE} image."
                echo
            else
                echo
                echo "Force pull base image ${PYTHON_BASE_IMAGE}"
                echo
                verbose_docker pull "${PYTHON_BASE_IMAGE}" | tee -a "${OUTPUT_LOG}"
                echo
            fi
        fi
        local PULL_BUILD_IMAGE=${FORCE_PULL_IMAGES}
        local BUILD_IMAGE_HASH
        BUILD_IMAGE_HASH=$(docker images -q "${AIRFLOW_PROD_BUILD_IMAGE}" 2> /dev/null)
        if [[ "${BUILD_IMAGE_HASH}" == "" ]]; then
            PULL_BUILD_IMAGE="true"
        fi
        if [[ "${PULL_BUILD_IMAGE}" == "true" ]]; then
            echo
            echo "Pulling the image ${AIRFLOW_PROD_BUILD_IMAGE}"
            echo
            verbose_docker pull "${AIRFLOW_PROD_BUILD_IMAGE}" | tee -a "${OUTPUT_LOG}" || true
            echo
        fi
        local PULL_IMAGE=${FORCE_PULL_IMAGES}
        local IMAGE_HASH
        IMAGE_HASH=$(docker images -q "${AIRFLOW_PROD_IMAGE}" 2> /dev/null)
        if [[ "${IMAGE_HASH}" == "" ]]; then
            PULL_IMAGE="true"
        fi
        if [[ "${PULL_IMAGE}" == "true" ]]; then
            echo
            echo "Pulling the image ${AIRFLOW_PROD_IMAGE}"
            echo
            verbose_docker pull "${AIRFLOW_PROD_IMAGE}" | tee -a "${OUTPUT_LOG}" || true
            echo
        fi
    fi
}

function print_build_info() {
    print_info
    print_info "Airflow ${AIRFLOW_VERSION} Python: ${PYTHON_MAJOR_MINOR_VERSION}. Image description: ${IMAGE_DESCRIPTION}"
    print_info
}

function spin() {
    local FILE_TO_MONITOR=${1}
    local SPIN=("-" "\\" "|" "/")
    echo -n "
Build log: ${FILE_TO_MONITOR}
" > "${DETECTED_TERMINAL}"

    LAST_STEP=""
    while "true"
    do
      for i in "${SPIN[@]}"
      do
            echo -ne "\r${LAST_STEP}$i" > "${DETECTED_TERMINAL}"
            local LAST_FILE_SIZE
            local FILE_SIZE
            LAST_FILE_SIZE=$(set +e; wc -c "${FILE_TO_MONITOR}" 2>/dev/null | awk '{print $1}' || true)
            FILE_SIZE=${LAST_FILE_SIZE}
            while [[ "${LAST_FILE_SIZE}" == "${FILE_SIZE}" ]];
            do
                FILE_SIZE=$(set +e; wc -c "${FILE_TO_MONITOR}" 2>/dev/null | awk '{print $1}' || true)
                sleep 0.2
            done
            LAST_FILE_SIZE=FILE_SIZE
            sleep 0.2
            if [[ ! -f "${FILE_TO_MONITOR}" ]]; then
                exit
            fi
            LAST_LINE=$(set +e; grep "Step" <"${FILE_TO_MONITOR}" | tail -1 || true)
            [[ ${LAST_LINE} =~ ^(Step [0-9/]*)\ : ]] && LAST_STEP="${BASH_REMATCH[1]} :"
      done
    done
}

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

    if [[ "${DOCKER_CACHE}" == "no-cache" ]]; then
        export DOCKER_CACHE_CI_DIRECTIVE=("--no-cache")
    elif [[ "${DOCKER_CACHE}" == "local" ]]; then
        export DOCKER_CACHE_CI_DIRECTIVE=()
    elif [[ "${DOCKER_CACHE}" == "pulled" ]]; then
        export DOCKER_CACHE_CI_DIRECTIVE=(
            "--cache-from" "${AIRFLOW_CI_IMAGE}"
        )
    else
        echo 2>&1
        echo 2>&1 "Error - thee ${DOCKER_CACHE} cache is unknown!"
        echo 2>&1
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
        --build-arg AIRFLOW_CONTAINER_CI_OPTIMISED_BUILD="${AIRFLOW_CONTAINER_CI_OPTIMISED_BUILD}" \
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

function build_prod_image() {
    print_build_info
    pull_prod_images_if_needed

    if [[ "${DOCKER_CACHE}" == "no-cache" ]]; then
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
        echo 2>&1
        echo 2>&1 "Error - thee ${DOCKER_CACHE} cache is unknown!"
        echo 2>&1
        exit 1
    fi
    set +u
    verbose_docker build \
        "${EXTRA_DOCKER_PROD_BUILD_FLAGS[@]}" \
        --build-arg PYTHON_BASE_IMAGE="${PYTHON_BASE_IMAGE}" \
        --build-arg PYTHON_MAJOR_MINOR_VERSION="${PYTHON_MAJOR_MINOR_VERSION}" \
        --build-arg AIRFLOW_VERSION="${AIRFLOW_VERSION}" \
        --build-arg AIRFLOW_EXTRAS="${AIRFLOW_EXTRAS}" \
        "${DOCKER_CACHE_PROD_BUILD_DIRECTIVE[@]}" \
        -t "${AIRFLOW_PROD_BUILD_IMAGE}" \
        --target "airflow-build-image" \
        . -f Dockerfile | tee -a "${OUTPUT_LOG}"
    verbose_docker build \
        "${EXTRA_DOCKER_PROD_BUILD_FLAGS[@]}" \
        --build-arg PYTHON_BASE_IMAGE="${PYTHON_BASE_IMAGE}" \
        --build-arg PYTHON_MAJOR_MINOR_VERSION="${PYTHON_MAJOR_MINOR_VERSION}" \
        --build-arg AIRFLOW_VERSION="${AIRFLOW_VERSION}" \
        --build-arg AIRFLOW_EXTRAS="${AIRFLOW_EXTRAS}" \
        "${DOCKER_CACHE_PROD_DIRECTIVE[@]}" \
        -t "${AIRFLOW_PROD_IMAGE}" \
        --target "main" \
        . -f Dockerfile | tee -a "${OUTPUT_LOG}"
    set -u
    if [[ -n "${DEFAULT_IMAGE:=}" ]]; then
        verbose_docker tag "${AIRFLOW_PROD_IMAGE}" "${DEFAULT_IMAGE}" | tee -a "${OUTPUT_LOG}"
    fi
}


function remove_all_images() {
    echo
    "${AIRFLOW_SOURCES}/confirm" "Removing all local images ."
    echo
    verbose_docker rmi "${PYTHON_BASE_IMAGE}" || true
    verbose_docker rmi "${AIRFLOW_CI_IMAGE}" || true
    echo
    echo "###################################################################"
    echo "NOTE!! Removed Airflow images for Python version ${PYTHON_MAJOR_MINOR_VERSION}."
    echo "       But the disk space in docker will be reclaimed only after"
    echo "       running 'docker system prune' command."
    echo "###################################################################"
    echo
}

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
# We fix it by removing write permissions for other/group for all files that are in the Docker context.
#
# Since we can't (easily) tell what dockerignore would restrict, we'll just to
# it to "all" files in the git repo, making sure to exclude the www/static/docs
# symlink which is broken until the docs are built.
function filterout_deleted_files {
  # Take NUL-separated stdin, return only files that exist on stdout NUL-separated
  # This is to cope with users deleting files or folders locally but not doing `git rm`
  xargs -0 "$STAT_BIN" --printf '%n\0' 2>/dev/null || true;
}

function fix_group_permissions() {
    if [[ ${PERMISSIONS_FIXED:=} == "true" ]]; then
        echo
        echo "Permissions already fixed"
        echo
        return
    fi
    echo
    echo "Fixing group permissions"
    pushd "${AIRFLOW_SOURCES}" >/dev/null
    # This deals with files
    git ls-files -z -- ./ | filterout_deleted_files | xargs -0 chmod og-w
    # and this deals with directories
    git ls-tree -z -r -d --name-only HEAD | filterout_deleted_files | xargs -0 chmod og-w,og+x
    popd >/dev/null
    echo "Fixed group permissions"
    echo
    export PERMISSIONS_FIXED="true"
}

function prepare_ci_build() {
    export AIRFLOW_CI_BASE_TAG="${DEFAULT_BRANCH}-python${PYTHON_MAJOR_MINOR_VERSION}-ci"
    export AIRFLOW_CI_LOCAL_MANIFEST_IMAGE="local/${DOCKERHUB_REPO}:${AIRFLOW_CI_BASE_TAG}-manifest"
    export AIRFLOW_CI_REMOTE_MANIFEST_IMAGE="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${AIRFLOW_CI_BASE_TAG}-manifest"
    export AIRFLOW_CI_IMAGE="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${AIRFLOW_CI_BASE_TAG}"
    export AIRFLOW_CI_IMAGE_DEFAULT="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${DEFAULT_BRANCH}-ci"
    export PYTHON_BASE_IMAGE="python:${PYTHON_MAJOR_MINOR_VERSION}-slim-buster"
    export BUILT_IMAGE_FLAG_FILE="${BUILD_CACHE_DIR}/${BRANCH_NAME}/.built_${PYTHON_MAJOR_MINOR_VERSION}"
    if [[ "${DEFAULT_PYTHON_MAJOR_MINOR_VERSION}" == "${PYTHON_MAJOR_MINOR_VERSION}" ]]; then
        export DEFAULT_IMAGE="${AIRFLOW_CI_IMAGE_DEFAULT}"
    else
        export DEFAULT_IMAGE=""
    fi
    export THE_IMAGE_TYPE="CI"
    export IMAGE_DESCRIPTION="Airflow CI"
    export AIRFLOW_CONTAINER_CI_OPTIMISED_BUILD="true"
    export AIRFLOW_EXTRAS="${AIRFLOW_EXTRAS:="${DEFAULT_CI_EXTRAS}"}"
    export AIRFLOW_IMAGE="${AIRFLOW_CI_IMAGE}"

    go_to_airflow_sources
    fix_group_permissions
}

function prepare_prod_build() {
    export AIRFLOW_PROD_BASE_TAG="${DEFAULT_BRANCH}-python${PYTHON_MAJOR_MINOR_VERSION}"
    export AIRFLOW_PROD_BUILD_IMAGE="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${AIRFLOW_PROD_BASE_TAG}-build"
    export AIRFLOW_PROD_IMAGE="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${AIRFLOW_PROD_BASE_TAG}"
    export AIRFLOW_PROD_IMAGE_DEFAULT="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${DEFAULT_BRANCH}"
    export PYTHON_BASE_IMAGE="python:${PYTHON_MAJOR_MINOR_VERSION}-slim-buster"
    if [[ "${DEFAULT_PYTHON_MAJOR_MINOR_VERSION}" == "${PYTHON_MAJOR_MINOR_VERSION}" ]]; then
        export DEFAULT_IMAGE="${AIRFLOW_PROD_IMAGE_DEFAULT}"
    else
        export DEFAULT_IMAGE=""
    fi
    export THE_IMAGE_TYPE="PROD"
    export IMAGE_DESCRIPTION="Airflow production"
    export AIRFLOW_EXTRAS="${AIRFLOW_EXTRAS:="${DEFAULT_PROD_EXTRAS}"}"
    export AIRFLOW_IMAGE="${AIRFLOW_PROD_IMAGE}"

    if [[ "${INSTALL_AIRFLOW_VERSION:=}" == "" && "${INSTALL_AIRFLOW_REFERENCE:=}" == "" ]]; then
        # When no airflow version/reference is specified, production image is built from local sources
        export EXTRA_DOCKER_PROD_BUILD_FLAGS=(
        )
    else
        if [[ "${INSTALL_AIRFLOW_REFERENCE:=}" != "" ]]; then
            # When --install-airflow-version is used then the image is build from github tag
            export EXTRA_DOCKER_PROD_BUILD_FLAGS=(
                "--build-arg" "AIRFLOW_INSTALL_SOURCES=https://github.com/apache/airflow/archive/${INSTALL_AIRFLOW_REFERENCE}.tar.gz#egg=apache-airflow"
            )
            export AIRFLOW_VERSION="${INSTALL_AIRFLOW_REFERENCE}"
        else
            # When --install-airflow-version is used then the image is build from github tag
            export EXTRA_DOCKER_PROD_BUILD_FLAGS=(
                "--build-arg" "AIRFLOW_INSTALL_SOURCES=apache-airflow"
                "--build-arg" "AIRFLOW_INSTALL_VERSION===${INSTALL_AIRFLOW_VERSION}"
            )
            export AIRFLOW_VERSION="${INSTALL_AIRFLOW_VERSION}"
        fi
        if [[ ${AIRFLOW_VERSION} =~ [^0-9]*1[^0-9]*10[^0-9]* ]]; then
            # We need to set special arguments to build 1.10 version of image
            EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
                "--build-arg" "AIRFLOW_SOURCES_FROM=Dockerfile"
                "--build-arg" "AIRFLOW_SOURCES_TO=/Dockerfile"
                "--build-arg" "WWW_FOLDER=www_rbac"
                # TODO We should change it to 1.10.10 after release to make the image build repeatable
                "--build-arg" "CONSTRAINT_REQUIREMENTS=https://raw.githubusercontent.com/apache/airflow/v1-10-test/requirements/requirements-python${PYTHON_MAJOR_MINOR_VERSION}.txt"
                # TODO We should uncomment it just before we merge this change to master/v1_10_test
                #"--build-arg" "ENTRYPOINT_FILE=https://raw.githubusercontent.com/apache/airflow/v1-10-test/entrypoint.sh"
            )
        else
            EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
                "--build-arg" "AIRFLOW_SOURCES_FROM=Dockerfile"
                "--build-arg" "AIRFLOW_SOURCES_TO=/Dockerfile"
                # TODO We should change it to 1.10.10 after release to make the image build repeatable
                "--build-arg" "CONSTRAINT_REQUIREMENTS=https://raw.githubusercontent.com/apache/airflow/v1-10-test/requirements/requirements-python${PYTHON_MAJOR_MINOR_VERSION}.txt"
                # TODO We should uncomment it just before we merge this change to master/v1_10_test
                #"--build-arg" "ENTRYPOINT_FILE=https://raw.githubusercontent.com/apache/airflow/v1-10-test/entrypoint.sh"
            )
        fi

    fi
    go_to_airflow_sources
}

function push_ci_image() {
    verbose_docker push "${AIRFLOW_CI_IMAGE}"
    verbose_docker tag "${AIRFLOW_CI_LOCAL_MANIFEST_IMAGE}" "${AIRFLOW_CI_REMOTE_MANIFEST_IMAGE}"
    verbose_docker push "${AIRFLOW_CI_REMOTE_MANIFEST_IMAGE}"
    if [[ -n ${DEFAULT_IMAGE:=""} ]]; then
        verbose_docker push "${DEFAULT_IMAGE}"
    fi
}

function push_prod_images() {
    verbose_docker push "${AIRFLOW_PROD_IMAGE}"
    verbose_docker push "${AIRFLOW_PROD_BUILD_IMAGE}"
    if [[ -n ${DEFAULT_IMAGE:=""} ]]; then
        verbose_docker push "${DEFAULT_IMAGE}"
    fi
}

function run_generate_requirements() {
        docker run "${EXTRA_DOCKER_FLAGS[@]}" \
            --entrypoint "/usr/local/bin/dumb-init"  \
            --env PYTHONDONTWRITEBYTECODE \
            --env VERBOSE \
            --env VERBOSE_COMMANDS \
            --env HOST_USER_ID="$(id -ur)" \
            --env HOST_GROUP_ID="$(id -gr)" \
            --env UPGRADE_WHILE_GENERATING_REQUIREMENTS \
            --env PYTHON_MAJOR_MINOR_VERSION \
            --env SHOW_GENERATE_REQUIREMENTS_INSTRUCTIONS \
            --rm \
            "${AIRFLOW_CI_IMAGE}" \
            "--" "/opt/airflow/scripts/ci/in_container/run_generate_requirements.sh" \
            | tee -a "${OUTPUT_LOG}"
}

function set_mysql_encoding() {
    export MYSQL_ENCODING="utf8mb4"

    if [[ ${MYSQL_VERSION:="5.7"} == "5.6" ]]; then
        # In MySQL 5.6 maximum size of the index is 1536 bytes and it is too small for
        # Airflow for utf8mb4 encoding (it takes 4 bytes per character and some indexes
        # Are bigger than 1536 in this case. So for MySQL 5.6 we need to change the encoding
        # to utf8 (which is an alias to utf8mb3)
        export MYSQL_ENCODING=utf8
    fi
}
