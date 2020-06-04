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

# extra flags passed to the docker run for CI image commands (as Bash array)
declare -a EXTRA_DOCKER_FLAGS
# extra flags passed to the docker run for PROD image commands(as Bash array)
declare -a EXTRA_DOCKER_PROD_BUILD_FLAGS
export EXTRA_DOCKER_FLAGS
export EXTRA_DOCKER_PROD_BUILD_FLAGS


# In case "VERBOSE_COMMANDS" is set to "true" set -x is used to enable debugging
function check_verbose_setup {
    if [[ ${VERBOSE_COMMANDS:="false"} == "true" ]]; then
        set -x
    else
        set +x
    fi
}


# In case "VERBOSE" is set to "true" (--verbose flag in Breeze) all docker commands run will be
# printed before execution
function verbose_docker {
    if [[ ${VERBOSE:="false"} == "true" && ${VERBOSE_COMMANDS:=} != "true" ]]; then
       # do not print echo if VERSBOSE_COMMAND is set (set -x does it already)
        echo "docker" "${@}"
    fi
    docker "${@}"
}

# Common environment that is initialized by both Breeze and CI scripts
function initialize_common_environment {
    # default python Major/Minor version
    PYTHON_MAJOR_MINOR_VERSION=${PYTHON_MAJOR_MINOR_VERSION:="3.6"}

    FILES_TO_CLEANUP_ON_EXIT=()

    # Sets to where airflow sources are located
    AIRFLOW_SOURCES=${AIRFLOW_SOURCES:=$(cd "${MY_DIR}/../../" && pwd)}
    export AIRFLOW_SOURCES

    # Sets to the build cache directory - status of build and convenience scripts are stored there
    BUILD_CACHE_DIR="${AIRFLOW_SOURCES}/.build"
    export BUILD_CACHE_DIR

    # File to keep the last forced answer. This is useful for pre-commits where you need to
    # only answer once if the image should be rebuilt or not and your answer is used for
    # All the subsequent questions
    LAST_FORCE_ANSWER_FILE="${BUILD_CACHE_DIR}/last_force_answer.sh"

    # Create useful directories if not yet created
    mkdir -p "${AIRFLOW_SOURCES}/.mypy_cache"
    mkdir -p "${AIRFLOW_SOURCES}/logs"
    mkdir -p "${AIRFLOW_SOURCES}/tmp"
    mkdir -p "${AIRFLOW_SOURCES}/files"
    mkdir -p "${AIRFLOW_SOURCES}/dist"

    # Read common values used across Breeze and CI scripts
    # shellcheck source=common/_common_values.sh
    . "${AIRFLOW_SOURCES}/common/_common_values.sh"
    # Read image-specific values used across Breeze and CI scripts
    # shellcheck source=common/_image_variables.sh
    . "${AIRFLOW_SOURCES}/common/_image_variables.sh"
    # Read information about files that are checked if image should be rebuilt
    # shellcheck source=common/_files_for_rebuild_check.sh
    . "${AIRFLOW_SOURCES}/common/_files_for_rebuild_check.sh"

    # Default branch name for triggered builds is the one configured in default branch
    export BRANCH_NAME=${BRANCH_NAME:=${DEFAULT_BRANCH}}

    export GITHUB_ORGANISATION=${GITHUB_ORGANISATION:="apache"}
    export GITHUB_REPO=${GITHUB_REPO:="airflow"}
    export CACHE_REGISTRY=${CACHE_REGISTRY:="docker.pkg.github.com"}
    export ENABLE_REGISTRY_CACHE=${ENABLE_REGISTRY_CACHE:="false"}

    # Default port numbers for forwarded ports
    export WEBSERVER_HOST_PORT=${WEBSERVER_HOST_PORT:="28080"}
    export POSTGRES_HOST_PORT=${POSTGRES_HOST_PORT:="25433"}
    export MYSQL_HOST_PORT=${MYSQL_HOST_PORT:="23306"}

    # Default MySQL/Postgres versions
    export POSTGRES_VERSION=${POSTGRES_VERSION:="9.6"}
    export MYSQL_VERSION=${MYSQL_VERSION:="5.7"}

    # Whether base python images should be pulled from cache
    export PULL_PYTHON_BASE_IMAGES_FROM_CACHE=${PULL_PYTHON_BASE_IMAGES_FROM_CACHE:="true"}

    # Disable writing .pyc files - slightly slower imports but not messing around when switching
    # Python version and avoids problems with root-owned .pyc files in host
    export PYTHONDONTWRITEBYTECODE=${PYTHONDONTWRITEBYTECODE:="true"}

    # By default we assume the kubernetes cluster is not being started
    export ENABLE_KIND_CLUSTER=${ENABLE_KIND_CLUSTER:="false"}
    #
    # Sets mounting of host volumes to container for static checks
    # unless MOUNT_HOST_AIRFLOW_VOLUME is not true
    #
    MOUNT_HOST_AIRFLOW_VOLUME=${MOUNT_HOST_AIRFLOW_VOLUME:="true"}
    export MOUNT_HOST_AIRFLOW_VOLUME

    # If this variable is set, we mount the whole sources directory to the host rather than
    # selected volumes. This is needed to check ALL source files during licence check
    # for example
    MOUNT_SOURCE_DIR_FOR_STATIC_CHECKS=${MOUNT_SOURCE_DIR_FOR_STATIC_CHECKS="false"}
    export MOUNT_SOURCE_DIR_FOR_STATIC_CHECKS

    # Set host user id to current user. This is used to set the ownership properly when exiting
    # The container on Linux - all files created inside docker are created with root user
    # but they should be restored back to the host user
    HOST_USER_ID="$(id -ur)"
    export HOST_USER_ID

    # Set host group id to current group This is used to set the ownership properly when exiting
    # The container on Linux - all files created inside docker are created with root user
    # but they should be restored back to the host user
    HOST_GROUP_ID="$(id -gr)"
    export HOST_GROUP_ID

    # Add the right volume mount for sources, depending which mount strategy is used
    if [[ ${MOUNT_SOURCE_DIR_FOR_STATIC_CHECKS} == "true" ]]; then
        print_info
        print_info "Mount whole airflow source directory for static checks"
        print_info
        EXTRA_DOCKER_FLAGS=( \
          "-v" "${AIRFLOW_SOURCES}:/opt/airflow" \
          "--env" "PYTHONDONTWRITEBYTECODE" \
        )
    elif [[ ${MOUNT_HOST_AIRFLOW_VOLUME} == "true" ]]; then
        print_info
        print_info "Mounting necessary host volumes to Docker"
        print_info

        read -r -a EXTRA_DOCKER_FLAGS <<< "$(convert_local_mounts_to_docker_params)"
    else
        print_info
        print_info "Skip mounting host volumes to Docker"
        print_info
        EXTRA_DOCKER_FLAGS=( \
            "--env" "PYTHONDONTWRITEBYTECODE" \
        )
    fi

    # In case of the CI build get environment variables from codecov.io and
    # set it as the extra docker flags. As described in https://docs.codecov.io/docs/testing-with-docker
    if [[ ${CI:=} == "true" ]]; then
        CI_CODECOV_ENV="$(bash <(curl -s https://codecov.io/env))"
        for ENV_PARAM in ${CI_CODECOV_ENV}
        do
            EXTRA_DOCKER_FLAGS+=("${ENV_PARAM}")
        done
    fi
    EXTRA_DOCKER_PROD_BUILD_FLAGS=()

    # We use pulled docker image cache by default to speed up the builds
    # but we can also set different docker caching strategy (for example we can use local cache
    # to build the images in case we iterate with the image
    export DOCKER_CACHE=${DOCKER_CACHE:="pulled"}

    # By default we are not upgrading to latest requirements when building Docker CI image
    # This will only be done in cron jobs
    export UPGRADE_TO_LATEST_REQUIREMENTS=${UPGRADE_TO_LATEST_REQUIREMENTS:="false"}

    # In case of MacOS we need to use gstat - gnu version of the stats
    STAT_BIN=stat
    if [[ "${OSTYPE}" == "darwin"* ]]; then
        STAT_BIN=gstat
    fi

    # Read airflow version from the version.py
    AIRFLOW_VERSION=$(grep version "airflow/version.py" | awk '{print $3}' | sed "s/['+]//g")
    export AIRFLOW_VERSION

    # default version of python used to tag the "master" and "latest" images in DockerHub
    export DEFAULT_PYTHON_MAJOR_MINOR_VERSION=3.6

    # In case we are not in CI - we assume we run locally. There are subtle changes if you run
    # CI scripts locally - for example requirements are eagerly updated if you do local run
    # in generate requirements
    if [[ ${CI:="false"} == "true" ]]; then
        export LOCAL_RUN="false"
    else
        export LOCAL_RUN="true"
    fi

    # eager upgrade while generating requirements should only happen in locally run
    # pre-commits or in cron job
    if [[ ${LOCAL_RUN} == "true" ]]; then
        export UPGRADE_WHILE_GENERATING_REQUIREMENTS="true"
    else
        export UPGRADE_WHILE_GENERATING_REQUIREMENTS=${UPGRADE_WHILE_GENERATING_REQUIREMENTS:="false"}
    fi

    # Default extras used for building CI image
    export DEFAULT_CI_EXTRAS="devel_ci"

    # Default extras used for building Production image. The master of this information is in the Dockerfile
    DEFAULT_PROD_EXTRAS=$(grep "ARG AIRFLOW_EXTRAS=" "${AIRFLOW_SOURCES}/Dockerfile"|
            awk 'BEGIN { FS="=" } { print $2 }' | tr -d '"')

    # By default we build CI images  but when we specify `--production-image` we switch to production image
    export PRODUCTION_IMAGE="false"

    # The SQLlite URL used for sqlite runs
    export SQLITE_URL="sqlite:////root/airflow/airflow.db"

    # Determines if airflow should be installed from a specified reference in GitHub
    export INSTALL_AIRFLOW_REFERENCE=""

    # Version suffix for PyPI packaging
    export VERSION_SUFFIX_FOR_PYPI=""

    # Artifact name suffix for SVN packaging
    export VERSION_SUFFIX_FOR_SVN=""

    # Version of Kubernetes to run
    export KUBERNETES_VERSION="${KUBERNETES_VERSION:="v1.15.3"}"

    # Name of the KinD cluster to connect to
    export KIND_CLUSTER_NAME=${KIND_CLUSTER_NAME:="airflow-python-${PYTHON_MAJOR_MINOR_VERSION}-${KUBERNETES_VERSION}"}

    # Name of the KinD cluster to connect to when referred to via kubectl
    export KUBECTL_CLUSTER_NAME=kind-${KIND_CLUSTER_NAME}

}

# Prints verbose information in case VERBOSE variable is set
function print_info() {
    if [[ ${VERBOSE:="false"} == "true" ]]; then
        echo "$@"
    fi
}

# Those are files that are mounted locally when mounting local sources is requested
# By default not the whole airflow sources directory is mounted because there are often
# artifacts created there (for example .egg-info files) that are breaking the capability
# of running different python versions in Breeze. So we only mount what is needed by default.
function generate_local_mounts_list {
    local prefix="$1"
    LOCAL_MOUNTS=(
        "$prefix".bash_aliases:/root/.bash_aliases:cached
        "$prefix".bash_history:/root/.bash_history:cached
        "$prefix".coveragerc:/opt/airflow/.coveragerc:cached
        "$prefix".dockerignore:/opt/airflow/.dockerignore:cached
        "$prefix".flake8:/opt/airflow/.flake8:cached
        "$prefix".github:/opt/airflow/.github:cached
        "$prefix".inputrc:/root/.inputrc:cached
        "$prefix".kube:/root/.kube:cached
        "$prefix".rat-excludes:/opt/airflow/.rat-excludes:cached
        "$prefix"CHANGELOG.txt:/opt/airflow/CHANGELOG.txt:cached
        "$prefix"Dockerfile.ci:/opt/airflow/Dockerfile.ci:cached
        "$prefix"LICENSE:/opt/airflow/LICENSE:cached
        "$prefix"MANIFEST.in:/opt/airflow/MANIFEST.in:cached
        "$prefix"NOTICE:/opt/airflow/NOTICE:cached
        "$prefix"airflow:/opt/airflow/airflow:cached
        "$prefix"backport_packages:/opt/airflow/backport_packages:cached
        "$prefix"common:/opt/airflow/common:cached
        "$prefix"dags:/opt/airflow/dags:cached
        "$prefix"dev:/opt/airflow/dev:cached
        "$prefix"docs:/opt/airflow/docs:cached
        "$prefix"files:/files:cached
        "$prefix"dist:/dist:cached
        "$prefix"hooks:/opt/airflow/hooks:cached
        "$prefix"logs:/root/airflow/logs:cached
        "$prefix"pylintrc:/opt/airflow/pylintrc:cached
        "$prefix"pytest.ini:/opt/airflow/pytest.ini:cached
        "$prefix"requirements:/opt/airflow/requirements:cached
        "$prefix"scripts:/opt/airflow/scripts:cached
        "$prefix"scripts/ci/in_container/entrypoint_ci.sh:/entrypoint_ci.sh:cached
        "$prefix"setup.cfg:/opt/airflow/setup.cfg:cached
        "$prefix"setup.py:/opt/airflow/setup.py:cached
        "$prefix"tests:/opt/airflow/tests:cached
        "$prefix"kubernetes_tests:/opt/airflow/kubernetes_tests:cached
        "$prefix"tmp:/opt/airflow/tmp:cached
    )
}

# Converts the local mounts that we defined above to the right set of -v
# volume mappings in docker-compose file. This is needed so that we only
# maintain the volumes in one place (above)
function convert_local_mounts_to_docker_params() {
    generate_local_mounts_list "${AIRFLOW_SOURCES}/"
    # Bash can't "return" arrays, so we need to quote any special characters
    printf -- '-v %q ' "${LOCAL_MOUNTS[@]}"
}

# Fixes a file that is expected to be a file. If - for whatever reason - the local file is not created
# When mounting it to container, docker assumes it is a missing directory and creates it. Such mistakenly
# Created directories should be removed and replaced with files
function sanitize_file() {
    if [[ -d "${1}" ]]; then
        rm -rf "${1}"
    fi
    touch "${1}"

}

# Those files are mounted into container when run locally
# .bash_history is preserved and you can modify .bash_aliases and .inputrc
# according to your liking
function sanitize_mounted_files() {
    sanitize_file "${AIRFLOW_SOURCES}/.bash_history"
    sanitize_file "${AIRFLOW_SOURCES}/.bash_aliases"
    sanitize_file "${AIRFLOW_SOURCES}/.inputrc"

    # When KinD cluster is created, the folder keeps authentication information
    # across sessions
    mkdir -p "${AIRFLOW_SOURCES}/.kube" >/dev/null 2>&1
}

#
# Creates cache directory where we will keep temporary files needed for the docker build
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

# Removes the cache temporary directory
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
function calculate_file_md5sum {
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
    mkdir -pv "${BUILD_CACHE_DIR}/${BRANCH_NAME}"
    touch "${BUILT_IMAGE_FLAG_FILE}"
}


function calculate_md5sum_for_all_files() {
    FILES_MODIFIED="false"
    set +e
    for FILE in "${FILES_FOR_REBUILD_CHECK[@]}"
    do
        if ! calculate_file_md5sum "${AIRFLOW_SOURCES}/${FILE}"; then
            FILES_MODIFIED="true"
        fi
    done
    set +e
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
        calculate_md5sum_for_all_files
        export NEEDS_DOCKER_BUILD="true"
    else
        calculate_md5sum_for_all_files
        if [[ ${FILES_MODIFIED} == "true" ]]; then
            export NEEDS_DOCKER_BUILD="true"
        fi
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
    if ! verbose_docker pull "${AIRFLOW_CI_REMOTE_MANIFEST_IMAGE}" >/dev/null; then
        echo
        echo "Remote docker registry unreachable"
        echo
        REMOTE_DOCKER_REGISTRY_UNREACHABLE="true"
        return
    fi
    set -e

    # Docker needs the file passed to --cidfile to not exist, so we can't use mktemp
    TMP_CONTAINER_ID="remote-airflow-manifest-$$.container_id"
    FILES_TO_CLEANUP_ON_EXIT+=("$TMP_CONTAINER_ID")

    TMP_MANIFEST_REMOTE_JSON=$(mktemp)
    TMP_MANIFEST_REMOTE_SHA=$(mktemp)
    # Create container out of the manifest image without running it
    verbose_docker create --cidfile "${TMP_CONTAINER_ID}" "${AIRFLOW_CI_REMOTE_MANIFEST_IMAGE}"
    # Extract manifest and store it in local file
    verbose_docker cp "$(cat "${TMP_CONTAINER_ID}"):${AIRFLOW_CI_BASE_TAG}.json" "${TMP_MANIFEST_REMOTE_JSON}"
    # Filter everything except SHAs of image layers
    sed 's/ *//g' "${TMP_MANIFEST_REMOTE_JSON}" | grep '^"sha256:' >"${TMP_MANIFEST_REMOTE_SHA}"
    verbose_docker rm --force "$( cat "${TMP_CONTAINER_ID}")"
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

#
# Starts the script.
# If VERBOSE_COMMANDS variable is set to true, it enables verbose output of commands executed
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
# Trap function executed always at the end of the script. In case of verbose output it also
# Prints the exit code that the script exits with. Removes verbosity of commands in case it was run with
# command verbosity and in case the script was not run from Breeze (so via ci scripts) it displays
# total time spent in the script so that we can easily see it.
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

    if [[ ${#FILES_TO_CLEANUP_ON_EXIT[@]} -gt 0 ]]; then
      rm -rf -- "${FILES_TO_CLEANUP_ON_EXIT[@]}"
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

# Changes directory to local sources
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

# In case of the pylint checks we filter out some files which are still in pylint_todo.txt list
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

# Checks if any of the files match the regexp specified the parameters here should be
# match_files_regexp REGEXP FILE FILE ...
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

# Retrieves CI environment variables needed - depending on the CI system we run it in.
# We try to be CI - agnostic and our scripts should run the same way on different CI systems
# (This makes it easy to move between different CI systems)
# This function maps CI-specific variables into a generic ones (prefixed with CI_) that
# we used in other scripts
function get_ci_environment() {
    export CI_EVENT_TYPE="manual"
    export CI_TARGET_REPO="apache/airflow"
    export CI_TARGET_BRANCH="master"
    export CI_SOURCE_REPO="apache/airflow"
    export CI_SOURCE_BRANCH="master"
    export CI_BUILD_ID="default-build-id"
    export CI_JOB_ID="default-job-id"
    if [[ ${CI:=} != "true" ]]; then
        echo
        echo "This is not a CI environment!. Staying with the defaults"
        echo
    else
        if [[ ${TRAVIS:=} == "true" ]]; then
            export CI_TARGET_REPO="${TRAVIS_REPO_SLUG}"
            export CI_TARGET_BRANCH="${TRAVIS_BRANCH}"
            export CI_BUILD_ID="${TRAVIS_BUILD_ID}"
            export CI_JOB_ID="${TRAVIS_JOB_ID}"
            if [[ "${TRAVIS_PULL_REQUEST:=}" == "true" ]]; then
                export CI_EVENT_TYPE="pull_request"
                export CI_SOURCE_REPO="${TRAVIS_PULL_REQUEST_SLUG}"
                export CI_SOURCE_BRANCH="${TRAVIS_PULL_REQUEST_BRANCH}"
            elif [[ "${TRAVIS_EVENT_TYPE:=}" == "cron" ]]; then
                export CI_EVENT_TYPE="schedule"
            else
                export CI_EVENT_TYPE="push"
            fi
        elif [[ ${GITHUB_ACTIONS:=} == "true" ]]; then
            export CI_TARGET_REPO="${GITHUB_REPOSITORY}"
            export CI_TARGET_BRANCH="${GITHUB_BASE_REF}"
            export CI_BUILD_ID="${GITHUB_RUN_ID}"
            export CI_JOB_ID="${GITHUB_JOB}"
            if [[ ${GITHUB_EVENT_NAME:=} == "pull_request" ]]; then
                export CI_EVENT_TYPE="pull_request"
                # default name of the source repo (assuming it's forked without rename)
                export SOURCE_AIRFLOW_REPO=${SOURCE_AIRFLOW_REPO:="airflow"}
                # For Pull Requests it's ambiguous to find the PR and we need to
                # assume that name of repo is airflow but it could be overridden in case it's not
                export CI_SOURCE_REPO="${GITHUB_ACTOR}/${SOURCE_AIRFLOW_REPO}"
                export CI_SOURCE_BRANCH="${GITHUB_HEAD_REF}"
                BRANCH_EXISTS=$(git ls-remote --heads \
                    "https://github.com/${CI_SOURCE_REPO}.git" "${CI_SOURCE_BRANCH}")
                if [[ ${BRANCH_EXISTS} == "" ]]; then
                    echo
                    echo "https://github.com/${CI_SOURCE_REPO}.git Branch ${CI_SOURCE_BRANCH} does not exist"
                    echo
                    echo
                    echo "Fallback to https://github.com/${CI_TARGET_REPO}.git Branch ${CI_TARGET_BRANCH}"
                    echo
                    # Fallback to the target repository if the repo does not exist
                    export CI_SOURCE_REPO="${CI_TARGET_REPO}"
                    export CI_SOURCE_BRANCH="${CI_TARGET_BRANCH}"
                fi
            elif [[ ${GITHUB_EVENT_TYPE:=} == "schedule" ]]; then
                export CI_EVENT_TYPE="schedule"
            else
                export CI_EVENT_TYPE="push"
            fi
        else
            echo
            echo "ERROR! Unknown CI environment. Exiting"
            exit 1
        fi
    fi
    echo
    echo "Detected CI build environment"
    echo
    echo "CI_EVENT_TYPE=${CI_EVENT_TYPE}"
    echo "CI_TARGET_REPO=${CI_TARGET_REPO}"
    echo "CI_TARGET_BRANCH=${CI_TARGET_BRANCH}"
    echo "CI_SOURCE_REPO=${CI_SOURCE_REPO}"
    echo "CI_SOURCE_BRANCH=${CI_SOURCE_BRANCH}"
    echo "CI_BUILD_ID=${CI_BUILD_ID}"
    echo "CI_JOB_ID=${CI_JOB_ID}"
    echo
}

# Builds the CI image in the CI environment.
# Depending on the type of build (push/pr/scheduled) it will either build it incrementally or
# from the scratch without cache (the latter for scheduled builds only)
function build_ci_image_on_ci() {
    get_ci_environment

    # In case of CRON jobs we run builds without cache and upgrade to latest requirements
    if [[ "${CI_EVENT_TYPE:=}" == "schedule" ]]; then
        echo
        echo "Disabling cache for scheduled jobs"
        echo
        export DOCKER_CACHE="no-cache"
        echo
        echo "Requirements are upgraded to latest while running Docker build"
        echo
        export UPGRADE_TO_LATEST_REQUIREMENTS="true"
    fi

    prepare_ci_build

    rm -rf "${BUILD_CACHE_DIR}"
    mkdir -pv "${BUILD_CACHE_DIR}"

    rebuild_ci_image_if_needed

    # Disable force pulling forced above this is needed for the subsequent scripts so that
    # They do not try to pull/build images again
    unset FORCE_PULL_IMAGES
    unset FORCE_BUILD
}

# Reads environment variable passed as first parameter from the .build cache file
function read_from_file {
    cat "${BUILD_CACHE_DIR}/.$1" 2>/dev/null || true
}

# Saves environment variable passed as first parameter to the .build cache file
function save_to_file {
    # shellcheck disable=SC2005
    echo "$(eval echo "\$$1")" > "${BUILD_CACHE_DIR}/.$1"
}

# check if parameter set for the variable is allowed (should be on the _BREEZE_ALLOWED list)
# and if it is, it saves it to .build cache file. In case the parameter is wrong, the
# saved variable is removed (so that bad value is not used again in case it comes from there)
# and exits with an error
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

# Docker command to build documentation
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
            "--" "/opt/airflow/docs/build" \
            | tee -a "${OUTPUT_LOG}"
}

# Pulls image in case it is needed (either has never been pulled or pulling was forced
# Should be run with set +e
# Parameters:
#   $1 -> image to pull
function pull_image_if_needed() {
    local IMAGE_TO_PULL="${1}"
    local IMAGE_HASH
    IMAGE_HASH=$(docker images -q "${IMAGE_TO_PULL}" 2> /dev/null || true)
    local PULL_IMAGE=${FORCE_PULL_IMAGES}

    if [[ "${IMAGE_HASH}" == "" ]]; then
        PULL_IMAGE="true"
    fi
    if [[ "${PULL_IMAGE}" == "true" ]]; then
        echo
        echo "Pulling the image ${IMAGE_TO_PULL}"
        echo
        verbose_docker pull "${IMAGE_TO_PULL}" | tee -a "${OUTPUT_LOG}"
        EXIT_VALUE="$?"
        echo
        return ${EXIT_VALUE}
    fi
}

# Pulls image if needed but tries to pull it from cache (for example GitHub registry) before
# It attempts to pull it from the main repository. This is used to speed up the builds
# In GitHub Actions.
# Parameters:
#   $1 -> image to pull
#   $2 -> cache image to pull first
function pull_image_possibly_from_cache() {
    local IMAGE="${1}"
    local CACHED_IMAGE="${2}"
    local IMAGE_PULL_RETURN_VALUE=-1

    set +e
    if [[ ${CACHED_IMAGE:=} != "" ]]; then
        pull_image_if_needed "${CACHED_IMAGE}"
        IMAGE_PULL_RETURN_VALUE="$?"
        if [[ ${IMAGE_PULL_RETURN_VALUE} == "0" ]]; then
            # Tag the image to be the target one
            verbose_docker tag "${CACHED_IMAGE}" "${IMAGE}"
        fi
    fi
    if [[ ${IMAGE_PULL_RETURN_VALUE} != "0" ]]; then
        pull_image_if_needed "${IMAGE}"
    fi
    set -e
}

# Pulls CI image in case caching strategy is "pulled" and the image needs to be pulled
function pull_ci_image_if_needed() {
    # Whether to force pull images to populate cache
    export FORCE_PULL_IMAGES=${FORCE_PULL_IMAGES:="false"}

    if [[ "${DOCKER_CACHE}" == "pulled" ]]; then
        if [[ "${FORCE_PULL_IMAGES}" == "true" ]]; then
            echo
            echo "Force pull base image ${PYTHON_BASE_IMAGE}"
            echo
            if [[ -n ${DETECTED_TERMINAL:=""} ]]; then
                echo -n "
Docker pulling ${PYTHON_BASE_IMAGE}.
                    " > "${DETECTED_TERMINAL}"
            fi
            if [[ ${PULL_PYTHON_BASE_IMAGES_FROM_CACHE:="true"} == "true" ]]; then
                pull_image_possibly_from_cache "${PYTHON_BASE_IMAGE}" "${CACHED_PYTHON_BASE_IMAGE}"
            else
                verbose_docker pull "${PYTHON_BASE_IMAGE}" | tee -a "${OUTPUT_LOG}"
            fi
            echo
        fi
        pull_image_possibly_from_cache "${AIRFLOW_CI_IMAGE}" "${CACHED_AIRFLOW_CI_IMAGE}"
    fi
}


# Pulls PROD image in case caching strategy is "pulled" and the image needs to be pulled
function pull_prod_images_if_needed() {
    # Whether to force pull images to populate cache
    export FORCE_PULL_IMAGES=${FORCE_PULL_IMAGES:="false"}

    if [[ "${DOCKER_CACHE}" == "pulled" ]]; then
        if [[ "${FORCE_PULL_IMAGES}" == "true" ]]; then
            echo
            echo "Force pull base image ${PYTHON_BASE_IMAGE}"
            echo
            if [[ ${PULL_PYTHON_BASE_IMAGES_FROM_CACHE:="true"} == "true" ]]; then
                pull_image_possibly_from_cache "${PYTHON_BASE_IMAGE}" "${CACHED_PYTHON_BASE_IMAGE}"
            else
                verbose_docker pull "${PYTHON_BASE_IMAGE}" | tee -a "${OUTPUT_LOG}"
            fi
            echo
        fi
        # "Build" segment of production image
        pull_image_possibly_from_cache "${AIRFLOW_PROD_BUILD_IMAGE}" "${CACHED_AIRFLOW_PROD_BUILD_IMAGE}"
        # Main segment of production image
        pull_image_possibly_from_cache "${AIRFLOW_PROD_IMAGE}" "${CACHED_AIRFLOW_PROD_IMAGE}"
    fi
}

# Prints summary of the build parameters
function print_build_info() {
    print_info
    print_info "Airflow ${AIRFLOW_VERSION} Python: ${PYTHON_MAJOR_MINOR_VERSION}. Image description: ${IMAGE_DESCRIPTION}"
    print_info
}

# Function to spin ASCII spinner during pull and build in pre-commits to give the user indication that
# Pull/Build is happening. It only spins if the output log changes, so if pull/build is stalled
# The spinner will not move.
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

# Builds CI image - depending on the caching strategy (pulled, local, no-cache) it
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
        --build-arg ADDITIONAL_AIRFLOW_EXTRAS="${ADDITIONAL_AIRFLOW_EXTRAS}" \
        --build-arg ADDITIONAL_PYTHON_DEPS="${ADDITIONAL_PYTHON_DEPS}" \
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

# Builds PROD image - depending on the caching strategy (pulled, local, no-cache) it
# passes the necessary docker build flags via DOCKER_CACHE_PROD_DIRECTIVE and
# DOCKER_CACHE_PROD_BUILD_DIRECTIVE (separate caching options are needed for "build" segment of the image)
# it also passes the right Build args depending on the configuration of the build
# selected by Breeze flags or environment variables.
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
        --build-arg ADDITIONAL_AIRFLOW_EXTRAS="${ADDITIONAL_AIRFLOW_EXTRAS}" \
        --build-arg ADDITIONAL_PYTHON_DEPS="${ADDITIONAL_PYTHON_DEPS}" \
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


# Removes airflow CI and base images
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

# Fixes permissions for groups for all the files that are quickly filtered using the filterout_deleted_files
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

# Prepares all variables needed by the CI build. Depending on the configuration used (python version
# DockerHub user etc. the variables are set so that other functions can use those variables.
function prepare_ci_build() {
    export AIRFLOW_CI_BASE_TAG="${DEFAULT_BRANCH}-python${PYTHON_MAJOR_MINOR_VERSION}-ci"
    export AIRFLOW_CI_LOCAL_MANIFEST_IMAGE="local/${DOCKERHUB_REPO}:${AIRFLOW_CI_BASE_TAG}-manifest"
    export AIRFLOW_CI_REMOTE_MANIFEST_IMAGE="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${AIRFLOW_CI_BASE_TAG}-manifest"
    export AIRFLOW_CI_IMAGE="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${AIRFLOW_CI_BASE_TAG}"
    if [[ ${ENABLE_REGISTRY_CACHE="false"} == "true" ]]; then
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
    export ADDITIONAL_AIRFLOW_EXTRAS="${ADDITIONAL_AIRFLOW_EXTRAS:=""}"
    export ADDITIONAL_PYTHON_DEPS="${ADDITIONAL_PYTHON_DEPS:=""}"
    export AIRFLOW_IMAGE="${AIRFLOW_CI_IMAGE}"
    go_to_airflow_sources
    fix_group_permissions
}

# For remote installation of airflow (from GitHub or Pypi) when building the image, you need to
# pass build flags depending on the version and method of the installation (for example to
# get proper requirement constraint files)
function add_build_args_for_remote_install() {
    # entrypoint is used as AIRFLOW_SOURCES_FROM/TO in order to avoid costly copying of all sources of
    # Airflow - those are not needed for remote install at all. Entrypoint is later overwritten by
    # ENTRYPOINT_FILE - downloaded entrypoint.sh so this is only for the purpose of iteration on Dockerfile
    EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
        "--build-arg" "AIRFLOW_SOURCES_FROM=entrypoint.sh"
        "--build-arg" "AIRFLOW_SOURCES_TO=/entrypoint"
    )
    if [[ ${AIRFLOW_VERSION} =~ [^0-9]*1[^0-9]*10[^0-9]([0-9]*) ]]; then
        # All types of references/versions match this regexp for 1.10 series
        # for example v1_10_test, 1.10.10, 1.10.9 etc. ${BASH_REMATCH[1]} is the () group matches last
        # minor digit of version and it's length is 0 for v1_10_test, 1 for 1.10.9 and 2 for 1.10.10+
        if [[ ${#BASH_REMATCH[1]} == "1" ]]; then
            # This is only for 1.10.0 - 1.10.9
            EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
                "--build-arg" "CONSTRAINT_REQUIREMENTS=https://raw.githubusercontent.com/apache/airflow/1.10.10/requirements/requirements-python${PYTHON_MAJOR_MINOR_VERSION}.txt"
                "--build-arg" "ENTRYPOINT_FILE=https://raw.githubusercontent.com/apache/airflow/1.10.10/entrypoint.sh"
            )
        else
            EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
                # For 1.10.10+ and v1-10-test it's ok to use AIRFLOW_VERSION as reference
                "--build-arg" "CONSTRAINT_REQUIREMENTS=https://raw.githubusercontent.com/apache/airflow/${AIRFLOW_VERSION}/requirements/requirements-python${PYTHON_MAJOR_MINOR_VERSION}.txt"
                "--build-arg" "ENTRYPOINT_FILE=https://raw.githubusercontent.com/apache/airflow/${AIRFLOW_VERSION}/entrypoint.sh"
            )
        fi
    else
        # For all other (master, 2.0+) we just match ${AIRFLOW_VERSION}
        EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
            "--build-arg" "CONSTRAINT_REQUIREMENTS=https://raw.githubusercontent.com/apache/airflow/${AIRFLOW_VERSION}/requirements/requirements-python${PYTHON_MAJOR_MINOR_VERSION}.txt"
            "--build-arg" "ENTRYPOINT_FILE=https://raw.githubusercontent.com/apache/airflow/${AIRFLOW_VERSION}/entrypoint.sh"
        )
    fi
}

# Prepares all variables needed by the CI build. Depending on the configuration used (python version
# DockerHub user etc. the variables are set so that other functions can use those variables.
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
    export ADDITIONAL_AIRFLOW_EXTRAS="${ADDITIONAL_AIRFLOW_EXTRAS:=""}"
    export ADDITIONAL_PYTHON_DEPS="${ADDITIONAL_PYTHON_DEPS:=""}"
    export AIRFLOW_IMAGE="${AIRFLOW_PROD_IMAGE}"

    if [[ ${ENABLE_REGISTRY_CACHE="false"} == "true" ]]; then
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
    export AIRFLOW_KUBERNETES_IMAGE=${AIRFLOW_PROD_IMAGE}-kubernetes
    AIRFLOW_KUBERNETES_IMAGE_NAME=$(echo "${AIRFLOW_KUBERNETES_IMAGE}" | cut -f 1 -d ":")
    export AIRFLOW_KUBERNETES_IMAGE_NAME
    AIRFLOW_KUBERNETES_IMAGE_TAG=$(echo "${AIRFLOW_KUBERNETES_IMAGE}" | cut -f 2 -d ":")
    export AIRFLOW_KUBERNETES_IMAGE_TAG

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
    go_to_airflow_sources
}

# Pushes Ci image and it's manifest to the registry. In case the image was taken from cache registry
# it is pushed to the cache, not to the main registry. Manifest is only pushed to the main registry
function push_ci_image() {
    if [[ ${CACHED_AIRFLOW_CI_IMAGE:=} != "" ]]; then
        verbose_docker tag "${AIRFLOW_CI_IMAGE}" "${CACHED_AIRFLOW_CI_IMAGE}"
        IMAGE_TO_PUSH="${CACHED_AIRFLOW_CI_IMAGE}"
    else
        IMAGE_TO_PUSH="${AIRFLOW_CI_IMAGE}"
    fi
    verbose_docker push "${IMAGE_TO_PUSH}"
    if [[ ${CACHED_AIRFLOW_CI_IMAGE} == "" ]]; then
        # Only push manifest image for builds that are not using CI cache
        verbose_docker tag "${AIRFLOW_CI_LOCAL_MANIFEST_IMAGE}" "${AIRFLOW_CI_REMOTE_MANIFEST_IMAGE}"
        verbose_docker push "${AIRFLOW_CI_REMOTE_MANIFEST_IMAGE}"
        if [[ -n ${DEFAULT_IMAGE:=""} ]]; then
            verbose_docker push "${DEFAULT_IMAGE}"
        fi
    fi
    if [[ ${CACHED_PYTHON_BASE_IMAGE} != "" ]]; then
        verbose_docker tag "${PYTHON_BASE_IMAGE}" "${CACHED_PYTHON_BASE_IMAGE}"
        verbose_docker push "${CACHED_PYTHON_BASE_IMAGE}"
    fi

}

# Pushes PROD image to the registry. In case the image was taken from cache registry
# it is also pushed to the cache, not to the main registry
function push_prod_images() {
    if [[ ${CACHED_AIRFLOW_PROD_IMAGE:=} != "" ]]; then
        verbose_docker tag "${AIRFLOW_PROD_IMAGE}" "${CACHED_AIRFLOW_PROD_IMAGE}"
        IMAGE_TO_PUSH="${CACHED_AIRFLOW_PROD_IMAGE}"
    else
        IMAGE_TO_PUSH="${AIRFLOW_PROD_IMAGE}"
    fi
    if [[ ${CACHED_AIRFLOW_PROD_BUILD_IMAGE:=} != "" ]]; then
        verbose_docker tag "${AIRFLOW_PROD_BUILD_IMAGE}" "${CACHED_AIRFLOW_PROD_BUILD_IMAGE}"
        IMAGE_TO_PUSH_BUILD="${CACHED_AIRFLOW_PROD_BUILD_IMAGE}"
    else
        IMAGE_TO_PUSH_BUILD="${AIRFLOW_PROD_BUILD_IMAGE}"
    fi
    verbose_docker push "${IMAGE_TO_PUSH}"
    verbose_docker push "${IMAGE_TO_PUSH_BUILD}"
    if [[ -n ${DEFAULT_IMAGE:=""} && ${CACHED_AIRFLOW_PROD_IMAGE} == "" ]]; then
        verbose_docker push "${DEFAULT_IMAGE}"
    fi

    # we do not need to push PYTHON base image here - they are already pushed in the CI push
}

# Docker command to generate constraint requirement files.
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
        --env CHECK_REQUIREMENTS_ONLY \
        --rm \
        "${AIRFLOW_CI_IMAGE}" \
        "--" "/opt/airflow/scripts/ci/in_container/run_generate_requirements.sh" \
        | tee -a "${OUTPUT_LOG}"
}

# Docker command to prepare backport packages
function run_prepare_backport_packages() {
    docker run "${EXTRA_DOCKER_FLAGS[@]}" \
        --entrypoint "/usr/local/bin/dumb-init"  \
        --env PYTHONDONTWRITEBYTECODE \
        --env VERBOSE \
        --env VERBOSE_COMMANDS \
        --env HOST_USER_ID="$(id -ur)" \
        --env HOST_GROUP_ID="$(id -gr)" \
        --env UPGRADE_WHILE_GENERATING_REQUIREMENTS \
        --env PYTHON_MAJOR_MINOR_VERSION \
        --env CHECK_REQUIREMENTS_ONLY \
        --env VERSION_SUFFIX_FOR_PYPI \
        --env VERSION_SUFFIX_FOR_SVN \
        -t \
        -v "${AIRFLOW_SOURCES}:/opt/airflow" \
        --rm \
        "${AIRFLOW_CI_IMAGE}" \
        "--" "/opt/airflow/scripts/ci/in_container/run_prepare_backport_packages.sh" "${@}" \
        | tee -a "${OUTPUT_LOG}"
}

# Docker command to generate release notes for backport packages
function run_prepare_backport_readme() {
    docker run "${EXTRA_DOCKER_FLAGS[@]}" \
        --entrypoint "/usr/local/bin/dumb-init"  \
        --env PYTHONDONTWRITEBYTECODE \
        --env VERBOSE \
        --env VERBOSE_COMMANDS \
        --env HOST_USER_ID="$(id -ur)" \
        --env HOST_GROUP_ID="$(id -gr)" \
        --env UPGRADE_WHILE_GENERATING_REQUIREMENTS \
        --env PYTHON_MAJOR_MINOR_VERSION \
        --env CHECK_REQUIREMENTS_ONLY \
        -t \
        -v "${AIRFLOW_SOURCES}:/opt/airflow" \
        --rm \
        "${AIRFLOW_CI_IMAGE}" \
        "--" "/opt/airflow/scripts/ci/in_container/run_prepare_backport_readme.sh" "${@}" \
        | tee -a "${OUTPUT_LOG}"
}

# Retrieves version of airflow stored in the production image (used to display the actual
# Version we use if it was build from PyPI or GitHub
function get_airflow_version_from_production_image() {
     docker run --entrypoint /bin/bash "${AIRFLOW_PROD_IMAGE}" -c 'echo "${AIRFLOW_VERSION}"'
}

function dump_kind_logs() {
    echo "###########################################################################################"
    echo "                   Dumping logs from KIND"
    echo "###########################################################################################"

    FILE_NAME="${1}"
    kind --name "${KIND_CLUSTER_NAME}" export logs "${FILE_NAME}"
}


function send_kubernetes_logs_to_file_io() {
    echo "##############################################################################"
    echo
    echo "   DUMPING LOG FILES FROM KIND AND SENDING THEM TO file.io"
    echo
    echo "##############################################################################"
    DUMP_DIR_NAME=$(date "+%Y-%m-%d")_kind_${CI_BUILD_ID:="default"}_${CI_JOB_ID:="default"}
    DUMP_DIR=/tmp/${DUMP_DIR_NAME}
    dump_kind_logs "${DUMP_DIR}"
    tar -cvzf "${DUMP_DIR}.tar.gz" -C /tmp "${DUMP_DIR_NAME}"
    echo
    echo "   Logs saved to ${DUMP_DIR}.tar.gz"
    echo
    echo "##############################################################################"
    curl -F "file=@${DUMP_DIR}.tar.gz" https://file.io
}

function check_kind_and_kubectl_are_installed() {
    SYSTEM=$(uname -s| tr '[:upper:]' '[:lower:]')
    KIND_VERSION="v0.7.0"
    KIND_URL="https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VERSION}/kind-${SYSTEM}-amd64"
    KIND_PATH="${BUILD_CACHE_DIR}/bin/kind"
    KUBECTL_VERSION="v1.15.3"
    KUBECTL_URL="https://storage.googleapis.com/kubernetes-release/release/${KUBECTL_VERSION}/bin/${SYSTEM}/amd64/kubectl"
    KUBECTL_PATH="${BUILD_CACHE_DIR}/bin/kubectl"
    mkdir -pv "${BUILD_CACHE_DIR}/bin"
    if [[ ! -f "${KIND_PATH}" ]]; then
        echo
        echo "Downloading Kind version ${KIND_VERSION}"
        echo
        curl --fail --location "${KIND_URL}" --output "${KIND_PATH}"
        chmod +x "${KIND_PATH}"
    fi
    if [[ ! -f "${KUBECTL_PATH}" ]]; then
        echo
        echo "Downloading Kubectl version ${KUBECTL_VERSION}"
        echo
        curl --fail --location "${KUBECTL_URL}" --output "${KUBECTL_PATH}"
        chmod +x "${KUBECTL_PATH}"
    fi
    PATH=${PATH}:${BUILD_CACHE_DIR}/bin
}

function create_cluster() {
    if [[ "${TRAVIS:="false"}" == "true" ]]; then
        # Travis CI does not handle the nice output of Kind well, so we need to capture it
        # And display only if kind fails to start
        start_output_heartbeat "Creating kubernetes cluster" 10
        set +e
        if ! OUTPUT=$(kind create cluster \
                        --name "${KIND_CLUSTER_NAME}" \
                        --config "${AIRFLOW_SOURCES}/scripts/ci/kubernetes/kind-cluster-conf.yaml" \
                        --image "kindest/node:${KUBERNETES_VERSION}" 2>&1); then
            echo "${OUTPUT}"
        fi
        stop_output_heartbeat
    else
        kind create cluster \
            --name "${KIND_CLUSTER_NAME}" \
            --config "${AIRFLOW_SOURCES}/scripts/ci/kubernetes/kind-cluster-conf.yaml" \
            --image "kindest/node:${KUBERNETES_VERSION}"
    fi
    echo
    echo "Created cluster ${KIND_CLUSTER_NAME}"
    echo

}

function delete_cluster() {
    kind delete cluster --name "${KIND_CLUSTER_NAME}"
    echo
    echo "Deleted cluster ${KIND_CLUSTER_NAME}"
    echo
    rm -rf "${HOME}/.kube/*"
}

function perform_kind_cluster_operation() {
    OPERATION="${1}"
    ALL_CLUSTERS=$(kind get clusters || true)

    echo
    echo "Kubernetes mode: ${KUBERNETES_MODE}"
    echo

    if [[ ${OPERATION} == "status" ]]; then
        if [[ ${ALL_CLUSTERS} == *"${KIND_CLUSTER_NAME}"* ]]; then
            echo
            echo "Cluster name: ${KIND_CLUSTER_NAME}"
            echo
            kind get nodes --name "${KIND_CLUSTER_NAME}"
            echo
            exit
        else
            echo
            echo "Cluster ${KIND_CLUSTER_NAME} is not running"
            echo
            exit
        fi
    fi
    if [[ ${ALL_CLUSTERS} == *"${KIND_CLUSTER_NAME}"* ]]; then
        if [[ ${OPERATION} == "start" ]]; then
            echo
            echo "Cluster ${KIND_CLUSTER_NAME} is already created"
            echo "Reusing previously created cluster"
            echo
        elif [[ ${OPERATION} == "restart" ]]; then
            echo
            echo "Recreating cluster"
            echo
            delete_cluster
            create_cluster
        elif [[ ${OPERATION} == "stop" ]]; then
            echo
            echo "Deleting cluster"
            echo
            delete_cluster
            exit
        elif [[ ${OPERATION} == "deploy" ]]; then
            echo
            echo "Deploying Airflow to KinD"
            echo
            get_ci_environment
            check_kind_and_kubectl_are_installed
            build_kubernetes_image
            load_image_to_kind_cluster
            prepare_kubernetes_app_variables
            prepare_kubernetes_resources
            apply_kubernetes_resources
            wait_for_airflow_pods_up_and_running
            wait_for_airflow_webserver_up_and_running
        elif [[ ${OPERATION} == "test" ]]; then
            echo
            echo "Testing with kind to KinD"
            echo
            "${AIRFLOW_SOURCES}/scripts/ci/ci_run_kubernetes_tests.sh"
        else
            echo
            echo "Wrong cluster operation: ${OPERATION}. Should be one of:"
            echo "${FORMATTED_KIND_OPERATIONS}"
            echo
            exit 1
        fi
    else
        if [[ ${OPERATION} == "start" ]]; then
            echo
            echo "Creating cluster"
            echo
            create_cluster
        elif [[ ${OPERATION} == "recreate" ]]; then
            echo
            echo "Cluster ${KIND_CLUSTER_NAME} does not exist. Creating rather than recreating"
            echo "Creating cluster"
            echo
            create_cluster
        elif [[ ${OPERATION} == "stop" || ${OEPRATON} == "deploy" || ${OPERATION} == "test" ]]; then
            echo
            echo "Cluster ${KIND_CLUSTER_NAME} does not exist. It should exist for ${OPERATION} operation"
            echo
            exit 1
        else
            echo
            echo "Wrong cluster operation: ${OPERATION}. Should be one of:"
            echo "${FORMATTED_KIND_OPERATIONS}"
            echo
            exit 1
        fi
    fi
}

function check_cluster_ready_for_airflow() {
    kubectl cluster-info --cluster "${KUBECTL_CLUSTER_NAME}"
    kubectl get nodes --cluster "${KUBECTL_CLUSTER_NAME}"
    echo
    echo "Showing storageClass"
    echo
    kubectl get storageclass --cluster "${KUBECTL_CLUSTER_NAME}"
    echo
    echo "Showing kube-system pods"
    echo
    kubectl get -n kube-system pods --cluster "${KUBECTL_CLUSTER_NAME}"
    echo
    echo "Airflow environment on kubernetes is good to go!"
    echo
    kubectl create namespace test-namespace --cluster "${KUBECTL_CLUSTER_NAME}"
}


function build_kubernetes_image() {
    cd "${AIRFLOW_SOURCES}" || exit 1
    prepare_prod_build
    if [[ $(docker images -q "${AIRFLOW_PROD_IMAGE}") == "" ||
            ${FORCE_BUILD_IMAGES:="false"} == "true" ]]; then
        build_prod_image
    else
        echo
        echo "Skip rebuilding prod image. Use --force-build-images to rebuild prod image."
        echo
    fi
    echo
    echo "Adding kubernetes-specific scripts to prod image."
    echo "Building ${AIRFLOW_KUBERNETES_IMAGE} from ${AIRFLOW_PROD_IMAGE} with latest sources."
    echo
    docker build \
        --build-arg AIRFLOW_PROD_IMAGE="${AIRFLOW_PROD_IMAGE}" \
        --cache-from "${AIRFLOW_PROD_IMAGE}" \
        --tag="${AIRFLOW_KUBERNETES_IMAGE}" \
        -f- . << 'EOF'
    ARG AIRFLOW_PROD_IMAGE
    FROM ${AIRFLOW_PROD_IMAGE}

    ARG AIRFLOW_SOURCES=/home/airflow/airflow_sources/
    ENV AIRFLOW_SOURCES=${AIRFLOW_SOURCES}

    USER root

    COPY --chown=airflow:airflow . ${AIRFLOW_SOURCES}

    COPY scripts/ci/kubernetes/docker/airflow-test-env-init-db.sh /tmp/airflow-test-env-init-db.sh
    COPY scripts/ci/kubernetes/docker/airflow-test-env-init-dags.sh /tmp/airflow-test-env-init-dags.sh
    COPY scripts/ci/kubernetes/docker/bootstrap.sh /bootstrap.sh

    RUN chmod +x /bootstrap.sh


    USER airflow


    ENTRYPOINT ["/bootstrap.sh"]
EOF

    echo "The ${AIRFLOW_KUBERNETES_IMAGE} is prepared for deployment."
}

function load_image_to_kind_cluster() {
    echo
    echo "Loading ${AIRFLOW_KUBERNETES_IMAGE} to ${KIND_CLUSTER_NAME}"
    echo
    kind load docker-image --name "${KIND_CLUSTER_NAME}" "${AIRFLOW_KUBERNETES_IMAGE}"
}

function prepare_kubernetes_app_variables() {
    echo
    echo "Preparing kubernetes variables"
    echo
    KUBERNETES_APP_DIR="${AIRFLOW_SOURCES}/scripts/ci/kubernetes/app"
    TEMPLATE_DIRNAME="${KUBERNETES_APP_DIR}/templates"
    BUILD_DIRNAME="${KUBERNETES_APP_DIR}/build"

    # shellcheck source=common/_image_variables.sh
    . "${AIRFLOW_SOURCES}/common/_image_variables.sh"

    # Source branch will be set in DockerHub
    SOURCE_BRANCH=${SOURCE_BRANCH:=${DEFAULT_BRANCH}}
    BRANCH_NAME=${BRANCH_NAME:=${SOURCE_BRANCH}}

    if [[ ! -d "${BUILD_DIRNAME}" ]]; then
        mkdir -p "${BUILD_DIRNAME}"
    fi

    rm -f "${BUILD_DIRNAME}"/*
    rm -f "${BUILD_DIRNAME}"/*

    if [[ "${KUBERNETES_MODE}" == "image" ]]; then
        INIT_DAGS_VOLUME_NAME=airflow-dags
        POD_AIRFLOW_DAGS_VOLUME_NAME=airflow-dags
        CONFIGMAP_DAGS_FOLDER=/opt/airflow/dags
        CONFIGMAP_GIT_DAGS_FOLDER_MOUNT_POINT=
        CONFIGMAP_DAGS_VOLUME_CLAIM=airflow-dags
    else
        INIT_DAGS_VOLUME_NAME=airflow-dags-fake
        POD_AIRFLOW_DAGS_VOLUME_NAME=airflow-dags-git
        CONFIGMAP_DAGS_FOLDER=/opt/airflow/dags/repo/airflow/example_dags
        CONFIGMAP_GIT_DAGS_FOLDER_MOUNT_POINT=/opt/airflow/dags
        CONFIGMAP_DAGS_VOLUME_CLAIM=
    fi


    CONFIGMAP_GIT_REPO=${CI_SOURCE_REPO}
    CONFIGMAP_BRANCH=${CI_SOURCE_BRANCH}
}

function prepare_kubernetes_resources() {
    echo
    echo "Preparing kubernetes resources"
    echo
    if [[ "${KUBERNETES_MODE}" == "image" ]]; then
        sed -e "s/{{INIT_GIT_SYNC}}//g" \
            "${TEMPLATE_DIRNAME}/airflow.template.yaml" >"${BUILD_DIRNAME}/airflow.yaml"
    else
        sed -e "/{{INIT_GIT_SYNC}}/{r ${TEMPLATE_DIRNAME}/init_git_sync.template.yaml" -e 'd}' \
            "${TEMPLATE_DIRNAME}/airflow.template.yaml" >"${BUILD_DIRNAME}/airflow.yaml"
    fi
    sed -i "s|{{AIRFLOW_KUBERNETES_IMAGE}}|${AIRFLOW_KUBERNETES_IMAGE}|g" "${BUILD_DIRNAME}/airflow.yaml"

    sed -i "s|{{CONFIGMAP_GIT_REPO}}|${CONFIGMAP_GIT_REPO}|g" "${BUILD_DIRNAME}/airflow.yaml"
    sed -i "s|{{CONFIGMAP_BRANCH}}|${CONFIGMAP_BRANCH}|g" "${BUILD_DIRNAME}/airflow.yaml"
    sed -i "s|{{INIT_DAGS_VOLUME_NAME}}|${INIT_DAGS_VOLUME_NAME}|g" "${BUILD_DIRNAME}/airflow.yaml"
    sed -i "s|{{POD_AIRFLOW_DAGS_VOLUME_NAME}}|${POD_AIRFLOW_DAGS_VOLUME_NAME}|g" \
        "${BUILD_DIRNAME}/airflow.yaml"

    sed "s|{{CONFIGMAP_DAGS_FOLDER}}|${CONFIGMAP_DAGS_FOLDER}|g" \
        "${TEMPLATE_DIRNAME}/configmaps.template.yaml" >"${BUILD_DIRNAME}/configmaps.yaml"
    sed -i "s|{{CONFIGMAP_GIT_REPO}}|${CONFIGMAP_GIT_REPO}|g" "${BUILD_DIRNAME}/configmaps.yaml"
    sed -i "s|{{CONFIGMAP_BRANCH}}|${CONFIGMAP_BRANCH}|g" "${BUILD_DIRNAME}/configmaps.yaml"
    sed -i "s|{{CONFIGMAP_GIT_DAGS_FOLDER_MOUNT_POINT}}|${CONFIGMAP_GIT_DAGS_FOLDER_MOUNT_POINT}|g" \
        "${BUILD_DIRNAME}/configmaps.yaml"
    sed -i "s|{{CONFIGMAP_DAGS_VOLUME_CLAIM}}|${CONFIGMAP_DAGS_VOLUME_CLAIM}|g" \
        "${BUILD_DIRNAME}/configmaps.yaml"
    sed -i "s|{{AIRFLOW_KUBERNETES_IMAGE_NAME}}|${AIRFLOW_KUBERNETES_IMAGE_NAME}|g" \
        "${BUILD_DIRNAME}/configmaps.yaml"
    sed -i "s|{{AIRFLOW_KUBERNETES_IMAGE_TAG}}|${AIRFLOW_KUBERNETES_IMAGE_TAG}|g" \
        "${BUILD_DIRNAME}/configmaps.yaml"
}

function apply_kubernetes_resources() {
    echo
    echo "Apply kubernetes resources."
    echo


    kubectl delete -f "${KUBERNETES_APP_DIR}/postgres.yaml" --cluster "${KUBECTL_CLUSTER_NAME}" \
        2>&1 | grep -v "NotFound" || true
    kubectl delete -f "${BUILD_DIRNAME}/airflow.yaml" --cluster "${KUBECTL_CLUSTER_NAME}" \
        2>&1 | grep -v "NotFound" || true
    kubectl delete -f "${KUBERNETES_APP_DIR}/secrets.yaml" --cluster "${KUBECTL_CLUSTER_NAME}" \
        2>&1 | grep -v "NotFound" || true

    set -e

    kubectl apply -f "${KUBERNETES_APP_DIR}/secrets.yaml" --cluster "${KUBECTL_CLUSTER_NAME}"
    kubectl apply -f "${BUILD_DIRNAME}/configmaps.yaml" --cluster "${KUBECTL_CLUSTER_NAME}"
    kubectl apply -f "${KUBERNETES_APP_DIR}/postgres.yaml" --cluster "${KUBECTL_CLUSTER_NAME}"
    kubectl apply -f "${KUBERNETES_APP_DIR}/volumes.yaml" --cluster "${KUBECTL_CLUSTER_NAME}"
    kubectl apply -f "${BUILD_DIRNAME}/airflow.yaml" --cluster "${KUBECTL_CLUSTER_NAME}"
}


function dump_kubernetes_logs() {
    POD=$(kubectl get pods -o go-template --template '{{range .items}}{{.metadata.name}}{{"\n"}}{{end}}' \
        --cluster "${KUBECTL_CLUSTER_NAME}" | grep airflow | head -1)
    echo "------- pod description -------"
    kubectl describe pod "${POD}" --cluster "${KUBECTL_CLUSTER_NAME}"
    echo "------- webserver init container logs - init -------"
    kubectl logs "${POD}" -c init --cluster "${KUBECTL_CLUSTER_NAME}" || true
    if [[ "${KUBERNETES_MODE}" == "git" ]]; then
        echo "------- webserver init container logs - git-sync-clone -------"
        kubectl logs "${POD}" -c git-sync-clone --cluster "${KUBECTL_CLUSTER_NAME}" || true
    fi
    echo "------- webserver logs -------"
    kubectl logs "${POD}" -c webserver --cluster "${KUBECTL_CLUSTER_NAME}" || true
    echo "------- scheduler logs -------"
    kubectl logs "${POD}" -c scheduler --cluster "${KUBECTL_CLUSTER_NAME}" || true
    echo "--------------"
}

function wait_for_airflow_pods_up_and_running() {
    set +o pipefail
    # wait for up to 10 minutes for everything to be deployed
    PODS_ARE_READY="0"
    for i in {1..150}; do
        echo "------- Running kubectl get pods: $i -------"
        PODS=$(kubectl get pods --cluster "${KUBECTL_CLUSTER_NAME}" | awk 'NR>1 {print $0}')
        echo "$PODS"
        NUM_AIRFLOW_READY=$(echo "${PODS}" | grep airflow | awk '{print $2}' | grep -cE '([0-9])\/(\1)' \
            | xargs)
        NUM_POSTGRES_READY=$(echo "${PODS}" | grep postgres | awk '{print $2}' | grep -cE '([0-9])\/(\1)' \
            | xargs)
        if [[ "${NUM_AIRFLOW_READY}" == "1" && "${NUM_POSTGRES_READY}" == "1" ]]; then
            PODS_ARE_READY="1"
            break
        fi
        sleep 4
    done
    POD=$(kubectl get pods -o go-template --template '{{range .items}}{{.metadata.name}}{{"\n"}}{{end}}' \
        --cluster "${KUBECTL_CLUSTER_NAME}" | grep airflow | head -1)

    if [[ "${PODS_ARE_READY}" == "1" ]]; then
        echo "PODS are ready."
        set -o pipefail
    else
        echo >&2 "PODS are not ready after waiting for a long time. Exiting..."
        dump_kubernetes_logs
        exit 1
    fi
}


function wait_for_airflow_webserver_up_and_running() {
    set +o pipefail
    # Wait until Airflow webserver is up
    KUBERNETES_HOST=localhost
    AIRFLOW_WEBSERVER_IS_READY="0"
    CONSECUTIVE_SUCCESS_CALLS=0
    for i in {1..30}; do
        echo "------- Wait until webserver is up: $i -------"
        PODS=$(kubectl get pods --cluster "${KUBECTL_CLUSTER_NAME}" | awk 'NR>1 {print $0}')
        echo "$PODS"
        HTTP_CODE=$(curl -LI "http://${KUBERNETES_HOST}:30809/health" -o /dev/null -w '%{http_code}\n' -sS) \
            || true
        if [[ "${HTTP_CODE}" == 200 ]]; then
            ((CONSECUTIVE_SUCCESS_CALLS += 1))
        else
            CONSECUTIVE_SUCCESS_CALLS="0"
        fi
        if [[ "${CONSECUTIVE_SUCCESS_CALLS}" == 3 ]]; then
            AIRFLOW_WEBSERVER_IS_READY="1"
            break
        fi
        sleep 10
    done
    set -o pipefail
    if [[ "${AIRFLOW_WEBSERVER_IS_READY}" == "1" ]]; then
        echo
        echo "Airflow webserver is ready."
        echo
    else
        echo >&2
        echo >&2 "Airflow webserver is not ready after waiting for a long time. Exiting..."
        echo >&2
        dump_kubernetes_logs
        exit 1
    fi
}
