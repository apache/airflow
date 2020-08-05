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

# Common environment that is initialized by both Breeze and CI scripts
function initialize_common_environment {
    # default python Major/Minor version
    PYTHON_MAJOR_MINOR_VERSION=${PYTHON_MAJOR_MINOR_VERSION:="3.6"}

    # extra flags passed to docker run for CI image
    # shellcheck disable=SC2034
    EXTRA_DOCKER_FLAGS=()

    # extra flags passed to docker run for PROD image
    # shellcheck disable=SC2034
    EXTRA_DOCKER_PROD_BUILD_FLAGS=()

    # files that should be cleaned up when the script exits
    # shellcheck disable=SC2034
    FILES_TO_CLEANUP_ON_EXIT=()

    # Sets to the build cache directory - status of build and convenience scripts are stored there
    BUILD_CACHE_DIR="${AIRFLOW_SOURCES}/.build"
    export BUILD_CACHE_DIR

    # File to keep the last forced answer. This is useful for pre-commits where you need to
    # only answer once if the image should be rebuilt or not and your answer is used for
    # All the subsequent questions
    export LAST_FORCE_ANSWER_FILE="${BUILD_CACHE_DIR}/last_force_answer.sh"

    # This folder is mounted to inside the container in /files folder. This is the way how
    # We can exchange DAGs, scripts, packages etc with the container environment
    export FILES_DIR="${AIRFLOW_SOURCES}/files"
    # Temporary dir used well ... temporarily
    export TMP_DIR="${AIRFLOW_SOURCES}/tmp"

    # Create useful directories if not yet created
    mkdir -p "${TMP_DIR}"
    mkdir -p "${FILES_DIR}"
    mkdir -p "${AIRFLOW_SOURCES}/.mypy_cache"
    mkdir -p "${AIRFLOW_SOURCES}/logs"
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
    export USE_GITHUB_REGISTRY=${USE_GITHUB_REGISTRY:="false"}

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

    # Set host OS. This is used to set the ownership properly when exiting
    # The container on Linux - all files created inside docker are created with root user
    # but they should be restored back to the host user
    HOST_OS="$(uname -s)"
    export HOST_OS


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
        EXTRA_DOCKER_FLAGS+=("-v" "${AIRFLOW_SOURCES}/files:/files" )
    else
        print_info
        print_info "Skip mounting host volumes to Docker"
        print_info
        EXTRA_DOCKER_FLAGS=( \
            "--env" "PYTHONDONTWRITEBYTECODE" \
        )
    fi

    # By default we are not upgrading to latest version of constraints when building Docker CI image
    # This will only be done in cron jobs
    export UPGRADE_TO_LATEST_CONSTRAINTS=${UPGRADE_TO_LATEST_CONSTRAINTS:="false"}

    # In case of MacOS we need to use gstat - gnu version of the stats
    export STAT_BIN=stat
    if [[ "${OSTYPE}" == "darwin"* ]]; then
        export STAT_BIN=gstat
    fi

    # Read airflow version from the version.py
    AIRFLOW_VERSION=$(grep version "${AIRFLOW_SOURCES}/airflow/version.py" | awk '{print $3}' | sed "s/['+]//g")
    export AIRFLOW_VERSION

    # default version of python used to tag the "master" and "latest" images in DockerHub
    export DEFAULT_PYTHON_MAJOR_MINOR_VERSION=3.6

    # In case we are not in CI - we assume we run locally.
    if [[ ${CI:="false"} == "true" ]]; then
        export LOCAL_RUN="false"
    else
        export LOCAL_RUN="true"
    fi

    # Default extras used for building CI image
    export DEFAULT_CI_EXTRAS="devel_ci"

    # Default extras used for building Production image. The master of this information is in the Dockerfile
    DEFAULT_PROD_EXTRAS=$(grep "ARG AIRFLOW_EXTRAS=" "${AIRFLOW_SOURCES}/Dockerfile"|
            awk 'BEGIN { FS="=" } { print $2 }' | tr -d '"')
    export DEFAULT_PROD_EXTRAS

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

    # Default Kubernetes version
    export DEFAULT_KUBERNETES_VERSION="v1.18.6"

    # Default KinD version
    export DEFAULT_KIND_VERSION="v0.8.0"

    # Default Helm version
    export DEFAULT_HELM_VERSION="v3.2.4"

    # Version of Kubernetes to run
    export KUBERNETES_VERSION="${KUBERNETES_VERSION:=${DEFAULT_KUBERNETES_VERSION}}"

    # folder with DAGs to embed into production image
    export EMBEDDED_DAGS=${EMBEDDED_DAGS:="empty"}

    # Namespace where airflow is installed via helm
    export HELM_AIRFLOW_NAMESPACE="airflow"

}

# Retrieves CI environment variables needed - depending on the CI system we run it in.
# We try to be CI - agnostic and our scripts should run the same way on different CI systems
# (This makes it easy to move between different CI systems)
# This function maps CI-specific variables into a generic ones (prefixed with CI_) that
# we used in other scripts
function get_environment_for_builds_on_ci() {
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
            export CI_TARGET_BRANCH="${GITHUB_BASE_REF:=${CI_TARGET_BRANCH}}"
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
                    "https://github.com/${CI_SOURCE_REPO}.git" "${CI_SOURCE_BRANCH}" || true)
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
        elif [[ "${LOCAL_CI_TESTING:=}" == "true" ]]; then
            export CI_TARGET_REPO="apache/airflow"
            export CI_TARGET_BRANCH="${DEFAULT_BRANCH:="master"}"
            export CI_BUILD_ID="0"
            export CI_JOB_ID="0"
            export CI_EVENT_TYPE="pull_request"
            export CI_SOURCE_REPO="apache/airflow"
            export CI_SOURCE_BRANCH="${DEFAULT_BRANCH:="master"}"
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
