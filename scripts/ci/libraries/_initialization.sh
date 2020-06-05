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

    # Sets to where airflow sources are located
    AIRFLOW_SOURCES=${AIRFLOW_SOURCES:=$(cd "${MY_DIR}/../../" && pwd)}
    export AIRFLOW_SOURCES

    # Sets to the build cache directory - status of build and convenience scripts are stored there
    BUILD_CACHE_DIR="${AIRFLOW_SOURCES}/.build"
    export BUILD_CACHE_DIR

    # File to keep the last forced answer. This is useful for pre-commits where you need to
    # only answer once if the image should be rebuilt or not and your answer is used for
    # All the subsequent questions
    export LAST_FORCE_ANSWER_FILE="${BUILD_CACHE_DIR}/last_force_answer.sh"

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

    # We use pulled docker image cache by default to speed up the builds
    # but we can also set different docker caching strategy (for example we can use local cache
    # to build the images in case we iterate with the image
    export DOCKER_CACHE=${DOCKER_CACHE:="pulled"}

    # By default we are not upgrading to latest requirements when building Docker CI image
    # This will only be done in cron jobs
    export UPGRADE_TO_LATEST_REQUIREMENTS=${UPGRADE_TO_LATEST_REQUIREMENTS:="false"}

    # In case of MacOS we need to use gstat - gnu version of the stats
    export STAT_BIN=stat
    if [[ "${OSTYPE}" == "darwin"* ]]; then
        export STAT_BIN=gstat
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

    # Version of Kubernetes to run
    export KUBERNETES_VERSION="${KUBERNETES_VERSION:="v1.15.3"}"

    # Name of the KinD cluster to connect to
    export KIND_CLUSTER_NAME=${KIND_CLUSTER_NAME:="airflow-python-${PYTHON_MAJOR_MINOR_VERSION}-${KUBERNETES_VERSION}"}

    # Name of the KinD cluster to connect to when referred to via kubectl
    export KUBECTL_CLUSTER_NAME=kind-${KIND_CLUSTER_NAME}

}
