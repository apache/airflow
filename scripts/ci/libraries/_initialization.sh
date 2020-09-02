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

# Needs to be declared outside function in MacOS
CURRENT_PYTHON_MAJOR_MINOR_VERSIONS=()

# Very basic variables that MUST be set
function initialize_base_variables() {
    # This folder is mounted to inside the container in /files folder. This is the way how
    # We can exchange DAGs, scripts, packages etc with the container environment
    export FILES_DIR="${AIRFLOW_SOURCES}/files"
    # Temporary dir used well ... temporarily
    export TMP_DIR="${AIRFLOW_SOURCES}/tmp"

    # By default we are not in CI environment GitHub Actions sets CI to "true"
    export CI="${CI="false"}"

    # Create useful directories if not yet created
    mkdir -p "${TMP_DIR}"
    mkdir -p "${FILES_DIR}"
    mkdir -p "${AIRFLOW_SOURCES}/.mypy_cache"
    mkdir -p "${AIRFLOW_SOURCES}/logs"
    mkdir -p "${AIRFLOW_SOURCES}/dist"

    # Default port numbers for forwarded ports
    export WEBSERVER_HOST_PORT=${WEBSERVER_HOST_PORT:="28080"}
    export POSTGRES_HOST_PORT=${POSTGRES_HOST_PORT:="25433"}
    export MYSQL_HOST_PORT=${MYSQL_HOST_PORT:="23306"}

    # Default MySQL/Postgres versions
    export POSTGRES_VERSION=${POSTGRES_VERSION:="9.6"}
    export MYSQL_VERSION=${MYSQL_VERSION:="5.7"}

    # The SQLite URL used for sqlite runs
    export SQLITE_URL="sqlite:////root/airflow/airflow.db"

    # Disable writing .pyc files - slightly slower imports but not messing around when switching
    # Python version and avoids problems with root-owned .pyc files in host
    export PYTHONDONTWRITEBYTECODE=${PYTHONDONTWRITEBYTECODE:="true"}

    # Sets to the build cache directory - status of build and convenience scripts are stored there
    BUILD_CACHE_DIR="${AIRFLOW_SOURCES}/.build"
    export BUILD_CACHE_DIR

    # Currently supported major/minor versions of python
    CURRENT_PYTHON_MAJOR_MINOR_VERSIONS+=("3.6" "3.7" "3.8")
    export CURRENT_PYTHON_MAJOR_MINOR_VERSIONS

    # default python Major/Minor version
    PYTHON_MAJOR_MINOR_VERSION=${PYTHON_MAJOR_MINOR_VERSION:="3.6"}

    # If set to true, the database will be reset at entry. Works for Postgres and MySQL
    export DB_RESET=${DB_RESET:="false"}

    # Read airflow version from the version.py
    AIRFLOW_VERSION=$(grep version "${AIRFLOW_SOURCES}/airflow/version.py" | awk '{print $3}' | sed "s/['+]//g")
    export AIRFLOW_VERSION

    # default version of python used to tag the "master" and "latest" images in DockerHub
    export DEFAULT_PYTHON_MAJOR_MINOR_VERSION=3.6
}

# Determine current branch
function initialize_branch_variables() {
    # Default branch used - this will be different in different branches
    export DEFAULT_BRANCH="master"
    export DEFAULT_CONSTRAINTS_BRANCH="constraints-master"

    # Default branch name for triggered builds is the one configured in default branch
    # We need to read it here as it comes from _common_values.sh
    export BRANCH_NAME=${BRANCH_NAME:=${DEFAULT_BRANCH}}
}

# Determine dockerhub user/repo used for push/pull
function initialize_dockerhub_variables() {
    # You can override DOCKERHUB_USER to use your own DockerHub account and play with your
    # own docker images. In this case you can build images locally and push them
    export DOCKERHUB_USER=${DOCKERHUB_USER:="apache"}

    # You can override DOCKERHUB_REPO to use your own DockerHub repository and play with your
    # own docker images. In this case you can build images locally and push them
    export DOCKERHUB_REPO=${DOCKERHUB_REPO:="airflow"}
}

# Determine available integrations
function initialize_available_integrations() {
    export AVAILABLE_INTEGRATIONS="cassandra kerberos mongo openldap presto rabbitmq redis"
}


# Needs to be declared outside of function for MacOS
FILES_FOR_REBUILD_CHECK=()

# Determine which files trigger rebuild check
function initialize_files_for_rebuild_check() {
    FILES_FOR_REBUILD_CHECK+=(
        "setup.py"
        "setup.cfg"
        "Dockerfile.ci"
        ".dockerignore"
        "airflow/version.py"
        "airflow/www/package.json"
        "airflow/www/yarn.lock"
        "airflow/www/webpack.config.js"
    )
}


# Needs to be declared outside of function for MacOS

# extra flags passed to docker run for PROD image
# shellcheck disable=SC2034
EXTRA_DOCKER_PROD_BUILD_FLAGS=()

# files that should be cleaned up when the script exits
# shellcheck disable=SC2034
FILES_TO_CLEANUP_ON_EXIT=()

# extra flags passed to docker run for CI image
# shellcheck disable=SC2034
EXTRA_DOCKER_FLAGS=()

# Determine behaviour of mounting sources to the container
function initialize_mount_variables() {

    # Whether necessary for airflow run local sources are mounted to docker
    export MOUNT_LOCAL_SOURCES=${MOUNT_LOCAL_SOURCES:="true"}

    # Whether files folder from local sources are mounted to docker
    export MOUNT_FILES=${MOUNT_FILES:="true"}

    if [[ ${MOUNT_LOCAL_SOURCES} == "true" ]]; then
        print_info
        print_info "Mounting necessary host volumes to Docker"
        print_info
        read -r -a EXTRA_DOCKER_FLAGS <<<"$(convert_local_mounts_to_docker_params)"
    else
        print_info
        print_info "Skip mounting host volumes to Docker"
        print_info
    fi

    if [[ ${MOUNT_FILES} == "true" ]]; then
        print_info
        print_info "Mounting files folder to Docker"
        print_info
        EXTRA_DOCKER_FLAGS+=(
            "-v" "${AIRFLOW_SOURCES}/files:/files"
        )
    fi

    EXTRA_DOCKER_FLAGS+=(
        "--rm"
        "--env-file" "${AIRFLOW_SOURCES}/scripts/ci/libraries/_docker.env"
    )
    export EXTRA_DOCKER_FLAGS
}

# Determine values of force settings
function initialize_force_variables() {
    # Whether necessary for airflow run local sources are mounted to docker
    export FORCE_PULL_IMAGES=${FORCE_PULL_IMAGES:="false"}

    # Determines whether to force build without checking if it is needed
    # Can be overridden by '--force-build-images' flag.
    export FORCE_BUILD_IMAGES=${FORCE_BUILD_IMAGES:="false"}

    # File to keep the last forced answer. This is useful for pre-commits where you need to
    # only answer once if the image should be rebuilt or not and your answer is used for
    # All the subsequent questions
    export LAST_FORCE_ANSWER_FILE="${BUILD_CACHE_DIR}/last_force_answer.sh"

    # Can be set to "yes/no/quit" in order to force specified answer to all questions asked to the user.
    export FORCE_ANSWER_TO_QUESTIONS=${FORCE_ANSWER_TO_QUESTIONS:=""}

    # Can be set to true to skip if the image is newer in registry
    export SKIP_CHECK_REMOTE_IMAGE=${SKIP_CHECK_REMOTE_IMAGE:="false"}

}

# Determine information about the host
function initialize_host_variables() {
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

    # Home directory of the host user
    export HOST_HOME="${HOME}"

    # Sources of Airflow on the host.
    export HOST_AIRFLOW_SOURCES="${HOST_AIRFLOW_SOURCES:=${AIRFLOW_SOURCES}}"

    # In case of MacOS we need to use gstat - gnu version of the stats
    export STAT_BIN=stat
    if [[ "${OSTYPE}" == "darwin"* ]]; then
        export STAT_BIN=gstat
    fi
}

# Determine image augmentation parameters
function initialize_image_build_variables() {
    # Default extras used for building CI image
    export DEFAULT_CI_EXTRAS="devel_ci"

    export CI_BUILD_ID="${CI_BUILD_ID:="0"}"

    # Default extras used for building Production image. The master of this information is in the Dockerfile
    DEFAULT_PROD_EXTRAS=$(grep "ARG AIRFLOW_EXTRAS=" "${AIRFLOW_SOURCES}/Dockerfile" |
        awk 'BEGIN { FS="=" } { print $2 }' | tr -d '"')
    export DEFAULT_PROD_EXTRAS

    # Installs different airflow version than current from the sources
    export INSTALL_AIRFLOW_VERSION=${INSTALL_AIRFLOW_VERSION:=""}

    # Determines if airflow should be installed from a specified reference in GitHub
    export INSTALL_AIRFLOW_REFERENCE=${INSTALL_AIRFLOW_REFERENCE:=""}

    # By default we are not upgrading to latest version of constraints when building Docker CI image
    # This will only be done in cron jobs
    export UPGRADE_TO_LATEST_CONSTRAINTS=${UPGRADE_TO_LATEST_CONSTRAINTS:="false"}

    # Checks if the image should be rebuilt
    export CHECK_IMAGE_FOR_REBUILD="${CHECK_IMAGE_FOR_REBUILD:="true"}"

}

# Determine version suffixes used to build backport packages
function initialize_version_suffixes_for_package_building() {
    # Version suffix for PyPI packaging
    export VERSION_SUFFIX_FOR_PYPI=""

    # Artifact name suffix for SVN packaging
    export VERSION_SUFFIX_FOR_SVN=""
}

# Determine versions of kubernetes cluster and tools used
function initialize_kubernetes_variables() {
    # By default we assume the kubernetes cluster is not being started
    export ENABLE_KIND_CLUSTER=${ENABLE_KIND_CLUSTER:="false"}

    # Default Kubernetes version
    export DEFAULT_KUBERNETES_VERSION="v1.18.6"

    # Default KinD version
    export DEFAULT_KIND_VERSION="v0.8.0"

    # Default Helm version
    export DEFAULT_HELM_VERSION="v3.2.4"

    # Namespace where airflow is installed via helm
    export HELM_AIRFLOW_NAMESPACE="airflow"

    COMMIT_SHA="$(git rev-parse HEAD || echo "Unknown")"
    export COMMIT_SHA

    export CI_BUILD_ID="0"
}

function initialize_git_variables() {
    # SHA of the commit for the current sources
    COMMIT_SHA="$(git rev-parse HEAD || echo "Unknown")"
    export COMMIT_SHA
}

function initialize_github_variables() {
    # Defaults for interacting with GitHub
    export GITHUB_REPOSITORY=${GITHUB_REPOSITORY:="apache/airflow"}
    GITHUB_REPOSITORY_LOWERCASE="$(echo "${GITHUB_REPOSITORY}" |tr '[:upper:]' '[:lower:]')"
    export GITHUB_REPOSITORY_LOWERCASE
    export GITHUB_REGISTRY=${GITHUB_REGISTRY:="docker.pkg.github.com"}
    export USE_GITHUB_REGISTRY=${USE_GITHUB_REGISTRY:="false"}
    export GITHUB_REGISTRY_WAIT_FOR_IMAGE=${GITHUB_REGISTRY_WAIT_FOR_IMAGE:="false"}
    export GITHUB_REGISTRY_PULL_IMAGE_TAG=${GITHUB_REGISTRY_PULL_IMAGE_TAG:="latest"}
    export GITHUB_REGISTRY_PUSH_IMAGE_TAG=${GITHUB_REGISTRY_PUSH_IMAGE_TAG:="latest"}

}

# Common environment that is initialized by both Breeze and CI scripts
function initialize_common_environment() {
    initialize_base_variables
    initialize_branch_variables
    initialize_available_integrations
    initialize_files_for_rebuild_check
    initialize_dockerhub_variables
    initialize_mount_variables
    initialize_force_variables
    initialize_host_variables
    initialize_image_build_variables
    initialize_version_suffixes_for_package_building
    initialize_kubernetes_variables
    initialize_git_variables
    initialize_github_variables
}

function summarize_ci_environment() {
    cat <<EOF

Configured build variables:

Basic variables:

    PYTHON_MAJOR_MINOR_VERSION: ${PYTHON_MAJOR_MINOR_VERSION}
    DB_RESET: ${DB_RESET}

DockerHub variables:

    DOCKERHUB_USER=${DOCKERHUB_USER}
    DOCKERHUB_REPO=${DOCKERHUB_REPO}

Mount variables:

    MOUNT_LOCAL_SOURCES: ${MOUNT_LOCAL_SOURCES}
    MOUNT_FILES: ${MOUNT_FILES}

Force variables:

    FORCE_PULL_IMAGES: ${FORCE_PULL_IMAGES}
    FORCE_BUILD_IMAGES: ${FORCE_BUILD_IMAGES}
    FORCE_ANSWER_TO_QUESTIONS: ${FORCE_ANSWER_TO_QUESTIONS}
    SKIP_CHECK_REMOTE_IMAGE: ${SKIP_CHECK_REMOTE_IMAGE}

Host variables:

    HOST_USER_ID=${HOST_USER_ID}
    HOST_GROUP_ID=${HOST_GROUP_ID}
    HOST_OS=${HOST_OS}
    HOST_HOME=${HOST_HOME}
    HOST_AIRFLOW_SOURCES=${HOST_AIRFLOW_SOURCES}

Image variables:

    INSTALL_AIRFLOW_VERSION=${INSTALL_AIRFLOW_VERSION}
    INSTALL_AIRFLOW_REFERENCE=${INSTALL_AIRFLOW_REFERENCE}

Version suffix variables:

    VERSION_SUFFIX_FOR_PYPI=${VERSION_SUFFIX_FOR_PYPI}
    VERSION_SUFFIX_FOR_SVN=${VERSION_SUFFIX_FOR_SVN}

Git variables:

    COMMIT_SHA = ${COMMIT_SHA}

Verbosity variables:

    VERBOSE: ${VERBOSE}
    VERBOSE_COMMANDS: ${VERBOSE_COMMANDS}

Image build variables:

    UPGRADE_TO_LATEST_CONSTRAINTS: ${UPGRADE_TO_LATEST_CONSTRAINTS}
    CHECK_IMAGE_FOR_REBUILD: ${CHECK_IMAGE_FOR_REBUILD}


Detected GitHub environment:

    USE_GITHUB_REGISTRY=${USE_GITHUB_REGISTRY}
    GITHUB_REGISTRY=${GITHUB_REGISTRY}
    GITHUB_REPOSITORY=${GITHUB_REPOSITORY}
    GITHUB_REPOSITORY_LOWERCASE=${GITHUB_REPOSITORY_LOWERCASE}
    GITHUB_USERNAME=${GITHUB_USERNAME}
    GITHUB_TOKEN=${GITHUB_TOKEN}
    GITHUB_REGISTRY_WAIT_FOR_IMAGE=${GITHUB_REGISTRY_WAIT_FOR_IMAGE}
    GITHUB_REGISTRY_PULL_IMAGE_TAG=${GITHUB_REGISTRY_PULL_IMAGE_TAG}
    GITHUB_REGISTRY_PUSH_IMAGE_TAG=${GITHUB_REGISTRY_PUSH_IMAGE_TAG}
    GITHUB_ACTIONS=${GITHUB_ACTIONS}

Detected CI build environment:

    CI_TARGET_REPO=${CI_TARGET_REPO}
    CI_TARGET_BRANCH=${CI_TARGET_BRANCH}
    CI_BUILD_ID=${CI_BUILD_ID}
    CI_JOB_ID=${CI_JOB_ID}
    CI_EVENT_TYPE=${CI_EVENT_TYPE}
    CI_SOURCE_REPO=${CI_SOURCE_REPO}
    CI_SOURCE_BRANCH=${CI_SOURCE_BRANCH}

EOF

}

# Retrieves CI environment variables needed - depending on the CI system we run it in.
# We try to be CI - agnostic and our scripts should run the same way on different CI systems
# (This makes it easy to move between different CI systems)
# This function maps CI-specific variables into a generic ones (prefixed with CI_) that
# we used in other scripts
function get_environment_for_builds_on_ci() {
    if [[ ${GITHUB_ACTIONS:=} == "true" ]]; then
        export CI_TARGET_REPO="${GITHUB_REPOSITORY}"
        export CI_TARGET_BRANCH="${GITHUB_BASE_REF:="master"}"
        export CI_BUILD_ID="${GITHUB_RUN_ID}"
        export CI_JOB_ID="${GITHUB_JOB}"
        export CI_EVENT_TYPE="${GITHUB_EVENT_NAME}"
        export CI_REF="${GITHUB_REF:=}"
        if [[ ${CI_EVENT_TYPE:=} == "pull_request" ]]; then
            # default name of the source repo (assuming it's forked without rename)
            export SOURCE_AIRFLOW_REPO=${SOURCE_AIRFLOW_REPO:="airflow"}
            # For Pull Requests it's ambiguous to find the PR and we need to
            # assume that name of repo is airflow but it could be overridden in case it's not
            export CI_SOURCE_REPO="${GITHUB_ACTOR}/${SOURCE_AIRFLOW_REPO}"
            export CI_SOURCE_BRANCH="${GITHUB_HEAD_REF}"
            BRANCH_EXISTS=$(git ls-remote --heads \
                "https://github.com/${CI_SOURCE_REPO}.git" "${CI_SOURCE_BRANCH}" || true)
            if [[ ${BRANCH_EXISTS} == "" ]]; then
                print_info
                print_info "https://github.com/${CI_SOURCE_REPO}.git Branch ${CI_SOURCE_BRANCH} does not exist"
                print_info
                print_info
                print_info "Fallback to https://github.com/${CI_TARGET_REPO}.git Branch ${CI_TARGET_BRANCH}"
                print_info
                # Fallback to the target repository if the repo does not exist
                export CI_SOURCE_REPO="${CI_TARGET_REPO}"
                export CI_SOURCE_BRANCH="${CI_TARGET_BRANCH}"
            fi
        else
            export CI_SOURCE_REPO="${CI_TARGET_REPO}"
            export CI_SOURCE_BRANCH="${CI_TARGET_BRANCH}"
        fi
    else
        # CI PR settings
        export CI_TARGET_REPO="${CI_TARGET_REPO:="apache/airflow"}"
        export CI_TARGET_BRANCH="${DEFAULT_BRANCH:="master"}"
        export CI_BUILD_ID="${CI_BUILD_ID:="0"}"
        export CI_JOB_ID="${CI_JOB_ID:="0"}"
        export CI_EVENT_TYPE="${CI_EVENT_TYPE:="pull_request"}"
        export CI_REF="${CI_REF:="refs/head/master"}"

        export CI_SOURCE_REPO="${CI_SOURCE_REPO:="apache/airflow"}"
        export CI_SOURCE_BRANCH="${DEFAULT_BRANCH:="master"}"

        # Github Registry settings (in Github Actions they come from .github/workflow/ci.yml)
        export USE_GITHUB_REGISTRY=${USE_GITHUB_REGISTRY:="true"}
        export GITHUB_REGISTRY=${GITHUB_REGISTRY:="docker.pkg.github.com"}
        export GITHUB_REPOSITORY="${GITHUB_REPOSITORY:="apache/airflow"}"
        export GITHUB_USERNAME="${GITHUB_USERNAME=""}"
        export GITHUB_TOKEN="${GITHUB_TOKEN=""}"
        export GITHUB_REGISTRY_WAIT_FOR_IMAGE=${GITHUB_REGISTRY_WAIT_FOR_IMAGE:="false"}
        export GITHUB_REGISTRY_PULL_IMAGE_TAG=${GITHUB_REGISTRY_PULL_IMAGE_TAG:="latest"}
        export GITHUB_REGISTRY_PUSH_IMAGE_TAG=${GITHUB_REGISTRY_PUSH_IMAGE_TAG:="latest"}

        # DockerHub settings
        export DOCKERHUB_USER=${DOCKERHUB_USER:="apache"}
        export DOCKERHUB_REPO=${DOCKERHUB_REPO:="airflow"}
    fi

    if [[ ${VERBOSE} == "true" && ${PRINT_INFO_FROM_SCRIPTS} == "true" ]]; then
        summarize_ci_environment
    fi
}
