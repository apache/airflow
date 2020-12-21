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
    else
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
                    # For specified minor version of 1.10 or v1 branch use specific reference constraints
                    "--build-arg" "AIRFLOW_CONSTRAINTS_REFERENCE=constraints-${AIRFLOW_VERSION}"
                )
            fi
        elif  [[ ${AIRFLOW_VERSION} =~ v?2.* ]]; then
            EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
                # For specified minor version of 2.0 or v2 branch use specific reference constraints
                "--build-arg" "AIRFLOW_CONSTRAINTS_REFERENCE=constraints-${AIRFLOW_VERSION}"
            )
        else
            # For all other we just get the default constraint branch coming from the _initialization.sh
            EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
                "--build-arg" "AIRFLOW_CONSTRAINTS_REFERENCE=${DEFAULT_CONSTRAINTS_BRANCH}"
            )
        fi
    fi
    if [[ "${AIRFLOW_CONSTRAINTS_LOCATION}" != "" ]]; then
        EXTRA_DOCKER_PROD_BUILD_FLAGS+=(
            "--build-arg" "AIRFLOW_CONSTRAINTS_LOCATION=${AIRFLOW_CONSTRAINTS_LOCATION}"
        )
    fi
    # Depending on the version built, we choose the right branch for preloading the packages from
    # If we run build for v1-10-test builds we should choose v1-10-test, for v2-0-test we choose v2-0-test
    # all other builds when you choose a specific version (1.0 or 2.0 series) should choose stable branch
    # to preload. For all other builds we use the default branch defined in _initialization.sh
    if [[ ${AIRFLOW_VERSION} == 'v1-10-test' ]]; then
        AIRFLOW_BRANCH_FOR_PYPI_PRELOADING="v1-10-test"
    elif [[ ${AIRFLOW_VERSION} =~ v?1.* ]]; then
        AIRFLOW_BRANCH_FOR_PYPI_PRELOADING="v1-10-stable"
    elif [[ ${AIRFLOW_VERSION} == 'v2-0-test' ]]; then
        AIRFLOW_BRANCH_FOR_PYPI_PRELOADING="v2-0-test"
    elif [[ ${AIRFLOW_VERSION} =~ v?2.* ]]; then
        AIRFLOW_BRANCH_FOR_PYPI_PRELOADING="v2-0-stable"
    else
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

function build_images::confirm_via_terminal() {
    echo >"${DETECTED_TERMINAL}"
    echo >"${DETECTED_TERMINAL}"
    echo "${COLOR_YELLOW_WARNING}Make sure that you rebased to latest master before rebuilding!${COLOR_RESET}" >"${DETECTED_TERMINAL}"
    echo >"${DETECTED_TERMINAL}"
    # Make sure to use output of tty rather than stdin/stdout when available - this way confirm
    # will works also in case of pre-commits (git does not pass stdin/stdout to pre-commit hooks)
    # shellcheck disable=SC2094
    "${AIRFLOW_SOURCES}/confirm" "${ACTION} image ${THE_IMAGE_TYPE}-python${PYTHON_MAJOR_MINOR_VERSION}" \
        <"${DETECTED_TERMINAL}" >"${DETECTED_TERMINAL}"
    RES=$?
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
        # answered in the first pre-commit check (build) and then used in another (pylint/mypy/flake8 etc).
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
        echo
        echo
        echo "${COLOR_YELLOW_WARNING}Make sure that you rebased to latest master before rebuilding!${COLOR_RESET}"
        echo
        # Check if this script is run interactively with stdin open and terminal attached
        "${AIRFLOW_SOURCES}/confirm" "${ACTION} image ${THE_IMAGE_TYPE}-python${PYTHON_MAJOR_MINOR_VERSION}"
        RES=$?
    elif [[ ${DETECTED_TERMINAL:=$(tty)} != "not a tty" ]]; then
        export DETECTED_TERMINAL
        build_images::confirm_via_terminal
    elif [[ -c /dev/tty ]]; then
        export DETECTED_TERMINAL=/dev/tty
        build_images::confirm_via_terminal
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
        echo
        echo  "${COLOR_RED_ERROR} The ${THE_IMAGE_TYPE} needs to be rebuilt - it is outdated.   ${COLOR_RESET}"
        echo """

   Make sure you build the images bu running

      ./breeze --python ${PYTHON_MAJOR_MINOR_VERSION} build-image

   If you run it via pre-commit as individual hook, you can run 'pre-commit run build'.

"""
        exit 1
    else
        # Force "yes" also to subsequent questions
        export FORCE_ANSWER_TO_QUESTIONS="yes"
    fi
}

function build_images::confirm_non-empty-docker-context-files() {
    local num_docker_context_files
    num_docker_context_files=$(find "${AIRFLOW_SOURCES}/docker-context-files/" -type f |\
        grep -c v "README.md" )
    if [[ ${num_docker_context_files} == "0" ]]; then
        if [[ ${INSTALL_FROM_DOCKER_CONTEXT_FILES} == "false" ]]; then
            >&2 echo
            >&2 echo "ERROR! You want to install packages from docker-context-files"
            >&2 echo "       but there are no packages to install in this folder."
            >&2 echo
            exit 1
        fi
    else
        if [[ ${INSTALL_FROM_DOCKER_CONTEXT_FILES} == "false" ]]; then
            >&2 echo
            >&2 echo "ERROR! There are some extra files in docker-context-files except README.md"
            >&2 echo "       And you did not choose --install-from-docker-context-files flag"
            >&2 echo "       This might result in unnecessary cache invalidation and long build times"
            >&2 echo "       Exiting now - please remove those files (except README.md) and retry"
            >&2 echo
            exit 2
        fi
    fi
}

# Builds local image manifest
# It contains only one .json file - result of docker inspect - describing the image
# We cannot use docker registry APIs as they are available only with authorisation
# But this image can be pulled without authentication
function build_images::build_ci_image_manifest() {
    docker build \
        --tag="${AIRFLOW_CI_LOCAL_MANIFEST_IMAGE}" \
        -f- . <<EOF
FROM scratch

COPY "manifests/local-build-cache-hash" /build-cache-hash

CMD ""
EOF
}

#
# Retrieves information about build cache hash random file from the local image
#
function build_images::get_local_build_cache_hash() {

    set +e
    # Remove the container just in case
    docker rm --force "local-airflow-ci-container" 2>/dev/null >/dev/null
    if ! docker create --name "local-airflow-ci-container" "${AIRFLOW_CI_IMAGE}" 2>/dev/null; then
        verbosity::print_info
        verbosity::print_info "Local airflow CI image not available"
        verbosity::print_info
        LOCAL_MANIFEST_IMAGE_UNAVAILABLE="true"
        export LOCAL_MANIFEST_IMAGE_UNAVAILABLE
        touch "${LOCAL_IMAGE_BUILD_CACHE_HASH_FILE}"
        return
    fi
    docker cp "local-airflow-ci-container:/build-cache-hash" \
        "${LOCAL_IMAGE_BUILD_CACHE_HASH_FILE}" 2>/dev/null ||
        touch "${LOCAL_IMAGE_BUILD_CACHE_HASH_FILE}"
    set -e
    verbosity::print_info
    verbosity::print_info "Local build cache hash: '$(cat "${LOCAL_IMAGE_BUILD_CACHE_HASH_FILE}")'"
    verbosity::print_info
}

# Retrieves information about the build cache hash random file from the remote image.
# We actually use manifest image for that, which is a really, really small image to pull!
# The problem is that inspecting information about remote image cannot be done easily with existing APIs
# of Dockerhub because they require additional authentication even for public images.
# Therefore instead we are downloading a specially prepared manifest image
# which is built together with the main image and pushed with it. This special manifest image is prepared
# during building of the main image and contains single file which is randomly built during the docker
# build in the right place in the image (right after installing all dependencies of Apache Airflow
# for the first time). When this random file gets regenerated it means that either base image has
# changed or some of the earlier layers was modified - which means that it is usually faster to pull
# that image first and then rebuild it - because this will likely be faster
function build_images::get_remote_image_build_cache_hash() {
    set +e
    # Pull remote manifest image
    if ! docker pull "${AIRFLOW_CI_REMOTE_MANIFEST_IMAGE}" 2>/dev/null >/dev/null; then
        verbosity::print_info
        verbosity::print_info "Remote docker registry unreachable"
        verbosity::print_info
        REMOTE_DOCKER_REGISTRY_UNREACHABLE="true"
        export REMOTE_DOCKER_REGISTRY_UNREACHABLE
        touch "${REMOTE_IMAGE_BUILD_CACHE_HASH_FILE}"
        return
    fi
    set -e
    rm -f "${REMOTE_IMAGE_CONTAINER_ID_FILE}"
    # Create container dump out of the manifest image without actually running it
    docker create --cidfile "${REMOTE_IMAGE_CONTAINER_ID_FILE}" "${AIRFLOW_CI_REMOTE_MANIFEST_IMAGE}"
    # Extract manifest and store it in local file
    docker cp "$(cat "${REMOTE_IMAGE_CONTAINER_ID_FILE}"):/build-cache-hash" \
        "${REMOTE_IMAGE_BUILD_CACHE_HASH_FILE}"
    docker rm --force "$(cat "${REMOTE_IMAGE_CONTAINER_ID_FILE}")"
    rm -f "${REMOTE_IMAGE_CONTAINER_ID_FILE}"
    verbosity::print_info
    verbosity::print_info "Remote build cache hash: '$(cat "${REMOTE_IMAGE_BUILD_CACHE_HASH_FILE}")'"
    verbosity::print_info
}

# Compares layers from both remote and local image and set FORCE_PULL_IMAGES to true in case
# More than the last NN layers are different.
function build_images::compare_local_and_remote_build_cache_hash() {
    set +e
    local remote_hash
    remote_hash=$(cat "${REMOTE_IMAGE_BUILD_CACHE_HASH_FILE}")
    local local_hash
    local_hash=$(cat "${LOCAL_IMAGE_BUILD_CACHE_HASH_FILE}")

    if [[ ${remote_hash} != "${local_hash}" || ${local_hash} == "" ]] \
        ; then
        echo
        echo
        echo "Your image and the dockerhub have different or missing build cache hashes."
        echo "Local hash: '${local_hash}'. Remote hash: '${remote_hash}'."
        echo
        echo "Forcing pulling the images. It will be faster than rebuilding usually."
        echo "You can avoid it by setting SKIP_CHECK_REMOTE_IMAGE to true"
        echo
        export FORCE_PULL_IMAGES="true"
    else
        echo
        echo "No need to pull the image. Yours and remote cache hashes are the same!"
        echo
    fi
    set -e
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
    export AIRFLOW_CI_BASE_TAG="${BRANCH_NAME}-python${PYTHON_MAJOR_MINOR_VERSION}-ci"
    # CI image to build
    export AIRFLOW_CI_IMAGE="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${AIRFLOW_CI_BASE_TAG}"
    # Default CI image
    export AIRFLOW_CI_PYTHON_IMAGE="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:python${PYTHON_MAJOR_MINOR_VERSION}-${BRANCH_NAME}"
    # CI image to build
    export AIRFLOW_CI_IMAGE="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${AIRFLOW_CI_BASE_TAG}"

    # Base production image tag - used to build kubernetes tag as well
    if [[ ${FORCE_AIRFLOW_PROD_BASE_TAG=} == "" ]]; then
        export AIRFLOW_PROD_BASE_TAG="${BRANCH_NAME}-python${PYTHON_MAJOR_MINOR_VERSION}"
    else
        export AIRFLOW_PROD_BASE_TAG="${FORCE_AIRFLOW_PROD_BASE_TAG}"
    fi

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

    # GitHub Registry names must be lowercase :(
    github_repository_lowercase="$(echo "${GITHUB_REPOSITORY}" | tr '[:upper:]' '[:lower:]')"
    export GITHUB_REGISTRY_AIRFLOW_PROD_IMAGE="${GITHUB_REGISTRY}/${github_repository_lowercase}/${AIRFLOW_PROD_BASE_TAG}"
    export GITHUB_REGISTRY_AIRFLOW_PROD_BUILD_IMAGE="${GITHUB_REGISTRY}/${github_repository_lowercase}/${AIRFLOW_PROD_BASE_TAG}-build"
    export GITHUB_REGISTRY_PYTHON_BASE_IMAGE="${GITHUB_REGISTRY}/${github_repository_lowercase}/python:${PYTHON_BASE_IMAGE_VERSION}-slim-buster"

    export GITHUB_REGISTRY_AIRFLOW_CI_IMAGE="${GITHUB_REGISTRY}/${github_repository_lowercase}/${AIRFLOW_CI_BASE_TAG}"
    export GITHUB_REGISTRY_PYTHON_BASE_IMAGE="${GITHUB_REGISTRY}/${github_repository_lowercase}/python:${PYTHON_BASE_IMAGE_VERSION}-slim-buster"
}

# If GitHub Registry is used, login to the registry using GITHUB_USERNAME and GITHUB_TOKEN
function build_image::login_to_github_registry_if_needed() {
    if [[ ${USE_GITHUB_REGISTRY} == "true" ]]; then
        if [[ -n ${GITHUB_TOKEN=} ]]; then
            echo "${GITHUB_TOKEN}" | docker login \
                --username "${GITHUB_USERNAME:-apache}" \
                --password-stdin \
                "${GITHUB_REGISTRY}"
        fi
    fi

}

# Prepares all variables needed by the CI build. Depending on the configuration used (python version
# DockerHub user etc. the variables are set so that other functions can use those variables.
function build_images::prepare_ci_build() {
    export AIRFLOW_CI_LOCAL_MANIFEST_IMAGE="local/${DOCKERHUB_REPO}:${AIRFLOW_CI_BASE_TAG}-manifest"
    export AIRFLOW_CI_REMOTE_MANIFEST_IMAGE="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${AIRFLOW_CI_BASE_TAG}-manifest"
    export THE_IMAGE_TYPE="CI"
    export IMAGE_DESCRIPTION="Airflow CI"

    # Those constants depend on the type of image run so they are only made constants here
    export AIRFLOW_EXTRAS="${AIRFLOW_EXTRAS:="${DEFAULT_CI_EXTRAS}"}"
    readonly AIRFLOW_EXTRAS

    export AIRFLOW_IMAGE="${AIRFLOW_CI_IMAGE}"
    readonly AIRFLOW_IMAGE

    build_image::login_to_github_registry_if_needed
    sanity_checks::go_to_airflow_sources
    permissions::fix_group_permissions
}

# Only rebuilds CI image if needed. It checks if the docker image build is needed
# because any of the important source files (from scripts/ci/libraries/_initialization.sh) has
# changed or in any of the edge cases (docker image removed, .build cache removed etc.
# In case rebuild is needed, it determines (by comparing layers in local and remote image)
# Whether pull is needed before rebuild.
function build_images::rebuild_ci_image_if_needed() {
    verbosity::print_info
    verbosity::print_info "Checking if pull or just build for ${THE_IMAGE_TYPE} is needed."
    verbosity::print_info
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
    build_images::get_local_build_cache_hash
    if [[ ${needs_docker_build} == "true" ]]; then
        if [[ ${SKIP_CHECK_REMOTE_IMAGE:=} != "true" && ${DOCKER_CACHE} == "pulled" ]]; then
            # Check if remote image is different enough to force pull
            # This is an optimisation pull vs. build time. When there
            # are enough changes (specifically after setup.py changes) it is faster to pull
            # and build the image rather than just build it
            echo
            echo "Checking if the remote image needs to be pulled"
            echo
            build_images::get_remote_image_build_cache_hash
            if [[ ${REMOTE_DOCKER_REGISTRY_UNREACHABLE:=} != "true" && ${LOCAL_MANIFEST_IMAGE_UNAVAILABLE:=} != "true" ]]; then
                build_images::compare_local_and_remote_build_cache_hash
            else
                FORCE_PULL_IMAGES="true"
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
            build_images::get_local_build_cache_hash
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
        echo
        echo  "${COLOR_RED_ERROR} The ${DOCKER_CACHE} cache is unknown!  ${COLOR_RESET}"
        echo
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
        --build-arg INSTALL_FROM_PYPI="${INSTALL_FROM_PYPI}" \
        --build-arg INSTALL_FROM_DOCKER_CONTEXT_FILES="${INSTALL_FROM_DOCKER_CONTEXT_FILES}" \
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
            "--build-arg" "AIRFLOW_INSTALLATION_METHOD=https://github.com/apache/airflow/archive/${INSTALL_AIRFLOW_REFERENCE}.tar.gz#egg=apache-airflow"
        )
        export AIRFLOW_VERSION="${INSTALL_AIRFLOW_REFERENCE}"
        build_images::add_build_args_for_remote_install
    elif [[ -n "${INSTALL_AIRFLOW_VERSION=}" ]]; then
        # When --install-airflow-version is used then the image is build from PIP package
        EXTRA_DOCKER_PROD_BUILD_FLAGS=(
            "--build-arg" "AIRFLOW_INSTALLATION_METHOD=apache-airflow"
            "--build-arg" "AIRFLOW_INSTALL_VERSION===${INSTALL_AIRFLOW_VERSION}"
            "--build-arg" "AIRFLOW_VERSION=${INSTALL_AIRFLOW_VERSION}"
        )
        export AIRFLOW_VERSION="${INSTALL_AIRFLOW_VERSION}"
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

    build_image::login_to_github_registry_if_needed

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
        echo
        echo  "${COLOR_RED_ERROR} The ${DOCKER_CACHE} cache is unknown  ${COLOR_RESET}"
        echo
        echo
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
        --build-arg INSTALL_FROM_PYPI="${INSTALL_FROM_PYPI}" \
        --build-arg INSTALL_FROM_DOCKER_CONTEXT_FILES="${INSTALL_FROM_DOCKER_CONTEXT_FILES}" \
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
        --build-arg INSTALL_FROM_PYPI="${INSTALL_FROM_PYPI}" \
        --build-arg INSTALL_FROM_DOCKER_CONTEXT_FILES="${INSTALL_FROM_DOCKER_CONTEXT_FILES}" \
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


function build_images::build_prod_images_from_packages() {
    # Cleanup dist and docker-context-files folders
    mkdir -pv "${AIRFLOW_SOURCES}/dist"
    mkdir -pv "${AIRFLOW_SOURCES}/docker-context-files"
    rm -f "${AIRFLOW_SOURCES}/dist/"*.{whl,tar.gz}
    rm -f "${AIRFLOW_SOURCES}/docker-context-files/"*.{whl,tar.gz}

    pip_download_command="pip download -d /dist '.[${INSTALLED_EXTRAS}]' --constraint 'https://raw.githubusercontent.com/apache/airflow/${DEFAULT_CONSTRAINTS_BRANCH}/constraints-${PYTHON_MAJOR_MINOR_VERSION}.txt'"

    # Download all dependencies needed
    docker run --rm --entrypoint /bin/bash \
        "${EXTRA_DOCKER_FLAGS[@]}" \
        "${AIRFLOW_CI_IMAGE}" -c "${pip_download_command}"

    # Remove all downloaded apache airflow packages
    rm -f "${AIRFLOW_SOURCES}/dist/"apache_airflow*.whl
    rm -f "${AIRFLOW_SOURCES}/dist/"apache-airflow*.tar.gz

    # Remove all downloaded apache airflow packages
    mv -f "${AIRFLOW_SOURCES}/dist/"* "${AIRFLOW_SOURCES}/docker-context-files/"

    # Build necessary provider packages
    runs::run_prepare_provider_packages "${INSTALLED_PROVIDERS[@]}"

    mv "${AIRFLOW_SOURCES}/dist/"* "${AIRFLOW_SOURCES}/docker-context-files/"

    # Build apache airflow packages
    build_airflow_packages::build_airflow_packages

    mv "${AIRFLOW_SOURCES}/dist/"* "${AIRFLOW_SOURCES}/docker-context-files/"
    build_images::build_prod_images
}

# Useful information for people who stumble upon a pip check failure
function build_images::inform_about_pip_check() {
        echo """
${COLOR_BLUE}***** Beginning of the instructions ****${COLOR_RESET}

The image did not pass 'pip check' verification. This means that there are some conflicting dependencies
in the image.

It can mean one of those:

1) The master is currently broken (other PRs will fail with the same error)
2) You changed some dependencies in setup.py or setup.cfg and they are conflicting.


In case 1) - apologies for the trouble.Please let committers know and they will fix it. You might
be asked to rebase to the latest master after the problem is fixed.

In case 2) - Follow the steps below:

* consult the committers if you are unsure what to do. Just comment in the PR that you need help, if you do,
  but try to follow those instructions first!

* ask the committer to set 'upgrade to newer dependencies'. All dependencies in your PR will be updated
  to latest 'good' versions and you will be able to check if they are not conflicting.

* run locally the image that is failing with Breeze:

    ./breeze ${1}--github-image-id ${GITHUB_REGISTRY_PULL_IMAGE_TAG} --backend ${BACKEND} --python ${PYTHON_MAJOR_MINOR_VERSION}

* your setup.py and setup.cfg will be mounted to the container. You will be able to iterate with
  different setup.py versions.

* in container your can run 'pipdeptree' to figure out where the dependency conflict comes from.

* Some useful commands that can help yoy to find out dependencies you have:

     * 'pipdeptree | less' (you can then search through the dependencies with vim-like shortcuts)

     * 'pipdeptree > /files/pipdeptree.txt' - this will produce a pipdeptree.txt file in your source
       'files' directory and you can open it in editor of your choice,

     * 'pipdeptree | grep YOUR_DEPENDENCY' - to see all the requirements your dependency has as specified
       by other packages

* figure out which dependency limits should be upgraded. Upgrade them in corresponding setup.py extras
  and run pip to upgrade your dependencies accordingly:

     pip install '.[all]' --upgrade --upgrade-strategy eager

* run pip check to figure out if the dependencies have been fixed. It should let you know which dependencies
  are conflicting or (hurray!) if there are no conflicts:

     pip check

* in some, rare, cases, pip will not limit the requirement in case you specify it in extras, you might
  need to add such requirement in 'install_requires' section of setup.cfg instead of extras in setup.py.

* iterate until all such dependency conflicts are fixed.

${COLOR_BLUE}***** End of the instructions ****${COLOR_RESET}

"""
}
