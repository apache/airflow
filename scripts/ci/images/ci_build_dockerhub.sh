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

# This is hook build used by DockerHub. We are also using it
# on CI to potentially rebuild (and refresh layers that
# are not cached) Docker images that are used to run CI jobs
export FORCE_ANSWER_TO_QUESTIONS="yes"
export VERBOSE_COMMANDS="true"

: "${DOCKER_REPO:?"ERROR: Please specify DOCKER_REPO variable following the pattern HOST/DOCKERHUB_USER/DOCKERHUB_REPO"}"

echo "DOCKER_REPO=${DOCKER_REPO}"

[[ ${DOCKER_REPO:=} =~ [^/]*/([^/]*)/([^/]*) ]] && \
    export DOCKERHUB_USER=${BASH_REMATCH[1]} &&
    export DOCKERHUB_REPO=${BASH_REMATCH[2]}

echo
echo "DOCKERHUB_USER=${DOCKERHUB_USER}"
echo "DOCKERHUB_REPO=${DOCKERHUB_REPO}"
echo

: "${DOCKER_TAG:?"ERROR: Please specify DOCKER_TAG variable following the pattern BRANCH-pythonX.Y[-ci]"}"

echo "DOCKER_TAG=${DOCKER_TAG}"


[[ ${DOCKER_TAG:=} =~ .*-python([0-9.]*)(.*) ]] && export PYTHON_MAJOR_MINOR_VERSION=${BASH_REMATCH[1]}

: "${PYTHON_MAJOR_MINOR_VERSION:?"The tag '${DOCKER_TAG}' should follow the pattern .*-pythonX.Y[-ci]"}"

echo "Detected PYTHON_MAJOR_MINOR_VERSION=${PYTHON_MAJOR_MINOR_VERSION}"
echo

# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"

if [[ ${DOCKER_TAG} == *python*-ci ]]; then
    echo
    echo "Building CI image"
    echo
    rm -rf "${BUILD_CACHE_DIR}"
    build_images::prepare_ci_build
    build_images::rebuild_ci_image_if_needed
    push_pull_remove_images::push_ci_images
elif [[ ${DOCKER_TAG} == *python* ]]; then
    echo
    echo "Building prod image"
    echo
    rm -rf "${BUILD_CACHE_DIR}"
    build_images::prepare_prod_build
    build_images::build_prod_images
    push_pull_remove_images::push_prod_images
else
    echo
    echo "Skipping the build in Dockerhub. The tag is not good: ${DOCKER_TAG}"
    echo
fi
