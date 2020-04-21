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

if [[ -z ${DOCKER_REPO} ]]; then
   echo
   echo "Error! Missing DOCKER_REPO environment variable"
   echo "Please specify DOCKER_REPO variable following the pattern HOST/DOCKERHUB_USER/DOCKERHUB_REPO"
   echo
   exit 1
else
   echo "DOCKER_REPO=${DOCKER_REPO}"
fi

[[ ${DOCKER_REPO:=} =~ [^/]*/([^/]*)/([^/]*) ]] && \
    export DOCKERHUB_USER=${BASH_REMATCH[1]} &&
    export DOCKERHUB_REPO=${BASH_REMATCH[2]}

echo
echo "DOCKERHUB_USER=${DOCKERHUB_USER}"
echo "DOCKERHUB_REPO=${DOCKERHUB_REPO}"
echo

if [[ -z ${DOCKER_TAG:=} ]]; then
   echo
   echo "Error! Missing DOCKER_TAG environment variable"
   echo "Please specify DOCKER_TAG variable following the pattern BRANCH-pythonX.Y[-ci]"
   echo
   exit 1
else
   echo "DOCKER_TAG=${DOCKER_TAG}"
fi

[[ ${DOCKER_TAG:=} =~ .*-python([0-9.]*)(.*) ]] && export PYTHON_MAJOR_MINOR_VERSION=${BASH_REMATCH[1]}

if [[ -z ${PYTHON_MAJOR_MINOR_VERSION:=} ]]; then
    echo
    echo "Error! Wrong DOCKER_TAG"
    echo "The tag '${DOCKER_TAG}' should follow the pattern .*-pythonX.Y[-ci]"
    echo
    exit 1
fi

echo "Detected PYTHON_MAJOR_MINOR_VERSION=${PYTHON_MAJOR_MINOR_VERSION}"
echo

# shellcheck source=scripts/ci/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/_script_init.sh"

DOCKER_IMAGE_SPEC="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${DOCKER_TAG}"

IMAGE_CREATED_AT=$(docker image ls "${DOCKER_IMAGE_SPEC}" --format '{{ .CreatedAt }}' | tr -d "[:upper:]")

if [[ -z ${IMAGE_CREATED_AT} ]]; then
    echo
    echo "Image never built. Rebuilding it."
    echo
else
    echo "Image created at ${IMAGE_CREATED_AT}"

    EPOCH_SECONDS_WHEN_IMAGE_CREATED=$(date --date "${IMAGE_CREATED_AT}" +"%s")

    echo "EPOCH seconds for when image created = ${EPOCH_SECONDS_WHEN_IMAGE_CREATED}"
    EPOCH_SECONDS_NOW=$(date +"%s")
    echo "EPOCH seconds now = ${EPOCH_SECONDS_NOW}"

    SECONDS_SINCE_CREATED=$((EPOCH_SECONDS_NOW - EPOCH_SECONDS_WHEN_IMAGE_CREATED))
    echo "Seconds since created = ${SECONDS_SINCE_CREATED}"
    echo "Hours since created = $((SECONDS_SINCE_CREATED / 3600 ))"

    if (( SECONDS_SINCE_CREATED > 3600 * 10 )) ; then
        echo
        echo "The image ${DOCKER_IMAGE_SPEC} is older than 10 hours (Created at ${IMAGE_CREATED_AT})."
        echo "Rebuilding the image."
        echo
    else
        echo
        echo "The image ${DOCKER_IMAGE_SPEC} is created recently (Created ${IMAGE_CREATED_AT})."
        echo "Skipping rebuilding the image."
        echo
        exit
    fi
fi

if [[ ${DOCKER_TAG} == *python*-ci ]]; then
    echo
    echo "Building CI image"
    echo
    rm -rf "${BUILD_CACHE_DIR}"
    prepare_ci_build
    rebuild_ci_image_if_needed
    push_ci_image
elif [[ ${DOCKER_TAG} == *python* ]]; then
    echo
    echo "Building prod image"
    echo
    rm -rf "${BUILD_CACHE_DIR}"
    prepare_prod_build
    build_prod_image
    push_prod_images
else
    echo
    echo "Skipping the build in Dockerhub. The tag is not good: ${DOCKER_TAG}"
    echo
fi
