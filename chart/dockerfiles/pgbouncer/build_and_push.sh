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
set -euo pipefail
DOCKERHUB_USER=${DOCKERHUB_USER:="apache"}
readonly DOCKERHUB_USER

DOCKERHUB_REPO=${DOCKERHUB_REPO:="airflow"}
readonly DOCKERHUB_REPO

PGBOUNCER_VERSION="1.22.1"
readonly PGBOUNCER_VERSION

PGBOUNCER_SHA256="2b018aa6ce7f592c9892bb9e0fd90262484eb73937fd2af929770a45373ba215"
readonly PGBOUNCER_SHA256

AIRFLOW_PGBOUNCER_VERSION="2024.09.19"
readonly AIRFLOW_PGBOUNCER_VERSION

COMMIT_SHA=$(git rev-parse HEAD)
readonly COMMIT_SHA

TAG="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:airflow-pgbouncer-${AIRFLOW_PGBOUNCER_VERSION}-${PGBOUNCER_VERSION}"
readonly TAG

function center_text() {
    columns=$(tput cols || echo 80)
    printf "%*s\n" $(( (${#1} + columns) / 2)) "$1"
}

cd "$( dirname "${BASH_SOURCE[0]}" )" || exit 1

center_text "Building image"

# Note, you need buildx and qemu installed for your docker. They come pre-installed with docker-desktop, but
# as described in:
# * https://docs.docker.com/build/install-buildx/
# * https://docs.docker.com/build/building/multi-platform/
# You can also install them easily on all docker-based systems
# You might also need to create a different builder to build multi-platform images
# For example by running `docker buildx create --use`

docker buildx build . \
    --platform linux/amd64,linux/arm64 \
    --pull \
    --push \
    --build-arg "PGBOUNCER_VERSION=${PGBOUNCER_VERSION}" \
    --build-arg "AIRFLOW_PGBOUNCER_VERSION=${AIRFLOW_PGBOUNCER_VERSION}"\
    --build-arg "PGBOUNCER_SHA256=${PGBOUNCER_SHA256}"\
    --build-arg "COMMIT_SHA=${COMMIT_SHA}" \
    --tag "${TAG}"

center_text "Checking image"

docker run --rm "${TAG}" pgbouncer --version
