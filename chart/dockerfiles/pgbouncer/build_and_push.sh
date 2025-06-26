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

# Sometimes the pgbouncer tag does not reliably correspond with the version name
# For example, it may have a `-fixed` suffix
PGBOUNCER_TAG="pgbouncer_1_23_1-fixed"
readonly PGBOUNCER_TAG

PGBOUNCER_VERSION="1.23.1"
readonly PGBOUNCER_VERSION

PGBOUNCER_SHA256="1963b497231d9a560a62d266e4a2eae6881ab401853d93e5d292c3740eec5084"
readonly PGBOUNCER_SHA256

AIRFLOW_PGBOUNCER_VERSION="2025.03.05"
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
    --build-arg "PGBOUNCER_TAG=${PGBOUNCER_TAG}" \
    --build-arg "PGBOUNCER_VERSION=${PGBOUNCER_VERSION}" \
    --build-arg "AIRFLOW_PGBOUNCER_VERSION=${AIRFLOW_PGBOUNCER_VERSION}"\
    --build-arg "PGBOUNCER_SHA256=${PGBOUNCER_SHA256}"\
    --build-arg "COMMIT_SHA=${COMMIT_SHA}" \
    --tag "${TAG}"

center_text "Checking image"

docker run --rm "${TAG}" pgbouncer --version
