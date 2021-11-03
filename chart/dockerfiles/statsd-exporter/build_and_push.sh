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

STATSD_VERSION="v0.17.0"
readonly STATSD_VERSION

AIRFLOW_STATSD_EXPORTER_VERSION="2021.04.28"
readonly AIRFLOW_STATSD_EXPORTER_VERSION

COMMIT_SHA=$(git rev-parse HEAD)
readonly COMMIT_SHA

TAG="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:airflow-statsd-exporter-${AIRFLOW_STATSD_EXPORTER_VERSION}-${STATSD_VERSION}"
readonly TAG

function center_text() {
    columns=$(tput cols || echo 80)
    printf "%*s\n" $(( (${#1} + columns) / 2)) "$1"
}

cd "$( dirname "${BASH_SOURCE[0]}" )" || exit 1

center_text "Building image"

docker build . \
    --pull \
    --build-arg "STATSD_VERSION=${STATSD_VERSION}" \
    --build-arg "AIRFLOW_STATSD_EXPORTER_VERSION=${AIRFLOW_STATSD_EXPORTER_VERSION}" \
    --build-arg "COMMIT_SHA=${COMMIT_SHA}" \
    --tag "${TAG}"

center_text "Checking image"

docker run --rm "${TAG}" --version

echo Image labels:
docker inspect "${TAG}" --format '{{ json .ContainerConfig.Labels }}' | python3 -m json.tool

center_text "Pushing image"

docker push "${TAG}"
