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
GITHUB_REPOSITORY=${GITHUB_REPOSITORY:="apache/airflow"}
readonly GITHUB_REPOSITORY

APACHERAT_VERSION="0.17"
readonly APACHERAT_VERSION

AIRFLOW_APACHERAT_VERSION="2025.10.24"
readonly AIRFLOW_APACHERAT_VERSION

COMMIT_SHA=$(git rev-parse HEAD)
readonly COMMIT_SHA

cd "$( dirname "${BASH_SOURCE[0]}" )" || exit 1

TAG="ghcr.io/${GITHUB_REPOSITORY}-apache-rat:${APACHERAT_VERSION}-${AIRFLOW_APACHERAT_VERSION}"
readonly TAG

docker buildx build . \
    --pull \
    --builder "airflow_cache" \
    --build-arg "APACHERAT_VERSION=${APACHERAT_VERSION}" \
    --build-arg "AIRFLOW_APACHERAT_VERSION=${AIRFLOW_APACHERAT_VERSION}" \
    --build-arg "COMMIT_SHA=${COMMIT_SHA}" \
    --label "org.opencontainers.image.source=https://github.com/${GITHUB_REPOSITORY}" \
    --platform "linux/amd64,linux/arm64" \
    --tag "${TAG}" \
    --push
