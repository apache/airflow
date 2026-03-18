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

AIRFLOW_KRB5KDCSERVER_VERSION="2021.07.04"
readonly AIRFLOW_KRB5KDCSERVER_VERSION

COMMIT_SHA=$(git rev-parse HEAD)
readonly COMMIT_SHA

cd "$( dirname "${BASH_SOURCE[0]}" )" || exit 1

TAG="ghcr.io/${GITHUB_REPOSITORY}-krb5-kdc-server:${AIRFLOW_KRB5KDCSERVER_VERSION}"
readonly TAG

docker build . \
    --pull \
    --build-arg "AIRFLOW_KRB5KDCSERVER_VERSION=${AIRFLOW_KRB5KDCSERVER_VERSION}" \
    --build-arg "COMMIT_SHA=${COMMIT_SHA}" \
    --label "org.opencontainers.image.source=https://github.com/${GITHUB_REPOSITORY}" \
    --tag "${TAG}"

docker push "${TAG}"
