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
DOCKERHUB_REPO=${DOCKERHUB_REPO:="airflow"}
readonly DOCKERHUB_USER
readonly DOCKERHUB_REPO
STRESS_VERSION="1.0.4"
readonly STRESS_VERSION

AIRFLOW_STRESS_VERSION="2020.07.10"
readonly AIRFLOW_STRESS_VERSION

COMMIT_SHA=$(git rev-parse HEAD)
readonly COMMIT_SHA

cd "$( dirname "${BASH_SOURCE[0]}" )" || exit 1

TAG="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:stress-${AIRFLOW_STRESS_VERSION}-${STRESS_VERSION}"
readonly TAG

docker build . \
    --pull \
    --build-arg "STRESS_VERSION=${STRESS_VERSION}" \
    --build-arg "AIRFLOW_STRESS_VERSION=${AIRFLOW_STRESS_VERSION}" \
    --build-arg "COMMIT_SHA=${COMMIT_SHA}" \
    --tag "${TAG}"

docker push "${TAG}"
