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
MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# shellcheck source=scripts/ci/_utils.sh
. "${MY_DIR}/_utils.sh"

export VERBOSE=${VERBOSE:="true"}

basic_sanity_checks

script_start

if [[ -f ${BUILD_CACHE_DIR}/.skip_tests ]]; then
    echo
    echo "Skipping running tests !!!!!"
    echo
    script_end
    exit
fi

rebuild_ci_image_if_needed

# Test environment
export BACKEND=${BACKEND:="sqlite"}

# Whether necessary for airflow run local sources are mounted to docker
export MOUNT_LOCAL_SOURCES=${MOUNT_LOCAL_SOURCES:="false"}

# whethere verbose output should be produced
export AIRFLOW_CI_VERBOSE=${VERBOSE}

# opposite - whether diagnostict messages should be silenced
export AIRFLOW_CI_SILENT=${AIRFLOW_CI_SILENT:="true"}

if [[ ${MOUNT_LOCAL_SOURCES} == "true" ]]; then
    DOCKER_COMPOSE_LOCAL=("-f" "${MY_DIR}/docker-compose-local.yml")
else
    DOCKER_COMPOSE_LOCAL=()
fi

export AIRFLOW_CI_IMAGE=${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${AIRFLOW_CONTAINER_BRANCH_NAME}-python${PYTHON_VERSION}-ci
export AIRFLOW_CI_KUBERNETES_IMAGE=${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${AIRFLOW_CONTAINER_BRANCH_NAME}-python${PYTHON_VERSION}-ci-kubernetes

echo
echo "Using docker image: ${AIRFLOW_CI_IMAGE} for docker compose runs"
echo "Using Kubernetes docker image: ${AIRFLOW_CI_KUBERNETES_IMAGE} for kubernetes images"
echo

HOST_USER_ID="$(id -ur)"
export HOST_USER_ID

HOST_GROUP_ID="$(id -gr)"
export HOST_GROUP_ID

if [[ ${START_KUBERNETES_CLUSTER} == "true" ]]; then
    export KUBERNETES_MODE=${KUBERNETES_MODE:="git_mode"}
    export KUBERNETES_VERSION=${KUBERNETES_VERSION:="v1.15.3"}

    build_and_save_kubernetes_image

    set +u
    docker-compose --log-level INFO \
      -f "${MY_DIR}/docker-compose.yml" \
      -f "${MY_DIR}/docker-compose-${BACKEND}.yml" \
      -f "${MY_DIR}/docker-compose-kubernetes.yml" \
      "${DOCKER_COMPOSE_LOCAL[@]}" \
         run airflow-testing /opt/airflow/scripts/ci/in_container/entrypoint_ci.sh;
    set -u
else
    set +u
    docker-compose --log-level INFO \
      -f "${MY_DIR}/docker-compose.yml" \
      -f "${MY_DIR}/docker-compose-${BACKEND}.yml" \
      "${DOCKER_COMPOSE_LOCAL[@]}" \
         run airflow-testing /opt/airflow/scripts/ci/in_container/entrypoint_ci.sh;
    set -u
fi

script_end
