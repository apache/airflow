#!/usr/bin/env bash

#
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

set -euo pipefail
MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# shellcheck source=./_utils.sh
. "${MY_DIR}/_utils.sh"

export VERBOSE=${VERBOSE:="true"}

basic_sanity_checks

script_start

rebuild_image_if_needed_for_tests

export BACKEND=${BACKEND:="sqlite"}
export ENV=${ENV:="docker"}
export MOUNT_LOCAL_SOURCES=${MOUNT_LOCAL_SOURCES:="false"}
export WEBSERVER_HOST_PORT=${WEBSERVER_HOST_PORT:="8080"}
export AIRFLOW_CI_VERBOSE=${VERBOSE}
export PYTHONDONTWRITEBYTECODE="true"

if [[ ${MOUNT_LOCAL_SOURCES} == "true" ]]; then
    DOCKER_COMPOSE_LOCAL=("-f" "${MY_DIR}/docker-compose-local.yml")
else
    DOCKER_COMPOSE_LOCAL=()
fi

export AIRFLOW_CONTAINER_DOCKER_IMAGE=\
${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${AIRFLOW_CONTAINER_BRANCH_NAME}-python${PYTHON_VERSION}-ci

echo
echo "Using docker image: ${AIRFLOW_CONTAINER_DOCKER_IMAGE} for docker compose runs"
echo

HOST_USER_ID="$(id -ur)"
export HOST_USER_ID

HOST_GROUP_ID="$(id -gr)"
export HOST_GROUP_ID

set +u
if [[ "${ENV}" == "docker" ]]; then
  docker-compose --log-level INFO \
      -f "${MY_DIR}/docker-compose.yml" \
      -f "${MY_DIR}/docker-compose-${BACKEND}.yml" \
      "${DOCKER_COMPOSE_LOCAL[@]}" \
        run airflow-testing /opt/airflow/scripts/ci/in_container/entrypoint_ci.sh;
else
  "${MY_DIR}/kubernetes/minikube/stop_minikube.sh" && "${MY_DIR}/kubernetes/setup_kubernetes.sh" && \
    "${MY_DIR}/kubernetes/kube/deploy.sh" -d persistent_mode
  MINIKUBE_IP=$(minikube ip)
  export MINIKUBE_IP
  docker-compose --log-level ERROR \
      -f "${MY_DIR}/docker-compose.yml" \
      -f "${MY_DIR}/docker-compose-${BACKEND}.yml" \
      -f "${MY_DIR}/docker-compose-kubernetes.yml" \
      "${DOCKER_COMPOSE_LOCAL[@]}" \
         run --no-deps airflow-testing /opt/airflow/scripts/ci/in_container/entrypoint_ci.sh;
  set +x
  "${MY_DIR}/kubernetes/minikube/stop_minikube.sh"

  "${MY_DIR}/kubernetes/minikube/stop_minikube.sh" && "${MY_DIR}/kubernetes/setup_kubernetes.sh" && \
    "${MY_DIR}/kubernetes/kube/deploy.sh" -d git_mode
  MINIKUBE_IP=$(minikube ip)
  export MINIKUBE_IP
  docker-compose --log-level ERROR \
      -f "${MY_DIR}/docker-compose.yml" \
      -f "${MY_DIR}/docker-compose-${BACKEND}.yml" \
      -f "${MY_DIR}/docker-compose-kubernetes.yml" \
      "${DOCKER_COMPOSE_LOCAL[@]}" \
         run --no-deps airflow-testing /opt/airflow/scripts/ci/in_container/entrypoint_ci.sh;
  "${MY_DIR}/kubernetes/minikube/stop_minikube.sh"
fi
set -u

script_end
