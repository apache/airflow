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

rebuild_ci_image_if_needed

# Test environment
export BACKEND=${BACKEND:="sqlite"}
export ENV=${ENV:="docker"}
export KUBERNETES_MODE=${KUBERNETES_MODE:="git_mode"}

# Whether local sources are mounted to docker
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
elif [[ "${ENV}" == "kubernetes" ]]; then
  echo
  echo "Running kubernetes tests in ${KUBERNETES_MODE}"
  echo
  docker-compose --log-level ERROR \
      -f "${MY_DIR}/docker-compose.yml" \
      -f "${MY_DIR}/docker-compose-${BACKEND}.yml" \
      "${DOCKER_COMPOSE_LOCAL[@]}" \
         run airflow-testing /opt/airflow/scripts/ci/in_container/entrypoint_ci.sh;
  echo
  echo "Finished Running kubernetes tests in ${KUBERNETES_MODE}"
  echo
elif [[ "${ENV}" == "bare" ]]; then
  docker-compose --log-level INFO \
      -f "${MY_DIR}/docker-compose.yml" \
      -f "${MY_DIR}/docker-compose-${BACKEND}.yml" \
      "${DOCKER_COMPOSE_LOCAL[@]}" \
        run --no-deps airflow-testing /opt/airflow/scripts/ci/in_container/entrypoint_ci.sh;
else
    echo >&2
    echo >&2 "ERROR:  The ENV variable should be one of [docker, kubernetes, bare] and is '${ENV}'"
    echo >&2
fi
set -u

script_end
