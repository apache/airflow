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

export VERBOSE=${VERBOSE:="true"}

AIRFLOW_SOURCES="$(cd "${MY_DIR}"/../../ && pwd )"
export AIRFLOW_SOURCES

# shellcheck source=scripts/ci/utils/_include_all.sh
. "${MY_DIR}/utils/_include_all.sh"

PYTHON_VERSION=${PYTHON_VERSION:=${DEFAULT_PYTHON_VERSION}}

script_start

initialize_environment

prepare_build

prepare_run

export FORCE_ANSWER_TO_QUESTIONS="yes"
rebuild_ci_image_if_needed

# Test environment
export BACKEND=${BACKEND:="sqlite"}

# Whether necessary for airflow run local sources are mounted to docker
export MOUNT_HOST_VOLUMES=${MOUNT_HOST_VOLUMES:="false"}

# whethere verbose output should be produced
export AIRFLOW_CI_VERBOSE=${VERBOSE}

# opposite - whether diagnostict messages should be silenced
export AIRFLOW_CI_SILENT=${AIRFLOW_CI_SILENT:="true"}

if [[ ${MOUNT_HOST_VOLUMES} == "true" ]]; then
    DOCKER_COMPOSE_LOCAL=("-f" "${MY_DIR}/docker-compose-local.yml")
else
    DOCKER_COMPOSE_LOCAL=()
fi

HOST_USER_ID="$(id -ur)"
export HOST_USER_ID

HOST_GROUP_ID="$(id -gr)"
export HOST_GROUP_ID

if [[ ${START_KUBERNETES_CLUSTER} == "true" ]]; then
    export KUBERNETES_MODE=${KUBERNETES_MODE:="git_mode"}
    export KUBERNETES_VERSION=${KUBERNETES_VERSION:="v1.15.3"}

    _build_and_save_kubernetes_image

    set +u
    # shellcheck disable=SC2016
    docker-compose --log-level INFO \
      -f "${MY_DIR}/docker-compose.yml" \
      -f "${MY_DIR}/docker-compose-${BACKEND}.yml" \
      -f "${MY_DIR}/docker-compose-kubernetes.yml" \
      "${DOCKER_COMPOSE_LOCAL[@]}" \
         run airflow-testing \
           '/opt/airflow/scripts/ci/in_container/entrypoint_ci.sh "${@}"' \
           /opt/airflow/scripts/ci/in_container/entrypoint_ci.sh "${@}"
         # Note the command is there twice (!) because it is passed via bash -c
         # and bash -c starts passing parameters from $0
    set -u
else
    set +u
    # shellcheck disable=SC2016
    docker-compose --log-level INFO \
      -f "${MY_DIR}/docker-compose.yml" \
      -f "${MY_DIR}/docker-compose-${BACKEND}.yml" \
      "${DOCKER_COMPOSE_LOCAL[@]}" \
         run airflow-testing \
           '/opt/airflow/scripts/ci/in_container/entrypoint_ci.sh "${@}"' \
           /opt/airflow/scripts/ci/in_container/entrypoint_ci.sh "${@}"
         # Note the command is there twice (!) because it is passed via bash -c
         # and bash -c starts passing parameters from $0
    set -u
fi

script_end
