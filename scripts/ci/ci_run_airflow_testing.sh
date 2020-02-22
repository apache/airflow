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
export VERBOSE=${VERBOSE:="true"}

# shellcheck source=scripts/ci/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/_script_init.sh"

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

# opposite - whether diagnostic messages should be silenced
export AIRFLOW_CI_SILENT=${AIRFLOW_CI_SILENT:="true"}

# Forwards host credentials to the container
export FORWARD_CREDENTIALS=${FORWARD_CREDENTIALS:="false"}

# Installs different airflow version than current from the sources
export INSTALL_AIRFLOW_VERSION=${INSTALL_AIRFLOW_VERSION:="current"}

if [[ ${MOUNT_LOCAL_SOURCES} == "true" ]]; then
    DOCKER_COMPOSE_LOCAL=("-f" "${MY_DIR}/docker-compose/local.yml")
else
    DOCKER_COMPOSE_LOCAL=()
fi

if [[ ${FORWARD_CREDENTIALS} == "true" ]]; then
    DOCKER_COMPOSE_LOCAL+=("-f" "${MY_DIR}/docker-compose/forward-credentials.yml")
fi

if [[ ${INSTALL_AIRFLOW_VERSION} != "current" ]]; then
    DOCKER_COMPOSE_LOCAL+=("-f" "${MY_DIR}/docker-compose/remove-sources.yml")
fi

echo
echo "Using docker image: ${AIRFLOW_CI_IMAGE} for docker compose runs"
echo

INTEGRATIONS=()

ENABLED_INTEGRATIONS=${ENABLED_INTEGRATIONS:=""}

for _INT in ${ENABLED_INTEGRATIONS}
do
    INTEGRATIONS+=("-f")
    INTEGRATIONS+=("${MY_DIR}/docker-compose/integration-${_INT}.yml")
done

RUN_INTEGRATION_TESTS=${RUN_INTEGRATION_TESTS:=""}

if [[ ${RUNTIME:=} == "kubernetes" ]]; then
    export KUBERNETES_MODE=${KUBERNETES_MODE:="git_mode"}
    export KUBERNETES_VERSION=${KUBERNETES_VERSION:="v1.15.3"}

    set +u
    # shellcheck disable=SC2016
    docker-compose --log-level INFO \
      -f "${MY_DIR}/docker-compose/base.yml" \
      -f "${MY_DIR}/docker-compose/backend-${BACKEND}.yml" \
      -f "${MY_DIR}/docker-compose/runtime-kubernetes.yml" \
      "${INTEGRATIONS[@]}" \
      "${DOCKER_COMPOSE_LOCAL[@]}" \
         run airflow-testing \
           '/opt/airflow/scripts/ci/in_container/entrypoint_ci.sh "${@}"' \
           /opt/airflow/scripts/ci/in_container/entrypoint_ci.sh "${@}"
         # Note the command is there twice (!) because it is passed via bash -c
         # and bash -c starts passing parameters from $0. TODO: fixme
    set -u
else
    set +u
    # shellcheck disable=SC2016
    docker-compose --log-level INFO \
      -f "${MY_DIR}/docker-compose/base.yml" \
      -f "${MY_DIR}/docker-compose/backend-${BACKEND}.yml" \
      "${INTEGRATIONS[@]}" \
      "${DOCKER_COMPOSE_LOCAL[@]}" \
         run airflow-testing \
           '/opt/airflow/scripts/ci/in_container/entrypoint_ci.sh "${@}"' \
           /opt/airflow/scripts/ci/in_container/entrypoint_ci.sh "${@}"
         # Note the command is there twice (!) because it is passed via bash -c
         # and bash -c starts passing parameters from $0. TODO: fixme
    set -u
fi
