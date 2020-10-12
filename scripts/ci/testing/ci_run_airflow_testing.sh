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
# shellcheck source=scripts/ci/libraries/_script_init.sh


. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"

INTEGRATIONS=()

ENABLED_INTEGRATIONS=${ENABLED_INTEGRATIONS:=""}

if [[ ${TEST_TYPE:=} == "Integration" ]]; then
    export ENABLED_INTEGRATIONS="${AVAILABLE_INTEGRATIONS}"
    export RUN_INTEGRATION_TESTS="${AVAILABLE_INTEGRATIONS}"
fi

for _INT in ${ENABLED_INTEGRATIONS}
do
    INTEGRATIONS+=("-f")
    INTEGRATIONS+=("${SCRIPTS_CI_DIR}/docker-compose/integration-${_INT}.yml")
done

readonly INTEGRATIONS

if [[ -f ${BUILD_CACHE_DIR}/.skip_tests ]]; then
    echo
    echo "Skipping running tests !!!!!"
    echo
    exit
fi

function run_airflow_testing_in_docker() {
    set +u
    set +e
    local exit_code
    for try_num in {1..3}
    do
        echo
        echo "Starting try number ${try_num}"
        echo
        docker-compose --log-level INFO \
          -f "${SCRIPTS_CI_DIR}/docker-compose/base.yml" \
          -f "${SCRIPTS_CI_DIR}/docker-compose/backend-${BACKEND}.yml" \
          "${INTEGRATIONS[@]}" \
          "${DOCKER_COMPOSE_LOCAL[@]}" \
             run airflow "${@}"
        exit_code=$?
        if [[ ${exit_code} == 254 ]]; then
            echo
            echo "Failed starting integration on ${try_num} try. Wiping-out docker-compose remnants"
            echo
            docker-compose --log-level INFO \
                -f "${SCRIPTS_CI_DIR}/docker-compose/base.yml" \
                down --remove-orphans -v --timeout 5
            echo
            echo "Sleeping 5 seconds"
            echo
            sleep 5
            continue
        else
            break
        fi
    done
    if [[ ${TEST_TYPE:=} == "Quarantined" ]]; then
        if [[ ${exit_code} == "1" ]]; then
            echo
            echo "Some Quarantined tests failed. but we recorded it in an issue"
            echo
            exit_code="0"
        else
            echo
            echo "All Quarantined tests succeeded"
            echo
        fi
    fi
    set -u
    set -e
    return "${exit_code}"
}

build_images::prepare_ci_build

build_images::rebuild_ci_image_if_needed

DOCKER_COMPOSE_LOCAL=()

if [[ ${MOUNT_LOCAL_SOURCES} == "true" ]]; then
    DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/local.yml")
fi

if [[ ${MOUNT_FILES} == "true" ]]; then
    DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/files.yml")
fi

if [[ ${GITHUB_ACTIONS} == "true" ]]; then
    DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/ga.yml")
fi

if [[ ${FORWARD_CREDENTIALS} == "true" ]]; then
    DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/forward-credentials.yml")
fi

if [[ -n ${INSTALL_AIRFLOW_VERSION=} || -n ${INSTALL_AIRFLOW_REFERENCE} ]]; then
    DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/remove-sources.yml")
fi

readonly DOCKER_COMPOSE_LOCAL

echo
echo "Using docker image: ${AIRFLOW_CI_IMAGE} for docker compose runs"
echo

RUN_INTEGRATION_TESTS=${RUN_INTEGRATION_TESTS:=""}
readonly RUN_INTEGRATION_TESTS


run_airflow_testing_in_docker "${@}"
