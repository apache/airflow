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

if [[ -f ${BUILD_CACHE_DIR}/.skip_tests ]]; then
    echo
    echo "Skipping running tests !!!!!"
    echo
    exit
fi

function run_airflow_testing_in_docker() {
    set +u
    set +e
    for TRY_NUM in {1..3}
    do
        echo
        echo "Starting try number ${TRY_NUM}"
        echo
        docker-compose --log-level INFO \
          -f "${SCRIPTS_CI_DIR}/docker-compose/base.yml" \
          -f "${SCRIPTS_CI_DIR}/docker-compose/backend-${BACKEND}.yml" \
          "${INTEGRATIONS[@]}" \
          "${DOCKER_COMPOSE_LOCAL[@]}" \
             run airflow "${@}"
        EXIT_CODE=$?
        if [[ ${EXIT_CODE} == 254 ]]; then
            echo
            echo "Failed starting integration on ${TRY_NUM} try. Wiping-out docker-compose remnants"
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
    if [[ ${ONLY_RUN_QUARANTINED_TESTS:=} == "true" ]]; then
        if [[ ${EXIT_CODE} == "1" ]]; then
            echo
            echo "Some Quarantined tests failed. but we recorded it in an issue"
            echo
            EXIT_CODE="0"
        else
            echo
            echo "All Quarantined tests succeeded"
            echo
        fi
    fi
    set -u
    set -e
    return "${EXIT_CODE}"
}

build_images::prepare_ci_build

build_images::rebuild_ci_image_if_needed

# Test environment
export BACKEND=${BACKEND:="sqlite"}

# Whether necessary for airflow run local sources are mounted to docker
export MOUNT_LOCAL_SOURCES=${MOUNT_LOCAL_SOURCES:="false"}

# Whether files folder is mounted to docker
export MOUNT_FILES=${MOUNT_FILES:="true"}

# whether verbose output should be produced
export VERBOSE=${VERBOSE:="false"}

# whether verbose commands output (set -x) should be used
export VERBOSE_COMMANDS=${VERBOSE_COMMANDS:="false"}

# Forwards host credentials to the container
export FORWARD_CREDENTIALS=${FORWARD_CREDENTIALS:="false"}

# Installs different airflow version than current from the sources
export INSTALL_AIRFLOW_VERSION=${INSTALL_AIRFLOW_VERSION:=""}

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

echo
echo "Using docker image: ${AIRFLOW_CI_IMAGE} for docker compose runs"
echo

INTEGRATIONS=()

ENABLED_INTEGRATIONS=${ENABLED_INTEGRATIONS:=""}

if [[ ${TEST_TYPE:=} == "Integration" ]]; then
    export ENABLED_INTEGRATIONS="${AVAILABLE_INTEGRATIONS}"
    export RUN_INTEGRATION_TESTS="${AVAILABLE_INTEGRATIONS}"
elif [[ ${TEST_TYPE:=} == "Long" ]]; then
    export ONLY_RUN_LONG_RUNNING_TESTS="true"
elif [[ ${TEST_TYPE:=} == "Quarantined" ]]; then
    export ONLY_RUN_QUARANTINED_TESTS="true"
    # Do not fail in quarantined tests
fi

for _INT in ${ENABLED_INTEGRATIONS}
do
    INTEGRATIONS+=("-f")
    INTEGRATIONS+=("${SCRIPTS_CI_DIR}/docker-compose/integration-${_INT}.yml")
done

RUN_INTEGRATION_TESTS=${RUN_INTEGRATION_TESTS:=""}

run_airflow_testing_in_docker "${@}"

if [[ ${TEST_TYPE:=} == "Quarantined" ]]; then
    export ONLY_RUN_QUARANTINED_TESTS="true"
fi
