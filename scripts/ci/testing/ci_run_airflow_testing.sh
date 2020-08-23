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
    local exit_code
    for try_num in {1..3}
    do
        echo
        echo "Making sure docker-compose is down"
        echo
        docker-compose --log-level INFO -f "${SCRIPTS_CI_DIR}/docker-compose/base.yml" \
            down --remove-orphans --timeout 10
        echo
        echo "System-prune docker"
        echo
        docker system prune --force
        echo
        echo "Check available space"
        echo
        df --human
        echo
        echo "Check available memory"
        echo
        free --human
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


if [[ ${TEST_TYPE=} != "" ]]; then
    # Handle case where test type is passed from outside
    export TEST_TYPES="${TEST_TYPE}"
fi

if [[ ${TEST_TYPES=} == "" ]]; then
    TEST_TYPES="Core Providers API CLI Integration Other WWW Heisentests"
    echo
    echo "Test types not specified. Running all: ${TEST_TYPES}"
    echo
fi

if [[ ${TEST_TYPE=} != "" ]]; then
    # Add Postgres/MySQL special test types in case we are running several test types
    if [[ ${BACKEND} == "postgres" ]]; then
        TEST_TYPES="${TEST_TYPES} Postgres"
    fi
    if [[ ${BACKEND} == "mysql" ]]; then
        TEST_TYPES="${TEST_TYPES} MySQL"
    fi
fi
readonly TEST_TYPES

export RUN_INTEGRATION_TESTS=${RUN_INTEGRATION_TESTS:=""}
export ENABLED_INTEGRATIONS=${ENABLED_INTEGRATIONS:=""}

if [[ " ${ENABLED_INTEGRATIONS[*]} " =~ " kerberos " ]]; then
    kerberos::create_kerberos_network
fi

echo "Running TEST_TYPES: ${TEST_TYPES}"

for TEST_TYPE in ${TEST_TYPES}
do
    INTEGRATIONS=()
    export INTEGRATIONS

    if [[ ${TEST_TYPE:=} == "Integration" ]]; then
        export ENABLED_INTEGRATIONS="${AVAILABLE_INTEGRATIONS}"
        export RUN_INTEGRATION_TESTS="${AVAILABLE_INTEGRATIONS}"
    fi

    for _INT in ${ENABLED_INTEGRATIONS}
    do
        INTEGRATIONS+=("-f")
        INTEGRATIONS+=("${SCRIPTS_CI_DIR}/docker-compose/integration-${_INT}.yml")
    done


    export TEST_TYPE
    echo "**********************************************************************************************"
    echo
    echo "                            TEST_TYPE: ${TEST_TYPE}"
    echo
    echo "**********************************************************************************************"

    run_airflow_testing_in_docker "${@}"
done
