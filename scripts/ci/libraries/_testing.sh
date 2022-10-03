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

export MEMORY_REQUIRED_FOR_HEAVY_TEST_PARALLEL_RUN=33000

function testing::get_docker_compose_local() {
    DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/files.yml")
    if [[ ${MOUNT_SELECTED_LOCAL_SOURCES} == "true" ]]; then
        DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/local.yml")
    fi
    if [[ ${MOUNT_ALL_LOCAL_SOURCES} == "true" ]]; then
        DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/local-all-sources.yml")
    fi

    if [[ ${GITHUB_ACTIONS} == "true" ]]; then
        DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/ga.yml")
    fi

    if [[ ${FORWARD_CREDENTIALS} == "true" ]]; then
        DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/forward-credentials.yml")
    fi

    if [[ -n ${USE_AIRFLOW_VERSION=} ]]; then
        DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/remove-sources.yml")
    fi
    readonly DOCKER_COMPOSE_LOCAL
}


function testing::dump_container_logs() {
    start_end::group_start "${COLOR_BLUE}Dumping container logs ${container}${COLOR_RESET}"
    local container="${1}"
    local dump_file
    dump_file=${AIRFLOW_SOURCES}/files/container_logs_${container}_$(date "+%Y-%m-%d")_${CI_BUILD_ID}_${CI_JOB_ID}.log
    echo "${COLOR_BLUE}###########################################################################################${COLOR_RESET}"
    echo "                   Dumping logs from ${container} container"
    echo "${COLOR_BLUE}###########################################################################################${COLOR_RESET}"
    docker_v logs "${container}" > "${dump_file}"
    echo "                   Container ${container} logs dumped to ${dump_file}"
    echo "${COLOR_BLUE}###########################################################################################${COLOR_RESET}"
    start_end::group_end
}

function testing::setup_docker_compose_backend() {
    local TEST_TYPE
    TEST_TYPE="${1}"
    if [[ ${BACKEND} == "mssql" ]]; then
        local backend_docker_compose=("-f" "${SCRIPTS_CI_DIR}/docker-compose/backend-${BACKEND}.yml")
        local docker_filesystem
        docker_filesystem=$(stat "-f" "-c" "%T" /var/lib/docker 2>/dev/null || echo "unknown")
        if [[ ${docker_filesystem} == "tmpfs" ]]; then
            # In case of tmpfs backend for docker, mssql fails because TMPFS does not support
            # O_DIRECT parameter for direct writing to the filesystem
            # https://github.com/microsoft/mssql-docker/issues/13
            # so we need to mount an external volume for its db location
            # the external db must allow for parallel testing so TEST_TYPE
            # is added to the volume name
            export MSSQL_DATA_VOLUME="${HOME}/tmp-mssql-volume-${TEST_TYPE/\[*\]/}-${MSSQL_VERSION}"
            mkdir -p "${MSSQL_DATA_VOLUME}"
            # MSSQL 2019 runs with non-root user by default so we have to make the volumes world-writeable
            # This is a bit scary and we could get by making it group-writeable but the group would have
            # to be set to "root" (GID=0) for the volume to work and this cannot be accomplished without sudo
            chmod a+rwx "${MSSQL_DATA_VOLUME}"
            backend_docker_compose+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/backend-mssql-tmpfs-volume.yml")

            # Runner user doesn't have blanket sudo access, but we can run docker as root. Go figure
            traps::add_trap "docker run -u 0 --rm -v ${MSSQL_DATA_VOLUME}:/mssql alpine sh -c 'rm -rvf -- /mssql/.* /mssql/*' || true" EXIT

            # Clean up at start too, in case a previous runner left it messy
            docker run --rm -u 0 -v "${MSSQL_DATA_VOLUME}":/mssql alpine sh -c 'rm -rfv -- /mssql/.* /mssql/*'  || true
            export BACKEND_DOCKER_COMPOSE=("${backend_docker_compose[@]}")
        else
            backend_docker_compose+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/backend-mssql-docker-volume.yml")
            export BACKEND_DOCKER_COMPOSE=("${backend_docker_compose[@]}")
        fi
    else
        local backend_docker_compose=("-f" "${SCRIPTS_CI_DIR}/docker-compose/backend-${BACKEND}.yml")
        export BACKEND_DOCKER_COMPOSE=("${backend_docker_compose[@]}")
    fi
}

function testing::run_command_in_docker(){
    set +u
    set +e
    local exit_code
    local docker_test_name
    local docker_test_name="${1}"
    local docker_cmd
    local docker_cmd="${2}"
    echo
    echo "Semaphore grabbed. Running ${docker_test_name} tests for ${BACKEND}"
    echo
    echo "Making sure docker-compose is down and remnants removed"
    echo
    docker-compose -f "${SCRIPTS_CI_DIR}/docker-compose/base.yml" \
        --project-name "airflow-${docker_test_name}-${BACKEND}" \
        down --remove-orphans \
        --volumes --timeout 10
    docker-compose --log-level INFO \
      -f "${SCRIPTS_CI_DIR}/docker-compose/base.yml" \
      "${BACKEND_DOCKER_COMPOSE[@]}" \
      "${DOCKER_COMPOSE_LOCAL[@]}" \
      --project-name "airflow-${docker_test_name}-${BACKEND}" \
         run airflow -c "${docker_cmd}"
    exit_code=$?
    docker ps
    if [[ ${exit_code} != "0" && ${CI} == "true" ]]; then
        docker ps --all
        local container
        for container in $(docker ps --all --format '{{.Names}}')
        do
            testing::dump_container_logs "${container}"
        done
    fi

    docker-compose --log-level INFO -f "${SCRIPTS_CI_DIR}/docker-compose/base.yml" \
        --project-name "airflow-${docker_test_name}-${BACKEND}" \
        down --remove-orphans \
        --volumes --timeout 10
    set -u
    set -e
    if [[ ${exit_code} == 0 ]]; then
        echo
        echo "${COLOR_GREEN}Test ${docker_test_name} succeeded.${COLOR_RESET}"
    else
        echo
        echo "${COLOR_RED}Test ${docker_test_name} failed.${COLOR_RESET}"
    fi
    return "${exit_code}"
}
