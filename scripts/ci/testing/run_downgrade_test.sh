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

function run_db_downgrade() {
    set +u
    set +e
    local exit_code
    echo
    echo "Semaphore grabbed. Running upgrade and downgrade tests for ${BACKEND}"
    echo
    echo "Making sure docker-compose is down and remnants removed"
    echo
    docker-compose -f "${SCRIPTS_CI_DIR}/docker-compose/base.yml" \
        --project-name "airflow-downgrade-${BACKEND}" \
        down --remove-orphans \
        --volumes --timeout 10
    docker-compose --log-level INFO \
      -f "${SCRIPTS_CI_DIR}/docker-compose/base.yml" \
      "${BACKEND_DOCKER_COMPOSE[@]}" \
      "${DOCKER_COMPOSE_LOCAL[@]}" \
      --project-name "airflow-downgrade-${BACKEND}" \
         run airflow -c "airflow db downgrade -r e959f08ac86c -y"
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
        --project-name "airflow-downgrade-${BACKEND}" \
        down --remove-orphans \
        --volumes --timeout 10
    set -u
    set -e
    if [[ ${exit_code} == 0 ]]; then
        echo
        echo "${COLOR_GREEN}Test downgrade succeeded.${COLOR_RESET}"
    else
        echo
        echo "${COLOR_RED}Test downgrade failed.${COLOR_RESET}"
    fi
    return "${exit_code}"
}


build_images::prepare_ci_build

build_images::rebuild_ci_image_if_needed_with_group
testing::get_docker_compose_local
testing::setup_docker_compose_backend "downgrade"
run_db_downgrade "${@}"
