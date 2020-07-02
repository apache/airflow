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

# Docker command to build documentation
function run_docs() {
    verbose_docker run "${EXTRA_DOCKER_FLAGS[@]}" -t \
            --entrypoint "/usr/local/bin/dumb-init"  \
            --env PYTHONDONTWRITEBYTECODE \
            --env VERBOSE \
            --env VERBOSE_COMMANDS \
            --env HOST_USER_ID="$(id -ur)" \
            --env HOST_GROUP_ID="$(id -gr)" \
            --env HOST_OS="$(uname -s)" \
            --env HOST_HOME="${HOME}" \
            --env HOST_AIRFLOW_SOURCES="${AIRFLOW_SOURCES}" \
            --rm \
            "${AIRFLOW_CI_IMAGE}" \
            "--" "/opt/airflow/docs/build" \
            | tee -a "${OUTPUT_LOG}"
}

# Docker command to generate constraint requirement files.
function run_generate_requirements() {
    docker run "${EXTRA_DOCKER_FLAGS[@]}" \
        --entrypoint "/usr/local/bin/dumb-init"  \
        --env PYTHONDONTWRITEBYTECODE \
        --env VERBOSE \
        --env VERBOSE_COMMANDS \
        --env HOST_USER_ID="$(id -ur)" \
        --env HOST_GROUP_ID="$(id -gr)" \
        --env HOST_OS="$(uname -s)" \
        --env HOST_HOME="${HOME}" \
        --env HOST_AIRFLOW_SOURCES="${AIRFLOW_SOURCES}" \
        --env UPGRADE_WHILE_GENERATING_REQUIREMENTS \
        --env PYTHON_MAJOR_MINOR_VERSION \
        --env CHECK_REQUIREMENTS_ONLY \
        --rm \
        "${AIRFLOW_CI_IMAGE}" \
        "--" "/opt/airflow/scripts/ci/in_container/run_generate_requirements.sh" \
        | tee -a "${OUTPUT_LOG}"
}
