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
            "${AIRFLOW_CI_IMAGE}" \
            "--" "/opt/airflow/docs/build" \
            | tee -a "${OUTPUT_LOG}"
}

# Docker command to generate constraint files.
function run_generate_constraints() {
    docker run "${EXTRA_DOCKER_FLAGS[@]}" \
        --entrypoint "/usr/local/bin/dumb-init"  \
        "${AIRFLOW_CI_IMAGE}" \
        "--" "/opt/airflow/scripts/ci/in_container/run_generate_constraints.sh" \
        | tee -a "${OUTPUT_LOG}"
}

# Docker command to prepare backport packages
function run_prepare_backport_packages() {
    docker run "${EXTRA_DOCKER_FLAGS[@]}" \
        --entrypoint "/usr/local/bin/dumb-init"  \
        -t \
        -v "${AIRFLOW_SOURCES}:/opt/airflow" \
        "${AIRFLOW_CI_IMAGE}" \
        "--" "/opt/airflow/scripts/ci/in_container/run_prepare_backport_packages.sh" "${@}" \
        | tee -a "${OUTPUT_LOG}"
}

# Docker command to generate release notes for backport packages
function run_prepare_backport_readme() {
    docker run "${EXTRA_DOCKER_FLAGS[@]}" \
        --entrypoint "/usr/local/bin/dumb-init"  \
        -t \
        -v "${AIRFLOW_SOURCES}:/opt/airflow" \
        "${AIRFLOW_CI_IMAGE}" \
        "--" "/opt/airflow/scripts/ci/in_container/run_prepare_backport_readme.sh" "${@}" \
        | tee -a "${OUTPUT_LOG}"
}
