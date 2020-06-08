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

export PYTHON_MAJOR_MINOR_VERSION=${PYTHON_MAJOR_MINOR_VERSION:-3.6}

# shellcheck source=scripts/ci/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/_script_init.sh"

function run_pylint_tests() {
    FILES=("$@")
    if [[ "${#FILES[@]}" == "0" ]]; then
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
            --rm \
            "${AIRFLOW_CI_IMAGE}" \
            "--" "/opt/airflow/scripts/ci/in_container/run_pylint_tests.sh" \
            | tee -a "${OUTPUT_LOG}"
    else
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
            --rm \
            "${AIRFLOW_CI_IMAGE}" \
            "--" "/opt/airflow/scripts/ci/in_container/run_pylint_tests.sh" "${FILES[@]}" \
            | tee -a "${OUTPUT_LOG}"
    fi
}

get_ci_environment

prepare_ci_build

rebuild_ci_image_if_needed

if [[ "${#@}" != "0" ]]; then
    filter_out_files_from_pylint_todo_list "$@"

    if [[ "${#FILTERED_FILES[@]}" == "0" ]]; then
        echo "Filtered out all files. Skipping pylint."
    else
        run_pylint_tests "${FILTERED_FILES[@]}"
    fi
else
    run_pylint_tests
fi
