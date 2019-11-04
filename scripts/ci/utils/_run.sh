#!/usr/bin/env bash
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

# Assume AIRFLOW_SOURCES are set to point to sources of Airflow

function _run_ci_script_with_docker() {
    local SCRIPT="$1"
    shift
    FILES=("$@")

    if [[ "${#FILES[@]}" == "0" ]]; then
        verbose_docker run "${EXTRA_DOCKER_FLAGS[@]}" \
            --entrypoint "/root/.local/bin/dumb-init"  \
            --env PYTHONDONTWRITEBYTECODE \
            --env AIRFLOW_CI_VERBOSE="${VERBOSE}" \
            --env AIRFLOW_CI_SILENT \
            --env HOST_USER_ID="$(id -ur)" \
            --env HOST_GROUP_ID="$(id -gr)" \
            --rm \
            "${AIRFLOW_CI_IMAGE}" \
            "--" "${SCRIPT}" \
            | tee -a "${OUTPUT_LOG}"
    else
        verbose_docker run "${EXTRA_DOCKER_FLAGS[@]}" \
            --entrypoint "/root/.local/bin/dumb-init"  \
            --env PYTHONDONTWRITEBYTECODE \
            --env AIRFLOW_CI_VERBOSE="${VERBOSE}" \
            --env AIRFLOW_CI_SILENT \
            --env HOST_USER_ID="$(id -ur)" \
            --env HOST_GROUP_ID="$(id -gr)" \
            --rm \
            "${AIRFLOW_CI_IMAGE}" \
            "--" "${SCRIPT}" "${FILES[@]}" \
            | tee -a "${OUTPUT_LOG}"
    fi
}

function run_flake8() {
    _run_ci_script_with_docker "/opt/airflow/scripts/ci/in_container/run_flake8.sh" "$@"
}

function run_isort() {
    _run_ci_script_with_docker "/opt/airflow/scripts/ci/in_container/run_isort.sh" "$@"
}


function run_docs() {
    _run_ci_script_with_docker "/opt/airflow/docs/build.sh"
}

function run_check_license() {
    _run_ci_script_with_docker "/opt/airflow/scripts/ci/in_container/run_check_licence.sh"
}

function run_mypy() {
    FILES=("$@")
    if [[ "${#FILES[@]}" == "0" ]]; then
        _run_ci_script_with_docker "/opt/airflow/scripts/ci/in_container/run_mypy.sh" "airflow" "tests" "docs"
    else
        _run_ci_script_with_docker "/opt/airflow/scripts/ci/in_container/run_mypy.sh" "$@"
    fi
}

function run_pylint_main() {
    _run_ci_script_with_docker "/opt/airflow/scripts/ci/in_container/run_pylint_main.sh" "$@"
}


function run_pylint_tests() {
    _run_ci_script_with_docker "/opt/airflow/scripts/ci/in_container/run_pylint_tests.sh" "$@"
}

function run_docker_lint() {
    FILES=("$@")
    if [[ "${#FILES[@]}" == "0" ]]; then
        echo
        echo "Running docker lint for all Dockerfiles"
        echo
        verbose_docker run \
            -v "$(pwd):/root" \
            -w /root \
            --rm \
            hadolint/hadolint /bin/hadolint Dockerfile* | tee -a "${OUTPUT_LOG}"
        echo
        echo "Docker pylint completed with no errors"
        echo
    else
        echo
        echo "Running docker lint for $*"
        echo
        verbose_docker run \
            -v "$(pwd):/root" \
            -w /root \
            --rm \
            hadolint/hadolint /bin/hadolint "$@" | tee -a "${OUTPUT_LOG}"
        echo
        echo "Docker pylint completed with no errors"
        echo
    fi
}

function filter_out_files_from_pylint_todo_list() {
  FILTERED_FILES=()
  set +e
  for FILE in "$@"
  do
      if [[ ${FILE} == "airflow/migrations/versions/"* ]]; then
          # Skip all generated migration scripts
          continue
      fi
      if ! grep -x "./${FILE}" <"${AIRFLOW_SOURCES}/scripts/ci/pylint_todo.txt" >/dev/null; then
          FILTERED_FILES+=("${FILE}")
      fi
  done
  set -e
  export FILTERED_FILES
}

function refresh_pylint_todo() {
    verbose_docker run "${EXTRA_DOCKER_FLAGS[@]}" \
        --entrypoint /opt/airflow/scripts/ci/in_container/refresh_pylint_todo.sh \
        --env PYTHONDONTWRITEBYTECODE \
        --env AIRFLOW_CI_VERBOSE="${VERBOSE}" \
        --env AIRFLOW_CI_SILENT \
        --env HOST_USER_ID="$(id -ur)" \
        --env HOST_GROUP_ID="$(id -gr)" \
        --rm \
        "${AIRFLOW_CI_IMAGE}" | tee -a "${OUTPUT_LOG}"
}


function prepare_run() {
    _create_temp_cache_directory
}
