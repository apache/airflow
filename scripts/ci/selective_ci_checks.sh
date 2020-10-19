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
. ./scripts/ci/libraries/_script_init.sh

# Parameter:
#
# $1 - Merge commit SHA. If this parameter is missing, this script does not check anything, it simply
#      sets all the version outputs that determine that all tests should be run. This happens in case
#      the even triggering the workflow is 'schedule' or 'push'. Merge commit is only
#      available in case of 'pull_request' triggered runs.
#
declare -a pattern_array

function output_all_basic_variables() {
    initialization::ga_output python-versions \
        "$(initialization::parameters_to_json "${CURRENT_PYTHON_MAJOR_MINOR_VERSIONS[@]}")"
    initialization::ga_output default-python-version "${DEFAULT_PYTHON_MAJOR_MINOR_VERSION}"
    initialization::ga_output all-python-versions \
        "$(initialization::parameters_to_json "${ALL_PYTHON_MAJOR_MINOR_VERSIONS[@]}")"

    initialization::ga_output kubernetes-versions \
        "$(initialization::parameters_to_json "${CURRENT_KUBERNETES_VERSIONS[@]}")"
    initialization::ga_output default-kubernetes-version "${KUBERNETES_VERSION}"

    initialization::ga_output kubernetes-modes \
        "$(initialization::parameters_to_json "${CURRENT_KUBERNETES_MODES[@]}")"
    initialization::ga_output default-kubernetes-mode "${KUBERNETES_MODE}"

    initialization::ga_output postgres-versions \
        "$(initialization::parameters_to_json "${CURRENT_POSTGRES_VERSIONS[@]}")"
    initialization::ga_output default-postgres-version "${POSTGRES_VERSION}"

    initialization::ga_output mysql-versions \
        "$(initialization::parameters_to_json "${CURRENT_MYSQL_VERSIONS[@]}")"
    initialization::ga_output default-mysql-version "${MYSQL_VERSION}"

    initialization::ga_output kind-versions \
        "$(initialization::parameters_to_json "${CURRENT_KIND_VERSIONS[@]}")"
    initialization::ga_output default-kind-version "${KIND_VERSION}"

    initialization::ga_output helm-versions \
        "$(initialization::parameters_to_json "${CURRENT_HELM_VERSIONS[@]}")"
    initialization::ga_output default-helm-version "${HELM_VERSION}"

    initialization::ga_output postgres-exclude '[{ "python-version": "3.6" }]'

    initialization::ga_output mysql-exclude '[{ "python-version": "3.7" }]'

    initialization::ga_output sqlite-exclude '[{ "python-version": "3.8" }]'

    initialization::ga_output kubernetes-exclude '[]'
}

function run_tests() {
    initialization::ga_output run-tests "${@}"
}

function run_kubernetes_tests() {
    initialization::ga_output run-kubernetes-tests "${@}"
}

function needs_helm_tests() {
    initialization::ga_output needs-helm-tests "${@}"
}

function needs_api_tests() {
    initialization::ga_output needs-api-tests "${@}"
}

function set_test_types() {
    initialization::ga_output test-types "${@}"
}

function set_basic_checks_only() {
    initialization::ga_output basic-checks-only "${@}"
}

ALL_TESTS="Core Other API CLI Providers WWW Integration Heisentests"
readonly ALL_TESTS

function set_outputs_run_everything_and_exit() {
    needs_api_tests "true"
    needs_helm_tests "true"
    run_tests "true"
    run_kubernetes_tests "true"
    set_test_types "${ALL_TESTS}"
    set_basic_checks_only "false"
    exit
}

function set_outputs_run_all_tests() {
    run_tests "true"
    run_kubernetes_tests "true"
    set_test_types "${ALL_TESTS}"
    set_basic_checks_only "false"
}

function set_output_skip_all_tests_and_exit() {
    run_tests "false"
    run_kubernetes_tests "false"
    set_test_types ""
    set_basic_checks_only "true"
    exit
}

function get_changed_files() {
    local commit_sha=${1}
    echo
    echo "Retrieved changed files from ${commit_sha}"
    echo
    CHANGED_FILES=$(git diff-tree --no-commit-id --name-only -r "${commit_sha}" "${commit_sha}^" || true)
    echo
    echo "Changed files:"
    echo
    echo "${CHANGED_FILES}"
    echo
    readonly CHANGED_FILES
}

# Converts array of patterns into single | pattern string
#    pattern_array - array storing regexp patterns
# Outputs - pattern string
function get_regexp_from_patterns() {
    local test_triggering_regexp=""
    local separator=""
    local pattern
    for pattern in "${pattern_array[@]}"; do
        test_triggering_regexp="${test_triggering_regexp}${separator}${pattern}"
        separator="|"
    done
    echo "${test_triggering_regexp}"
}

# Shows changed files in the commit vs. the target.
# Input:
#    pattern_array - array storing regexp patterns
function show_changed_files() {
    local the_regexp
    the_regexp=$(get_regexp_from_patterns)
    echo
    echo "Changed files matching the ${the_regexp} pattern:"
    echo
    echo "${CHANGED_FILES}" | grep -E "${the_regexp}" || true
    echo
}

# Counts changed files in the commit vs. the target
# Input:
#    pattern_array - array storing regexp patterns
# Output:
#    Count of changed files matching the patterns
function count_changed_files() {
    local count_changed_files
    count_changed_files=$(echo "${CHANGED_FILES}" | grep -c -E "$(get_regexp_from_patterns)" || true)
    echo "${count_changed_files}"
}

function check_if_api_tests_should_be_run() {
    local pattern_array=(
        "^airflow/api"
    )
    show_changed_files

    if [[ $(count_changed_files) == "0" ]]; then
        needs_api_tests "false"
    else
        needs_api_tests "true"
    fi
}

function check_if_helm_tests_should_be_run() {
    local pattern_array=(
        "^chart"
    )
    show_changed_files

    if [[ $(count_changed_files) == "0" ]]; then
        needs_helm_tests "false"
    else
        needs_helm_tests "true"
    fi
}

function check_if_docs_should_be_generated() {
    local pattern_array=(
        "^docs$"
        "\.py$"
        "^CHANGELOG\.txt"
    )
    show_changed_files

    if [[ $(count_changed_files) == "0" ]]; then
        echo "None of the docs changed"
    else
        image_build_needed="true"
    fi
}

AIRFLOW_SOURCES_TRIGGERING_TESTS=(
        "^airflow"
        "^chart"
        "^tests"
        "^kubernetes_tests"
)
readonly AIRFLOW_SOURCES_TRIGGERING_TESTS

function check_if_tests_are_needed_at_all() {
    local pattern_array=("${AIRFLOW_SOURCES_TRIGGERING_TESTS[@]}")
    show_changed_files

    if [[ $(count_changed_files) == "0" ]]; then
        echo "None of the important files changed, Skipping tests"
        set_output_skip_all_tests_and_exit
    else
        image_build_needed="true"
        tests_needed="true"
    fi
}

function run_all_tests_if_environment_files_changed() {
    local pattern_array=(
        "^.github/workflows/"
        "^Dockerfile"
        "^scripts"
        "^setup.py"
    )
    show_changed_files

    if [[ $(count_changed_files) != "0" ]]; then
        echo "Important environment files changed. Running everything"
        set_outputs_run_everything_and_exit
    fi
}

function get_count_all_files() {
    echo
    echo "Count All airflow source files"
    echo
    local pattern_array=("${AIRFLOW_SOURCES_TRIGGERING_TESTS[@]}")
    show_changed_files
    COUNT_ALL_CHANGED_FILES=$(count_changed_files)
    echo "Files count: ${COUNT_ALL_CHANGED_FILES}"
    readonly COUNT_ALL_CHANGED_FILES
}

function get_count_api_files() {
    echo
    echo "Count API files"
    echo
    local pattern_array=(
        "^airflow/api"
        "^airflow/api_connexion"
        "^tests/api"
        "^tests/api_connexion"
    )
    show_changed_files
    COUNT_API_CHANGED_FILES=$(count_changed_files)
    echo "Files count: ${COUNT_API_CHANGED_FILES}"
    readonly COUNT_API_CHANGED_FILES
}

function get_count_cli_files() {
    echo
    echo "Count CLI files"
    echo
    local pattern_array=(
        "^airflow/cli"
        "^tests/cli"
    )
    show_changed_files
    COUNT_CLI_CHANGED_FILES=$(count_changed_files)
    echo "Files count: ${COUNT_CLI_CHANGED_FILES}"
    readonly COUNT_CLI_CHANGED_FILES
}

function get_count_providers_files() {
    echo
    echo "Count Providers files"
    echo
    local pattern_array=(
        "^airflow/providers"
        "^tests/providers"
    )
    show_changed_files
    COUNT_PROVIDERS_CHANGED_FILES=$(count_changed_files)
    echo "Files count: ${COUNT_PROVIDERS_CHANGED_FILES}"
    readonly COUNT_PROVIDERS_CHANGED_FILES
}

function get_count_www_files() {
    echo
    echo "Count WWW files"
    echo
    local pattern_array=(
        "^airflow/www"
        "^tests/www"
    )
    show_changed_files
    COUNT_WWW_CHANGED_FILES=$(count_changed_files)
    echo "Files count: ${COUNT_WWW_CHANGED_FILES}"
    readonly COUNT_WWW_CHANGED_FILES
}

function get_count_kubernetes_files() {
    echo
    echo "Count Kubernetes files"
    echo
    local pattern_array=(
        "^airflow/kubernetes"
        "^chart"
        "^tests/kubernetes_tests"
    )
    show_changed_files
    COUNT_KUBERNETES_CHANGED_FILES=$(count_changed_files)
    echo "Files count: ${COUNT_KUBERNETES_CHANGED_FILES}"
    readonly COUNT_KUBERNETES_CHANGED_FILES
}

function calculate_test_types_to_run() {
    echo
    echo "Count Core/Other files"
    echo
    COUNT_CORE_OTHER_CHANGED_FILES=$((COUNT_ALL_CHANGED_FILES - COUNT_WWW_CHANGED_FILES - COUNT_PROVIDERS_CHANGED_FILES - COUNT_CLI_CHANGED_FILES - COUNT_API_CHANGED_FILES - COUNT_KUBERNETES_CHANGED_FILES))

    readonly COUNT_CORE_OTHER_CHANGED_FILES
    echo
    echo "Files count: ${COUNT_CORE_OTHER_CHANGED_FILES}"
    echo
    if [[ ${COUNT_CORE_OTHER_CHANGED_FILES} -gt 0 ]]; then
        # Running all tests because some core or other files changed
        echo
        echo "Looks like ${COUNT_CORE_OTHER_CHANGED_FILES} files changed in the core/other area and"
        echo "We have to run all tests. This will take longer than usual"
        echo
        set_outputs_run_all_tests
    else
        if [[ ${COUNT_KUBERNETES_CHANGED_FILES} != "0" ]]; then
            kubernetes_tests_needed="true"
        fi
        tests_needed="true"
        SELECTED_TESTS=""
        if [[ ${COUNT_API_CHANGED_FILES} != "0" ]]; then
            echo
            echo "Adding API to selected files as ${COUNT_API_CHANGED_FILES} API files changed"
            echo
            SELECTED_TESTS="${SELECTED_TESTS} API"
        fi
        if [[ ${COUNT_CLI_CHANGED_FILES} != "0" ]]; then
            echo
            echo "Adding CLI to selected files as ${COUNT_CLI_CHANGED_FILES} CLI files changed"
            echo
            SELECTED_TESTS="${SELECTED_TESTS} CLI"
        fi
        if [[ ${COUNT_PROVIDERS_CHANGED_FILES} != "0" ]]; then
            echo
            echo "Adding Providers to selected files as ${COUNT_PROVIDERS_CHANGED_FILES} Provider files changed"
            echo
            SELECTED_TESTS="${SELECTED_TESTS} Providers"
        fi
        if [[ ${COUNT_WWW_CHANGED_FILES} != "0" ]]; then
            echo
            echo "Adding WWW to selected files as ${COUNT_WWW_CHANGED_FILES} WWW files changed"
            echo
            SELECTED_TESTS="${SELECTED_TESTS} WWW"
        fi
        initialization::ga_output test-types "Integration Heisentests ${SELECTED_TESTS}"
    fi
}

output_all_basic_variables

if (($# < 1)); then
    echo
    echo "No merge commit SHA - running all tests!"
    echo
    set_outputs_run_everything_and_exit
fi

MERGE_COMMIT_SHA="${1}"
readonly MERGE_COMMIT_SHA

echo
echo "Merge commit SHA: ${MERGE_COMMIT_SHA}"
echo

image_build_needed="false"
tests_needed="false"
kubernetes_tests_needed="false"

get_changed_files "${MERGE_COMMIT_SHA}"
run_all_tests_if_environment_files_changed
check_if_docs_should_be_generated
check_if_helm_tests_should_be_run
check_if_api_tests_should_be_run
check_if_tests_are_needed_at_all
get_count_all_files
get_count_api_files
get_count_cli_files
get_count_providers_files
get_count_www_files
get_count_kubernetes_files
calculate_test_types_to_run

if [[ ${image_build_needed} == "true" ]]; then
    set_basic_checks_only "false"
else
    set_basic_checks_only "true"
fi

if [[ ${tests_needed} == "true" ]]; then
    run_tests
else
    skip_running_tests
fi

if [[ ${kubernetes_tests_needed} == "true" ]]; then
    run_kubernetes_tests
else
    skip_running_kubernetes_tests
fi
