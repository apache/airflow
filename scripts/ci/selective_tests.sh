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

declare -a pattern_array

function output_all_basic_variables() {
    initialization::ga_output python-versions \
        "$(initialization::parameters_to_json "${CURRENT_PYTHON_MAJOR_MINOR_VERSIONS[@]}")"
    initialization::ga_output default-python-version "${DEFAULT_PYTHON_MAJOR_MINOR_VERSION}"

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

function set_outputs_run_all_tests() {
    initialization::ga_output run-tests "true"
    initialization::ga_output run-kubernetes-tests "true"
    initialization::ga_output test-types \
        '["Core", "Other", "API", "CLI", "Providers", "WWW", "Integration", "Heisentests"]'
}

function set_output_skip_all_tests() {
    initialization::ga_output run-tests "false"
    initialization::ga_output run-kubernetes-tests "false"
    initialization::ga_output test-types '[]'
}

function initialize_git_repo() {
    git remote add target "https://github.com/${CI_TARGET_REPO}"
    git fetch target "${CI_TARGET_BRANCH}:${CI_TARGET_BRANCH}" --depth=1
    echo
    echo "My commit SHA: ${COMMIT_SHA}"
    echo
    echo
    echo "Retrieved changed files from ${COMMIT_SHA} comparing to ${CI_TARGET_BRANCH} in ${CI_TARGET_REPO}"
    echo
    CHANGED_FILES=$(git diff-tree --no-commit-id --name-only -r "${COMMIT_SHA}" "${CI_TARGET_BRANCH}" || true)
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
    count_changed_files=$(echo "${CHANGED_FILES}" | grep -c -E "$(get_regexp_from_patterns)" || true)
    echo "${count_changed_files}"
}

function run_all_tests_when_push_or_schedule() {
    if [[ ${GITHUB_EVENT_NAME} == "push" || ${GITHUB_EVENT_NAME} == "schedule" ]]; then
        echo
        echo "Always run all tests in case of push/schedule events"
        echo
        set_outputs_run_all_tests
        exit
    fi
}

function check_if_tests_should_be_run_at_all() {
    TEST_TRIGGERING_PATTERNS=(
        "^airflow"
        "^.github/workflows/"
        "^Dockerfile"
        "^scripts"
        "^chart"
        "^setup.py"
        "^tests"
        "^kubernetes_tests"
    )
    readonly TEST_TRIGGERING_PATTERNS

    pattern_array=("${TEST_TRIGGERING_PATTERNS[@]}")
    show_changed_files "$(get_regexp_from_patterns)"

    if [[ $(count_changed_files) == "0" ]]; then
        echo "None of the important files changed, Skipping tests"
        set_output_skip_all_tests
        exit
    else
        initialization::ga_output run-tests "true"
    fi
}

function check_if_environment_files_changed() {
    ENVIRONMENT_TRIGGERING_PATTERNS=(
        "^.github/workflows/"
        "^Dockerfile"
        "^scripts"
        "^setup.py"
    )
    readonly ENVIRONMENT_TRIGGERING_PATTERNS

    pattern_array=("${ENVIRONMENT_TRIGGERING_PATTERNS[@]}")
    show_changed_files "$(get_regexp_from_patterns)"

    if [[ $(count_changed_files) != "0" ]]; then
        echo "Important environment files changed. Running all tests"
        set_outputs_run_all_tests
        exit
    fi
}

function get_count_all_files() {
    echo
    echo "Count All files"
    echo
    ALL_TRIGGERING_PATTERNS=(
        "^airflow"
        "^chart"
        "^tests"
        "^kubernetes_tests"
    )
    readonly ALL_TRIGGERING_PATTERNS
    pattern_array=("${ALL_TRIGGERING_PATTERNS[@]}")
    show_changed_files
    COUNT_ALL_CHANGED_FILES=$(count_changed_files)
    echo "Files count: ${COUNT_ALL_CHANGED_FILES}"
    readonly COUNT_ALL_CHANGED_FILES
}

function get_count_api_files() {
    echo
    echo "Count API files"
    echo
    API_TRIGGERING_PATTERNS=(
        "^airflow/api"
        "^airflow/api_connexion"
        "^tests/api"
        "^tests/api_connexion"
    )
    readonly API_TRIGGERING_PATTERNS
    pattern_array=("${API_TRIGGERING_PATTERNS[@]}")
    show_changed_files
    COUNT_API_CHANGED_FILES=$(count_changed_files)
    echo "Files count: ${COUNT_API_CHANGED_FILES}"
    readonly COUNT_API_CHANGED_FILES
}

function get_count_cli_files() {
    echo
    echo "Count CLI files"
    echo
    CLI_TRIGGERING_PATTERNS=(
        "^airflow/cli"
        "^tests/cli"
    )
    readonly CLI_TRIGGERING_PATTERNS
    pattern_array=("${CLI_TRIGGERING_PATTERNS[@]}")
    show_changed_files
    COUNT_CLI_CHANGED_FILES=$(count_changed_files)
    echo "Files count: ${COUNT_CLI_CHANGED_FILES}"
    readonly COUNT_CLI_CHANGED_FILES
}

function get_count_providers_files() {
    echo
    echo "Count Providers files"
    echo
    PROVIDERS_TRIGGERING_PATTERNS=(
        "^airflow/providers"
        "^tests/providers"
    )
    readonly PROVIDERS_TRIGGERING_PATTERNS
    pattern_array=("${PROVIDERS_TRIGGERING_PATTERNS[@]}")
    show_changed_files
    COUNT_PROVIDERS_CHANGED_FILES=$(count_changed_files)
    echo "Files count: ${COUNT_PROVIDERS_CHANGED_FILES}"
    readonly COUNT_PROVIDERS_CHANGED_FILES
}

function get_count_www_files() {
    echo
    echo "Count WWW files"
    echo
    WWW_TRIGGERING_PATTERNS=(
        "^airflow/www"
        "^tests/www"
    )
    readonly WWW_TRIGGERING_PATTERNS
    pattern_array=("${WWW_TRIGGERING_PATTERNS[@]}")
    show_changed_files
    COUNT_WWW_CHANGED_FILES=$(count_changed_files)
    echo "Files count: ${COUNT_WWW_CHANGED_FILES}"
    readonly COUNT_WWW_CHANGED_FILES
}

function get_count_kubernetes_files() {
    echo
    echo "Count Kubernetes files"
    echo
    KUBERNETES_TRIGGERING_PATTERNS=(
        "^airflow/kubernetes"
        "^chart"
        "^tests/kubernetes_tests"
    )
    readonly KUBERNETES_TRIGGERING_PATTERNS
    pattern_array=("${KUBERNETES_TRIGGERING_PATTERNS[@]}")
    show_changed_files
    COUNT_KUBERNETES_CHANGED_FILES=$(count_changed_files)
    echo "Files count: ${COUNT_KUBERNETES_CHANGED_FILES}"
    readonly COUNT_KUBERNETES_CHANGED_FILES
}

function calculate_test_types_to_run() {
    echo
    echo "Count Core/Other files"
    echo
    COUNT_CORE_OTHER_CHANGED_FILES=$((COUNT_ALL_CHANGED_FILES - COUNT_WWW_CHANGED_FILES - \
        COUNT_PROVIDERS_CHANGED_FILES - COUNT_CLI_CHANGED_FILES - COUNT_API_CHANGED_FILES - \
        COUNT_KUBERNETES_CHANGED_FILES))

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
            initialization::ga_output run-kubernetes-tests "true"
        else
            initialization::ga_output run-kubernetes-tests "false"
        fi
        initialization::ga_output run-tests "true"
        SELECTED_TESTS=""
        if [[ ${COUNT_API_CHANGED_FILES} != "0" ]]; then
            echo
            echo "Adding API to selected files as ${COUNT_API_CHANGED_FILES} API files changed"
            echo
            SELECTED_TESTS="${SELECTED_TESTS}, \"API\""
        fi
        if [[ ${COUNT_CLI_CHANGED_FILES} != "0" ]]; then
            echo
            echo "Adding CLI to selected files as ${COUNT_CLI_CHANGED_FILES} CLI files changed"
            echo
            SELECTED_TESTS="${SELECTED_TESTS}, \"CLI\""
        fi
        if [[ ${COUNT_PROVIDERS_CHANGED_FILES} != "0" ]]; then
            echo
            echo "Adding Providers to selected files as ${COUNT_PROVIDERS_CHANGED_FILES} Provider files changed"
            echo
            SELECTED_TESTS="${SELECTED_TESTS}, \"Providers\""
        fi
        if [[ ${COUNT_WWW_CHANGED_FILES} != "0" ]]; then
            echo
            echo "Adding WWW to selected files as ${COUNT_WWW_CHANGED_FILES} WWW files changed"
            echo
            SELECTED_TESTS="${SELECTED_TESTS}, \"WWW\""
        fi
        initialization::ga_output test-types "[ \"Integration\", \"Heisentests\" ${SELECTED_TESTS} ]"
    fi
}

output_all_basic_variables
run_all_tests_when_push_or_schedule
initialize_git_repo
check_if_tests_should_be_run_at_all
check_if_environment_files_changed
get_count_all_files
get_count_api_files
get_count_cli_files
get_count_providers_files
get_count_www_files
get_count_kubernetes_files
calculate_test_types_to_run
