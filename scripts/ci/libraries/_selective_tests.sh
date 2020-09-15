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

SCRIPTS_PATTERNS=(
    "^Dockerfile"
    "^scripts"
    "^.github/workflows/"
    "^setup.py"
)
readonly SCRIPTS_PATTERNS

AIRFLOW_PATTERNS=(
    "^airflow"
    "^tests"
)
readonly AIRFLOW_PATTERNS

AIRFLOW_PROVIDERS_PATTERNS=(
    "^airflow/providers"
    "^tests/providers"
)
readonly AIRFLOW_PROVIDERS_PATTERNS

KUBERNETES_PATTERNS=(
    "^airflow/kubernetes"
    "^airflow/providers/cncf"
    "^tests/kubernetes"
    "^tests/providers/cncf"
    "^kubernetes_tests"
)
readonly KUBERNETES_PATTERNS


function selective_tests::get_changed_files() {
    echo
    echo "GitHub SHA: ${COMMIT_SHA}"
    echo

    git remote add target "https://github.com/${CI_TARGET_REPO}"

    git fetch target "${CI_TARGET_BRANCH}:${CI_TARGET_BRANCH}" --depth=1

    echo
    echo "Retrieve changed files from ${COMMIT_SHA} comparing to ${CI_TARGET_BRANCH}"
    echo

    changed_files=$(git diff-tree --no-commit-id --name-only -r "${COMMIT_SHA}" "${CI_TARGET_BRANCH}" || true)

    echo
    echo "Changed files:"
    echo
    echo "${changed_files}"
    echo
}

function selective_tests::build_regexp_string() {
    local changed_files_regexp=""
    local separator=""
    for pattern in "${@}"
    do
        changed_files_regexp="${changed_files_regexp}${separator}${pattern}"
        separator="|"
    done
    echo "${changed_files_regexp}"
}

function selective_tests::print_changed_files_matching_patterns() {
    local regexp
    regexp=$(selective_tests::build_regexp_string "${@}")
    echo
    echo "Changed files"
    echo
    echo "${changed_files}" | grep -E "${regexp}" || true
}

function selective_tests::count_changed_files_matching_patterns() {
    local regexp
    regexp=$(selective_tests::build_regexp_string "${@}")
    count_changed_files=$(echo "${changed_files}" | grep -c -E "${regexp}" || true)
    echo "${count_changed_files}"
}


function selective_tests::run_all_test() {
    echo "::set-output name=test-types::$(initialization::parameters_to_json "${ALL_TEST_TYPES[@]}")"
    echo "::set-output name=backends::$(initialization::parameters_to_json "${CURRENT_BACKENDS[@]}")"
    echo '::set-output name=directories::["."]'
    exit
}

function selective_tests::run_only_selected_providers() {
    # Provider tests only are run only using sqlite
    echo "::set-output name=test-types::$(initialization::parameters_to_json "${ALL_TEST_TYPES[@]}")"
    echo '::set-output name=backends::["sqlite"]'
    echo "::set-output name=directories::$(initialization::parameters_to_json "${@}")"
    exit
}

function selective_tests::skip_tests() {
    echo '::set-output name=test-types::[]'
    echo '::set-output name=directories::["."]'
    exit
}


function selective_tests::check_event_type() {
    if [[ ${CI_EVENT_TYPE} == "push" ]]; then
        echo
        echo "Always run all tests on push"
        echo
        selective_tests::run_all_test
    fi
}

function selective_tests::check_scripts_changed() {
    local changed_scripts_count
    changed_scripts_count=$(selective_tests::count_changed_files_matching_patterns "${SCRIPTS_PATTERNS[@]}")
    if [[ ${changed_scripts_count} != 0 ]];
    then
        echo
        echo "Always run all tests when scripts change"
        echo
        selective_tests::print_changed_files_matching_patterns "${SCRIPTS_PATTERNS[@]}"
        echo
        selective_tests::run_all_test
    fi
}

function selective_tests::enable_kubernetes_tests_if_changed() {
    local changed_kubernetes_count
    changed_kubernetes_count=$(selective_tests::count_changed_files_matching_patterns "${KUBERNETES_PATTERNS[@]}")
    if [[ ${changed_kubernetes_count} != 0 ]];
    then
        echo
        echo "Additionally run all kubernetes tests as the Kubernetes files changed,"
        echo
        selective_tests::print_changed_files_matching_patterns "${KUBERNETES_PATTERNS[@]}"
        echo
        echo
        echo '::set-output name=kubernetes-tests::true'
    else
        echo
        echo "Skip all kubernetes tests as the Kubernetes files has not changed,"
        echo
        echo '::set-output name=kubernetes-tests::false'
    fi
}



function selective_tests::check_providers_only_changed() {
    local changed_airflow_count
    changed_airflow_count=$(count_changed_files_matching_patterns "${AIRFLOW_PATTERNS[@]}")
    local changed_airflow_providers_count
    changed_airflow_providers_count=$(count_changed_files_matching_patterns "${AIRFLOW_PROVIDERS_PATTERNS[@]}")
    local changed_kubernetes_count
    changed_kubernetes_count=$(count_changed_files_matching_patterns "${KUBERNETES_PATTERNS[@]}")
    if [[ ${changed_airflow_count} != 0 && \
        ${changed_airflow_count} == "${changed_airflow_providers_count}" && \
        ${changed_kubernetes_count} == 0 ]];
    then
        echo
        echo "Only some providers changed but no core/no kubernetes. Run only those provider's tests"
        echo
        selective_tests::print_changed_files_matching_patterns "${AIRFLOW_PROVIDERS_PATTERNS[@]}"
        echo
        echo
        local selected_directories
        selected_directories=$(xargs -n 1 dirname <"${changed_files}" | sort | uniq)
        selective_tests::run_only_selected_providers "${selected_directories}"
    fi
}


function selective_tests::check_core_changed() {
    local changed_airflow_count
    changed_airflow_count=$(count_changed_files_matching_patterns "${AIRFLOW_PATTERNS[@]}")
    if [[ ${changed_airflow_count} != 0 ]];
    then
        echo
        echo "Always run all tests when core files change"
        echo
        selective_tests::print_changed_files_matching_patterns "${AIRFLOW_PATTERNS[@]}"
        echo
        echo
        selective_tests::run_all_test
    else
        selective_tests::skip_tests
    fi
}
