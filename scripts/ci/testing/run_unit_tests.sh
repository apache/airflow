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

export COLOR_RED=$'\e[31m'
export COLOR_BLUE=$'\e[34m'
export COLOR_YELLOW=$'\e[33m'
export COLOR_RESET=$'\e[0m'

if [[ ! "$#" -eq 2 ]]; then
    echo "${COLOR_RED}You must provide 2 arguments: Group, Scope!.${COLOR_RESET}"
    exit 1
fi

TEST_GROUP=${1}
TEST_SCOPE=${2}

function core_tests() {
    echo "${COLOR_BLUE}Running core tests${COLOR_RESET}"
    set +e
    if [[ "${TEST_SCOPE}" == "DB" ]]; then
        set -x
        breeze testing core-tests --run-in-parallel --run-db-tests-only
        RESULT=$?
        set +x
    elif [[ "${TEST_SCOPE}" == "Non-DB" ]]; then
        set -x
        breeze testing core-tests --use-xdist --skip-db-tests --no-db-cleanup --backend none
        RESULT=$?
        set +x
    elif [[ "${TEST_SCOPE}" == "All" ]]; then
        set -x
        breeze testing core-tests --run-in-parallel
        RESULT=$?
        set +x
    elif [[ "${TEST_SCOPE}" == "Quarantined" ]]; then
        set -x
        breeze testing core-tests --test-type "All-Quarantined" || true
        RESULT=$?
        set +x
    elif [[ "${TEST_SCOPE}" == "ARM collection" ]]; then
        set -x
        breeze testing core-tests --collect-only --remove-arm-packages --test-type "All" --no-db-reset
        RESULT=$?
        set +x
    elif [[  "${TEST_SCOPE}" == "System" ]]; then
        set -x
        breeze testing system-tests tests/system/example_empty.py
        RESULT=$?
        set +x
    else
        echo "Unknown test scope: ${TEST_SCOPE}"
        set -e
        exit 1
    fi
    set -e
    if [[ ${RESULT} != "0" ]]; then
        echo
        echo "${COLOR_RED}The ${TEST_GROUP} test ${TEST_SCOPE} failed! Giving up${COLOR_RESET}"
        echo
        exit "${RESULT}"
    fi
    echo "${COLOR_GREEN}Core tests completed successfully${COLOR_RESET}"
}

function providers_tests() {
    echo "${COLOR_BLUE}Running providers tests${COLOR_RESET}"
    set +e
    if [[ "${TEST_SCOPE}" == "DB" ]]; then
        set -x
        breeze testing providers-tests --run-in-parallel --run-db-tests-only
        RESULT=$?
        set +x
    elif [[ "${TEST_SCOPE}" == "Non-DB" ]]; then
        set -x
        breeze testing providers-tests --use-xdist --skip-db-tests --no-db-cleanup --backend none
        RESULT=$?
        set +x
    elif [[ "${TEST_SCOPE}" == "All" ]]; then
        set -x
        breeze testing providers-tests --run-in-parallel
        RESULT=$?
        set +x
    elif [[ "${TEST_SCOPE}" == "Quarantined" ]]; then
        set -x
        breeze testing providers-tests --test-type "All-Quarantined" || true
        RESULT=$?
        set +x
    elif [[ "${TEST_SCOPE}" == "ARM collection" ]]; then
        set -x
        breeze testing providers-tests --collect-only --remove-arm-packages --test-type "All" --no-db-reset
        RESULT=$?
        set +x
    elif [[  "${TEST_SCOPE}" == "System" ]]; then
        set -x
        breeze testing system-tests providers/tests/system/example_empty.py
        RESULT=$?
        set +x
    else
        echo "Unknown test scope: ${TEST_SCOPE}"
        set -e
        exit 1
    fi
    set -e
    if [[ ${RESULT} != "0" ]]; then
        echo
        echo "${COLOR_RED}The ${TEST_GROUP} test ${TEST_SCOPE} failed! Giving up${COLOR_RESET}"
        echo
        exit "${RESULT}"
    fi
    echo "${COLOR_GREEB}Providers tests completed successfully${COLOR_RESET}"
}


function task_sdk_tests() {
    echo "${COLOR_BLUE}Running Task SDK tests${COLOR_RESET}"
    set -x
    breeze testing task-sdk-tests
    set +x
    echo "${COLOR_BLUE}Task SDK tests completed${COLOR_RESET}"
}


function run_tests() {
    if [[ "${TEST_GROUP}" == "core" ]]; then
        core_tests
    elif [[ "${TEST_GROUP}" == "providers" ]]; then
        providers_tests
    elif [[ "${TEST_GROUP}" == "task_sdk" ]]; then
        task_sdk_tests
    else
        echo "Unknown test group: ${TEST_GROUP}"
        exit 1
    fi
}

run_tests
