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

#
# Asserts that you are actually in container
#
function assert_in_container() {
    if [[ ! -f /.dockerenv ]]; then
        echo >&2
        echo >&2 "You are not inside the Airflow docker container!"
        echo >&2 "You should only run this script in the Airflow docker container as it may override your files."
        echo >&2 "Learn more about how we develop and test airflow in:"
        echo >&2 "https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst"
        echo >&2
        exit 1
    fi
}

function in_container_script_start() {
    if [[ ${AIRFLOW_CI_VERBOSE:="false"} == "true" ]]; then
        set -x
    fi
}

function in_container_script_end() {
    if [[ ${AIRFLOW_CI_VERBOSE:="false"} == "true" ]]; then
        set +x
    fi
}

function print_in_container_info() {
    if [[ ${AIRFLOW_CI_SILENT:="false"} != "true" ]]; then
        echo "$@"
    fi
}

#
# Cleans up PYC files (in case they come in mounted folders)
#
function in_container_cleanup_pyc() {
    print_in_container_info
    print_in_container_info "Cleaning up .pyc files"
    print_in_container_info
    set +o pipefail
    NUM_FILES=$(sudo find . \
        -path "./airflow/www/node_modules" -prune -o \
        -path "./airflow/www_rbac/node_modules" -prune -o \
        -path "./.eggs" -prune -o \
        -path "./docs/_build" -prune -o \
        -path "./build" -prune -o \
        -name "*.pyc" | grep ".pyc$" | sudo xargs rm -vf | wc -l)
    print_in_container_info "Number of deleted .pyc files: ${NUM_FILES}"
    set -o pipefail
    print_in_container_info
    print_in_container_info
}

#
# Cleans up __pycache__ directories (in case they come in mounted folders)
#
function in_container_cleanup_pycache() {
    print_in_container_info
    print_in_container_info "Cleaning up __pycache__ directories"
    print_in_container_info
    set +o pipefail
    NUM_FILES=$(find . \
        -path "./airflow/www/node_modules" -prune -o \
        -path "./airflow/www_rbac/node_modules" -prune -o \
        -path "./.eggs" -prune -o \
        -path "./docs/_build" -prune -o \
        -path "./build" -prune -o \
        -name "__pycache__" | grep "__pycache__" | sudo xargs rm -rvf | wc -l)
    print_in_container_info "Number of deleted __pycache__ dirs (and files): ${NUM_FILES}"
    set -o pipefail
    print_in_container_info
    print_in_container_info
}

#
# Fixes ownership of files generated in container - if they are owned by root, they will be owned by
# The host user.
#
function in_container_fix_ownership() {
    print_in_container_info
    print_in_container_info "Changing ownership of root-owned files to ${HOST_USER_ID}.${HOST_GROUP_ID}"
    print_in_container_info
    set +o pipefail
    sudo find . -user root | sudo xargs chown -v "${HOST_USER_ID}.${HOST_GROUP_ID}" | wc -l | \
        xargs -n 1 echo "Number of files with changed ownership:"
    set -o pipefail
    print_in_container_info
    print_in_container_info
}

function in_container_go_to_airflow_sources() {
    pushd "${AIRFLOW_SOURCES}"  &>/dev/null || exit 1
    print_in_container_info
    print_in_container_info "Running in $(pwd)"
    print_in_container_info
}

function in_container_basic_sanity_check() {
    assert_in_container
    in_container_go_to_airflow_sources
    in_container_cleanup_pyc
    in_container_cleanup_pycache
}

function in_container_refresh_pylint_todo() {
    print_in_container_info
    print_in_container_info "Refreshing list of all  non-pylint compliant files. This can take some time."
    print_in_container_info

    print_in_container_info
    print_in_container_info "Finding list  all non-pylint compliant files everywhere except 'tests' folder"
    print_in_container_info

    # Using path -prune is much better in the local environment on OSX because we have host
    # Files mounted and node_modules is a huge directory which takes many seconds to even scan
    # -prune works better than -not path because it skips traversing the whole directory. -not path traverses
    # the directory and only excludes it after all of it is scanned
    find . \
        -path "./airflow/www/node_modules" -prune -o \
        -path "./airflow/www_rbac/node_modules" -prune -o \
        -path "./airflow/_vendor" -prune -o \
        -path "./airflow/migrations/versions" -prune -o \
        -path "./.eggs" -prune -o \
        -path "./docs/_build" -prune -o \
        -path "./build" -prune -o \
        -path "./tests" -prune -o \
        -name "*.py" \
        -not -name 'webserver_config.py' | \
        grep  ".*.py$" | \
        xargs pylint | tee "${MY_DIR}/../pylint_todo_main.txt"

    grep -v "\*\*" < "${MY_DIR}/../pylint_todo_main.txt" | \
       grep -v "^$" | grep -v "\-\-\-" | grep -v "^Your code has been" | \
       awk 'BEGIN{FS=":"}{print "./"$1}' | sort | uniq > "${MY_DIR}/../pylint_todo_new.txt"

    print_in_container_info
    print_in_container_info "So far found $(wc -l <"${MY_DIR}/../pylint_todo_new.txt") files"
    print_in_container_info

    print_in_container_info
    print_in_container_info "Finding list of all non-pylint compliant files in 'tests' folder"
    print_in_container_info

    find "./tests" -name "*.py" -print0 | \
        xargs -0 pylint --disable="${DISABLE_CHECKS_FOR_TESTS}" | tee "${MY_DIR}/../pylint_todo_tests.txt"

    grep -v "\*\*" < "${MY_DIR}/../pylint_todo_tests.txt" | \
        grep -v "^$" | grep -v "\-\-\-" | grep -v "^Your code has been" | \
        awk 'BEGIN{FS=":"}{print "./"$1}' | sort | uniq >> "${MY_DIR}/../pylint_todo_new.txt"

    rm -fv "${MY_DIR}/../pylint_todo_main.txt" "${MY_DIR}/../pylint_todo_tests.txt"
    mv -v "${MY_DIR}/../pylint_todo_new.txt" "${MY_DIR}/../pylint_todo.txt"

    print_in_container_info
    print_in_container_info "Found $(wc -l <"${MY_DIR}/../pylint_todo.txt") files"
    print_in_container_info
}

export DISABLE_CHECKS_FOR_TESTS="missing-docstring,no-self-use,too-many-public-methods,protected-access,do-not-use-asserts"

function start_output_heartbeat() {
    MESSAGE=${1:="Still working!"}
    INTERVAL=${2:=10}
    echo
    echo "Starting output heartbeat"
    echo

    bash <<EOF 2> /dev/null &
while true; do
  echo "\$(date): ${MESSAGE} "
  sleep ${INTERVAL}
done
EOF
    export HEARTBEAT_PID=$!
}

function stop_output_heartbeat() {
    kill "${HEARTBEAT_PID}"
}
