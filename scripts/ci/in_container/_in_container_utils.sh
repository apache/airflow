#!/usr/bin/env bash
#
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

#
# Asserts that you are actually in container
#
function assert_in_container() {
    if [[ ! -f /.dockerenv ]]; then
        echo >&2
        echo >&2 "You are not inside the Airflow docker container!"
        echo >&2 "You should only run this script in the Airflow docker container as it may override your files."
        echo >&2 "Learn more about how we develop and test airflow in:"
        echo >&2 "https://github.com/apache/airflow/blob/master/CONTRIBUTING.md"
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

#
# Cleans up PYC files (in case they come in mounted folders)
#
function in_container_cleanup_pyc() {
    echo
    echo "Cleaning up .pyc files"
    echo
    set +o pipefail
    sudo find . \
        -path "./airflow/www/node_modules" -prune -o \
        -path "./.eggs" -prune -o \
        -path "./docs/_build" -prune -o \
        -path "./build" -prune -o \
        -name "*.pyc" | grep ".pyc$" | sudo xargs rm -vf | wc -l | \
        xargs -n 1 echo "Number of deleted .pyc files:"
    set -o pipefail
    echo
    echo
}

#
# Cleans up __pycache__ directories (in case they come in mounted folders)
#
function in_container_cleanup_pycache() {
    echo
    echo "Cleaning up __pycache__ directories"
    echo
    set +o pipefail
    sudo find . \
        -path "./airflow/www/node_modules" -prune -o \
        -path "./.eggs" -prune -o \
        -path "./docs/_build" -prune -o \
        -path "./build" -prune -o \
        -name "__pycache__" | grep "__pycache__" | sudo xargs rm -rvf | wc -l | \
        xargs -n 1 echo "Number of deleted __pycache__ dirs (and files):"
    set -o pipefail
    echo
    echo
}

#
# Fixes ownership of files generated in container - if they are owned by root, they will be owned by
# The host user.
#
function in_container_fix_ownership() {
    echo
    echo "Changing ownership of root-owned files to ${HOST_USER_ID}.${HOST_GROUP_ID}"
    echo
    set +o pipefail
    sudo find . -user root | sudo xargs chown -v "${HOST_USER_ID}.${HOST_GROUP_ID}" | wc -l | \
        xargs -n 1 echo "Number of files with changed ownership:"
    set -o pipefail
    echo
    echo
}

function in_container_go_to_airflow_sources() {
    pushd "${AIRFLOW_SOURCES}"  &>/dev/null || exit 1
    echo
    echo "Running in $(pwd)"
    echo
}

function in_container_basic_sanity_check() {
    assert_in_container
    in_container_go_to_airflow_sources
    in_container_cleanup_pyc
    in_container_cleanup_pycache
}
