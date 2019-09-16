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

# Script to run Pylint on test code. Can be started from any working directory
set -uo pipefail

MY_DIR=$(cd "$(dirname "$0")" || exit 1; pwd)

# shellcheck source=scripts/ci/in_container/_in_container_utils.sh
. "${MY_DIR}/_in_container_utils.sh"

in_container_basic_sanity_check

in_container_script_start

DISABLE_CHECKS="missing-docstring,no-self-use,too-many-public-methods,protected-access"

if [[ ${#@} == "0" ]]; then
    echo
    echo "Running pylint for 'tests' folder"
    echo
    find "./tests" -name "*.py" | \
    grep -vFf scripts/ci/pylint_todo.txt | \
    xargs pylint --disable="${DISABLE_CHECKS}" --output-format=colorized
    RES=$?
else
    print_in_container_info
    print_in_container_info "Running Pylint for tests with parameters: $*"
    print_in_container_info
    pylint --disable="${DISABLE_CHECKS}" --output-format=colorized "$@"
    RES=$?
fi

in_container_script_end

if [[ "${RES}" != 0 ]]; then
    echo >&2
    echo >&2 "There were some pylint errors. Exiting"
    echo >&2
    exit 1
else
    echo
    echo "Pylint check succeeded"
    echo
fi
