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
# Bash sanity settings (error on exit, complain for undefined vars, error when pipe fails)
set -euo pipefail

MY_DIR=$(cd "$(dirname "$0")" || exit 1; pwd)

# shellcheck source=scripts/ci/in_container/_in_container_utils.sh
. "${MY_DIR}/_in_container_utils.sh"

in_container_basic_sanity_check

in_container_script_start

# You can run the system tests against any version of airflow - it will be reinstalled if
# different than current
export SYSTEM_TEST_AIRFLOW_VERSION=${SYSTEM_TEST_AIRFLOW_VERSION:="current"}

if [[ ${SYSTEM_TEST_AIRFLOW_VERSION} == *1.10* ]]; then
    export RUN_TESTS_FOR_AIRFLOW_1_10="true"
fi

cd "${AIRFLOW_SOURCES}"

if [[ ${SYSTEM_TEST_AIRFLOW_VERSION} == "current" ]]; then
    pip install -e .
    export PYTHONPATH="${AIRFLOW_SOURCES}"
else
    install_old_airflow_version "${SYSTEM_TEST_AIRFLOW_VERSION}"
fi

# any argument received is overriding the default nose execution arguments:
PYTEST_ARGS=( "$@" )

echo
echo "Starting the tests with those pytest arguments: ${PYTEST_ARGS[*]}"
echo
set +e

pytest "${PYTEST_ARGS[@]}"

RES=$?

set +x
if [[ "${RES}" == "0" && ${CI} == "true" ]]; then
    echo "All tests successful"
fi

if [[ ${CI} == "true" ]]; then
    send_docker_logs_to_file_io
    send_airflow_logs_to_file_io
fi

in_container_script_end

exit "${RES}"
