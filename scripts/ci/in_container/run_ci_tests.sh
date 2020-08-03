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
# shellcheck source=scripts/ci/in_container/_in_container_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/_in_container_script_init.sh"

echo
echo "Starting the tests with those pytest arguments:" "${@}"
echo
set +e

pytest "${@}"

RES=$?

set +x
if [[ "${RES}" == "0" && ${CI:="false"} == "true" ]]; then
    echo "All tests successful"
    bash <(curl -s https://codecov.io/bash)
fi

MAIN_GITHUB_REPOSITORY="apache/airflow"

if [[ ${ONLY_RUN_QUARANTINED_TESTS:=} = "true" ]]; then
    if [[ ${GITHUB_REPOSITORY} == "${MAIN_GITHUB_REPOSITORY}" ]]; then
        if [[ ${RES} == "1" || ${RES} == "0" ]]; then
            echo
            echo "Pytest exited with ${RES} result. Updating Quarantine Issue!"
            echo
            "${IN_CONTAINER_DIR}/update_quarantined_test_status.py" "${RESULT_LOG_FILE}"
        else
            echo
            echo "Pytest exited with ${RES} result. NOT Updating Quarantine Issue!"
            echo
        fi
    else
        echo
        echo "Github repository '${GITHUB_REPOSITORY}'. NOT Updating Quarantine Issue!"
        echo
    fi
else
    echo
    echo "Regular tests. NOT Updating Quarantine Issue!"
    echo
fi

if [[ ${CI:=} == "true" ]]; then
    send_airflow_logs_to_file_io
fi

exit "${RES}"
