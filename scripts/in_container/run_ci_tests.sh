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
# shellcheck source=scripts/in_container/_in_container_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/_in_container_script_init.sh"

echo
echo "Starting the tests with those pytest arguments:" "${@}"
echo
set +e

pytest "${@}" | python "$( dirname "${BASH_SOURCE[0]}" )/filter_out_warnings.py"
RES=$?

if [[ -f ${WARNINGS_FILE} ]]; then
    echo "Number of warnings: $(wc -l "${WARNINGS_FILE}")"
fi


if [[ ${RES} == "139" ]]; then
    echo "${COLOR_YELLOW}Sometimes Pytest fails at exiting with segfault, but all tests actually passed${COLOR_RESET}"
    echo "${COLOR_YELLOW}We should ignore such case. Checking if junitxml file ${RESULT_LOG_FILE} is there with 0 errors and failures${COLOR_RESET}"
    if [[ -f ${RESULT_LOG_FILE} ]]; then
        python "${AIRFLOW_SOURCES}/scripts/in_container/check_junitxml_result.py" "${RESULT_LOG_FILE}"
        RES=$?
    else
        echo "${COLOR_YELLOW}JunitXML file ${RESULT_LOG_FILE} does not exist. Proceeding with failure as we cannot check if there were no failures.${COLOR_RESET}"
    fi
fi

set +x
if [[ "${RES}" == "0" && ( ${CI:="false"} == "true" || ${CI} == "True" ) ]]; then
    echo "All tests successful"
    cp .coverage /files
fi

MAIN_GITHUB_REPOSITORY="apache/airflow"

if [[ ${TEST_TYPE:=} == "Quarantined" ]]; then
    if [[ ${GITHUB_REPOSITORY=} == "${MAIN_GITHUB_REPOSITORY}" ]]; then
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
    fi
fi

if [[ ${CI:="false"} == "true" || ${CI} == "True" ]]; then
    if [[ ${RES} != "0" ]]; then
        echo
        echo "Dumping logs on error"
        echo
        dump_airflow_logs
    fi
fi

exit "${RES}"
