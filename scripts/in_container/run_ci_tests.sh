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

pytest "${@}"
RES=$?

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

if [[ ${CI:="false"} == "true" && ${RES} != "0" && ${USE_AIRFLOW_VERSION=} != "" ]]; then
    echo
    echo "${COLOR_YELLOW}Failing compatibility test of providers for for ${USE_AIRFLOW_VERSION} Airflow and you need to make sure it passes for it as well or deal with compatibility.${COLOR_RESET}"
    echo
    echo "${COLOR_BLUE}Read more on how to run the test locally and how to deal with Provider's compatibility with older Airflow versions at:${COLOR_RESET}"
    echo "https://github.com/apache/airflow/blob/main/contributing-docs/testing/unit_tests.rst#compatibility-provider-unit-tests-against-older-airflow-releases"
    echo
fi

if [[ ${CI:="false"} == "true" && ${RES} != "0" && ${FORCE_LOWEST_DEPENDENCIES=} == "true" ]]; then
    echo
    echo "${COLOR_YELLOW}Failing 'lowest-direct-dependencies' test. You need to make sure to set proper lower bounds in hatch_build.py or corresponding provider.yaml.${COLOR_RESET}"
    echo
    echo "${COLOR_BLUE}Read more on how to run the 'lowest-direct-dependencies' locally and how to solve problems:${COLOR_RESET}"
    echo "https://github.com/apache/airflow/blob/main/contributing-docs/testing/unit_tests.rst#lowest-direct-dependency-resolution-tests"
    echo
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
