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

set +x
if [[ "${RES}" == "0" && ${CI:="false"} == "true" ]]; then
    echo "All tests successful"
    cp .coverage /files
elif [[ "${RES}" != "0" ]]; then
    EXTRA_ARGS=""
    if [[ ${BACKEND} == "postgres" ]]; then
        EXTRA_ARGS="--postgres-version ${POSTGRES_VERSION} "
    elif [[ ${BACKEND} == "mysql" ]]; then
        EXTRA_ARGS="--mysql-version ${MYSQL_VERSION} "
    fi

    >&2 echo "***********************************************************************************************"
    >&2 echo "*"
    >&2 echo "* ERROR! Some tests failed, unfortunately. Those might be transient errors,"
    >&2 echo "*        but usually you have to fix something."
    >&2 echo "*        See the above log for details."
    >&2 echo "*"
    >&2 echo "***********************************************************************************************"
    >&2 echo "*  You can easily reproduce the failed tests on your dev machine/"
    >&2 echo "*"
    >&2 echo "*   When you have the source branch checked out locally:"
    >&2 echo "*"
    >&2 echo "*     Run all tests:"
    >&2 echo "*"
    >&2 echo "*       ./breeze --backend ${BACKEND} ${EXTRA_ARGS}--python ${PYTHON_MAJOR_MINOR_VERSION} --db-reset --skip-mounting-local-sources --test-type ${TEST_TYPE}  tests"
    >&2 echo "*"
    >&2 echo "*     Enter docker shell:"
    >&2 echo "*"
    >&2 echo "*       ./breeze --backend ${BACKEND} ${EXTRA_ARGS}--python ${PYTHON_MAJOR_MINOR_VERSION} --db-reset --skip-mounting-local-sources --test-type ${TEST_TYPE}  shell"
    >&2 echo "*"
    if [[ ${GITHUB_REGISTRY_PULL_IMAGE_TAG=} != "" ]]; then
        >&2 echo "*   When you do not have sources:"
        >&2 echo "*"
        >&2 echo "*     Run all tests:"
        >&2 echo "*"
        >&2 echo "*      ./breeze --github-image-id ${GITHUB_REGISTRY_PULL_IMAGE_TAG} --backend ${BACKEND} ${EXTRA_ARGS}--python ${PYTHON_MAJOR_MINOR_VERSION} --db-reset --skip-mounting-local-sources --test-type ${TEST_TYPE} tests"
        >&2 echo "*"
        >&2 echo "*     Enter docker shell:"
        >&2 echo "*"
        >&2 echo "*      ./breeze --github-image-id ${GITHUB_REGISTRY_PULL_IMAGE_TAG} --backend ${BACKEND} ${EXTRA_ARGS}--python ${PYTHON_MAJOR_MINOR_VERSION} --db-reset --skip-mounting-local-sources --test-type ${TEST_TYPE} shell"
        >&2 echo "*"
    fi
    >&2 echo "*"
    >&2 echo "*   NOTE! Once you are in the docker shell, you can run failed test with:"
    >&2 echo "*"
    >&2 echo "*            pytest [TEST_NAME]"
    >&2 echo "*"
    >&2 echo "*   You can copy the test name from the output above"
    >&2 echo "*"
    >&2 echo "***********************************************************************************************"
fi

MAIN_GITHUB_REPOSITORY="apache/airflow"

if [[ ${TEST_TYPE:=} == "Quarantined" ]]; then
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
    fi
fi

if [[ ${CI:=} == "true" ]]; then
    if [[ ${RES} != "0" ]]; then
        echo
        echo "Dumping logs on error"
        echo
        dump_airflow_logs
    fi
fi

exit "${RES}"
