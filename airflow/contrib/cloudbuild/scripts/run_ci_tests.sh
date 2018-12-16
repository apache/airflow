#!/usr/bin/env bash
#
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

set -euo pipefail
set -x

MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# environment
export AIRFLOW_HOME="${AIRFLOW_HOME:=/airflow}"
export AIRFLOW_SOURCES="${AIRFLOW_SOURCES:=/workspace}"
export AIRFLOW_OUTPUT="${AIRFLOW_SOURCES}/output"
export AIRFLOW_BREEZE_TEST_SUITE="${AIRFLOW_BREEZE_TEST_SUITE:=none}"
export BUILD_ID="${BUILD_ID:=build}"

# Force unit tests mode
export AIRFLOW__CORE__UNIT_TEST_MODE=True

export TEST_OUTPUT_DIR=${AIRFLOW_OUTPUT}/${BUILD_ID}/tests
export LOG_OUTPUT_DIR=/logs/

mkdir -pv ${AIRFLOW_HOME}/logs
rm -rvf ${AIRFLOW_HOME}/logs/*
mkdir -pv ${LOG_OUTPUT_DIR}
rm -rvf ${LOG_OUTPUT_DIR}/*

# add test/contrib to PYTHONPATH
export PYTHONPATH=${PYTHONPATH:=""}:${AIRFLOW_SOURCES}/tests/test_utils

# Generate the `airflow` executable if needed
which airflow > /dev/null || python setup.py develop

echo "Initializing the DB"
yes | airflow initdb || true
yes | airflow resetdb || true

rm -rfv ${TEST_OUTPUT_DIR}/${AIRFLOW_BREEZE_TEST_SUITE}-*.xml

pushd ${AIRFLOW_SOURCES}

FAILED="false"

export TEST_SUITE_FAILURE_FILE=${TEST_OUTPUT_DIR}/${AIRFLOW_BREEZE_TEST_SUITE}-failure.txt
rm -f ${TEST_SUITE_FAILURE_FILE}

for MODULE_TO_TEST in ${AIRFLOW_BREEZE_CI_TEST_MODULES:=""}; do
    if [[ ${MODULE_TO_TEST} == "" ]]; then
       continue
    fi
    echo "Running tests for '" ${MODULE_TO_TEST} "'"
    export XUNIT_FILE=${TEST_OUTPUT_DIR}/${AIRFLOW_BREEZE_TEST_SUITE}-${MODULE_TO_TEST}.xml
    export TEST_FAILURE_FILE=${TEST_OUTPUT_DIR}/${AIRFLOW_BREEZE_TEST_SUITE}-${MODULE_TO_TEST}-failure.txt

    mkdir -pv $(dirname ${XUNIT_FILE})
    rm -fv ${XUNIT_FILE} ${XUNIT_FILE}.html

    mkdir -pv $(dirname ${TEST_FAILURE_FILE})
    rm -fv ${TEST_FAILURE_FILE}

    NOSE_ARGS="${MODULE_TO_TEST}"

    # Add coverage if all tests are run
    if [[ "${MODULE_TO_TEST}" == "." ]]; then
        NOSE_ARGS="--with-coverage \
        --cover-erase \
        --cover-html \
        --cover-package=airflow \
        --cover-html-dir=${AIRFLOW_SOURCES}/airflow/www/static/coverage"
    fi

    # Add common parameters to nose
    NOSE_ARGS="${NOSE_ARGS} \
    --with-xunit \
    --xunit-file=${XUNIT_FILE} \
    --xunit-testsuite-name=${AIRFLOW_BREEZE_TEST_SUITE} \
    --with-ignore-docstrings \
    --rednose \
    --with-timer \
    -v \
    --logging-level=DEBUG "

    echo "Starting the unit tests with the following nose arguments: "${NOSE_ARGS}


    # We do not fail if the tests fail as we want to do post-processing and
    # send results anyway, but we mark the test as failed
    set +e

    nosetests ${NOSE_ARGS}

    if [[ $? != 0 ]]; then
        touch ${TEST_FAILURE_FILE}
        FAILED="true"
    fi
    set -e
done

if [[ "${FAILED}" == "true" ]]; then
    touch ${TEST_SUITE_FAILURE_FILE}
fi

cp -rv ${AIRFLOW_HOME}/logs/* ${LOG_OUTPUT_DIR}

popd
