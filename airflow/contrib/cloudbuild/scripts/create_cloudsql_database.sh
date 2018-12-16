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

export AIRFLOW_HOME="${AIRFLOW_HOME:=/airflow}"
export AIRFLOW_SOURCES="${AIRFLOW_SOURCES:=/workspace}"
export AIRFLOW_OUTPUT="${AIRFLOW_SOURCES}/output"
export BUILD_ID="${BUILD_ID:=build}"
export AIRFLOW_BREEZE_CONFIG_DIR="${HOME}/airflow-breeze-config"
export AIRFLOW_BREEZE_TEST_SUITES=${AIRFLOW_BREEZE_TEST_SUITES:=""}

for AIRFLOW_BREEZE_TEST_SUITE in ${AIRFLOW_BREEZE_TEST_SUITES}; do
    export AIRFLOW_BREEZE_TEST_SUITE
    # Re-source variables for test suite
    set -a
    source ${AIRFLOW_BREEZE_CONFIG_DIR}/variables.env
    set +a
    echo "Creating CloudSQL database for test suite ${AIRFLOW_BREEZE_TEST_SUITE}"
    python ${AIRFLOW_SOURCES}/tests/contrib/operators/test_gcp_sql_operator.py --action=create
    echo "Created CloudSQL database for test suite ${AIRFLOW_BREEZE_TEST_SUITE}"
done
