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

set -o verbose
set -euo pipefail
set -x

MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

pwd

AIRFLOW_ROOT="$(cd ${MY_DIR}; pwd)"
export AIRFLOW__CORE__DAGS_FOLDER="S{AIRFLOW_ROOT}/tests/dags"

# add test/contrib to PYTHONPATH
export PYTHONPATH=${PYTHONPATH:-${AIRFLOW_ROOT}/tests/test_utils}

# environment
export AIRFLOW_HOME=${AIRFLOW_HOME:=${HOME}}

echo Airflow home: ${AIRFLOW_HOME}

export AIRFLOW__CORE__UNIT_TEST_MODE=True

# add test/test_utils to PYTHONPATH TODO: Do we need that ??? Looks fishy.
export PYTHONPATH=${PYTHONPATH}:${MY_DIR}/tests/test_utils

# any argument received is overriding the default nose execution arguments:
NOSE_ARGS=$@

echo "Initializing the DB"
yes | airflow initdb || true
yes | airflow resetdb || true

if [[ -z "${NOSE_ARGS}" ]]; then
  NOSE_ARGS="--with-coverage \
  --cover-erase \
  --cover-html \
  --cover-package=airflow \
  --cover-html-dir=airflow/www/static/coverage \
  --with-ignore-docstrings \
  --rednose \
  --with-timer \
  -v \
  --logging-level=DEBUG"
fi

# For impersonation tests running on SQLite on Travis, make the database world readable so other
# users can update it
AIRFLOW_DB="${HOME}/airflow.db"

if [[ -f "${AIRFLOW_DB}" ]]; then
  chmod a+rw "${AIRFLOW_DB}"
  chmod g+rwx "${AIRFLOW_HOME}"
fi

echo "Starting the tests with the following nose arguments: ${NOSE_ARGS}"
nosetests ${NOSE_ARGS}

# To run individual tests:
# nosetests tests.core:CoreTest.test_scheduler_job
