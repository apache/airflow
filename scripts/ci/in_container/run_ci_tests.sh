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

set -euo pipefail

MY_DIR=$(cd "$(dirname "$0")"; pwd)

if [[ ${AIRFLOW_CI_VERBOSE:="false"} == "true" ]]; then
    set -x
fi

if [[ -z "${HADOOP_HOME:=}" ]]; then
    echo "HADOOP_HOME not set - abort" >&2
    exit 1
fi

export HADOOP_HOME

if [[ -z "${AIRFLOW__CORE__SQL_ALCHEMY_CONN:=}" ]]; then
    echo "AIRFLOW__CORE__SQL_ALCHEMY_CONN not set - abort" >&2
    exit 2
fi

echo "Using ${HADOOP_DISTRO:=} distribution of Hadoop from ${HADOOP_HOME}"

export HADOOP_DISTRO

pwd

echo "Using CI airflow.cfg"

cp -f ${MY_DIR}/airflow_ci.cfg ~/unittests.cfg

AIRFLOW_ROOT="$(cd ${MY_DIR}; cd ../../..; pwd)"
export AIRFLOW__CORE__DAGS_FOLDER="${AIRFLOW_ROOT}/tests/dags"

# add test/contrib to PYTHONPATH
export PYTHONPATH=${PYTHONPATH:-${AIRFLOW_ROOT}/tests/test_utils}

echo Backend connection: ${AIRFLOW__CORE__SQL_ALCHEMY_CONN}

# environment
export AIRFLOW_HOME=${AIRFLOW_HOME:=${HOME}}

echo Airflow home: ${AIRFLOW_HOME}

export AIRFLOW__CORE__UNIT_TEST_MODE=True

# add test/test_utils to PYTHONPATH TODO: Do we need that ??? Looks fishy.
export PYTHONPATH=${PYTHONPATH}:${MY_DIR}/tests/test_utils

# any argument received is overriding the default nose execution arguments:
NOSE_ARGS=$@

which airflow 

# Fix codecov build path
# TODO: Check this - this should be made travis-independent
if [[ ! -h /home/travis/build/apache/airflow ]]; then
  sudo mkdir -p /home/travis/build/apache
  sudo ln -s ${AIRFLOW_ROOT} /home/travis/build/apache/airflow
fi

if [[ -z "${KUBERNETES_VERSION:=}" ]]; then
  echo "Initializing the DB"
  yes | airflow initdb || true
  yes | airflow resetdb || true
fi

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
    --logging-level=INFO"
    echo
    echo "Running ALL Tests"
    echo
else
    echo
    echo "Running tests with ${NOSE_ARGS}"
    echo
fi

if [[ -z "${KUBERNETES_VERSION}" ]]; then
    if [[ -z "${KRB5_KTNAME:=}" ]]; then
        echo "KRB5_KTNAME not set - abort" >&2
        exit 3
    fi
  # kdc init happens in setup_kdc.sh
  kinit -kt "${KRB5_KTNAME}" airflow
fi

# For impersonation tests running on SQLite on Travis, make the database world readable so other
# users can update it
AIRFLOW_DB="${HOME}/airflow.db"

if [[ -f "${AIRFLOW_DB}" ]]; then
  chmod a+rw "${AIRFLOW_DB}"
  chmod g+rwx "${AIRFLOW_HOME}"
fi

echo "Starting the tests with the following nose arguments: ${NOSE_ARGS}"

set +
nosetests ${NOSE_ARGS}
