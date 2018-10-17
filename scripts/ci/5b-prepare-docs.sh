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

set -o verbose

pwd

echo "Using travis airflow.cfg"
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cp -f ${DIR}/airflow_travis.cfg ~/unittests.cfg

ROOTDIR="$(dirname $(dirname ${DIR}))"
export AIRFLOW__CORE__DAGS_FOLDER="$ROOTDIR/tests/dags"

# add test/contrib to PYTHONPATH
export PYTHONPATH=${PYTHONPATH:-$ROOTDIR/tests/test_utils}

# environment
export AIRFLOW_HOME=${AIRFLOW_HOME:=${HOME}}
export AIRFLOW__CORE__UNIT_TEST_MODE=True

# configuration test
export AIRFLOW__TESTSECTION__TESTKEY=testvalue

# any argument received is overriding the default nose execution arguments:
nose_args=$@

# Generate the `airflow` executable if needed
which airflow > /dev/null || python setup.py develop

# For impersonation tests on Travis, make airflow accessible to other users via the global PATH
# (which contains /usr/local/bin)
sudo ln -sf "${VIRTUAL_ENV}/bin/airflow" /usr/local/bin/

# kdc init happens in setup_kdc.sh
kinit -kt ${KRB5_KTNAME} airflow

# For impersonation tests running on SQLite on Travis, make the database world readable so other
# users can update it
AIRFLOW_DB="$HOME/airflow.db"

if [ -f "${AIRFLOW_DB}" ]; then
  chmod a+rw "${AIRFLOW_DB}"
  chmod g+rwx "${AIRFLOW_HOME}"
fi

cd /app/docs
./build.sh

