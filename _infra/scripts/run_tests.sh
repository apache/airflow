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

if [ -z "$HADOOP_HOME" ]; then
    echo "HADOOP_HOME not set - abort" >&2
    exit 1
fi

mkdir -p ~/airflow/

CURRENT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOTDIR="$(dirname $(dirname $CURRENT_DIR))"

echo CURRENT_DIR: $CURRENT_DIR
echo ROOTDIR: $ROOTDIR

echo "cp ${CURRENT_DIR}/airflow_ci.cfg ~/airflow/unittests.cfg"
cp -f ${CURRENT_DIR}/airflow_ci.cfg ~/airflow/unittests.cfg
cat ~/airflow/unittests.cfg | sed -e "s/broker_url = amqp:\/\/guest:guest@localhost:5672\//broker_url = redis:\/\/localhost:6379\/0/" > ~/airflow/tmp.cfg
mv ~/airflow/tmp.cfg ~/airflow/unittests.cfg

export AIRFLOW__CORE__DAGS_FOLDER="$ROOTDIR/tests/dags"
export AIRFLOW_HOME=${AIRFLOW_HOME:=~/airflow}
export AIRFLOW__CORE__UNIT_TEST_MODE=True
export AIRFLOW__TESTSECTION__TESTKEY=testvalue
export AIRFLOW_USE_NEW_IMPORTS=1
export PYTHONPATH=$PYTHONPATH:${ROOTDIR}/tests/test_utils

# Generate the `airflow` executable if needed
which airflow > /dev/null || python setup.py develop

echo "Initializing the DB"
yes | airflow resetdb > /dev/null

nose_args=$@
if [ -z "$nose_args" ]; then
  nose_args="--with-coverage \
--cover-erase \
--cover-html \
--cover-package=airflow \
--cover-html-dir=airflow/www/static/coverage \
--with-ignore-docstrings \
--rednose \
--with-timer \
-s \
--logging-level=WARN "
fi

# to parallel
if [ "$JORB_JOB_INDEX" -eq 0 ]; then
  nose_args="${nose_args} -s tests/jobs.py"
elif [ "$JORB_JOB_INDEX" -eq 1 ]; then
  nose_args="${nose_args} -s tests/www_rbac/"
else
  echo "rm some files"
  sudo rm -f $ROOTDIR/tests/jobs.py*
  sudo rm -rf $ROOTDIR/tests/www_rbac/
fi

mysql -D airflow -e "update connection set login = \"root\" where conn_id=\"sftp_default\";"

echo "xxxxxxxxxxxxxxxxxxxxxxxxxxxx Some Test Envs xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
echo "Using ${HADOOP_DISTRO} distribution of Hadoop from ${HADOOP_HOME}"
echo airflow_cfg: "${CURRENT_DIR}/airflow_ci.cfg"
echo Dags_folder: $AIRFLOW__CORE__DAGS_FOLDER
echo Backend: $AIRFLOW__CORE__SQL_ALCHEMY_CONN
echo nose_args: $nose_args
echo "xxxxxxxxxxxxxxxxxxxxxxxxxxxx Some Test Envs xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

echo "Starting the unit tests with the following nose arguments: "$nose_args
nosetests $nose_args
result=$?

ps aux | grep webserver | awk '{ print $2 }' | xargs kill -9

exit $result
