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

if [ -z "$HADOOP_HOME" ]; then
    echo "HADOOP_HOME not set - abort" >&2
    exit 1
fi

echo "Using ${HADOOP_DISTRO} distribution of Hadoop from ${HADOOP_HOME}"

pwd

mkdir ~/airflow/

if [ "${TRAVIS}" ]; then
    echo "Using travis airflow.cfg"
    DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    cp -f ${DIR}/airflow_travis.cfg ~/airflow/unittests.cfg

    ROOTDIR="$(dirname $(dirname $DIR))"
    export AIRFLOW__CORE__DAGS_FOLDER="$ROOTDIR/tests/dags"

    # kdc init happens in setup_kdc.sh
    kinit -kt ${KRB5_KTNAME} airflow
fi

echo Backend: $AIRFLOW__CORE__SQL_ALCHEMY_CONN
./run_unit_tests.sh $@
