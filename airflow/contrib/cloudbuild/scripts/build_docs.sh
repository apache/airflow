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
export AIRFLOW_BREEZE_TEST_SUITE="${AIRFLOW_BREEZE_TEST_SUITE:=docs}"
export BUILD_ID="${BUILD_ID:=build}"

export DOC_SOURCES_DIR=${DOC_SOURCES_DIR:=${AIRFLOW_SOURCES}/docs}
export DOC_OUTPUT_DIR=${AIRFLOW_OUTPUT}/${BUILD_ID}/docs

pushd ${DOC_SOURCES_DIR}

./build.sh

mkdir -pv ${DOC_OUTPUT_DIR}
rm -rvf ${DOC_OUTPUT_DIR}
cp -rv ${DOC_SOURCES_DIR}/_build/html/ ${DOC_OUTPUT_DIR}

popd
