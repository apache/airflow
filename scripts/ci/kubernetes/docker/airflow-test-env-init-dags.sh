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

set -euo pipefail

echo
echo "Copying airflow dags"
echo


# Create DAGS folder if it does not exist
mkdir -pv "${AIRFLOW_HOME}/dags"
ls -la "${AIRFLOW_HOME}/dags/"
rm -rvf "${AIRFLOW_HOME}/dags/*"

# Copy DAGS from current sources
cp -Rv "${AIRFLOW_SOURCES}"/airflow/example_dags/* "${AIRFLOW_HOME}/dags/"

echo
echo "Copied airflow dags"
echo
