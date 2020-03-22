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
# shellcheck source=scripts/ci/in_container/_in_container_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/_in_container_script_init.sh"

if [[ ${UPGRADE_WHILE_GENERATING_REQUIREMENTS} == "true" ]]; then
    echo
    echo "Upgrading requirements"
    echo
    pip install -e ".[${AIRFLOW_EXTRAS}]" --upgrade
fi

echo
echo "Freezing requirements"
echo

pip freeze | \
    grep -v "apache_airflow" | \
    grep -v "/opt/airflow" >"${AIRFLOW_SOURCES}/requirements.txt"

echo
echo "Requirements generated in requirements.txt"
echo

in_container_fix_ownership
