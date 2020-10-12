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
# shellcheck source=scripts/in_container/_in_container_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/_in_container_script_init.sh"

add_trap "in_container_fix_ownership" EXIT HUP INT TERM

CONSTRAINTS_DIR="/files/constraints-${PYTHON_MAJOR_MINOR_VERSION}"

LATEST_CONSTRAINT_FILE="${CONSTRAINTS_DIR}/original-constraints-${PYTHON_MAJOR_MINOR_VERSION}.txt"
CURRENT_CONSTRAINT_FILE="${CONSTRAINTS_DIR}/constraints-${PYTHON_MAJOR_MINOR_VERSION}.txt"

mkdir -pv "${CONSTRAINTS_DIR}"

CONSTRAINTS_LOCATION="https://raw.githubusercontent.com/apache/airflow/${DEFAULT_CONSTRAINTS_BRANCH}/constraints-${PYTHON_MAJOR_MINOR_VERSION}.txt"
readonly CONSTRAINTS_LOCATION

curl "${CONSTRAINTS_LOCATION}" --output "${LATEST_CONSTRAINT_FILE}"

echo
echo "Freezing constraints to ${CURRENT_CONSTRAINT_FILE}"
echo

pip freeze | sort | \
    grep -v "apache_airflow" | \
    grep -v "/opt/airflow" >"${CURRENT_CONSTRAINT_FILE}"

echo
echo "Constraints generated in ${CURRENT_CONSTRAINT_FILE}"
echo

set +e
diff --color=always "${LATEST_CONSTRAINT_FILE}" "${CURRENT_CONSTRAINT_FILE}"

exit 0
