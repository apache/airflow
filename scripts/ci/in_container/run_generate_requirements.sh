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

# adding trap to exiting trap
HANDLERS="$( trap -p EXIT | cut -f2 -d \' )"
# shellcheck disable=SC2064
trap "${HANDLERS}${HANDLERS:+;}in_container_fix_ownership" EXIT

STORED_SETUP_PY_HASH_FILE="${AIRFLOW_SOURCES}/requirements/setup-${PYTHON_MAJOR_MINOR_VERSION}.md5"

CURRENT_SETUP_PY_HASH=$(md5sum "${AIRFLOW_SOURCES}/setup.py")
STORED_SETUP_PY_HASH=$(cat "${STORED_SETUP_PY_HASH_FILE}" 2>/dev/null || true)

if [[ ${STORED_SETUP_PY_HASH} != "${CURRENT_SETUP_PY_HASH}" && ${CHECK_REQUIREMENTS_ONLY:=} == "true" ]]; then
    echo
    echo "ERROR! Setup.py changed since last time requirements were generated"
    echo
    echo "     When you update setup.py, you have to run the following command and commit the change:"
    echo
    echo "           breeze generate-requirements --python ${PYTHON_MAJOR_MINOR_VERSION}"
    echo
    echo
    exit 1
fi

# Upgrading requirements will happen only in CRON job to see that we have some
# new requirements released
if [[ ${UPGRADE_WHILE_GENERATING_REQUIREMENTS} == "true" ]]; then
    echo
    echo "Upgrading requirements to latest ones"
    echo
    pip install -e ".[${AIRFLOW_EXTRAS}]" --upgrade --upgrade-strategy eager
fi

OLD_REQUIREMENTS_FILE="/tmp/requirements-python${PYTHON_MAJOR_MINOR_VERSION}.txt"
GENERATED_REQUIREMENTS_FILE="${AIRFLOW_SOURCES}/requirements/requirements-python${PYTHON_MAJOR_MINOR_VERSION}.txt"

echo
echo "Copying requirements ${GENERATED_REQUIREMENTS_FILE} -> ${OLD_REQUIREMENTS_FILE}"
echo
cp "${GENERATED_REQUIREMENTS_FILE}" "${OLD_REQUIREMENTS_FILE}"

echo
echo "Freezing requirements to ${GENERATED_REQUIREMENTS_FILE}"
echo

pip freeze | sort | \
    grep -v "apache_airflow" | \
    grep -v "/opt/airflow" >"${GENERATED_REQUIREMENTS_FILE}"

echo
echo "Requirements generated in ${GENERATED_REQUIREMENTS_FILE}"
echo

echo
echo "Storing setup.py hash in ${STORED_SETUP_PY_HASH_FILE}"
echo
echo "${CURRENT_SETUP_PY_HASH}" > "${STORED_SETUP_PY_HASH_FILE}"

set +e
diff --color=always "${OLD_REQUIREMENTS_FILE}" "${GENERATED_REQUIREMENTS_FILE}"

exit 0
