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

if [[ ${STORED_SETUP_PY_HASH} != "${CURRENT_SETUP_PY_HASH}" ]]; then
    echo
    echo "Setup.py changed since last time requirements were generated"
    echo
    echo "Switching to eager update strategy for the requirements"
    echo
    UPGRADE_WHILE_GENERATING_REQUIREMENTS="true"
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
# Fail in case diff shows difference
diff --color=always "${OLD_REQUIREMENTS_FILE}" "${GENERATED_REQUIREMENTS_FILE}"
RES=$?

if [[ ${FAIL_WHEN_REQUIREMENTS_UPDATED:="true"} == "true" ]]; then
    if [[ ${RES} != "0" && ${SHOW_GENERATE_REQUIREMENTS_INSTRUCTIONS:=} == "true" ]]; then
        echo
        echo " ERROR! Requirements need to be updated!"
        echo
        echo "     Please generate requirements with:"
        echo
        echo "           breeze generate-requirements --python ${PYTHON_MAJOR_MINOR_VERSION}"
        echo
    fi
    exit "${RES}"
else
    if [[ ${RES} != "0"  && ${SHOW_GENERATE_REQUIREMENTS_INSTRUCTIONS:=} == "true" ]]; then
        echo
        echo " WARNING! Above are the updated requirements for Airflow "
        echo
        echo "     Next time when you update setup.py, you will need to run"
        echo
        echo "           breeze generate-requirements --python ${PYTHON_MAJOR_MINOR_VERSION}"
        echo
        echo " Or you can run it now and make a separate 'requirements update' commit."
        echo
    fi
    exit 0
fi
