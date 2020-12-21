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

export PRINT_INFO_FROM_SCRIPTS="false"

# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"

cd "${AIRFLOW_SOURCES}"

. ./breeze-complete

PRE_COMMIT_CONFIG_FILE="${AIRFLOW_SOURCES}/.pre-commit-config.yaml"
readonly PRE_COMMIT_CONFIG_FILE

STATIC_CODE_CHECKS_FILE="${AIRFLOW_SOURCES}/STATIC_CODE_CHECKS.rst"

all_pre_commits=$(grep "id:" <"${PRE_COMMIT_CONFIG_FILE}" | sort |uniq | awk '{ print $3 }')

error="false"
for pre_commit in ${all_pre_commits}
do
    if ! grep -q "${pre_commit}" "${STATIC_CODE_CHECKS_FILE}"; then
        error="true"
        echo
        echo """
${COLOR_RED_ERROR} Pre-commit ${pre_commit} is not described in ${STATIC_CODE_CHECKS_FILE}
ERROR: Pre-commit ${pre_commit} is not described in ${STATIC_CODE_CHECKS_FILE}

FIX: Please add ${pre_commit} in the table in the 'Pre-commit hooks' chapter in ${STATIC_CODE_CHECKS_FILE}
${COLOR_RESET}
"""
        echo
    fi
    # shellcheck disable=SC2154
    if [[ ! ${_breeze_allowed_static_checks} == *${pre_commit}* ]]; then
        error="true"
        echo """
${COLOR_RED_ERROR}: Pre-commit ${pre_commit} is missing in _breeze_allowed_static_checks variable in breeze-complete

FIX: Please add ${pre_commit} in the table in the '_breeze_allowed_static_checks' constant in ${AIRFLOW_SOURCES}/breeze-complete
${COLOR_RESET}
"""
    fi
done

if [[ ${error} == "true" ]]; then
    echo
    echo  "${COLOR_RED_ERROR} Some pre-commits are not synchronized! Please fix the errors above!  ${COLOR_RESET}"
    echo
    exit 1
else
    echo
    echo "${COLOR_GREEN_OK} All pre-commits are synchronized!  ${COLOR_RESET}"
    echo
fi
