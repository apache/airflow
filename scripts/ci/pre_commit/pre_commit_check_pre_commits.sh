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

. breeze-complete

PRE_COMMIT_CONFIG_FILE="${AIRFLOW_SOURCES}/.pre-commit-config.yaml"
readonly PRE_COMMIT_CONFIG_FILE

STATIC_CODE_CHECKS_FILE="${AIRFLOW_SOURCES}/STATIC_CODE_CHECKS.rst"

all_pre_commits=$(grep "id:" <"${PRE_COMMIT_CONFIG_FILE}" | sort |uniq | awk '{ print $3 }')

error="false"
for pre_commit in ${all_pre_commits}
do
    if ! grep -q "${pre_commit}" "${STATIC_CODE_CHECKS_FILE}"; then
        error="true"
        >&2 echo
        >&2 echo "ERROR: Pre-commit ${pre_commit} is not described in ${STATIC_CODE_CHECKS_FILE}"
        >&2 echo
        >&2 echo "FIX: Please add ${pre_commit} in the table in the 'Pre-commit hooks' chapter in ${STATIC_CODE_CHECKS_FILE}"
        >&2 echo
    fi
    if [[ ! ${_BREEZE_ALLOWED_STATIC_CHECKS} == *${pre_commit}* ]]; then
        error="true"
        >&2 echo
        >&2 echo "ERROR: Pre-commit ${pre_commit} is missing in _BREEZE_ALLOWED_STATIC_CHECKS variable in breeze-complete"
        >&2 echo
        >&2 echo "FIX: Please add ${pre_commit} in the table in the '_BREEZE_ALLOWED_STATIC_CHECKS' constant in ${AIRFLOW_SOURCES}/breeze-complete"
        >&2 echo
    fi
done

if [[ ${error} == "true" ]]; then
  >&2 echo
  >&2 echo "Some pre-commits are not synchronized! Please fix the errors above!"
  >&2 echo
  exit 1
else
  echo
  echo "All pre-commits are synchronized!"
  echo
fi
