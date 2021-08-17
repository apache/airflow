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
readonly PRINT_INFO_FROM_SCRIPTS

PRE_COMMIT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
readonly PRE_COMMIT_DIR

AIRFLOW_SOURCES=$(cd "${PRE_COMMIT_DIR}/../../../" && pwd);
readonly AIRFLOW_SOURCES

cd "${AIRFLOW_SOURCES}" || exit 1

TMP_FILE=$(mktemp)
readonly TMP_FILE

TMP_OUTPUT=$(mktemp)
readonly TMP_OUTPUT

find  "licenses" -type f -exec echo "  " {} \; | LC_ALL=C sort >>"${TMP_FILE}"

SETUP_CFG_FILE="${AIRFLOW_SOURCES}/setup.cfg"
readonly SETUP_CFG_FILE

lead_marker='^# Start of licenses generated automatically$'
tail_marker='^# End of licences generated automatically$'

beginning_of_generated_help_line_number=$(grep -n "${lead_marker}" <"${SETUP_CFG_FILE}" | sed 's/\(.*\):.*/\1/g')
end_of_generated_help_line_number=$(grep -n "${tail_marker}" <"${SETUP_CFG_FILE}" | sed 's/\(.*\):.*/\1/g')
cat <(head -n "${beginning_of_generated_help_line_number}" "${SETUP_CFG_FILE}") \
    "${TMP_FILE}" \
    <(tail -n +"${end_of_generated_help_line_number}" "${SETUP_CFG_FILE}") \
    >"${TMP_OUTPUT}"

mv "${TMP_OUTPUT}" "${SETUP_CFG_FILE}"
