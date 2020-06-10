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

MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TMP_FILE=$(mktemp)
TMP_OUTPUT=$(mktemp)

cd "${MY_DIR}/../../" || exit;

find  "licenses" -type f -exec echo "  " {} \; | LC_ALL=C sort >>"${TMP_FILE}"

SETUP_CFG_FILE="${MY_DIR}/../../setup.cfg"

LEAD='^# Start of licenses generated automatically$'
TAIL='^# End of licences generated automatically$'

BEGIN_GEN=$(grep -n "${LEAD}" <"${SETUP_CFG_FILE}" | sed 's/\(.*\):.*/\1/g')
END_GEN=$(grep -n "${TAIL}" <"${SETUP_CFG_FILE}" | sed 's/\(.*\):.*/\1/g')
cat <(head -n "${BEGIN_GEN}" "${SETUP_CFG_FILE}") \
    "${TMP_FILE}" \
    <(tail -n +"${END_GEN}" "${SETUP_CFG_FILE}") \
    >"${TMP_OUTPUT}"

mv "${TMP_OUTPUT}" "${SETUP_CFG_FILE}"
