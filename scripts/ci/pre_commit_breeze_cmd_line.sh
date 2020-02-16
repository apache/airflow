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

echo "
.. code-block:: text
" >"${TMP_FILE}"

export SEPARATOR_WIDTH=100
export AIRFLOW_CI_SILENT="true"
./breeze --help | sed 's/^/  /' | sed 's/ *$//' >>"${TMP_FILE}"

BREEZE_RST_FILE="${MY_DIR}/../../BREEZE.rst"

LEAD='^ \.\. START BREEZE HELP MARKER$'
TAIL='^ \.\. END BREEZE HELP MARKER$'

BEGIN_GEN=$(grep -n "${LEAD}" <"${BREEZE_RST_FILE}" | sed 's/\(.*\):.*/\1/g')
END_GEN=$(grep -n "${TAIL}" <"${BREEZE_RST_FILE}" | sed 's/\(.*\):.*/\1/g')
cat <(head -n "${BEGIN_GEN}" "${BREEZE_RST_FILE}") \
    "${TMP_FILE}" \
    <(tail -n +"${END_GEN}" "${BREEZE_RST_FILE}") \
    >"${TMP_OUTPUT}"

mv "${TMP_OUTPUT}" "${BREEZE_RST_FILE}"
