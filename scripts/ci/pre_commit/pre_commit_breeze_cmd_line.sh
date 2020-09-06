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

PRE_COMMIT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
AIRFLOW_SOURCES=$(cd "${PRE_COMMIT_DIR}/../../../" && pwd);
cd "${AIRFLOW_SOURCES}" || exit 1
export PRINT_INFO_FROM_SCRIPTS="false"
export SKIP_CHECK_REMOTE_IMAGE="true"


# For Pre-commits run in non-interactive shell so aliases do not work for them we need to add
# local echo script to path so that it can be silenced.
PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd ):${PATH}"
export PATH


TMP_FILE=$(mktemp)
TMP_OUTPUT=$(mktemp)

echo "
.. code-block:: text
" >"${TMP_FILE}"

export MAX_SCREEN_WIDTH=100
export FORCE_SCREEN_WIDTH="true"
export VERBOSE="false"

./breeze help-all | sed 's/^/  /' | sed 's/ *$//' >>"${TMP_FILE}"

MAX_LEN=$(awk '{ print length($0); }' "${TMP_FILE}" | sort -n | tail -1 )

# 2 spaces added in front of the width for .rst formatting
if (( MAX_LEN > MAX_SCREEN_WIDTH + 2 )); then
    cat "${TMP_FILE}"
    echo
    echo "ERROR! Some lines in generate breeze help-all command are too long. See above ^^"
    echo
    echo
    exit 1
fi

BREEZE_RST_FILE="${AIRFLOW_SOURCES}/BREEZE.rst"

LEAD='^ \.\. START BREEZE HELP MARKER$'
TAIL='^ \.\. END BREEZE HELP MARKER$'

BEGIN_GEN=$(grep -n "${LEAD}" <"${BREEZE_RST_FILE}" | sed 's/\(.*\):.*/\1/g')
END_GEN=$(grep -n "${TAIL}" <"${BREEZE_RST_FILE}" | sed 's/\(.*\):.*/\1/g')
cat <(head -n "${BEGIN_GEN}" "${BREEZE_RST_FILE}") \
    "${TMP_FILE}" \
    <(tail -n +"${END_GEN}" "${BREEZE_RST_FILE}") \
    >"${TMP_OUTPUT}"

mv "${TMP_OUTPUT}" "${BREEZE_RST_FILE}"
