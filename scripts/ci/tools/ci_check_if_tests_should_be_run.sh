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

# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"

CHANGED_FILES_PATTERNS=(
    "^airflow"
    "^.github/workflows/"
    "^Dockerfile"
    "^scripts"
    "^chart"
    "^setup.py"
    "^tests"
    "^kubernetes_tests"
)

CHANGED_FILES_REGEXP=""

SEPARATOR=""
for PATTERN in "${CHANGED_FILES_PATTERNS[@]}"
do
    CHANGED_FILES_REGEXP="${CHANGED_FILES_REGEXP}${SEPARATOR}${PATTERN}"
    SEPARATOR="|"
done

echo
echo "GitHub SHA: ${COMMIT_SHA}"
echo

set +e
"${SCRIPTS_CI_DIR}/tools/ci_count_changed_files.sh" "${CHANGED_FILES_REGEXP}"
COUNT_CHANGED_FILES=$?
set -e

if [[ ${COUNT_CHANGED_FILES} == "0" ]]; then
    echo "::set-output name=run-tests::false"
else
    echo "::set-output name=run-tests::true"
fi
