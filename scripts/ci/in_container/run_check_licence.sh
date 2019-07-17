#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Script to run Pylint on all code. Can be started from any working directory
# ./scripts/ci/run_pylint.sh

set -uo pipefail

MY_DIR=$(cd "$(dirname "$0")" || exit 1; pwd)

# shellcheck source=./_in_container_utils.sh
. "${MY_DIR}/_in_container_utils.sh"

in_container_basic_sanity_check

in_container_script_start

echo
echo "Running Licence check"
echo

sudo chown -R "${AIRFLOW_USER}.${AIRFLOW_USER}" "${AIRFLOW_SOURCES}/logs"

# This is the target of a symlink in airflow/www/static/docs -
# and rat exclude doesn't cope with the symlink target doesn't exist
sudo mkdir -p docs/_build/html/

echo "Running license checks. This can take a while."

if ! java -jar "${RAT_JAR}" -E "${AIRFLOW_SOURCES}"/.rat-excludes \
    -d "${AIRFLOW_SOURCES}" > "${AIRFLOW_SOURCES}/logs/rat-results.txt"; then
   echo >&2 "RAT exited abnormally"
   exit 1
fi

ERRORS=$(grep -e "??" "${AIRFLOW_SOURCES}/logs/rat-results.txt")

in_container_script_end

in_container_fix_ownership

if test ! -z "${ERRORS}"; then
    echo >&2
    echo >&2 "Could not find Apache license headers in the following files:"
    echo >&2 "${ERRORS}"
    exit 1
    echo >&2
else
    echo "RAT checks passed."
    echo
fi
