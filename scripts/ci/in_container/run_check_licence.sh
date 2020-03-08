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

echo
echo "Running Licence check"
echo

# This is the target of a symlink in airflow/www/static/docs -
# and rat exclude doesn't cope with the symlink target doesn't exist
sudo mkdir -p docs/_build/html/

echo "Running license checks. This can take a while."

if ! java -jar "/opt/apache-rat.jar" -E "${AIRFLOW_SOURCES}"/.rat-excludes \
    -d "${AIRFLOW_SOURCES}" | tee "${AIRFLOW_SOURCES}/logs/rat-results.txt" ; then
   echo >&2 "RAT exited abnormally"
   exit 1
fi

set +e
ERRORS=$(grep -e "??" "${AIRFLOW_SOURCES}/logs/rat-results.txt")
set -e

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
    exit 0
fi
