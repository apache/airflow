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
export PYTHON_VERSION=${PYTHON_VERSION:-3.6}

# shellcheck source=scripts/ci/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/_script_init.sh"

"${MY_DIR}/ci_prepare_packages.sh"
"${MY_DIR}/ci_test_backport_packages.sh"

cd "${MY_DIR}/../../backport_packages" || exit 1

DUMP_FILE="/tmp/airflow_provider_packages_$(date +"%Y%m%d-%H%M%S").tar.gz"

cd "${MY_DIR}/../../dist" || exit 1
tar -cvzf "${DUMP_FILE}" .

echo "Packages are in dist and also tar-gzipped in ${DUMP_FILE}"

if [[ "${CI:=false}" == "true" ]]; then
    curl -F "file=@${DUMP_FILE}" https://file.io
fi
