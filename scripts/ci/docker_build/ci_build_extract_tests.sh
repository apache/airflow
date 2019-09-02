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

# Bash sanity settings (error on exit, complain for undefined vars, error when pipe fails)
set -euxo pipefail

MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export AIRFLOW_SOURCES="${MY_DIR}/../../.."

gosu "${AIRFLOW_USER}" nosetests --collect-only --with-xunit --xunit-file="${HOME}/all_tests.xml"

gosu "${AIRFLOW_USER}" \
    python "${AIRFLOW_SOURCES}/tests/test_utils/get_all_tests.py" \
                    "${HOME}/all_tests.xml" >"${HOME}/all_tests.txt"; \

echo ". ${HOME}/.bash_completion" >> "${HOME}/.bashrc"

chmod +x "${HOME}/run-tests-complete"

chmod +x "${HOME}/run-tests"

chown "${AIRFLOW_USER}.${AIRFLOW_USER}" "${HOME}/.bashrc" "${HOME}/run-tests-complete" "${HOME}/run-tests"
