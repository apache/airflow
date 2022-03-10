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
export FORCE_ANSWER_TO_QUESTIONS=${FORCE_ANSWER_TO_QUESTIONS:="no"}
export PRINT_INFO_FROM_SCRIPTS="false"
export SKIP_CHECK_REMOTE_IMAGE="true"
export PYTHON_MAJOR_MINOR_VERSION="3.7"

# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"

function run_check_license() {
    echo
    echo "Running Licence check"
    echo

    # This is the target of a symlink in airflow/www/static/docs -
    # and rat exclude doesn't cope with the symlink target doesn't exist
    mkdir -p docs/_build/html/

    echo "Running license checks. This can take a while."
    # We mount ALL airflow files for the licence check. We want to check them all!
    if ! docker_v run -v "${AIRFLOW_SOURCES}:/opt/airflow" -t \
            --user "$(id -ur):$(id -gr)" \
            --rm --env-file "${AIRFLOW_SOURCES}/scripts/ci/docker-compose/_docker.env" \
            ghcr.io/apache/airflow-apache-rat:0.13-2021.07.04 \
            --exclude-file /opt/airflow/.rat-excludes \
            --d /opt/airflow | tee "${AIRFLOW_SOURCES}/logs/rat-results.txt" ; then
        echo
        echo  "${COLOR_RED}ERROR: RAT exited abnormally  ${COLOR_RESET}"
        echo
        exit 1
    fi

    set +e
    local errors
    errors=$(grep -F "??" "${AIRFLOW_SOURCES}/logs/rat-results.txt")
    set -e
    if test ! -z "${errors}"; then
        echo
        echo  "${COLOR_RED}ERROR: Could not find Apache license headers in the following files:  ${COLOR_RESET}"
        echo
        echo "${errors}"
        exit 1
    else
        echo
        echo "${COLOR_GREEN}OK. RAT checks passed.  ${COLOR_RESET}"
        echo
    fi
}

if [[ $(uname -m) == "arm64" || $(uname -m) == "aarch64" ]]; then
    echo "Skip RAT check on ARM devices util we push multiplatform images"
else
    run_check_license
fi
