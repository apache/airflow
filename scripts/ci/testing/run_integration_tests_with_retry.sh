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

export COLOR_RED=$'\e[31m'
export COLOR_YELLOW=$'\e[33m'
export COLOR_RESET=$'\e[0m'

if [[ ! "$#" -eq 1 ]]; then
    echo "${COLOR_RED}You must provide exactly one argument!.${COLOR_RESET}"
    exit 1
fi

INTEGRATION=${1}

breeze down
set +e
breeze testing integration-tests --integration "${INTEGRATION}"
RESULT=$?
set -e
if [[ ${RESULT} != "0" ]]; then
    echo
    echo "${COLOR_YELLOW}Integration Tests failed. Retrying once${COLOR_RESET}"
    echo
    echo "This could be due to a flaky test, re-running once to re-check it After restarting docker."
    echo
    sudo service docker restart
    breeze down
    set +e
    breeze testing integration-tests --integration "${INTEGRATION}"
    RESULT=$?
    set -e
    if [[ ${RESULT} != "0" ]]; then
        echo
        echo "${COLOR_RED}The integration tests failed for the second time! Giving up${COLOR_RESET}"
        echo
        exit ${RESULT}
    fi
fi
