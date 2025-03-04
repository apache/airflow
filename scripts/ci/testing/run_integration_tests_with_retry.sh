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

if [[ ! "$#" -eq 2 ]]; then
    echo "${COLOR_RED}You must provide 2 arguments. Test group and integration!.${COLOR_RESET}"
    exit 1
fi

TEST_GROUP=${1}
INTEGRATION=${2}

breeze down

# Check for Gremlin container before starting tests
echo "${COLOR_YELLOW}Checking for Gremlin container...${COLOR_RESET}"
echo "Listing all containers:"
docker ps -a
echo "Filtering for Gremlin containers:"
docker ps -a --filter "name=gremlin"
# If a Gremlin container exists, inspect it
GREMLIN_CONTAINER=$(docker ps -aq --filter "name=gremlin" | head -n 1)
if [[ -n "$GREMLIN_CONTAINER" ]]; then
    echo "Gremlin container found: $GREMLIN_CONTAINER"
    echo "Detailed inspection:"
    docker inspect "$GREMLIN_CONTAINER"
    echo "Formatted details (Name - IP - Ports):"
    docker inspect "$GREMLIN_CONTAINER" -f '{{.Name}} - {{.NetworkSettings.IPAddress}} - {{range $p, $conf := .NetworkSettings.Ports}}{{$p}} -> {{range $conf}}{{.HostIp}}:{{.HostPort}}{{end}}{{end}}'
else
    echo "${COLOR_RED}No Gremlin container found yet.${COLOR_RESET}"
    echo "Breeze testing will start the environment next—check logs after."
fi

set +e
breeze testing "${TEST_GROUP}-integration-tests" --integration "${INTEGRATION}"
RESULT=$?
set -e
if [[ ${RESULT} != "0" ]]; then
    echo
    echo "${COLOR_YELLOW}The ${TEST_GROUP} Integration Tests failed. Retrying once${COLOR_RESET}"
    echo
    echo "This could be due to a flaky test, re-running once to re-check it After restarting docker."
    echo
    sudo service docker restart
    breeze down
    set +e
    breeze testing "${TEST_GROUP}-integration-tests" --integration "${INTEGRATION}"
    RESULT=$?
    set -e
    if [[ ${RESULT} != "0" ]]; then
        echo
        echo "${COLOR_RED}The ${TEST_GROUP} integration tests failed for the second time! Giving up${COLOR_RESET}"
        echo
        exit ${RESULT}
    fi
fi
