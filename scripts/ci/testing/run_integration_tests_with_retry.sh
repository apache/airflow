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

# Function to check Gremlin container in parallel
check_gremlin_container() {
    echo "${COLOR_YELLOW}Starting Gremlin container check...${COLOR_RESET}"
    local max_attempts=30
    local sleep_interval=2
    local attempt=1

    while [[ $attempt -le $max_attempts ]]; do
        GREMLIN_CONTAINER=$(docker ps -q --filter "name=gremlin")
        if [[ -n "$GREMLIN_CONTAINER" ]]; then
            echo "${COLOR_YELLOW}Gremlin container found: $GREMLIN_CONTAINER${COLOR_RESET}"
            echo "Detailed inspection:"
            docker inspect "$GREMLIN_CONTAINER"
            echo "Formatted details (Name - IP - Ports):"
            docker inspect "$GREMLIN_CONTAINER" -f '{{.Name}} - {{(index .NetworkSettings.Networks "airflow-test-all_default").IPAddress}} - {{range $p, $conf := .NetworkSettings.Ports}}{{$p}} -> {{range $conf}}{{.HostIp}}:{{.HostPort}} {{end}}{{end}}'
            break
        fi
        echo "Attempt $attempt/$max_attempts: No running Gremlin container yet..."
        sleep $sleep_interval
        ((attempt++))
    done
    if [[ -z "$GREMLIN_CONTAINER" ]]; then
        echo "${COLOR_RED}No running Gremlin container after $max_attempts attempts!${COLOR_RESET}"
        docker logs gremlin
    fi
}

# Run the container check in the background
check_gremlin_container &

# Run breeze testing (foreground)
set +e
breeze testing "${TEST_GROUP}-integration-tests" --integration "${INTEGRATION}"
RESULT=$?
set -e

# Wait for background job to finish
wait

# Exit with the test result
exit ${RESULT}
