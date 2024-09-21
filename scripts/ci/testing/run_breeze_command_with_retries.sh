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

# If you want different number of retries for your breeze command, please set NUMBER_OF_ATTEMPT environment variable.
# Default number of retries is 3 unless NUMBER_OF_ATTEMPT is set.
export COLOR_RED=$'\e[31m'
export COLOR_YELLOW=$'\e[33m'
export COLOR_RESET=$'\e[0m'

NUMBER_OF_ATTEMPT="${NUMBER_OF_ATTEMPT:-3}"

for i in $(seq 1 "$NUMBER_OF_ATTEMPT") ; do
    breeze down
    set +e
    if breeze "$@"; then
        exit 0
    else
        echo
        echo "${COLOR_YELLOW}Breeze Command failed. Retrying again.${COLOR_RESET}"
        echo
        echo "This could be due to a flaky test, re-running once to re-check it After restarting docker."
        echo "Current Attempt: ${i}, Attempt Left: $((NUMBER_OF_ATTEMPT-i))"
        echo
    fi
    set -e
    sudo service docker restart
done
