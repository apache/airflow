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

# This script should be only used commands without waiting interactive inputs.
# For example: 'apt-get install [package_name]' will ask you to input 'y' while installing.
# This is an interactive input which you cannot provide.
export COLOR_RED=$'\e[31m'
export COLOR_YELLOW=$'\e[33m'
export COLOR_RESET=$'\e[0m'

if [ ! "$#" -eq 2 ]; then
    echo "${COLOR_RED}You must provide exactly two argument!.${COLOR_RESET}"
    exit 1
fi

# Param 1: Breeze Command
# Param 2: Number of Retry(ies)
COMMAND_TO_ATTEMPT=$1
NUMBER_OF_ATTEMPT=$2
NUMBER_OF_ATTEMPT="${NUMBER_OF_ATTEMPT:-1}"
CURRENT_ATTEMPT=2
ATTEMPT_SUCCESS=false

breeze down
set +e
CURRENT_RUN_OUTPUT=$($COMMAND_TO_ATTEMPT)
RESULT=$?
echo "$CURRENT_RUN_OUTPUT"
set -e

while [ "${ATTEMPT_SUCCESS}" = false ] && [ "${CURRENT_ATTEMPT}" -le "${NUMBER_OF_ATTEMPT}" ]; do
  if [ "${RESULT}" != "0" ]; then
    ATTEMPT_LEFT=$((NUMBER_OF_ATTEMPT-CURRENT_ATTEMPT))
    echo
    echo "${COLOR_YELLOW}Breeze Command failed. Retrying once${COLOR_RESET}"
    echo
    echo "This could be due to a flaky test, re-running once to re-check it After restarting docker."
    echo "Current Attempt: CURRENT_ATTEMPT, Attempt Left: ${ATTEMPT_LEFT}"
    echo
    sudo service docker restart
    breeze down
    set +e
    CURRENT_RUN_OUTPUT=$($COMMAND_TO_ATTEMPT)
    RESULT=$?
    echo "$CURRENT_RUN_OUTPUT"
    set -e
    CURRENT_ATTEMPT=$(( CURRENT_ATTEMPT + 1 ))
  else
    ATTEMPT_SUCCESS=true
  fi
done
