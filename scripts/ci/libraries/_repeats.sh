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

# Repeat the command up to n times in case of failure
# Parameters:
#   $1 - how many times to repeat
#   $2 - command to repeat (run through bash -c)
function repeats::run_with_retry() {
    local num_repeats="${1}"
    local command="${2}"
    for ((n=0;n<num_repeats;n++));
    do
        local res
        echo "Attempt no. ${n} to execute ${command}"
        set +e
        bash -c "${command}"
        res=$?
        set -e
        if [[ ${res} == "0" ]]; then
            return 0
        fi
        echo
        echo  "${COLOR_YELLOW}WARNING: Unsuccessful attempt no. ${n}. Result: ${res}  ${COLOR_RESET}"
        echo
        echo
    done
    echo
    echo  "${COLOR_RED}ERROR: Giving up after ${num_repeats} attempts!  ${COLOR_RESET}"
    echo
    return ${res}
}
