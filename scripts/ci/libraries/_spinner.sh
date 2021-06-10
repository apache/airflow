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

# Function to spin ASCII spinner during pull and build in pre-commits to give the user indication that
# Pull/Build is happening. It only spins if the output log changes, so if pull/build is stalled
# The spinner will not move.
function spinner::spin() {
    local file_to_monitor=${1}
    SPIN=("-" "\\" "|" "/")
    readonly SPIN
    echo -n "
Build log: ${file_to_monitor}
" > "${DETECTED_TERMINAL}"

    local last_step=""
    while "true"
    do
      for i in "${SPIN[@]}"
      do
            echo -ne "\r${last_step}$i" > "${DETECTED_TERMINAL}"
            local last_file_size
            local file_size
            last_file_size=$(set +e; wc -c "${file_to_monitor}" 2>/dev/null | awk '{print $1}' || true)
            file_size=${last_file_size}
            while [[ "${last_file_size}" == "${file_size}" ]];
            do
                file_size=$(set +e; wc -c "${file_to_monitor}" 2>/dev/null | awk '{print $1}' || true)
                sleep 0.2
            done
            last_file_size=file_size
            sleep 0.2
            if [[ ! -f "${file_to_monitor}" ]]; then
                exit
            fi
            local last_line
            last_line=$(set +e; grep "Step" <"${file_to_monitor}" | tail -1 || true)
            [[ ${last_line} =~ ^(Step [0-9/]*)\ : ]] && last_step="${BASH_REMATCH[1]} :"
      done
    done
}
