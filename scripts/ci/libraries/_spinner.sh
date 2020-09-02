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
function spin() {
    local FILE_TO_MONITOR=${1}
    local SPIN=("-" "\\" "|" "/")
    echo -n "
Build log: ${FILE_TO_MONITOR}
" > "${DETECTED_TERMINAL}"

    LAST_STEP=""
    while "true"
    do
      for i in "${SPIN[@]}"
      do
            echo -ne "\r${LAST_STEP}$i" > "${DETECTED_TERMINAL}"
            local LAST_FILE_SIZE
            local FILE_SIZE
            LAST_FILE_SIZE=$(set +e; wc -c "${FILE_TO_MONITOR}" 2>/dev/null | awk '{print $1}' || true)
            FILE_SIZE=${LAST_FILE_SIZE}
            while [[ "${LAST_FILE_SIZE}" == "${FILE_SIZE}" ]];
            do
                FILE_SIZE=$(set +e; wc -c "${FILE_TO_MONITOR}" 2>/dev/null | awk '{print $1}' || true)
                sleep 0.2
            done
            LAST_FILE_SIZE=FILE_SIZE
            sleep 0.2
            if [[ ! -f "${FILE_TO_MONITOR}" ]]; then
                exit
            fi
            LAST_LINE=$(set +e; grep "Step" <"${FILE_TO_MONITOR}" | tail -1 || true)
            [[ ${LAST_LINE} =~ ^(Step [0-9/]*)\ : ]] && LAST_STEP="${BASH_REMATCH[1]} :"
      done
    done
}
