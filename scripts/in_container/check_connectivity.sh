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
EXIT_CODE=0

function run_nc() {
    local host=${1}
    local port=${2}
    local ip
    ip=$(python -c "import socket; print(socket.getaddrinfo('${host}', None)[0][4][0])")
    if ! nc -zvvn "${ip}" "${port}"; then
        # If the first check fails it is possible, that the IP lookup responded with an IPv6-address, but
        # the corresponding system was only listening on IPv4. This happens because IPv6 is generally preferred to IPv4.
        # To prevent failures on systems listening only on IPv4 we explicitly check IPv4 here again.
        ip=$(python -c "import socket; print(socket.getaddrinfo('${host}', None, family=socket.AF_INET)[0][4][0])")
        nc -zvvn "${ip}" "${port}"
    fi
}

function check_service_connection {
    local label=$1
    local call=$2
    local max_check=${3:=1}

    echo -n "${label}: "
    while true
    do
        set +e
        local last_check_result
        last_check_result=$(eval "${call}" 2>&1)
        local res=$?
        set -e
        if [[ ${res} == 0 ]]; then
            echo  "${COLOR_GREEN}OK.  ${COLOR_RESET}"
            break
        else
            echo -n "."
            max_check=$((max_check-1))
        fi
        if [[ ${max_check} == 0 ]]; then
            echo "${COLOR_RED}ERROR: Maximum number of retries while checking service. Exiting ${COLOR_RESET}"
            break
        else
            sleep 1
        fi
    done
    if [[ ${res} != 0 ]]; then
        echo "Service could not be started!"
        echo
        echo "$ ${call}"
        echo "${last_check_result}"
        echo
        EXIT_CODE=${res}
    fi
    return "${EXIT_CODE}"
}
