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

: "${INSTALL_MSSQL_CLIENT:?Should be true or false}"

COLOR_BLUE=$'\e[34m'
readonly COLOR_BLUE
COLOR_RESET=$'\e[0m'
readonly COLOR_RESET

function install_mssql_client() {
    echo
    echo "${COLOR_BLUE}Installing mssql client${COLOR_RESET}"
    echo
    curl --silent https://packages.microsoft.com/keys/microsoft.asc | apt-key add - >/dev/null 2>&1
    curl --silent https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list
    apt-get update -yqq
    apt-get upgrade -yqq
    ACCEPT_EULA=Y apt-get -yqq install -y --no-install-recommends msodbcsql17 mssql-tools
    rm -rf /var/lib/apt/lists/*
    apt-get autoremove -yqq --purge
    apt-get clean && rm -rf /var/lib/apt/lists/*
}

# Install MsSQL client from Microsoft repositories
if [[ ${INSTALL_MSSQL_CLIENT:="true"} == "true" ]]; then
    install_mssql_client "${@}"
fi
