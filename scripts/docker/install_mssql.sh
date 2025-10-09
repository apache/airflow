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
# shellcheck shell=bash
# shellcheck source=scripts/docker/common.sh
. "$( dirname "${BASH_SOURCE[0]}" )/common.sh"

set -euo pipefail

common::get_colors
declare -a packages

: "${INSTALL_MSSQL_CLIENT:?Should be true or false}"


function install_mssql_client() {
    # Install MsSQL client from Microsoft repositories
    if [[ ${INSTALL_MSSQL_CLIENT:="true"} != "true" ]]; then
        echo
        echo "${COLOR_BLUE}Skip installing mssql client${COLOR_RESET}"
        echo
        return
    fi
    packages=("msodbcsql18")

    common::import_trusted_gpg "EB3E94ADBE1229CF" "microsoft"

    echo
    echo "${COLOR_BLUE}Installing mssql client${COLOR_RESET}"
    echo

    echo "deb [arch=amd64,arm64] https://packages.microsoft.com/debian/$(lsb_release -rs)/prod $(lsb_release -cs) main" > \
        /etc/apt/sources.list.d/mssql-release.list &&
    mkdir -p /opt/microsoft/msodbcsql18 &&
    touch /opt/microsoft/msodbcsql18/ACCEPT_EULA &&
    apt-get update -yqq &&
    apt-get upgrade -yqq &&
    apt-get -yqq install --no-install-recommends "${packages[@]}" &&
    apt-get autoremove -yqq --purge &&
    apt-get clean &&
    rm -rf /var/lib/apt/lists/*
}

install_mssql_client "${@}"
