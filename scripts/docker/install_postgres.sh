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

: "${INSTALL_POSTGRES_CLIENT:?Should be true or false}"

install_postgres_client() {
    echo
    echo "${COLOR_BLUE}Installing postgres client${COLOR_RESET}"
    echo

    if [[ "${1}" == "dev" ]]; then
        packages=("libpq-dev" "postgresql-client")
    elif [[ "${1}" == "prod" ]]; then
        packages=("postgresql-client")
    else
        echo
        echo "Specify either prod or dev"
        echo
        exit 1
    fi

    common::import_trusted_gpg "7FCC7D46ACCC4CF8" "postgres"

    echo "deb [arch=amd64,arm64] https://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" > \
        /etc/apt/sources.list.d/pgdg.list
    apt-get update
    apt-get install --no-install-recommends -y "${packages[@]}"
    apt-get autoremove -yqq --purge
    apt-get clean && rm -rf /var/lib/apt/lists/*
}

# Install Postgres client from Postgres repositories
# But only if it is not disabled
if [[ ${INSTALL_POSTGRES_CLIENT:="true"} == "true" ]]; then
    install_postgres_client "${@}"
fi
