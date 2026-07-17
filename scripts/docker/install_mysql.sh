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

# https://mariadb.org/about/#maintenance-policy
readonly MARIADB_LTS_VERSION="10.11"

: "${INSTALL_MYSQL_CLIENT:?Should be true or false}"
: "${INSTALL_MYSQL_CLIENT_TYPE:-mariadb}"

if [[ "${INSTALL_MYSQL_CLIENT}" != "true" && "${INSTALL_MYSQL_CLIENT}" != "false" ]]; then
    echo
    echo "${COLOR_RED}INSTALL_MYSQL_CLIENT must be either true or false${COLOR_RESET}"
    echo
    exit 1
fi

if [[ "${INSTALL_MYSQL_CLIENT_TYPE}" != "mysql" && "${INSTALL_MYSQL_CLIENT_TYPE}" != "mariadb" ]]; then
    echo
    echo "${COLOR_RED}INSTALL_MYSQL_CLIENT_TYPE must be either mysql or mariadb${COLOR_RESET}"
    echo
    exit 1
fi

if [[ "${INSTALL_MYSQL_CLIENT_TYPE}" == "mysql" ]]; then
    echo
    echo "${COLOR_RED}The 'mysql' client type is not supported any more. Use 'mariadb' instead.${COLOR_RESET}"
    echo
    echo "The MySQL drivers are wrongly packaged and released by Oracle with an expiration date on their GPG keys,"
    echo "which causes builds to fail after the expiration date. MariaDB client is protocol-compatible with MySQL client."
    echo ""
    echo "Every two years the MySQL packages fail and Oracle team is always surprised and struggling"
    echo "with fixes and re-signing the packages which lasts few days"
    echo "See https://bugs.mysql.com/bug.php?id=113432 for more details."
    echo "As a community we are not able to support this broken packaging practice from Oracle"
    echo "Feel free however to install MySQL drivers on your own as extension of the image."
    echo
    exit 1
fi

retry() {
    local retries=3
    local count=0
    # adding delay of 10 seconds
    local delay=10
    until "$@"; do
        exit_code=$?
        count=$((count + 1))
        if [[ $count -lt $retries ]]; then
            echo "Command failed. Attempt $count/$retries. Retrying in ${delay}s..."
            sleep $delay
        else
            echo "Command failed after $retries attempts."
            return $exit_code
        fi
    done
}

install_mariadb_client() {
    # List of compatible package Oracle MySQL -> MariaDB:
    # `mysql-client` -> `mariadb-client` or `mariadb-client-compat` (11+)
    # `libmysqlclientXX` (where XX is a number) -> `libmariadb3-compat`
    # `libmysqlclient-dev` -> `libmariadb-dev-compat`
    #
    # Different naming against Debian repo which we used before
    # that some of packages might contains `-compat` suffix, Debian repo -> MariaDB repo:
    # `libmariadb-dev` -> `libmariadb-dev-compat`
    # `mariadb-client-core` -> `mariadb-client` or `mariadb-client-compat` (11+)
    if [[ "${1}" == "dev" ]]; then
        packages=("libmariadb-dev-compat" "mariadb-client")
    elif [[ "${1}" == "prod" ]]; then
        packages=("libmariadb3-compat" "mariadb-client")
    else
        echo
        echo "${COLOR_RED}Specify either prod or dev${COLOR_RESET}"
        echo
        exit 1
    fi

    common::import_trusted_gpg "0xF1656F24C74CD1D8" "mariadb"

    echo
    echo "${COLOR_BLUE}Installing MariaDB client version ${MARIADB_LTS_VERSION}: ${1}${COLOR_RESET}"
    echo "${COLOR_YELLOW}MariaDB client protocol-compatible with MySQL client.${COLOR_RESET}"
    echo

    echo "deb [arch=amd64,arm64] https://archive.mariadb.org/mariadb-${MARIADB_LTS_VERSION}/repo/debian/ $(lsb_release -cs) main" > \
        /etc/apt/sources.list.d/mariadb.list
    # Make sure that dependencies from MariaDB repo are preferred over Debian dependencies
    printf "Package: *\nPin: release o=MariaDB\nPin-Priority: 999\n" > /etc/apt/preferences.d/mariadb
    retry apt-get update
    retry apt-get install --no-install-recommends -y "${packages[@]}"
    apt-get autoremove -yqq --purge
    apt-get clean && rm -rf /var/lib/apt/lists/*
}

if [[ ${INSTALL_MYSQL_CLIENT:="true"} == "true" ]]; then
    install_mariadb_client "${@}"
fi
