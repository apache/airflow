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

# https://dev.mysql.com/blog-archive/introducing-mysql-innovation-and-long-term-support-lts-versions/
readonly MYSQL_LTS_VERSION="8.0"
# https://mariadb.org/about/#maintenance-policy
readonly MARIADB_LTS_VERSION="10.11"

: "${INSTALL_MYSQL_CLIENT:?Should be true or false}"
: "${INSTALL_MYSQL_CLIENT_TYPE:-mariadb}"

install_mysql_client() {
    if [[ "${1}" == "dev" ]]; then
        packages=("libmysqlclient-dev" "mysql-client")
    elif [[ "${1}" == "prod" ]]; then
        # `libmysqlclientXX` where XX is number, and it should be increased every new GA MySQL release, for example
        # 18 - MySQL 5.6.48
        # 20 - MySQL 5.7.42
        # 21 - MySQL 8.0.34
        # 22 - MySQL 8.1
        packages=("libmysqlclient21" "mysql-client")
    else
        echo
        echo "${COLOR_RED}Specify either prod or dev${COLOR_RESET}"
        echo
        exit 1
    fi

    common::import_trusted_gpg "B7B3B788A8D3785C" "mysql"

    echo
    echo "${COLOR_BLUE}Installing Oracle MySQL client version ${MYSQL_LTS_VERSION}: ${1}${COLOR_RESET}"
    echo

    echo "deb http://repo.mysql.com/apt/debian/ $(lsb_release -cs) mysql-${MYSQL_LTS_VERSION}" > \
        /etc/apt/sources.list.d/mysql.list
    apt-get update
    apt-get install --no-install-recommends -y "${packages[@]}"
    apt-get autoremove -yqq --purge
    apt-get clean && rm -rf /var/lib/apt/lists/*

    # Remove mysql repository from sources.list.d as MySQL repos have a basic flaw that they put expiry
    # date on their GPG signing keys and they sign their repo with those keys. This means that after a
    # certain date, the GPG key becomes invalid and if you have the repository added in your sources.list
    # then you will not be able to install anything from any other repository. This id unlike any other
    # repository we have seen (for example Postgres, MariaDB, MsSQL - all have non-expiring signing keys)
    rm /etc/apt/sources.list.d/mysql.list
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

    curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
    echo "deb [arch=amd64,arm64] https://archive.mariadb.org/mariadb-${MARIADB_LTS_VERSION}/repo/debian/ $(lsb_release -cs) main" > \
        /etc/apt/sources.list.d/mariadb.list
    # Make sure that dependencies from MariaDB repo are preferred over Debian dependencies
    printf "Package: *\nPin: release o=MariaDB\nPin-Priority: 999\n" > /etc/apt/preferences.d/mariadb
    apt-get update
    apt-get install --no-install-recommends -y "${packages[@]}"
    apt-get autoremove -yqq --purge
    apt-get clean && rm -rf /var/lib/apt/lists/*
}

# Install MySQL client only if it is not disabled.
# INSTALL_MYSQL_CLIENT_TYPE=mysql : Install MySQL client from Oracle repository.
# INSTALL_MYSQL_CLIENT_TYPE=mariadb : Install MariaDB client from MariaDB repository.
# https://mariadb.com/kb/en/mariadb-clientserver-tcp-protocol/
# For ARM64 INSTALL_MYSQL_CLIENT_TYPE ignored and always install MariaDB.
if [[ ${INSTALL_MYSQL_CLIENT:="true"} == "true" ]]; then
    if [[ $(uname -m) == "arm64" || $(uname -m) == "aarch64" ]]; then
        INSTALL_MYSQL_CLIENT_TYPE="mariadb"
        echo
        echo "${COLOR_YELLOW}Client forced to mariadb for ARM${COLOR_RESET}"
        echo
    fi

    if [[ "${INSTALL_MYSQL_CLIENT_TYPE}" == "mysql" ]]; then
        install_mysql_client "${@}"
    elif [[ "${INSTALL_MYSQL_CLIENT_TYPE}" == "mariadb" ]]; then
        install_mariadb_client "${@}"
    else
        echo
        echo "${COLOR_RED}Specify either mysql or mariadb, got ${INSTALL_MYSQL_CLIENT_TYPE}${COLOR_RESET}"
        echo
        exit 1
    fi
fi
