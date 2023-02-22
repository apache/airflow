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
set -euo pipefail
declare -a packages

MYSQL_VERSION="8.0"
readonly MYSQL_VERSION
MARIADB_VERSION="10.5"
readonly MARIADB_VERSION

COLOR_BLUE=$'\e[34m'
readonly COLOR_BLUE
COLOR_YELLOW=$'\e[1;33m'
readonly COLOR_YELLOW
COLOR_RESET=$'\e[0m'
readonly COLOR_RESET

: "${INSTALL_MYSQL_CLIENT:?Should be true or false}"

install_mysql_client() {
    if [[ "${1}" == "dev" ]]; then
        packages=("libmysqlclient-dev" "mysql-client")
    elif [[ "${1}" == "prod" ]]; then
        packages=("libmysqlclient21" "mysql-client")
    else
        echo
        echo "Specify either prod or dev"
        echo
        exit 1
    fi

    echo
    echo "${COLOR_BLUE}Installing mysql client version ${MYSQL_VERSION}: ${1}${COLOR_RESET}"
    echo

    local key="467B942D3A79BD29"
    readonly key

    GNUPGHOME="$(mktemp -d)"
    export GNUPGHOME
    set +e
    for keyserver in $(shuf -e ha.pool.sks-keyservers.net hkp://p80.pool.sks-keyservers.net:80 \
                               keyserver.ubuntu.com hkp://keyserver.ubuntu.com:80)
    do
        gpg --keyserver "${keyserver}" --recv-keys "${key}" 2>&1 && break
    done
    set -e
    gpg --export "${key}" > /etc/apt/trusted.gpg.d/mysql.gpg
    gpgconf --kill all
    rm -rf "${GNUPGHOME}"
    unset GNUPGHOME
    echo "deb http://repo.mysql.com/apt/debian/ $(lsb_release -cs) mysql-${MYSQL_VERSION}" > /etc/apt/sources.list.d/mysql.list
    apt-get update
    apt-get install --no-install-recommends -y "${packages[@]}"
    apt-get autoremove -yqq --purge
    apt-get clean && rm -rf /var/lib/apt/lists/*
}

install_mariadb_client() {
    if [[ "${1}" == "dev" ]]; then
        packages=("libmariadb-dev" "mariadb-client-core-${MARIADB_VERSION}")
    elif [[ "${1}" == "prod" ]]; then
        packages=("mariadb-client-core-${MARIADB_VERSION}")
    else
        echo
        echo "Specify either prod or dev"
        echo
        exit 1
    fi

    echo
    echo "${COLOR_BLUE}Installing MariaDB client version ${MARIADB_VERSION}: ${1}${COLOR_RESET}"
    echo "${COLOR_YELLOW}MariaDB client binary compatible with MySQL client.${COLOR_RESET}"
    echo
    apt-get update
    apt-get install --no-install-recommends -y "${packages[@]}"
    apt-get autoremove -yqq --purge
    apt-get clean && rm -rf /var/lib/apt/lists/*
}

# Install MySQL client only if it is not disabled.
# For amd64 (x86_64) install MySQL client from Oracle repositories.
# For arm64 install MariaDB client from Debian repository, see:
# https://mariadb.com/kb/en/mariadb-clientserver-tcp-protocol/
if [[ ${INSTALL_MYSQL_CLIENT:="true"} == "true" ]]; then
    if [[ $(uname -m) == "arm64" || $(uname -m) == "aarch64" ]]; then
        install_mariadb_client "${@}"
    else
        install_mysql_client "${@}"
    fi
fi
