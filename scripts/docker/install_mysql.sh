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
declare -a packages

MYSQL_VERSION="8.0"
readonly MYSQL_VERSION

COLOR_BLUE=$'\e[34m'
readonly COLOR_BLUE
COLOR_RESET=$'\e[0m'
readonly COLOR_RESET

: "${INSTALL_MYSQL_CLIENT:?Should be true or false}"

install_mysql_client() {
    echo
    echo "${COLOR_BLUE}Installing mysql client version ${MYSQL_VERSION}${COLOR_RESET}"
    echo

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
    echo "deb http://repo.mysql.com/apt/debian/ buster mysql-${MYSQL_VERSION}" | tee -a /etc/apt/sources.list.d/mysql.list
    apt-get update
    apt-get install --no-install-recommends -y "${packages[@]}"
    apt-get autoremove -yqq --purge
    apt-get clean && rm -rf /var/lib/apt/lists/*
}

# Install MySQL client from Oracle repositories (Debian installs mariadb)
# But only if it is not disabled
if [[ ${INSTALL_MYSQL_CLIENT:="true"} == "true" ]]; then
    install_mysql_client "${@}"
fi
