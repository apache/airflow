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
set -exuo pipefail

MARIADB_VERSION="10.6.4"
readonly MARIADB_VERSION


function install_mariadb_client() {
    echo
    echo Installing mariadb client
    echo
    apt-key adv --fetch-keys 'https://mariadb.org/mariadb_release_signing_key.asc'
    echo "deb https://archive.mariadb.org/mariadb-${MARIADB_VERSION}/repo/debian/ buster main" | tee -a /etc/apt/sources.list.d/mariadb.list
    apt-get update -yqq
    apt-get upgrade -yqq
    apt-get install --no-install-recommends -y libmariadb-dev mariadb-client
    ln -s /usr/bin/mariadb_config /usr/bin/mysql_config
    rm -rf /var/lib/apt/lists/*
    apt-get autoremove -yqq --purge
    apt-get clean && rm -rf /var/lib/apt/lists/*
}

# Install MsSQL client from Microsoft repositories
if [[ ${INSTALL_MARIADB_CLIENT:="true"} == "true" ]]; then
    install_mariadb_client "${@}"
fi
