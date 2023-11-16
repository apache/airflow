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
set -euo pipefail

# REMOVE THOSE 3 LINES BELOW TOGETHER WITH PYMSSQL INSTALLATION BELOW AFTER PYMSSQL
# INSTALLATION WITH CYTHON 3.0.0 IS FIXED SEE BELOW FOR MORE DETAILS
# shellcheck source=scripts/docker/common.sh
. "$( dirname "${BASH_SOURCE[0]}" )/common.sh"

: "${AIRFLOW_PIP_VERSION:?Should be set}"

: "${INSTALL_MSSQL_CLIENT:?Should be true or false}"

COLOR_BLUE=$'\e[34m'
readonly COLOR_BLUE
COLOR_RESET=$'\e[0m'
readonly COLOR_RESET

function install_mssql_client() {
    # Install MsSQL client from Microsoft repositories
    if [[ ${INSTALL_MSSQL_CLIENT:="true"} != "true" ]]; then
        echo
        echo "${COLOR_BLUE}Skip installing mssql client${COLOR_RESET}"
        echo
        return
    fi
    echo
    echo "${COLOR_BLUE}Installing mssql client${COLOR_RESET}"
    echo
    local distro
    local version
    distro=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
    version=$(lsb_release -rs)
    local driver=msodbcsql18
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | sudo gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
    curl --silent https://packages.microsoft.com/keys/microsoft.asc | apt-key add - >/dev/null 2>&1
    curl --silent "https://packages.microsoft.com/config/${distro}/${version}/prod.list" > \
        /etc/apt/sources.list.d/mssql-release.list
    apt-get update -yqq
    apt-get upgrade -yqq
    ACCEPT_EULA=Y apt-get -yqq install -y --no-install-recommends "${driver}"
    rm -rf /var/lib/apt/lists/*
    apt-get autoremove -yqq --purge
    apt-get clean && rm -rf /var/lib/apt/lists/*

    # Workaround an issue with installing pymssql on ARM architecture triggered by Cython 3.0.0 release as of
    # 18 July 2023. The problem is that pip uses latest Cython to compile pymssql and since we are using
    # setuptools, there is no easy way to fix version of Cython used to compile packages.
    #
    # This triggers a problem with newer `pip` versions that have build isolation enabled by default because
    # There is no (easy) way to pin build dependencies for dependent packages. If a package does not have
    # limit on build dependencies, it will use the latest version of them to build that particular package.
    #
    # The workaround to the problem suggest in the last thread by Pradyun Gedam - pip maintainer - is to
    # use PIP_CONSTRAINT environment variable and constraint the version of Cython used while installing
    # the package. Which is precisely what we are doing here.
    #
    # Note that it does not work if we pass ``--constraint`` option to pip because it will not be passed to
    # the package being build in isolation. The fact that the PIP_CONSTRAINT env variable works in the isolation
    # is a bit of side-effect on how env variables work and that they are passed to subprocesses as pip
    # launches a subprocess `pip` to build the package.
    #
    # This is a temporary solution until the issue is resolved in pymssql or Cython
    # Issues/discussions that track it:
    #
    # * https://github.com/cython/cython/issues/5541
    # * https://github.com/pymssql/pymssql/pull/827
    # * https://discuss.python.org/t/no-way-to-pin-build-dependencies/29833
    #
    # TODO: Remove this workaround when the issue is resolved.
    #       ALSO REMOVE THE TOP LINES ABOVE WITH common.sh IMPORT AS WELL AS COPYING common.sh ib
    #       Dockerfile AND Dockerfile.ci (look for capital PYMSSQL - there are several places to remove)
    if [[ "${1}" == "dev" ]]; then
        common::install_pip_version
        echo "Cython==0.29.36" >> /tmp/mssql-constraints.txt
        PIP_CONSTRAINT=/tmp/mssql-constraints.txt pip install pymssql
        rm /tmp/mssql-constraints.txt
    fi
}

install_mssql_client "${@}"
