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

if [[ "$#" != 1 ]]; then
    echo "ERROR! There should be 'runtime', 'ci' or 'dev' parameter passed as argument.".
    exit 1
fi

AIRFLOW_PYTHON_VERSION=${AIRFLOW_PYTHON_VERSION:-v3.10.10}
GOLANG_MAJOR_MINOR_VERSION=${GOLANG_MAJOR_MINOR_VERSION:-1.24.4}

if [[ "${1}" == "runtime" ]]; then
    INSTALLATION_TYPE="RUNTIME"
elif   [[ "${1}" == "dev" ]]; then
    INSTALLATION_TYPE="DEV"
elif   [[ "${1}" == "ci" ]]; then
    INSTALLATION_TYPE="CI"
else
    echo "ERROR! Wrong argument. Passed ${1} and it should be one of 'runtime', 'ci' or 'dev'.".
    exit 1
fi

function get_dev_apt_deps() {
    if [[ "${DEV_APT_DEPS=}" == "" ]]; then
        DEV_APT_DEPS="apt-transport-https apt-utils build-essential ca-certificates dirmngr \
freetds-bin freetds-dev git graphviz graphviz-dev krb5-user ldap-utils libev4 libev-dev libffi-dev libgeos-dev \
libkrb5-dev libldap2-dev libleveldb1d libleveldb-dev libsasl2-2 libsasl2-dev libsasl2-modules \
libssl-dev libxmlsec1 libxmlsec1-dev locales lsb-release openssh-client pkgconf sasl2-bin \
software-properties-common sqlite3 sudo unixodbc unixodbc-dev zlib1g-dev \
gdb lcov pkg-config libbz2-dev libgdbm-dev libgdbm-compat-dev liblzma-dev \
libncurses5-dev libreadline6-dev libsqlite3-dev lzma lzma-dev tk-dev uuid-dev \
libzstd-dev"
        export DEV_APT_DEPS
    fi
}

function get_runtime_apt_deps() {
    local debian_version
    local debian_version_apt_deps
    # Get debian version without installing lsb_release
    # shellcheck disable=SC1091
    debian_version=$(. /etc/os-release;   printf '%s\n' "$VERSION_CODENAME";)
    echo
    echo "DEBIAN CODENAME: ${debian_version}"
    echo
    debian_version_apt_deps="libffi8 libldap-2.5-0 libssl3 netcat-openbsd"
    echo
    echo "APPLIED INSTALLATION CONFIGURATION FOR DEBIAN VERSION: ${debian_version}"
    echo
    if [[ "${RUNTIME_APT_DEPS=}" == "" ]]; then
        RUNTIME_APT_DEPS="apt-transport-https apt-utils ca-certificates \
curl dumb-init freetds-bin git krb5-user libev4 libgeos-dev \
ldap-utils libsasl2-2 libsasl2-modules libxmlsec1 locales ${debian_version_apt_deps} \
lsb-release openssh-client python3-selinux rsync sasl2-bin sqlite3 sudo unixodbc"
        export RUNTIME_APT_DEPS
    fi
}

function install_docker_cli() {
    apt-get update
    apt-get install ca-certificates curl
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
    chmod a+r /etc/apt/keyrings/docker.asc
    # shellcheck disable=SC1091
    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian \
      $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
      tee /etc/apt/sources.list.d/docker.list > /dev/null
    apt-get update
    apt-get install -y --no-install-recommends docker-ce-cli
}

function install_debian_dev_dependencies() {
    apt-get update
    apt-get install -yqq --no-install-recommends apt-utils >/dev/null 2>&1
    apt-get install -y --no-install-recommends curl gnupg2 lsb-release
    # shellcheck disable=SC2086
    export ${ADDITIONAL_DEV_APT_ENV?}
    if [[ ${DEV_APT_COMMAND} != "" ]]; then
        bash -o pipefail -o errexit -o nounset -o nolog -c "${DEV_APT_COMMAND}"
    fi
    if [[ ${ADDITIONAL_DEV_APT_COMMAND} != "" ]]; then
        bash -o pipefail -o errexit -o nounset -o nolog -c "${ADDITIONAL_DEV_APT_COMMAND}"
    fi
    apt-get update
    local debian_version
    local debian_version_apt_deps
    # Get debian version without installing lsb_release
    # shellcheck disable=SC1091
    debian_version=$(. /etc/os-release;   printf '%s\n' "$VERSION_CODENAME";)
    echo
    echo "DEBIAN CODENAME: ${debian_version}"
    echo
    # shellcheck disable=SC2086
    apt-get install -y --no-install-recommends ${DEV_APT_DEPS} ${ADDITIONAL_DEV_APT_DEPS}
}

function install_debian_runtime_dependencies() {
    apt-get update
    apt-get install --no-install-recommends -yqq apt-utils >/dev/null 2>&1
    apt-get install -y --no-install-recommends curl gnupg2 lsb-release
    # shellcheck disable=SC2086
    export ${ADDITIONAL_RUNTIME_APT_ENV?}
    if [[ "${RUNTIME_APT_COMMAND}" != "" ]]; then
        bash -o pipefail -o errexit -o nounset -o nolog -c "${RUNTIME_APT_COMMAND}"
    fi
    if [[ "${ADDITIONAL_RUNTIME_APT_COMMAND}" != "" ]]; then
        bash -o pipefail -o errexit -o nounset -o nolog -c "${ADDITIONAL_RUNTIME_APT_COMMAND}"
    fi
    apt-get update
    # shellcheck disable=SC2086
    apt-get install -y --no-install-recommends ${RUNTIME_APT_DEPS} ${ADDITIONAL_RUNTIME_APT_DEPS}
    apt-get autoremove -yqq --purge
    apt-get clean
    rm -rf /var/lib/apt/lists/* /var/log/*
}

function install_python() {
    git clone --branch "${AIRFLOW_PYTHON_VERSION}" --depth 1 https://github.com/python/cpython.git
    cd cpython
    ./configure --enable-optimizations
    make -s -j "$(nproc)" all
    make -s -j "$(nproc)" install
    ln -s /usr/local/bin/python3 /usr/local/bin/python
    ln -s /usr/local/bin/pip3 /usr/local/bin/pip
    cd ..
    rm -rf cpython
}

function install_golang() {
    curl "https://dl.google.com/go/go${GOLANG_MAJOR_MINOR_VERSION}.linux-$(dpkg --print-architecture).tar.gz" -o "go${GOLANG_MAJOR_MINOR_VERSION}.linux.tar.gz"
    rm -rf /usr/local/go && tar -C /usr/local -xzf go"${GOLANG_MAJOR_MINOR_VERSION}".linux.tar.gz
}

if [[ "${INSTALLATION_TYPE}" == "RUNTIME" ]]; then
    get_runtime_apt_deps
    install_debian_runtime_dependencies
    install_docker_cli

else

    get_dev_apt_deps
    install_debian_dev_dependencies
    install_python
    if [[ "${INSTALLATION_TYPE}" == "CI" ]]; then
        install_golang
    fi
    install_docker_cli
fi
