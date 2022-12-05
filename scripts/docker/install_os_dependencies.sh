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

DOCKER_CLI_VERSION=20.10.9

if [[ "$#" != 1 ]]; then
    echo "ERROR! There should be 'runtime' or 'dev' parameter passed as argument.".
    exit 1
fi

if [[ "${1}" == "runtime" ]]; then
    INSTALLATION_TYPE="RUNTIME"
elif   [[ "${1}" == "dev" ]]; then
    INSTALLATION_TYPE="dev"
else
    echo "ERROR! Wrong argument. Passed ${1} and it should be one of 'runtime' or 'dev'.".
    exit 1
fi

function get_dev_apt_deps() {
    if [[ "${DEV_APT_DEPS=}" == "" ]]; then
        DEV_APT_DEPS="apt-transport-https apt-utils build-essential ca-certificates dirmngr \
freetds-bin freetds-dev git gosu graphviz graphviz-dev krb5-user ldap-utils libffi-dev \
libkrb5-dev libldap2-dev libsasl2-2 libsasl2-dev libsasl2-modules \
libssl-dev locales lsb-release openssh-client sasl2-bin \
software-properties-common sqlite3 sudo unixodbc unixodbc-dev"
        export DEV_APT_DEPS
    fi
}

function get_runtime_apt_deps() {
    if [[ "${RUNTIME_APT_DEPS=}" == "" ]]; then
        RUNTIME_APT_DEPS="apt-transport-https apt-utils ca-certificates \
curl dumb-init freetds-bin gosu krb5-user \
ldap-utils libffi7 libldap-2.4-2 libsasl2-2 libsasl2-modules libssl1.1 locales \
lsb-release netcat openssh-client python3-selinux rsync sasl2-bin sqlite3 sudo unixodbc"
        export RUNTIME_APT_DEPS
    fi
}

function install_docker_cli() {
    local platform
    if [[ $(uname -m) == "arm64" || $(uname -m) == "aarch64" ]]; then
        platform="aarch64"
    else
        platform="x86_64"
    fi
    curl --silent \
        "https://download.docker.com/linux/static/stable/${platform}/docker-${DOCKER_CLI_VERSION}.tgz" \
        |  tar -C /usr/bin --strip-components=1 -xvzf - docker/docker
}

function install_debian_dev_dependencies() {
    apt-get update
    apt-get install --no-install-recommends -yqq apt-utils >/dev/null 2>&1
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

if [[ "${INSTALLATION_TYPE}" == "RUNTIME" ]]; then
    get_runtime_apt_deps
    install_debian_runtime_dependencies
    install_docker_cli

else
    get_dev_apt_deps
    install_debian_dev_dependencies
    install_docker_cli
fi
