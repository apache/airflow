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

function common::get_colors() {
    COLOR_BLUE=$'\e[34m'
    COLOR_GREEN=$'\e[32m'
    COLOR_RED=$'\e[31m'
    COLOR_RESET=$'\e[0m'
    COLOR_YELLOW=$'\e[33m'
    export COLOR_BLUE
    export COLOR_GREEN
    export COLOR_RED
    export COLOR_RESET
    export COLOR_YELLOW
}

function common::get_packaging_tool() {
    : "${AIRFLOW_USE_UV:?Should be set}"

    ## IMPORTANT: IF YOU MODIFY THIS FUNCTION YOU SHOULD ALSO MODIFY CORRESPONDING FUNCTION IN
    ## `scripts/in_container/_in_container_utils.sh`
    if [[ ${AIRFLOW_USE_UV} == "true" ]]; then
        echo
        echo "${COLOR_BLUE}Using 'uv' to install Airflow${COLOR_RESET}"
        echo
        export PACKAGING_TOOL="uv"
        export PACKAGING_TOOL_CMD="uv pip"
        # --no-binary  is needed in order to avoid libxml and xmlsec using different version of libxml2
         # (binary lxml embeds its own libxml2, while xmlsec uses system one).
         # See https://bugs.launchpad.net/lxml/+bug/2110068
         if [[ ${AIRFLOW_INSTALLATION_METHOD=} == "." && -f "./pyproject.toml" ]]; then
            # for uv only install dev group when we install from sources
            export EXTRA_INSTALL_FLAGS="--group=dev --no-binary lxml --no-binary xmlsec"
        else
            export EXTRA_INSTALL_FLAGS="--no-binary lxml --no-binary xmlsec"
        fi
        export EXTRA_UNINSTALL_FLAGS=""
        export UPGRADE_TO_HIGHEST_RESOLUTION="--upgrade --resolution highest"
        export UPGRADE_IF_NEEDED="--upgrade"
        UV_CONCURRENT_DOWNLOADS=$(nproc --all)
        export UV_CONCURRENT_DOWNLOADS
        if [[ ${INCLUDE_PRE_RELEASE=} == "true" ]]; then
            EXTRA_INSTALL_FLAGS="${EXTRA_INSTALL_FLAGS} --prerelease if-necessary"
        fi
    else
        echo
        echo "${COLOR_BLUE}Using 'pip' to install Airflow${COLOR_RESET}"
        echo
        export PACKAGING_TOOL="pip"
        export PACKAGING_TOOL_CMD="pip"
        # --no-binary  is needed in order to avoid libxml and xmlsec using different version of libxml2
        # (binary lxml embeds its own libxml2, while xmlsec uses system one).
        # See https://bugs.launchpad.net/lxml/+bug/2110068
        export EXTRA_INSTALL_FLAGS="--root-user-action ignore --no-binary lxml,xmlsec"
        export EXTRA_UNINSTALL_FLAGS="--yes"
        export UPGRADE_TO_HIGHEST_RESOLUTION="--upgrade --upgrade-strategy eager"
        export UPGRADE_IF_NEEDED="--upgrade --upgrade-strategy only-if-needed"
        if [[ ${INCLUDE_PRE_RELEASE=} == "true" ]]; then
            EXTRA_INSTALL_FLAGS="${EXTRA_INSTALL_FLAGS} --pre"
        fi
    fi
}

function common::get_airflow_version_specification() {
    if [[ -z ${AIRFLOW_VERSION_SPECIFICATION=}
        && -n ${AIRFLOW_VERSION}
        && ${AIRFLOW_INSTALLATION_METHOD} != "." ]]; then
        AIRFLOW_VERSION_SPECIFICATION="==${AIRFLOW_VERSION}"
    fi
}

function common::get_constraints_location() {
    # auto-detect Airflow-constraint reference and location
    if [[ -z "${AIRFLOW_CONSTRAINTS_REFERENCE=}" ]]; then
        if  [[ ${AIRFLOW_VERSION} =~ v?2.* || ${AIRFLOW_VERSION} =~ v?3.* ]]; then
            AIRFLOW_CONSTRAINTS_REFERENCE=constraints-${AIRFLOW_VERSION}
        else
            AIRFLOW_CONSTRAINTS_REFERENCE=${DEFAULT_CONSTRAINTS_BRANCH}
        fi
    fi

    if [[ -z ${AIRFLOW_CONSTRAINTS_LOCATION=} ]]; then
        local constraints_base="https://raw.githubusercontent.com/${CONSTRAINTS_GITHUB_REPOSITORY}/${AIRFLOW_CONSTRAINTS_REFERENCE}"
        local python_version
        python_version=$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
        AIRFLOW_CONSTRAINTS_LOCATION="${constraints_base}/${AIRFLOW_CONSTRAINTS_MODE}-${python_version}.txt"
    fi

    if [[ ${AIRFLOW_CONSTRAINTS_LOCATION} =~ http.* ]]; then
        echo
        echo "${COLOR_BLUE}Downloading constraints from ${AIRFLOW_CONSTRAINTS_LOCATION} to ${HOME}/constraints.txt ${COLOR_RESET}"
        echo
        curl -sSf -o "${HOME}/constraints.txt" "${AIRFLOW_CONSTRAINTS_LOCATION}"
    else
        echo
        echo "${COLOR_BLUE}Copying constraints from ${AIRFLOW_CONSTRAINTS_LOCATION} to ${HOME}/constraints.txt ${COLOR_RESET}"
        echo
        cp "${AIRFLOW_CONSTRAINTS_LOCATION}" "${HOME}/constraints.txt"
    fi
}

function common::show_packaging_tool_version_and_location() {
   echo "PATH=${PATH}"
   echo "Installed pip: $(pip --version): $(which pip)"
   if [[ ${PACKAGING_TOOL} == "pip" ]]; then
       echo "${COLOR_BLUE}Using 'pip' to install Airflow${COLOR_RESET}"
   else
       echo "${COLOR_BLUE}Using 'uv' to install Airflow${COLOR_RESET}"
       echo "Installed uv: $(uv --version 2>/dev/null || echo "Not installed yet"): $(which uv 2>/dev/null)"
   fi
}

function common::install_packaging_tools() {
    : "${AIRFLOW_USE_UV:?Should be set}"
    if [[ "${VIRTUAL_ENV=}" != "" ]]; then
        echo
        echo "${COLOR_BLUE}Checking packaging tools in venv: ${VIRTUAL_ENV}${COLOR_RESET}"
        echo
    else
        echo
        echo "${COLOR_BLUE}Checking packaging tools for system Python installation: $(which python)${COLOR_RESET}"
        echo
    fi
    if [[ ${AIRFLOW_PIP_VERSION=} == "" ]]; then
        echo
        echo "${COLOR_BLUE}Installing latest pip version${COLOR_RESET}"
        echo
        pip install --root-user-action ignore --disable-pip-version-check --upgrade pip
    elif [[ ! ${AIRFLOW_PIP_VERSION} =~ ^[0-9].* ]]; then
        echo
        echo "${COLOR_BLUE}Installing pip version from spec ${AIRFLOW_PIP_VERSION}${COLOR_RESET}"
        echo
        # shellcheck disable=SC2086
        pip install --root-user-action ignore --disable-pip-version-check "pip @ ${AIRFLOW_PIP_VERSION}"
    else
        local installed_pip_version
        installed_pip_version=$(python -c 'from importlib.metadata import version; print(version("pip"))')
        if [[ ${installed_pip_version} != "${AIRFLOW_PIP_VERSION}" ]]; then
            echo
            echo "${COLOR_BLUE}(Re)Installing pip version: ${AIRFLOW_PIP_VERSION}${COLOR_RESET}"
            echo
            pip install --root-user-action ignore --disable-pip-version-check "pip==${AIRFLOW_PIP_VERSION}"
        fi
    fi
    if [[ ${AIRFLOW_UV_VERSION=} == "" ]]; then
        echo
        echo "${COLOR_BLUE}Installing latest uv version${COLOR_RESET}"
        echo
        pip install --root-user-action ignore --disable-pip-version-check --upgrade uv
    elif [[ ! ${AIRFLOW_UV_VERSION} =~ ^[0-9].* ]]; then
        echo
        echo "${COLOR_BLUE}Installing uv version from spec ${AIRFLOW_UV_VERSION}${COLOR_RESET}"
        echo
        # shellcheck disable=SC2086
        pip install --root-user-action ignore --disable-pip-version-check "uv @ ${AIRFLOW_UV_VERSION}"
    else
        local installed_uv_version
        installed_uv_version=$(python -c 'from importlib.metadata import version; print(version("uv"))' 2>/dev/null || echo "Not installed yet")
        if [[ ${installed_uv_version} != "${AIRFLOW_UV_VERSION}" ]]; then
            echo
            echo "${COLOR_BLUE}(Re)Installing uv version: ${AIRFLOW_UV_VERSION}${COLOR_RESET}"
            echo
            # shellcheck disable=SC2086
            pip install --root-user-action ignore --disable-pip-version-check "uv==${AIRFLOW_UV_VERSION}"
        fi
    fi
    if  [[ ${AIRFLOW_PREK_VERSION=} == "" ]]; then
        echo
        echo "${COLOR_BLUE}Installing latest prek, uv${COLOR_RESET}"
        echo
        uv tool install prek --with uv
        # make sure that the venv/user in .local exists
        mkdir -p "${HOME}/.local/bin"
    else
        echo
        echo "${COLOR_BLUE}Installing predefined versions of prek, uv:${COLOR_RESET}"
        echo "${COLOR_BLUE}prek(${AIRFLOW_PREK_VERSION}) uv(${AIRFLOW_UV_VERSION})${COLOR_RESET}"
        echo
        uv tool install "prek==${AIRFLOW_PREK_VERSION}" --with "uv==${AIRFLOW_UV_VERSION}"
        # make sure that the venv/user in .local exists
        mkdir -p "${HOME}/.local/bin"
    fi
}

function common::import_trusted_gpg() {
    common::get_colors

    local key=${1:?${COLOR_RED}First argument expects OpenPGP Key ID${COLOR_RESET}}
    local name=${2:?${COLOR_RED}Second argument expected trust storage name${COLOR_RESET}}
    # Please note that not all servers could be used for retrieve keys
    #  sks-keyservers.net: Unmaintained and DNS taken down due to GDPR requests.
    #  keys.openpgp.org: User ID Mandatory, not suitable for APT repositories
    #  keyring.debian.org: Only accept keys in Debian keyring.
    #  pgp.mit.edu: High response time.
    local keyservers=(
        "hkps://keyserver.ubuntu.com"
        "hkps://pgp.surf.nl"
    )

    GNUPGHOME="$(mktemp -d)"
    export GNUPGHOME
    set +e
    for keyserver in $(shuf -e "${keyservers[@]}"); do
        echo "${COLOR_BLUE}Try to receive GPG public key ${key} from ${keyserver}${COLOR_RESET}"
        gpg --keyserver "${keyserver}" --recv-keys "${key}" 2>&1 && break
        echo "${COLOR_YELLOW}Unable to receive GPG public key ${key} from ${keyserver}${COLOR_RESET}"
    done
    set -e
    gpg --export "${key}" > "/etc/apt/trusted.gpg.d/${name}.gpg"
    gpgconf --kill all
    rm -rf "${GNUPGHOME}"
    unset GNUPGHOME
}
