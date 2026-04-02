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

# Install airflow using regular 'pip install' command. This install airflow depending on the arguments:
#
# AIRFLOW_INSTALLATION_METHOD - determines where to install airflow form:
#             "." - installs airflow from local sources
#             "apache-airflow" - installs airflow from PyPI 'apache-airflow' package
#             "apache-airflow @ URL - installs from URL
#
# (example GitHub URL https://github.com/apache/airflow/archive/main.tar.gz)
#
# AIRFLOW_VERSION_SPECIFICATION - optional specification for Airflow version to install (
#                                 might be ==2.0.2 for example or <3.0.0
# UPGRADE_RANDOM_INDICATOR_STRING - if set with random value determines that dependency upgrade should be done
#                                   with highest resolution. It is random in order to trigger cache
#                                   invalidation when building images
#
# shellcheck shell=bash disable=SC2086
# shellcheck source=scripts/docker/common.sh
. "$( dirname "${BASH_SOURCE[0]}" )/common.sh"

function install_from_sources() {
    local extra_sync_flags
    extra_sync_flags=""
    if [[ ${VIRTUAL_ENV=} != "" ]]; then
        extra_sync_flags="--active"
    fi
    if [[ "${UPGRADE_RANDOM_INDICATOR_STRING=}" != "" ]]; then
        if [[ ${PACKAGING_TOOL_CMD} == "pip" ]]; then
            set +x
            echo
            echo "${COLOR_RED}We only support uv not pip installation for upgrading dependencies!.${COLOR_RESET}"
            echo
            exit 1
        fi
        set +x
        echo
        echo "${COLOR_BLUE}Attempting to upgrade all packages to highest versions.${COLOR_RESET}"
        echo
        # --no-binary  is needed in order to avoid libxml and xmlsec using different version of libxml2
        # (binary lxml embeds its own libxml2, while xmlsec uses system one).
        # See https://bugs.launchpad.net/lxml/+bug/2110068
        set -x
        uv sync --all-packages --resolution highest --group dev --group docs --group docs-gen \
            --group leveldb ${extra_sync_flags} --no-binary-package lxml --no-binary-package xmlsec \
            --no-python-downloads --no-managed-python
    else
        set +x
        echo
        echo "${COLOR_BLUE}Installing all packages from uv.lock (frozen).${COLOR_RESET}"
        echo
        # Use uv sync --frozen to install exactly what is pinned in uv.lock without re-resolving.
        # --no-binary-package is needed in order to avoid libxml and xmlsec using different version of
        # libxml2 (binary lxml embeds its own libxml2, while xmlsec uses system one).
        # See https://bugs.launchpad.net/lxml/+bug/2110068
        set -x
        if ! uv sync --all-packages --frozen --group dev --group docs --group docs-gen \
            --group leveldb ${extra_sync_flags} --no-binary-package lxml --no-binary-package xmlsec \
            --no-python-downloads --no-managed-python; then
            set +x
            if [[ ${AIRFLOW_FALLBACK_NO_CONSTRAINTS_INSTALLATION} != "true" ]]; then
                echo
                echo "${COLOR_RED}Failing because frozen uv.lock installation failed and fallback is disabled.${COLOR_RESET}"
                echo
                exit 1
            fi
            echo
            echo "${COLOR_YELLOW}Likely pyproject.toml has new dependencies not reflected in uv.lock.${COLOR_RESET}"
            echo
            echo "${COLOR_BLUE}Falling back to re-resolving dependencies (uv sync without --frozen).${COLOR_RESET}"
            echo
            set -x
            uv sync --all-packages --group dev --group docs --group docs-gen \
                --group leveldb ${extra_sync_flags} --no-binary-package lxml --no-binary-package xmlsec \
                --no-python-downloads --no-managed-python
            set +x
        fi
    fi
}

function install_from_external_spec() {
     local installation_command_flags
    if [[ ${AIRFLOW_INSTALLATION_METHOD} == "apache-airflow" ]]; then
        installation_command_flags="apache-airflow[${AIRFLOW_EXTRAS}]${AIRFLOW_VERSION_SPECIFICATION}"
    else
        echo
        echo "${COLOR_RED}The '${AIRFLOW_INSTALLATION_METHOD}' installation method is not supported${COLOR_RESET}"
        echo
        echo "${COLOR_YELLOW}Supported methods are ('.', 'apache-airflow')${COLOR_RESET}"
        echo
        exit 1
    fi
    if [[ "${UPGRADE_RANDOM_INDICATOR_STRING=}" != "" ]]; then
        echo
        echo "${COLOR_BLUE}Remove airflow and all provider distributions installed before potentially${COLOR_RESET}"
        echo
        set -x
        ${PACKAGING_TOOL_CMD} freeze | grep apache-airflow | xargs ${PACKAGING_TOOL_CMD} uninstall ${EXTRA_UNINSTALL_FLAGS} 2>/dev/null || true
        set +x
        echo
        echo "${COLOR_BLUE}Installing all packages with highest resolutions. Installation method: ${AIRFLOW_INSTALLATION_METHOD}${COLOR_RESET}"
        echo
        set -x
        ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} ${UPGRADE_TO_HIGHEST_RESOLUTION} ${ADDITIONAL_PIP_INSTALL_FLAGS} ${installation_command_flags}
        set +x
    else
        echo
        echo "${COLOR_BLUE}Installing all packages with constraints. Installation method: ${AIRFLOW_INSTALLATION_METHOD}${COLOR_RESET}"
        echo
        set -x
        if ! ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} ${ADDITIONAL_PIP_INSTALL_FLAGS} ${installation_command_flags} --constraint "${HOME}/constraints.txt"; then
            set +x
            if [[ ${AIRFLOW_FALLBACK_NO_CONSTRAINTS_INSTALLATION} != "true" ]]; then
                echo
                echo "${COLOR_RED}Failing because constraints installation failed and fallback is disabled.${COLOR_RESET}"
                echo
                exit 1
            fi
            echo
            echo "${COLOR_YELLOW}Likely pyproject.toml has new dependencies conflicting with constraints.${COLOR_RESET}"
            echo
            echo "${COLOR_BLUE}Falling back to no-constraints installation.${COLOR_RESET}"
            echo
            set -x
            ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} ${UPGRADE_IF_NEEDED} ${ADDITIONAL_PIP_INSTALL_FLAGS} ${installation_command_flags}
            set +x
        fi
    fi
}


function install_airflow_when_building_images() {
    # Remove mysql from extras if client is not going to be installed
    if [[ ${INSTALL_MYSQL_CLIENT} != "true" ]]; then
        AIRFLOW_EXTRAS=${AIRFLOW_EXTRAS/mysql,}
        echo "${COLOR_YELLOW}MYSQL client installation is disabled. Extra 'mysql' installations were therefore omitted.${COLOR_RESET}"
    fi
    # Remove postgres from extras if client is not going to be installed
    if [[ ${INSTALL_POSTGRES_CLIENT} != "true" ]]; then
        AIRFLOW_EXTRAS=${AIRFLOW_EXTRAS/postgres,}
        echo "${COLOR_YELLOW}Postgres client installation is disabled. Extra 'postgres' installations were therefore omitted.${COLOR_RESET}"
    fi
    # Determine the installation_command_flags based on AIRFLOW_INSTALLATION_METHOD method
    if [[ ${AIRFLOW_INSTALLATION_METHOD} == "." ]]; then
        install_from_sources
    else
        install_from_external_spec
    fi
    set +x
    common::install_packaging_tools
    echo
    echo "${COLOR_BLUE}Running 'pip check'${COLOR_RESET}"
    echo
    # We use pip check here to make sure that whatever `uv` installs, is also "correct" according to `pip`
    pip check
}

common::get_colors
common::get_packaging_tool
common::get_airflow_version_specification
common::get_constraints_location
common::show_packaging_tool_version_and_location

install_airflow_when_building_images
