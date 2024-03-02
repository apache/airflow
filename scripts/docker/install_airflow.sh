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
# AIRFLOW_INSTALLATION_METHOD - determines where to install airflow form:
#             "." - installs airflow from local sources
#             "apache-airflow" - installs airflow from PyPI 'apache-airflow' package
# AIRFLOW_VERSION_SPECIFICATION - optional specification for Airflow version to install (
#                                 might be ==2.0.2 for example or <3.0.0
# UPGRADE_TO_NEWER_DEPENDENCIES - determines whether eager-upgrade should be performed with the
#                                 dependencies (with EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS added)
#
# shellcheck shell=bash disable=SC2086
# shellcheck source=scripts/docker/common.sh
. "$( dirname "${BASH_SOURCE[0]}" )/common.sh"

function install_airflow() {
    # Coherence check for editable installation mode.
    if [[ ${AIRFLOW_INSTALLATION_METHOD} != "." && \
          ${AIRFLOW_INSTALL_EDITABLE_FLAG} == "--editable" ]]; then
        echo
        echo "${COLOR_RED}ERROR! You can only use --editable flag when installing airflow from sources!${COLOR_RESET}"
        echo "${COLOR_RED}       Current installation method is '${AIRFLOW_INSTALLATION_METHOD} and should be '.'${COLOR_RESET}"
        exit 1
    fi
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
    if [[ "${UPGRADE_TO_NEWER_DEPENDENCIES}" != "false" ]]; then
        echo
        echo "${COLOR_BLUE}Remove airflow and all provider packages installed before potentially${COLOR_RESET}"
        echo
        set -x
        ${PACKAGING_TOOL_CMD} freeze | grep apache-airflow | xargs ${PACKAGING_TOOL_CMD} uninstall ${EXTRA_UNINSTALL_FLAGS} 2>/dev/null || true
        set +x
        echo
        echo "${COLOR_BLUE}Installing all packages with eager upgrade with ${AIRFLOW_INSTALL_EDITABLE_FLAG} mode${COLOR_RESET}"
        echo
        set -x
        ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} --upgrade ${RESOLUTION_HIGHEST_FLAG} \
            ${ADDITIONAL_PIP_INSTALL_FLAGS} \
            ${AIRFLOW_INSTALL_EDITABLE_FLAG} \
            "${AIRFLOW_INSTALLATION_METHOD}[${AIRFLOW_EXTRAS}]${AIRFLOW_VERSION_SPECIFICATION}" \
            ${EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS=}
        set +x
        common::install_packaging_tool
        echo
        echo "${COLOR_BLUE}Running '${PACKAGING_TOOL} check'${COLOR_RESET}"
        echo
        pip check
    else
        echo
        echo "${COLOR_BLUE}Installing all packages with constraints or upgrade if needed${COLOR_RESET}"
        echo
        set -x
        # Install all packages with constraints
        if ! ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} ${AIRFLOW_INSTALL_EDITABLE_FLAG} \
            ${ADDITIONAL_PIP_INSTALL_FLAGS} \
            "${AIRFLOW_INSTALLATION_METHOD}[${AIRFLOW_EXTRAS}]${AIRFLOW_VERSION_SPECIFICATION}" \
            --constraint "${HOME}/constraints.txt"; then
            echo
            echo "${COLOR_YELLOW}Likely pyproject.toml has new dependencies conflicting with constraints.${COLOR_RESET}"
            echo
            echo "${COLOR_BLUE}Falling back to no-constraints, lowest-direct resolution installation.${COLOR_RESET}"
            echo
            ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} --upgrade ${RESOLUTION_LOWEST_DIRECT_FLAG} \
                ${ADDITIONAL_PIP_INSTALL_FLAGS} \
                ${AIRFLOW_INSTALL_EDITABLE_FLAG} \
                "${AIRFLOW_INSTALLATION_METHOD}[${AIRFLOW_EXTRAS}]${AIRFLOW_VERSION_SPECIFICATION}"
        fi
        common::install_packaging_tool
        set +x
        echo
        echo "${COLOR_BLUE}Running 'pip check'${COLOR_RESET}"
        echo
        pip check
    fi

}

common::get_colors
common::get_packaging_tool
common::get_airflow_version_specification
common::override_pip_version_if_needed
common::get_constraints_location
common::show_packaging_tool_version_and_location

install_airflow
