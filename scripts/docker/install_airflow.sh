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
# AIRFLOW_INSTALL_VERSION      - optionally specify version to install
# UPGRADE_TO_NEWER_DEPENDENCIES - determines whether eager-upgrade should be performed with the
#                                 dependencies (with EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS added)
#
# shellcheck disable=SC2086
set -euo pipefail

test -v AIRFLOW_INSTALLATION_METHOD
test -v AIRFLOW_INSTALL_EDITABLE_FLAG
test -v AIRFLOW_INSTALL_USER_FLAG
test -v INSTALL_MYSQL_CLIENT
test -v UPGRADE_TO_NEWER_DEPENDENCIES
test -v CONTINUE_ON_PIP_CHECK_FAILURE
test -v AIRFLOW_CONSTRAINTS_LOCATION

function install_airflow() {
    # Sanity check for editable installation mode.
    if [[ ${AIRFLOW_INSTALLATION_METHOD} != "." && \
          ${AIRFLOW_INSTALL_EDITABLE_FLAG} == "--editable" ]]; then
        echo
        echo "ERROR! You can only use --editable flag when installing airflow from sources!"
        echo "       Current installation method is '${AIRFLOW_INSTALLATION_METHOD} and should be '.'"
        exit 1
    fi
    # Remove mysql from extras if client is not going to be installed
    if [[ ${INSTALL_MYSQL_CLIENT} != "true" ]]; then
        AIRFLOW_EXTRAS=${AIRFLOW_EXTRAS/mysql,}
    fi
    if [[ "${UPGRADE_TO_NEWER_DEPENDENCIES}" != "false" ]]; then
        echo
        echo Installing all packages with eager upgrade
        echo
        # eager upgrade
        pip install ${AIRFLOW_INSTALL_USER_FLAG} --upgrade --upgrade-strategy eager \
            "${AIRFLOW_INSTALLATION_METHOD}[${AIRFLOW_EXTRAS}]${AIRFLOW_INSTALL_VERSION}" \
            ${EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS}
        # make sure correct PIP version is used
        pip install ${AIRFLOW_INSTALL_USER_FLAG} --upgrade "pip==${AIRFLOW_PIP_VERSION}"
        pip check || ${CONTINUE_ON_PIP_CHECK_FAILURE}
    else \
        echo
        echo Installing all packages with constraints and upgrade if needed
        echo
        pip install ${AIRFLOW_INSTALL_USER_FLAG} ${AIRFLOW_INSTALL_EDITABLE_FLAG} \
            "${AIRFLOW_INSTALLATION_METHOD}[${AIRFLOW_EXTRAS}]${AIRFLOW_INSTALL_VERSION}" \
            --constraint "${AIRFLOW_CONSTRAINTS_LOCATION}"
        # make sure correct PIP version is used
        pip install ${AIRFLOW_INSTALL_USER_FLAG} --upgrade "pip==${AIRFLOW_PIP_VERSION}"
        # then upgrade if needed without using constraints to account for new limits in setup.py
        pip install ${AIRFLOW_INSTALL_USER_FLAG} --upgrade --upgrade-strategy only-if-needed \
            ${AIRFLOW_INSTALL_EDITABLE_FLAG} \
            "${AIRFLOW_INSTALLATION_METHOD}[${AIRFLOW_EXTRAS}]${AIRFLOW_INSTALL_VERSION}" \
        # make sure correct PIP version is used
        pip install ${AIRFLOW_INSTALL_USER_FLAG} --upgrade "pip==${AIRFLOW_PIP_VERSION}"
        pip check || ${CONTINUE_ON_PIP_CHECK_FAILURE}
    fi
}

install_airflow
