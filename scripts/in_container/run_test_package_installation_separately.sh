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
# shellcheck source=scripts/in_container/_in_container_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/_in_container_script_init.sh"
OUT_FILE_PRINTED_ON_ERROR=$(mktemp)

echo
echo "Testing if all backport packages can be installed separately on Airflow 1.10 and cause no side effects"
echo

if [[ ! ${INSTALL_AIRFLOW_VERSION:=""} =~ 1.10* ]]; then
    echo
    echo "ERROR! You can only install providers package in 1.10. airflow series."
    echo "You have: ${INSTALL_AIRFLOW_VERSION}"
    echo "Set INSTALL_AIRFLOW_VERSION variable to the version you want to install before running!"
    exit 1
else
    # and install specified airflow from PyPI
    pip install "apache-airflow==${INSTALL_AIRFLOW_VERSION}" >>"${OUT_FILE_PRINTED_ON_ERROR}" 2>&1
fi

# Install all packages separately one-by-one
for PACKAGE_FILE in /dist/apache_airflow_backport_providers_*.whl
do
    if [[ ! ${PACKAGE_FILE} =~ /dist/(apache_airflow_backport_providers_[^-]*)-.* ]]; then
        echo
        echo "ERROR: ${PACKAGE_FILE} does not match providers package regexp"
        echo
    else
        PACKAGE_NAME_UNDERSCORE=${BASH_REMATCH[1]}
        PACKAGE_NAME=$(echo "${PACKAGE_NAME_UNDERSCORE}" | tr "_" "-")
    fi
    echo "==================================================================================="
    echo "Installing ${PACKAGE_NAME}"
    echo "-----------------------------------------------------------------------------------"
    pip install "${PACKAGE_FILE}" >>"${OUT_FILE_PRINTED_ON_ERROR}" 2>&1
    echo "Installed ${PACKAGE_NAME}"
    echo "-----------------------------------------------------------------------------------"
    echo "Uninstalling ${PACKAGE_NAME}"
    echo "-----------------------------------------------------------------------------------"
    pip uninstall -y "${PACKAGE_NAME}" >>"${OUT_FILE_PRINTED_ON_ERROR}" 2>&1
    echo "Uninstalled ${PACKAGE_NAME}"
    echo "-----------------------------------------------------------------------------------"
    AIRFLOW_VERSION=$(pip freeze | grep "apache-airflow==" | sed "s/apache-airflow==//")
    echo "Airflow version after installation ${AIRFLOW_VERSION}"
    if [[ ${AIRFLOW_VERSION} != "${INSTALL_AIRFLOW_VERSION}" ]]; then
        echo
        echo "ERROR! Installing ${PACKAGE_FILE} caused Airflow to upgrade to ${AIRFLOW_VERSION}"
        echo
        echo "Please fix dependencies in the package"
        exit 1
    fi
done
echo "==================================================================================="
