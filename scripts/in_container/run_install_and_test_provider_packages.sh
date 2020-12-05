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

setup_provider_packages

echo
echo "Testing if all classes in import packages can be imported"
echo

if [[ ${INSTALL_AIRFLOW_VERSION=""} == "" ]]; then
    echo
    echo  "${COLOR_RED_ERROR} You have to specify airflow version to install - might be from PyPI or local wheels  ${COLOR_RESET}"
    echo
    exit 1
fi

if (($# < 1)); then
    echo
    echo  "${COLOR_RED_ERROR} Missing installation type (whl/tar.gz) as first argument  ${COLOR_RESET}"
    echo
    exit 2
fi

INSTALL_TYPE=${1}
readonly INSTALL_TYPE

if [[ ${INSTALL_TYPE} != "whl" && ${INSTALL_TYPE} != "tar.gz" ]]; then
    echo
    echo  "${COLOR_RED_ERROR} Wrong install type ${INSTALL_TYPE}. Should be 'whl' or 'tar.gz'  ${COLOR_RESET}"
    echo
    exit 3
fi

if [[ ${INSTALL_AIRFLOW_VERSION} == "wheel"  ]]; then
    echo
    echo "Installing the airflow prepared from wheels"
    echo
    uninstall_airflow
    install_airflow_from_wheel
else
    uninstall_airflow
    install_released_airflow_version "${INSTALL_AIRFLOW_VERSION}" "[all]"
fi

install_remaining_dependencies

if [[ ${INSTALL_TYPE} == "whl" ]]; then
    install_all_provider_packages_from_wheels
elif [[ ${INSTALL_TYPE} == "tar.gz" ]]; then
    install_all_provider_packages_from_tar_gz_files
else
    echo
    echo  "${COLOR_RED_ERROR} Wrong package type ${1}. Should be whl or tar.gz  ${COLOR_RESET}"
    echo
    exit 1
fi

import_all_provider_classes

function discover_all_provider_packages() {
    echo
    echo Listing available providers via 'airflow providers list'
    echo

    airflow providers list

    local expected_number_of_providers=60
    local actual_number_of_providers
    actual_number_of_providers=$(airflow providers list --output table | grep -c apache-airflow-providers | xargs)
    if [[ ${actual_number_of_providers} != "${expected_number_of_providers}" ]]; then
        echo
        echo  "${COLOR_RED_ERROR} Number of providers installed is wrong  ${COLOR_RESET}"
        echo "Expected number was '${expected_number_of_providers}' and got '${actual_number_of_providers}'"
        echo
        echo "Either increase the number of providers if you added one or diagnose and fix the problem."
        echo
    fi
}

function discover_all_hooks() {
    echo
    echo Listing available hooks via 'airflow providers hooks'
    echo

    airflow providers hooks

    local expected_number_of_hooks=33
    local actual_number_of_hooks
    actual_number_of_hooks=$(airflow providers hooks --output table | grep -c conn_id | xargs)
    if [[ ${actual_number_of_hooks} != "${expected_number_of_hooks}" ]]; then
        echo
        echo  "${COLOR_RED_ERROR} Number of hooks registered is wrong  ${COLOR_RESET}"
        echo "Expected number was '${expected_number_of_hooks}' and got '${actual_number_of_hooks}'"
        echo
        echo "Either increase the number of hooks if you added one or diagnose and fix the problem."
        echo
    fi
}

function discover_all_extra_links() {
    echo
    echo Listing available extra links via 'airflow providers links'
    echo

    airflow providers links

    local expected_number_of_extra_links=4
    local actual_number_of_extra_links
    actual_number_of_extra_links=$(airflow providers links --output table | grep -c ^airflow.providers | xargs)
    if [[ ${actual_number_of_extra_links} != "${expected_number_of_extra_links}" ]]; then
        echo
        echo  "${COLOR_RED_ERROR} Number of links registered is wrong  ${COLOR_RESET}"
        echo "Expected number was '${expected_number_of_extra_links}' and got '${actual_number_of_extra_links}'"
        echo
        echo "Either increase the number of links if you added one or diagnose and fix the problem."
        echo
    fi
}


if [[ ${BACKPORT_PACKAGES} != "true" ]]; then
    discover_all_provider_packages
    discover_all_hooks
    discover_all_extra_links
fi
