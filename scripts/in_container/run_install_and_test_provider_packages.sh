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
    >&2 echo
    >&2 echo "You have to specify airflow version to install - might be from PyPI or local wheels"
    >&2 echo
    exit 1
fi

if (($# < 1)); then
    >&2 echo
    >&2 echo "Missing installation type (whl/tar.gz) as first argument"
    >&2 echo
    exit 2
fi

INSTALL_TYPE=${1}
readonly INSTALL_TYPE

if [[ ${INSTALL_TYPE} != "whl" && ${INSTALL_TYPE} != "tar.gz" ]]; then
    >&2 echo
    >&2 echo "ERROR! Wrong install type ${INSTALL_TYPE}. Should be 'whl' or 'tar.gz'"
    >&2 echo
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
    >&2 echo
    >&2 echo "ERROR! Wrong package type ${1}. Should be whl or tar.gz"
    >&2 echo
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
    actual_number_of_providers=$(airflow providers list --output simple | grep -c apache-airflow-providers | xargs)
    if [[ ${actual_number_of_providers} != "${expected_number_of_providers}" ]]; then
        >&2 echo "ERROR! Number of providers installed is wrong!"
        >&2 echo "Expected number was '${expected_number_of_providers}' and got '${actual_number_of_providers}'"
        >&2 echo
        >&2 echo "Either increase the number of providers if you added one or fix the problem with imports if you see one."
        >&2 echo
    fi
}

if [[ ${BACKPORT_PACKAGES} != "true" ]]; then
    discover_all_provider_packages
fi
