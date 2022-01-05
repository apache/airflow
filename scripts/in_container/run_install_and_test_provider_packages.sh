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

function verify_parameters(){
    group_start "Verify parameters passed"
    echo
    echo "Testing if all classes in import packages can be imported"
    echo

    if [[ -z "${USE_AIRFLOW_VERSION=""}" ]]; then
        echo
        echo "${COLOR_RED}ERROR: You have to specify airflow version to install.${COLOR_RESET}"
        echo
        echo "It might be version from PyPI, wheel with extras or none to uninstall airflow"
        echo
        exit 1
    fi

    PACKAGE_FORMAT=${PACKAGE_FORMAT=}

    if [[ ${PACKAGE_FORMAT} != "wheel" && ${PACKAGE_FORMAT} != "sdist" ]]; then
        echo
        echo  "${COLOR_RED}ERROR: Wrong install type ${PACKAGE_FORMAT}. Should be 'wheel' or 'sdist'  ${COLOR_RESET}"
        echo
        exit 3
    fi
    group_end
}

function install_airflow_as_specified() {
    group_start "Install Airflow as specified"
    if [[ ${USE_AIRFLOW_VERSION} == "none"  ]]; then
        echo
        echo "Skip installing airflow - only install wheel packages that are present locally"
        echo
        uninstall_airflow_and_providers
    elif [[ ${USE_AIRFLOW_VERSION} == "wheel"  ]]; then
        echo
        echo "Install airflow from wheel including [${AIRFLOW_EXTRAS}] extras"
        echo
        uninstall_airflow_and_providers
        install_airflow_from_wheel "[${AIRFLOW_EXTRAS}]"
        uninstall_providers
    elif [[ ${USE_AIRFLOW_VERSION} == "sdist"  ]]; then
        echo
        echo "Install airflow from sdist including [${AIRFLOW_EXTRAS}] extras"
        echo
        uninstall_airflow_and_providers
        install_airflow_from_sdist "[${AIRFLOW_EXTRAS}]"
        uninstall_providers
    else
        echo
        echo "Install airflow from PyPI without extras"
        echo
        install_released_airflow_version "${USE_AIRFLOW_VERSION}"
        uninstall_providers
    fi
    group_end
}

function install_provider_packages() {
    group_start "Install provider packages"
    if [[ ${PACKAGE_FORMAT} == "wheel" ]]; then
        install_all_provider_packages_from_wheels
    elif [[ ${PACKAGE_FORMAT} == "sdist" ]]; then
        install_all_provider_packages_from_sdist
    else
        echo
        echo "${COLOR_RED}ERROR: Wrong package format ${PACKAGE_FORMAT}. Should be wheel or sdist${COLOR_RESET}"
        echo
        exit 1
    fi
    group_end
}

function twine_check_provider_packages() {
    group_start "Twine check provider packages"
    if [[ ${PACKAGE_FORMAT} == "wheel" ]]; then
       twine_check_provider_packages_from_wheels
    elif [[ ${PACKAGE_FORMAT} == "sdist" ]]; then
       twine_check_provider_packages_from_sdist
    else
        echo
        echo "${COLOR_RED}ERROR: Wrong package format ${PACKAGE_FORMAT}. Should be wheel or sdist${COLOR_RESET}"
        echo
        exit 1
    fi
    group_end
}


function discover_all_provider_packages() {
    group_start "Listing available providers via 'airflow providers list'"
    # Columns is to force it wider, so it doesn't wrap at 80 characters
    COLUMNS=180 airflow providers list

    local actual_number_of_providers
    actual_providers=$(airflow providers list --output yaml | grep package_name)
    actual_number_of_providers=$(wc -l <<<"$actual_providers")
    if (( actual_number_of_providers < 67 )); then
        echo
        echo  "${COLOR_RED}ERROR:Number of providers installed is wrong: ${actual_number_of_providers}${COLOR_RESET}"
        echo
        echo "Providers were:"
        echo
        echo "$actual_providers"
        exit 1
    fi
    group_end
}

function discover_all_hooks() {
    group_start "Listing available hooks via 'airflow providers hooks'"
    COLUMNS=180 airflow providers hooks
    local actual_number_of_hooks
    actual_number_of_hooks=$(airflow providers hooks --output table | grep -c "| apache" | xargs)
    if (( actual_number_of_hooks < 70 )); then
        echo
        echo  "${COLOR_RED}ERROR: Number of hooks registered is wrong: ${actual_number_of_hooks} ${COLOR_RESET}"
        echo
        exit 1
    fi
    group_end
}

function discover_all_extra_links() {
    group_start "Listing available extra links via 'airflow providers links'"
    COLUMNS=180 airflow providers links

    local actual_number_of_extra_links
    actual_number_of_extra_links=$(airflow providers links --output table | grep -c ^airflow.providers | xargs)
    if (( actual_number_of_extra_links < 7 )); then
        echo
        echo  "${COLOR_RED}ERROR: Number of links registered is wrong: ${actual_number_of_extra_links}  ${COLOR_RESET}"
        echo
        exit 1
    fi
    group_end
}

function discover_all_connection_form_widgets() {
    group_start "Listing available widgets via 'airflow providers widgets'"

    COLUMNS=180 airflow providers widgets

    local actual_number_of_widgets
    actual_number_of_widgets=$(airflow providers widgets --output table | grep -c ^extra)
    if (( actual_number_of_widgets < 44 )); then
        echo
        echo  "${COLOR_RED}ERROR: Number of connections with widgets registered is wrong: ${actual_number_of_widgets}  ${COLOR_RESET}"
        echo
        exit 1
    fi
    group_end
}

function discover_all_field_behaviours() {
    group_start "Listing connections with custom behaviours via 'airflow providers behaviours'"
    COLUMNS=180 airflow providers behaviours

    local actual_number_of_connections_with_behaviours
    actual_number_of_connections_with_behaviours=$(airflow providers behaviours --output table | grep -v "===" | \
        grep -v field_behaviours | grep -cv "^ " | xargs)
    if (( actual_number_of_connections_with_behaviours < 21 )); then
        echo
        echo  "${COLOR_RED}ERROR: Number of connections with customized behaviours is wrong: ${actual_number_of_connections_with_behaviours} ${COLOR_RESET}"
        echo
        exit 1
    fi
    group_end
}

function discover_all_logging_handlers() {
    group_start "Listing available logging handlers via 'airflow providers logging'"
    COLUMNS=180 airflow providers logging

    local actual_number_of_logging
    actual_number_of_logging=$(airflow providers logging --output table | grep -c ^airflow.providers | xargs)
    if (( actual_number_of_logging < 6 )); then
        echo
        echo  "${COLOR_RED}ERROR: Number of logging handlers registered is wrong: ${actual_number_of_logging}  ${COLOR_RESET}"
        echo
        exit 1
    fi
    group_end
}

function discover_all_secrets_backends() {
    group_start "Listing available secrets backends via 'airflow providers secrets'"
    COLUMNS=180 airflow providers secrets

    local actual_number_of_secrets
    actual_number_of_secrets=$(airflow providers logging --output table | grep -c ^airflow.providers | xargs)
    if (( actual_number_of_secrets < 5 )); then
        echo
        echo  "${COLOR_RED}ERROR: Number of secrets backends registered is wrong: ${actual_number_of_secrets}  ${COLOR_RESET}"
        echo
        exit 1
    fi
    group_end
}

function discover_all_auth_backends() {
    group_start "Listing available API auth backends via 'airflow providers auth'"
    COLUMNS=180 airflow providers auth

    local actual_number_of_auth
    actual_number_of_auth=$(airflow providers logging --output table | grep -c ^airflow.providers | xargs)
    if (( actual_number_of_auth < 1 )); then
        echo
        echo  "${COLOR_RED}ERROR: Number of auth backends registered is wrong: ${actual_number_of_auth}  ${COLOR_RESET}"
        echo
        exit 1
    fi
    group_end
}

function ver() {
  # convert SemVer number to comparable string (strips pre-release version)
  # shellcheck disable=SC2086,SC2183
  printf "%04d%04d%04d%.0s" ${1//[.-]/ }
}

setup_provider_packages
verify_parameters
install_airflow_as_specified

if [[ ${SKIP_TWINE_CHECK=""} != "true" ]]; then
    # Airflow 2.1.0 installs importlib_metadata version that does not work well with twine
    # So we should skip twine check in this case
    twine_check_provider_packages
fi
install_provider_packages
import_all_provider_classes

discover_all_provider_packages
discover_all_hooks
discover_all_connection_form_widgets
discover_all_field_behaviours
discover_all_extra_links

# The commands below are available only for airflow version 2.2.0+
airflow_version=$(airflow version)
airflow_version_comparable=$(ver "${airflow_version}")
min_airflow_version_comparable=$(ver "2.2.0")
if (( airflow_version_comparable >= min_airflow_version_comparable )); then
    discover_all_logging_handlers
    discover_all_secrets_backends
    discover_all_auth_backends
fi
