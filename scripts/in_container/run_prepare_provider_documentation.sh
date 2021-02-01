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

function import_all_provider_classes() {
    group_start "Importing all classes"
    python3 "${AIRFLOW_SOURCES}/dev/import_all_classes.py" --path "airflow/providers"
    group_end
}

function verify_provider_packages_named_properly() {
    python3 "${PROVIDER_PACKAGES_DIR}/prepare_provider_packages.py" \
        "${OPTIONAL_BACKPORT_FLAG[@]}" \
        verify-provider-classes
}

function run_prepare_documentation() {
    local prepared_documentation=()
    local skipped_documentation=()
    local error_documentation=()

    local provider_package
    for provider_package in "${PROVIDER_PACKAGES[@]}"
    do
        set +e
        local res
        # There is a separate group created in logs for each provider package
        python3 "${PROVIDER_PACKAGES_DIR}/prepare_provider_packages.py" \
            --version-suffix "${TARGET_VERSION_SUFFIX}" \
            "${OPTIONAL_BACKPORT_FLAG[@]}" \
            "${OPTIONAL_RELEASE_VERSION_ARGUMENT[@]}" \
            update-package-documentation \
            "${provider_package}"
        res=$?
        if [[ ${res} == "64" ]]; then
            skipped_documentation+=("${provider_package}")
            continue
            echo "${COLOR_YELLOW}Skipping provider package '${provider_package}'${COLOR_RESET}"
        fi
        if [[ ${res} != "0" ]]; then
            echo "${COLOR_RED}Error when generating provider package '${provider_package}'${COLOR_RESET}"
            error_documentation+=("${provider_package}")
            continue
        fi
        prepared_documentation+=("${provider_package}")
        set -e
    done
    echo "${COLOR_BLUE}===================================================================================${COLOR_RESET}"
    echo
    echo "Summary of prepared documentations:"
    echo
    if [[ "${#prepared_documentation[@]}" != "0" ]]; then
        echo "${COLOR_GREEN}   Success:${COLOR_RESET}"
        echo "${prepared_documentation[@]}" | fold -w 100
    fi
    if [[ "${#skipped_documentation[@]}" != "0" ]]; then
        echo "${COLOR_YELLOW}   Skipped:${COLOR_RESET}"
        echo "${skipped_documentation[@]}" | fold -w 100
    fi
    if [[ "${#error_documentation[@]}" != "0" ]]; then
        echo "${COLOR_RED}   Errors:${COLOR_RESET}"
        echo "${error_documentation[@]}" | fold -w 100
    fi
    echo
    echo "${COLOR_BLUE}===================================================================================${COLOR_RESET}"
    if [[ ${#error_documentation[@]} != "0" ]]; then
        echo
        echo "${COLOR_RED}There were errors when preparing documentation. Exiting! ${COLOR_RESET}"
        exit 1
    fi
}


setup_provider_packages

cd "${AIRFLOW_SOURCES}" || exit 1

export PYTHONPATH="${AIRFLOW_SOURCES}"

verify_suffix_versions_for_package_preparation

install_supported_pip_version

# install extra packages missing in devel_ci
# TODO: remove it when devel_all == devel_ci
install_remaining_dependencies
reinstall_azure_storage_blob

if [[ ${BACKPORT_PACKAGES} != "true" ]]; then
    import_all_provider_classes
    verify_provider_packages_named_properly
fi

# We will be able to remove it when we get rid of BACKPORT_PACKAGES
OPTIONAL_RELEASE_VERSION_ARGUMENT=()
if [[ $# != "0" && ${1} =~ ^[0-9][0-9][0-9][0-9]\.[0-9][0-9]\.[0-9][0-9]$ ]]; then
    OPTIONAL_RELEASE_VERSION_ARGUMENT+=("--release-version" "${1}")
    shift
fi

PROVIDER_PACKAGES=("${@}")
get_providers_to_act_on "${@}"

run_prepare_documentation

echo
echo "${COLOR_GREEN}All good! Airflow Provider's documentation generated!${COLOR_RESET}"
echo
