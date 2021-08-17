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

function copy_sources() {
    group_start "Copy sources"
    echo "==================================================================================="
    echo " Copying sources for provider packages"
    echo "==================================================================================="
    pushd "${AIRFLOW_SOURCES}"
    rm -rf "provider_packages/airflow"
    cp -r airflow "provider_packages"
    popd

    group_end
}


function build_provider_packages() {
    rm -rf dist/*
    local package_format_args=()
    if [[ ${PACKAGE_FORMAT=} != "" ]]; then
        package_format_args=("--package-format" "${PACKAGE_FORMAT}")
    fi

    local prepared_packages=()
    local skipped_packages=()
    local error_packages=()

    echo "-----------------------------------------------------------------------------------"
    if [[ "${VERSION_SUFFIX_FOR_PYPI}" == '' ]]; then
        echo
        echo "Preparing official version of provider with no suffixes"
        echo
    else
        echo
        echo " Package Version of providers suffix set for PyPI version: ${VERSION_SUFFIX_FOR_PYPI}"
        echo
    fi
    echo "-----------------------------------------------------------------------------------"

    # Delete the remote, so that we fetch it and update it once, not once per package we build!
    git remote rm apache-https-for-providers 2>/dev/null || :

    local provider_package
    for provider_package in "${PROVIDER_PACKAGES[@]}"
    do
        rm -rf -- *.egg-info build/
        local res
        set +e
        python3 "${PROVIDER_PACKAGES_DIR}/prepare_provider_packages.py" \
            generate-setup-files \
            "${OPTIONAL_VERBOSE_FLAG[@]}" \
            --no-git-update \
            --version-suffix "${VERSION_SUFFIX_FOR_PYPI}" \
            "${provider_package}"
        res=$?
        set -e
        if [[ ${res} == "64" ]]; then
            skipped_packages+=("${provider_package}")
            continue
        fi
        if [[ ${res} != "0" ]]; then
            error_packages+=("${provider_package}")
            continue
        fi
        set +e
        package_suffix=""
        if [[ -n ${VERSION_SUFFIX_FOR_PYPI} ]]; then
            # only adds suffix to setup.py if version suffix for PyPI is set
            package_suffix="${VERSION_SUFFIX_FOR_PYPI}"
        fi
        python3 "${PROVIDER_PACKAGES_DIR}/prepare_provider_packages.py" \
            build-provider-packages \
            "${OPTIONAL_VERBOSE_FLAG[@]}" \
            --no-git-update \
            --version-suffix "${package_suffix}" \
            "${package_format_args[@]}" \
            "${provider_package}"
        res=$?
        set -e
        if [[ ${res} == "64" ]]; then
            skipped_packages+=("${provider_package}")
            continue
        fi
        if [[ ${res} != "0" ]]; then
            error_packages+=("${provider_package}")
            echo "${COLOR_RED}Error when preparing ${provider_package} package${COLOR_RESET}"
            continue
        fi
        prepared_packages+=("${provider_package}")
    done
    echo "${COLOR_BLUE}===================================================================================${COLOR_RESET}"
    echo
    echo "Summary of prepared packages:"
    echo
    if [[ "${#prepared_packages[@]}" != "0" ]]; then
        echo "${COLOR_GREEN}    Prepared:${COLOR_RESET}"
        echo "${prepared_packages[*]}" | fold -w 100
    fi
    if [[ "${#skipped_packages[@]}" != "0" ]]; then
        echo "${COLOR_YELLOW}    Skipped:${COLOR_RESET}"
        echo "${skipped_packages[*]}" | fold -w 100
    fi
    if [[ "${#error_packages[@]}" != "0" ]]; then
        echo "${COLOR_RED}    Errors:${COLOR_RESET}"
        echo "${error_packages[*]}" | fold -w 100
    fi
    echo
    echo "${COLOR_BLUE}===================================================================================${COLOR_RESET}"
    if [[ ${#error_packages[@]} != "0" ]]; then
        echo
        echo "${COLOR_RED}There were errors when preparing packages. Exiting! ${COLOR_RESET}"
        exit 1
    fi
}

setup_provider_packages

cd "${PROVIDER_PACKAGES_DIR}" || exit 1

install_supported_pip_version

PROVIDER_PACKAGES=("${@}")
get_providers_to_act_on "${@}"

copy_sources
build_provider_packages

echo
echo "${COLOR_GREEN}All good! Airflow packages are prepared in dist folder${COLOR_RESET}"
echo
