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

cd "${AIRFLOW_SOURCES}/provider_packages" || exit 1

PREPARE_PROVIDER_PACKAGES_PY="${AIRFLOW_SOURCES}/dev/provider_packages/prepare_provider_packages.py"
readonly PREPARE_PROVIDER_PACKAGES_PY

verify_suffix_versions_for_package_preparation

function check_missing_providers() {
    PACKAGE_ERROR="false"

    pushd "${AIRFLOW_SOURCES}/airflow/providers" >/dev/null 2>&1 || exit 1

    LIST_OF_DIRS_FILE=$(mktemp)
    find . -type d | sed 's!./!!; s!/!.!g' | grep -E 'hooks|operators|sensors|secrets|utils' \
        > "${LIST_OF_DIRS_FILE}"

    popd >/dev/null 2>&1 || exit 1

    # Check if all providers are included
    for PACKAGE in "${PROVIDER_PACKAGES[@]}"
    do
        if ! grep -E "^${PACKAGE}" <"${LIST_OF_DIRS_FILE}" >/dev/null; then
            echo "The package ${PACKAGE} is not available in providers dir"
            PACKAGE_ERROR="true"
        fi
        sed -i "/^${PACKAGE}.*/d" "${LIST_OF_DIRS_FILE}"
    done

    if [[ ${PACKAGE_ERROR} == "true" ]]; then
        echo
        echo "ERROR! Some packages from dev/provider_packages/prepare_provider_packages.py are missing in providers dir"
        exit 1
    fi

    if [[ $(wc -l < "${LIST_OF_DIRS_FILE}") != "0" ]]; then
        echo "ERROR! Some folders from providers package are not defined"
        echo "       Please add them to dev/provider_packages/prepare_provider_packages.py:"
        echo
        cat "${LIST_OF_DIRS_FILE}"
        echo

        rm "$LIST_OF_DIRS_FILE"
        exit 1
    fi
    rm "$LIST_OF_DIRS_FILE"
}

function copy_sources() {
    if [[ ${BACKPORT_PACKAGES} == "true" ]]; then
        group_start "Copy and refactor sources"
        echo "==================================================================================="
        echo " Copying sources and refactoring code for backport provider packages"
        echo "==================================================================================="
    else
        group_start "Copy sources"
        echo "==================================================================================="
        echo " Copying sources for provider packages"
        echo "==================================================================================="
    fi

    python3 "${AIRFLOW_SOURCES}/dev/provider_packages/refactor_provider_packages.py"

    group_end
}


function get_providers_to_act_on() {
    group_start "Get all providers"
    if [[ -z "$*" ]]; then
        if [[ ${BACKPORT_PACKAGES} == "true" ]]; then
          list_subcmd="list-backportable-packages"
        else
          list_subcmd="list-providers-packages"
        fi
        while IFS='' read -r line; do PROVIDER_PACKAGES+=("$line"); done < <(
          python3 "${PREPARE_PROVIDER_PACKAGES_PY}" "$list_subcmd"
        )

        if [[ "$BACKPORT_PACKAGES" != "true" ]]; then
            # Don't check for missing packages when we are building backports -- we have filtered some out,
            # and the non-backport build will check for any missing.
            check_missing_providers
        fi
    else
        if [[ "${1}" == "--help" ]]; then
            echo
            echo "Builds all provider packages."
            echo
            echo "You can provide list of packages to build out of:"
            echo
            python3 "${PREPARE_PROVIDER_PACKAGES_PY}" list-providers-packages | tr '\n ' ' ' | fold -w 100 -s
            echo
            echo
            exit
        fi
    fi
    group_end
}

function build_provider_packages() {
    rm -rf dist/*

    for PROVIDER_PACKAGE in "${PROVIDER_PACKAGES[@]}"
    do
        group_start " Preparing ${PACKAGE_TYPE} package ${PROVIDER_PACKAGE} format: ${PACKAGE_FORMAT}"
        rm -rf -- *.egg-info build/
        LOG_FILE=$(mktemp)
        python3 "${PREPARE_PROVIDER_PACKAGES_PY}" --version-suffix "${VERSION_SUFFIX_FOR_PYPI}" \
            generate-setup-files "${PROVIDER_PACKAGE}"
        if [[ "${VERSION_SUFFIX_FOR_PYPI}" == '' && "${VERSION_SUFFIX_FOR_SVN}" == ''
                && ${FILE_VERSION_SUFFIX} == '' ]]; then
            echo
            echo "Preparing official version"
            echo
        elif [[ ${FILE_VERSION_SUFFIX} != '' ]]; then
            echo
            echo " Preparing release candidate with file version suffix only: ${FILE_VERSION_SUFFIX}"
            echo
        elif [[ "${VERSION_SUFFIX_FOR_PYPI}" == '' ]]; then
            echo
            echo " Package Version for SVN release candidate: ${TARGET_VERSION_SUFFIX}"
            echo
        elif [[ "${VERSION_SUFFIX_FOR_SVN}" == '' ]]; then
            echo
            echo " Package Version for PyPI release candidate: ${TARGET_VERSION_SUFFIX}"
            echo
        else
            # Both SV/PYPI are set to the same version here!
            echo
            echo " Pre-release version: ${TARGET_VERSION_SUFFIX}"
            echo
        fi
        echo "-----------------------------------------------------------------------------------"
        set +e
        package_suffix=""
        if [[ ${VERSION_SUFFIX_FOR_SVN} == "" && ${VERSION_SUFFIX_FOR_PYPI} != "" ]]; then
            # only adds suffix to setup.py if version suffix for PyPI is set but the SVN one is not
            package_suffix="${VERSION_SUFFIX_FOR_PYPI}"
        fi
        python3 "${PREPARE_PROVIDER_PACKAGES_PY}" --version-suffix "${package_suffix}" \
            --packages "${PROVIDER_PACKAGE}">"${LOG_FILE}" 2>&1
        RES="${?}"
        set -e
        if [[ ${RES} != "0" ]]; then
            cat "${LOG_FILE}"
            exit "${RES}"
        fi
        echo " Prepared ${PACKAGE_TYPE} package ${PROVIDER_PACKAGE} format ${PACKAGE_FORMAT}"
        echo "==================================================================================="
        group_end
    done
}

function rename_packages_if_needed() {
    group_start "Renaming packages if needed"

    cd "${AIRFLOW_SOURCES}" || exit 1

    pushd dist >/dev/null 2>&1 || exit 1

    if [[ ${FILE_VERSION_SUFFIX} != "" ]]; then
        # In case we have FILE_VERSION_SUFFIX we rename prepared files
        if [[ "${PACKAGE_FORMAT}" == "sdist" || "${PACKAGE_FORMAT}" == "both" ]]; then
            for FILE in *.tar.gz
            do
                mv "${FILE}" "${FILE//\.tar\.gz/${FILE_VERSION_SUFFIX}-bin.tar.gz}"
            done
        fi
        if [[ "${PACKAGE_FORMAT}" == "wheel" || "${PACKAGE_FORMAT}" == "both" ]]; then
            for FILE in *.whl
            do
                mv "${FILE}" "${FILE//\-py3/${FILE_VERSION_SUFFIX}-py3}"
            done
        fi
    fi

    popd
    echo
    echo "Airflow packages are in dist folder "
    echo

    group_end
}

PROVIDER_PACKAGES=("${@}")

get_providers_to_act_on "${@}"
copy_sources
build_provider_packages
rename_packages_if_needed
