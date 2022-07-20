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

function prepare_airflow_packages() {
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

    rm -rf -- *egg-info*
    rm -rf -- build

    pip install --disable-pip-version-check "pip==${AIRFLOW_PIP_VERSION}" "wheel==${WHEEL_VERSION}"

    local packages=()

    if [[ ${PACKAGE_FORMAT} == "wheel" || ${PACKAGE_FORMAT} == "both" ]] ; then
        packages+=("bdist_wheel")
    fi
    if [[ ${PACKAGE_FORMAT} == "sdist" || ${PACKAGE_FORMAT} == "both" ]] ; then
        packages+=("sdist")
    fi
    local tag_build=()
    if [[ -n ${VERSION_SUFFIX_FOR_PYPI} ]]; then
        AIRFLOW_VERSION=$(python setup.py --version)
        if [[  ${AIRFLOW_VERSION} =~ ^[0-9\.]+$  ]]; then
            echo
            echo "${COLOR_BLUE}Adding ${VERSION_SUFFIX_FOR_PYPI} suffix to ${AIRFLOW_VERSION}${COLOR_RESET}"
            echo
            tag_build=('egg_info' '--tag-build' "${VERSION_SUFFIX_FOR_PYPI}")
        else
            if [[ ${AIRFLOW_VERSION} != *${VERSION_SUFFIX_FOR_PYPI} ]]; then
                echo
                echo "${COLOR_RED}The requested PyPI suffix ${VERSION_SUFFIX_FOR_PYPI} does not match the one in ${AIRFLOW_VERSION}. Exiting.${COLOR_RESET}"
                echo
                exit 1
            fi
        fi
    fi

    # Prepare airflow's wheel
    PYTHONUNBUFFERED=1 python setup.py "${tag_build[@]}" "${packages[@]}"

    # clean-up
    rm -rf -- *egg-info*
    rm -rf -- build

    echo "${COLOR_BLUE}===================================================================================${COLOR_RESET}"
    echo
    echo "${COLOR_GREEN}Airflow package prepared in format: ${PACKAGE_FORMAT}${COLOR_RESET}"
    echo
    echo "${COLOR_BLUE}===================================================================================${COLOR_RESET}"
    ls -w 1 ./dist
    echo "${COLOR_BLUE}===================================================================================${COLOR_RESET}"
}

install_supported_pip_version

prepare_airflow_packages

echo
echo "${COLOR_GREEN}All good! Airflow packages are prepared in dist folder${COLOR_RESET}"
echo
