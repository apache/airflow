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
# shellcheck shell=bash disable=SC2086

# Installs airflow and provider packages from locally present docker context files
# This is used in CI to install airflow and provider packages in the CI system of ours
# The packages are prepared from current sources and placed in the 'docker-context-files folder
# Then both airflow and provider packages are installed using those packages rather than
# PyPI
# shellcheck source=scripts/docker/common.sh
. "$( dirname "${BASH_SOURCE[0]}" )/common.sh"

# TODO: rewrite it all in Python (and all other scripts in scripts/docker)

function install_airflow_and_providers_from_docker_context_files(){
    local flags=()
    if [[ ${INSTALL_MYSQL_CLIENT} != "true" ]]; then
        AIRFLOW_EXTRAS=${AIRFLOW_EXTRAS/mysql,}
    fi
    if [[ ${INSTALL_POSTGRES_CLIENT} != "true" ]]; then
        AIRFLOW_EXTRAS=${AIRFLOW_EXTRAS/postgres,}
    fi

    if [[ ! -d /docker-context-files ]]; then
        echo
        echo "${COLOR_RED}You must provide a folder via --build-arg DOCKER_CONTEXT_FILES=<FOLDER> and you missed it!${COLOR_RESET}"
        echo
        exit 1
    fi

    # This is needed to get package names for local context packages
    ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} ${ADDITIONAL_PIP_INSTALL_FLAGS} --constraint ${HOME}/constraints.txt packaging

    if [[ -n ${AIRFLOW_EXTRAS=} ]]; then
        AIRFLOW_EXTRAS_TO_INSTALL="[${AIRFLOW_EXTRAS}]"
    else
        AIRFLOW_EXTRAS_TO_INSTALL=""
    fi

    # Find Apache Airflow package in docker-context files
    readarray -t install_airflow_package < <(EXTRAS="${AIRFLOW_EXTRAS_TO_INSTALL}" \
        python /scripts/docker/get_package_specs.py /docker-context-files/apache?airflow?[0-9]*.{whl,tar.gz} 2>/dev/null || true)
    echo
    echo "${COLOR_BLUE}Found airflow packages in docker-context-files folder: ${install_airflow_package[*]}${COLOR_RESET}"
    echo

    if [[ -z "${install_airflow_package[*]}" && ${AIRFLOW_VERSION=} != "" ]]; then
        # When we install only provider packages from docker-context files, we need to still
        # install airflow from PyPI when AIRFLOW_VERSION is set. This handles the case where
        # pre-release dockerhub image of airflow is built, but we want to install some providers from
        # docker-context files
        install_airflow_package=("apache-airflow[${AIRFLOW_EXTRAS}]==${AIRFLOW_VERSION}")
    fi

    # Find Provider/TaskSDK packages in docker-context files
    readarray -t airflow_packages< <(python /scripts/docker/get_package_specs.py /docker-context-files/apache?airflow?{providers,task?sdk}*.{whl,tar.gz} 2>/dev/null || true)
    echo
    echo "${COLOR_BLUE}Found provider packages in docker-context-files folder: ${airflow_packages[*]}${COLOR_RESET}"
    echo

    if [[ ${USE_CONSTRAINTS_FOR_CONTEXT_PACKAGES=} == "true" ]]; then
        local python_version
        python_version=$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
        local local_constraints_file=/docker-context-files/constraints-"${python_version}"/${AIRFLOW_CONSTRAINTS_MODE}-"${python_version}".txt

        if [[ -f "${local_constraints_file}" ]]; then
            echo
            echo "${COLOR_BLUE}Installing docker-context-files packages with constraints found in ${local_constraints_file}${COLOR_RESET}"
            echo
            # force reinstall all airflow + provider packages with constraints found in
            flags=(--upgrade --constraint "${local_constraints_file}")
            echo
            echo "${COLOR_BLUE}Copying ${local_constraints_file} to ${HOME}/constraints.txt${COLOR_RESET}"
            echo
            cp "${local_constraints_file}" "${HOME}/constraints.txt"
        else
            echo
            echo "${COLOR_BLUE}Installing docker-context-files packages with constraints from GitHub${COLOR_RESET}"
            echo
            flags=(--constraint "${HOME}/constraints.txt")
        fi
    else
        echo
        echo "${COLOR_BLUE}Installing docker-context-files packages without constraints${COLOR_RESET}"
        echo
        flags=()
    fi

    set -x
    ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} \
        ${ADDITIONAL_PIP_INSTALL_FLAGS} \
        "${flags[@]}" \
        "${install_airflow_package[@]}" "${airflow_packages[@]}"
    set +x
    common::install_packaging_tools
    pip check
}

# Simply install all other (non-apache-airflow) packages placed in docker-context files
# without dependencies. This is extremely useful in case you want to install via pip-download
# method on air-gaped system where you do not want to download any dependencies from remote hosts
# which is a requirement for serious installations
function install_all_other_packages_from_docker_context_files() {
    echo
    echo "${COLOR_BLUE}Force re-installing all other package from local files without dependencies${COLOR_RESET}"
    echo
    local reinstalling_other_packages
    # shellcheck disable=SC2010
    reinstalling_other_packages=$(ls /docker-context-files/*.{whl,tar.gz} 2>/dev/null | \
        grep -v apache_airflow | grep -v apache-airflow || true)
    if [[ -n "${reinstalling_other_packages}" ]]; then
        set -x
        ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} ${ADDITIONAL_PIP_INSTALL_FLAGS} \
            --force-reinstall --no-deps --no-index ${reinstalling_other_packages}
        common::install_packaging_tools
        set +x
    fi
}

common::get_colors
common::get_packaging_tool
common::get_airflow_version_specification
common::get_constraints_location
common::show_packaging_tool_version_and_location

install_airflow_and_providers_from_docker_context_files

install_all_other_packages_from_docker_context_files
