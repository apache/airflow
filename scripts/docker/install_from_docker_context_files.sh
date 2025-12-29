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

# Installs airflow and provider distributions from locally present docker context files
# This is used in CI to install airflow and provider distributions in the CI system of ours
# The distributions are prepared from current sources and placed in the 'docker-context-files folder
# Then both airflow and provider distributions are installed using those distributions rather than
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

    # This is needed to get distribution names for local context distributions
    ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} ${ADDITIONAL_PIP_INSTALL_FLAGS} --constraint ${HOME}/constraints.txt packaging

    if [[ -n ${AIRFLOW_EXTRAS=} ]]; then
        AIRFLOW_EXTRAS_TO_INSTALL="[${AIRFLOW_EXTRAS}]"
    else
        AIRFLOW_EXTRAS_TO_INSTALL=""
    fi

    # Find apache-airflow distribution in docker-context files
    readarray -t install_airflow_distribution < <(EXTRAS="${AIRFLOW_EXTRAS_TO_INSTALL}" \
        python /scripts/docker/get_distribution_specs.py /docker-context-files/apache?airflow?[0-9]*.{whl,tar.gz} 2>/dev/null || true)
    echo
    echo "${COLOR_BLUE}Found apache-airflow distributions in docker-context-files folder: ${install_airflow_distribution[*]}${COLOR_RESET}"
    echo

    if [[ -z "${install_airflow_distribution[*]}" && ${AIRFLOW_VERSION=} != "" ]]; then
        # When we install only provider distributions from docker-context files, we need to still
        # install airflow from PyPI when AIRFLOW_VERSION is set. This handles the case where
        # pre-release dockerhub image of airflow is built, but we want to install some providers from
        # docker-context files
        install_airflow_distribution=("apache-airflow[${AIRFLOW_EXTRAS}]==${AIRFLOW_VERSION}")
    fi

    # Find apache-airflow-core distribution in docker-context files
    readarray -t install_airflow_core_distribution < <(EXTRAS="" \
        python /scripts/docker/get_distribution_specs.py /docker-context-files/apache?airflow?core?[0-9]*.{whl,tar.gz} 2>/dev/null || true)
    echo
    echo "${COLOR_BLUE}Found apache-airflow-core distributions in docker-context-files folder: ${install_airflow_core_distribution[*]}${COLOR_RESET}"
    echo

    if [[ -z "${install_airflow_core_distribution[*]}" && ${AIRFLOW_VERSION=} != "" ]]; then
        # When we install only provider distributions from docker-context files, we need to still
        # install airflow from PyPI when AIRFLOW_VERSION is set. This handles the case where
        # pre-release dockerhub image of airflow is built, but we want to install some providers from
        # docker-context files
        install_airflow_core_distribution=("apache-airflow-core==${AIRFLOW_VERSION}")
    fi

    # Find Provider/TaskSDK/CTL distributions in docker-context files
    readarray -t airflow_distributions< <(python /scripts/docker/get_distribution_specs.py /docker-context-files/apache?airflow?{providers,task?sdk,airflowctl}*.{whl,tar.gz} 2>/dev/null || true)
    echo
    echo "${COLOR_BLUE}Found provider distributions in docker-context-files folder: ${airflow_distributions[*]}${COLOR_RESET}"
    echo

    if [[ ${USE_CONSTRAINTS_FOR_CONTEXT_DISTRIBUTIONS=} == "true" ]]; then
        local python_version
        python_version=$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
        local local_constraints_file=/docker-context-files/constraints-"${python_version}"/${AIRFLOW_CONSTRAINTS_MODE}-"${python_version}".txt

        if [[ -f "${local_constraints_file}" ]]; then
            echo
            echo "${COLOR_BLUE}Installing docker-context-files distributions with constraints found in ${local_constraints_file}${COLOR_RESET}"
            echo
            # force reinstall all airflow + provider distributions with constraints found in
            flags=(--upgrade --constraint "${local_constraints_file}")
            echo
            echo "${COLOR_BLUE}Copying ${local_constraints_file} to ${HOME}/constraints.txt${COLOR_RESET}"
            echo
            cp "${local_constraints_file}" "${HOME}/constraints.txt"
        else
            echo
            echo "${COLOR_BLUE}Installing docker-context-files distributions with constraints from GitHub${COLOR_RESET}"
            echo
            flags=(--constraint "${HOME}/constraints.txt")
        fi
    else
        echo
        echo "${COLOR_BLUE}Installing docker-context-files distributions without constraints${COLOR_RESET}"
        echo
        flags=()
    fi

    set -x
    ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} \
        ${ADDITIONAL_PIP_INSTALL_FLAGS} \
        "${flags[@]}" \
        "${install_airflow_distribution[@]}" "${install_airflow_core_distribution[@]}" "${airflow_distributions[@]}"
    set +x
    common::install_packaging_tools
    # We use pip check here to make sure that whatever `uv` installs, is also "correct" according to `pip`
    pip check
}

# Simply install all other (non-apache-airflow) distributions placed in docker-context files
# without dependencies. This is extremely useful in case you want to install via pip-download
# method on air-gaped system where you do not want to download any dependencies from remote hosts
# which is a requirement for serious installations
function install_all_other_distributions_from_docker_context_files() {
    echo
    echo "${COLOR_BLUE}Force re-installing all other distributions from local files without dependencies${COLOR_RESET}"
    echo
    local reinstalling_other_distributions
    # shellcheck disable=SC2010
    reinstalling_other_distributions=$(ls /docker-context-files/*.{whl,tar.gz} 2>/dev/null | \
        grep -v apache_airflow | grep -v apache-airflow || true)
    if [[ -n "${reinstalling_other_distributions}" ]]; then
        set -x
        ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} ${ADDITIONAL_PIP_INSTALL_FLAGS} \
            --force-reinstall --no-deps --no-index ${reinstalling_other_distributions}
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

install_all_other_distributions_from_docker_context_files
