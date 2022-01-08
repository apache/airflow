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
# shellcheck disable=SC2086

# Installs airflow and provider packages from locally present docker context files
# This is used in CI to install airflow and provider packages in the CI system of ours
# The packages are prepared from current sources and placed in the 'docker-context-files folder
# Then both airflow and provider packages are installed using those packages rather than
# PyPI
# shellcheck source=scripts/docker/common.sh
. "$( dirname "${BASH_SOURCE[0]}" )/common.sh"

: "${AIRFLOW_PIP_VERSION:?Should be set}"

function install_airflow_and_providers_from_docker_context_files(){
    if [[ ${INSTALL_MYSQL_CLIENT} != "true" ]]; then
        AIRFLOW_EXTRAS=${AIRFLOW_EXTRAS/mysql,}
    fi

    # shellcheck disable=SC2206
    local pip_flags=(
        # Don't quote this -- if it is empty we don't want it to create an
        # empty array element
        --find-links="file:///docker-context-files"
    )

    # Find Apache Airflow packages in docker-context files
    local reinstalling_apache_airflow_package
    reinstalling_apache_airflow_package=$(ls \
        /docker-context-files/apache?airflow?[0-9]*.{whl,tar.gz} 2>/dev/null || true)
    # Add extras when installing airflow
    if [[ -n "${reinstalling_apache_airflow_package}" ]]; then
        # When a provider depends on a dev version of Airflow, we need to
        # specify `apache-airflow==$VER`, otherwise pip will look for it on
        # pip, and fail to find it

        # This will work as long as the wheel file is correctly named, which it
        # will be if it was build by wheel tooling
        local ver
        ver=$(basename "$reinstalling_apache_airflow_package" | cut -d "-" -f 2)
        reinstalling_apache_airflow_package="apache-airflow[${AIRFLOW_EXTRAS}]==$ver"
    fi

    # Find Apache Airflow packages in docker-context files
    local reinstalling_apache_airflow_providers_packages
    reinstalling_apache_airflow_providers_packages=$(ls \
        /docker-context-files/apache?airflow?providers*.{whl,tar.gz} 2>/dev/null || true)
    if [[ -z "${reinstalling_apache_airflow_package}" && \
          -z "${reinstalling_apache_airflow_providers_packages}" ]]; then
        return
    fi

    if [[ "${UPGRADE_TO_NEWER_DEPENDENCIES}" != "false" ]]; then
        echo
        echo "${COLOR_BLUE}Force re-installing airflow and providers from local files with eager upgrade${COLOR_RESET}"
        echo
        # force reinstall all airflow + provider package local files with eager upgrade
        pip install "${pip_flags[@]}" --upgrade --upgrade-strategy eager \
            ${reinstalling_apache_airflow_package} ${reinstalling_apache_airflow_providers_packages} \
            ${EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS}
    else
        echo
        echo "${COLOR_BLUE}Force re-installing airflow and providers from local files with constraints and upgrade if needed${COLOR_RESET}"
        echo
        if [[ ${AIRFLOW_CONSTRAINTS_LOCATION} == "/"* ]]; then
            grep -ve '^apache-airflow' <"${AIRFLOW_CONSTRAINTS_LOCATION}" > /tmp/constraints.txt
        else
            # Remove provider packages from constraint files because they are locally prepared
            curl -L "${AIRFLOW_CONSTRAINTS_LOCATION}" | grep -ve '^apache-airflow' > /tmp/constraints.txt
        fi
        # force reinstall airflow + provider package local files with constraints + upgrade if needed
        pip install "${pip_flags[@]}" --force-reinstall \
            ${reinstalling_apache_airflow_package} ${reinstalling_apache_airflow_providers_packages} \
            --constraint /tmp/constraints.txt
        rm /tmp/constraints.txt
        # make sure correct PIP version is used \
        pip install "pip==${AIRFLOW_PIP_VERSION}"
        # then upgrade if needed without using constraints to account for new limits in setup.py
        pip install --upgrade --upgrade-strategy only-if-needed \
             ${reinstalling_apache_airflow_package} ${reinstalling_apache_airflow_providers_packages}
    fi

    # make sure correct PIP version is left installed
    pip install "pip==${AIRFLOW_PIP_VERSION}"
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
    if [[ -n "${reinstalling_other_packages}" ]]; then \
        pip install --force-reinstall --no-deps --no-index ${reinstalling_other_packages}
        # make sure correct PIP version is used
        pip install "pip==${AIRFLOW_PIP_VERSION}"
    fi
}

common::get_colors
common::get_airflow_version_specification
common::override_pip_version_if_needed
common::get_constraints_location
common::show_pip_version_and_location

install_airflow_and_providers_from_docker_context_files

common::show_pip_version_and_location
install_all_other_packages_from_docker_context_files
