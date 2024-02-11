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
# shellcheck shell=bash disable=SC2086

# Installs Airflow from $AIRFLOW_BRANCH tip. This is pure optimisation. It is done because we do not want
# to reinstall all dependencies from scratch when pyproject.toml changes. Problem with Docker caching is that
# when a file is changed, when added to docker context, it invalidates the cache and it causes Docker
# build to reinstall all dependencies from scratch. This can take a loooooot of time. Therefore we install
# the dependencies first from main (and uninstall airflow right after) so that we can start installing
# deps from those pre-installed dependencies. It saves few minutes of build time when pyproject.toml changes.
#
# If INSTALL_MYSQL_CLIENT is set to false, mysql extra is removed
# If INSTALL_POSTGRES_CLIENT is set to false, postgres extra is removed
#
# shellcheck source=scripts/docker/common.sh
. "$( dirname "${BASH_SOURCE[0]}" )/common.sh"

: "${AIRFLOW_REPO:?Should be set}"
: "${AIRFLOW_BRANCH:?Should be set}"
: "${INSTALL_MYSQL_CLIENT:?Should be true or false}"
: "${INSTALL_POSTGRES_CLIENT:?Should be true or false}"
: "${AIRFLOW_PIP_VERSION:?Should be set}"

function install_airflow_dependencies_from_branch_tip() {
    echo
    echo "${COLOR_BLUE}Installing airflow from ${AIRFLOW_BRANCH}. It is used to cache dependencies${COLOR_RESET}"
    echo
    if [[ ${INSTALL_MYSQL_CLIENT} != "true" ]]; then
       AIRFLOW_EXTRAS=${AIRFLOW_EXTRAS/mysql,}
    fi
    if [[ ${INSTALL_POSTGRES_CLIENT} != "true" ]]; then
       AIRFLOW_EXTRAS=${AIRFLOW_EXTRAS/postgres,}
    fi
    # Install latest set of dependencies - without constraints. This is to download a "base" set of
    # dependencies that we can cache and reuse when installing airflow using constraints and latest
    # pyproject.toml in the next step (when we install regular airflow).
    set -x
    pip install --root-user-action ignore \
      ${ADDITIONAL_PIP_INSTALL_FLAGS} \
      "apache-airflow[${AIRFLOW_EXTRAS}] @ https://github.com/${AIRFLOW_REPO}/archive/${AIRFLOW_BRANCH}.tar.gz"
    common::install_pip_version
    # Uninstall airflow to keep only the dependencies. In the future when planned https://github.com/pypa/pip/issues/11440
    # is implemented in pip we might be able to use this flag and skip the remove step.
    pip freeze | grep apache-airflow-providers | xargs pip uninstall --yes 2>/dev/null || true
    set +x
    echo
    echo "${COLOR_BLUE}Uninstalling just airflow. Dependencies remain. Now target airflow can be reinstalled using mostly cached dependencies${COLOR_RESET}"
    echo
    pip uninstall --yes apache-airflow || true
}

common::get_colors
common::get_airflow_version_specification
common::override_pip_version_if_needed
common::get_constraints_location
common::show_pip_version_and_location

install_airflow_dependencies_from_branch_tip
