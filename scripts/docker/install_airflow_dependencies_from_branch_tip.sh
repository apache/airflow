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

# Installs Airflow from $AIRFLOW_BRANCH tip. This is pure optimisation. It is done because we do not want
# to reinstall all dependencies from scratch when setup.py changes. Problem with Docker caching is that
# when a file is changed, when added to docker context, it invalidates the cache and it causes Docker
# build to reinstall all dependencies from scratch. This can take a loooooot of time. Therefore we install
# the dependencies first from main (and uninstall airflow right after) so that we can start installing
# deps from those pre-installed dependencies. It saves few minutes of build time when setup.py changes.
#
# If INSTALL_MYSQL_CLIENT is set to false, mysql extra is removed
#
# shellcheck source=scripts/docker/common.sh
. "$( dirname "${BASH_SOURCE[0]}" )/common.sh"

: "${AIRFLOW_REPO:?Should be set}"
: "${AIRFLOW_BRANCH:?Should be set}"
: "${INSTALL_MYSQL_CLIENT:?Should be true or false}"
: "${AIRFLOW_PIP_VERSION:?Should be set}"

function install_airflow_dependencies_from_branch_tip() {
    echo
    echo "${COLOR_BLUE}Installing airflow from ${AIRFLOW_BRANCH}. It is used to cache dependencies${COLOR_RESET}"
    echo
    if [[ ${INSTALL_MYSQL_CLIENT} != "true" ]]; then
       AIRFLOW_EXTRAS=${AIRFLOW_EXTRAS/mysql,}
    fi
    # Install latest set of dependencies using constraints. In case constraints were upgraded and there
    # are conflicts, this might fail, but it should be fixed in the following installation steps
    pip install \
      "https://github.com/${AIRFLOW_REPO}/archive/${AIRFLOW_BRANCH}.tar.gz#egg=apache-airflow[${AIRFLOW_EXTRAS}]" \
      --constraint "${AIRFLOW_CONSTRAINTS_LOCATION}" || true
    # make sure correct PIP version is used
    pip install --disable-pip-version-check "pip==${AIRFLOW_PIP_VERSION}"
    pip freeze | grep apache-airflow-providers | xargs pip uninstall --yes 2>/dev/null || true
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
