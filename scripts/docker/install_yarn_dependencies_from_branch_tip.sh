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

# Installs Yarn dependencies from $AIRFLOW_BRANCH tip. This is pure optimization. It is done because we do not want
# to reinstall all dependencies from scratch when package.json changes. Problem with Docker caching is that
# when a file is changed, when added to docker context, it invalidates the cache and it causes Docker
# build to reinstall all dependencies from scratch. This can take a loooooot of time. Therefore we install
# the dependencies first from main (and uninstall airflow right after) so that we can start installing
# deps from those pre-installed dependencies. It saves few minutes of build time when package.json changes.
#
# shellcheck source=scripts/docker/common.sh
. "$( dirname "${BASH_SOURCE[0]}" )/common.sh"

: "${AIRFLOW_REPO:?Should be set}"
: "${AIRFLOW_BRANCH:?Should be set}"
: "${AIRFLOW_SOURCES:?Should be set}"

function install_yarn_dependencies_from_branch_tip() {
    echo
    echo "${COLOR_BLUE}Installing Yarn dependencies from ${AIRFLOW_BRANCH}. It is used to cache dependencies${COLOR_RESET}"
    echo
    local TEMP_AIRFLOW_DIR
    TEMP_AIRFLOW_DIR=$(mktemp -d)
    # Download the source code from the specified branch
    set -x
    curl -fsSL "https://github.com/${AIRFLOW_REPO}/archive/${AIRFLOW_BRANCH}.tar.gz" | \
        tar xz -C "${TEMP_AIRFLOW_DIR}" --strip 1
    # Install Yarn dependencies
    cd "${TEMP_AIRFLOW_DIR}/airflow/www"
    yarn install --frozen-lockfile
    set +x
    echo "${COLOR_BLUE}Yarn dependencies installed successfully${COLOR_RESET}"
    # Clean up
    rm -rf "${TEMP_AIRFLOW_DIR}"
}

common::get_colors

install_yarn_dependencies_from_branch_tip
