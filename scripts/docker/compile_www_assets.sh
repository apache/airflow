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
set -euo pipefail

BUILD_TYPE=${BUILD_TYPE="prod"}

COLOR_BLUE=$'\e[34m'
readonly COLOR_BLUE
COLOR_RESET=$'\e[0m'
readonly COLOR_RESET

# Installs additional dependencies passed as Argument to the Docker build command
function compile_www_assets() {
    echo
    echo "${COLOR_BLUE}Compiling www assets${COLOR_RESET}"
    echo
    local md5sum_file
    md5sum_file="static/dist/sum.md5"
    readonly md5sum_file
    local www_dir
    if [[ ${AIRFLOW_INSTALLATION_METHOD=} == "." ]]; then
        # In case we are building from sources in production image, we should build the assets
        www_dir="${AIRFLOW_SOURCES_TO=${AIRFLOW_SOURCES}}/airflow/www"
    else
        www_dir="$(python -m site --user-site)/airflow/www"
    fi
    pushd ${www_dir} || exit 1
    set +e
    yarn install --frozen-lockfile --no-cache 2>/tmp/out-yarn-install.txt
    local res=$?
    if [[ ${res} != 0 ]]; then
        >&2 echo
        >&2 echo "Error when running yarn install:"
        >&2 echo
        >&2 cat /tmp/out-yarn-install.txt && rm -f /tmp/out-yarn-install.txt
        exit 1
    fi
    rm -f /tmp/out-yarn-install.txt
    yarn run "${BUILD_TYPE}" 2>/tmp/out-yarn-run.txt
    res=$?
    if [[ ${res} != 0 ]]; then
        >&2 echo
        >&2 echo "Error when running yarn install:"
        >&2 echo
        >&2 cat /tmp/out-yarn-run.txt && rm -rf /tmp/out-yarn-run.txt
        exit 1
    fi
    rm -f /tmp/out-yarn-run.txt
    set -e
    find package.json yarn.lock static/css static/js -type f | sort | xargs md5sum > "${md5sum_file}"
    rm -rf "${www_dir}/node_modules"
    rm -vf "${www_dir}"/{package.json,yarn.lock,.eslintignore,.eslintrc,.stylelintignore,.stylelintrc,compile_assets.sh,webpack.config.js}
    popd || exit 1
}

compile_www_assets
