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
# shellcheck source=scripts/docker/common.sh
. "$( dirname "${BASH_SOURCE[0]}" )/common.sh"

# Remove pnpm, npm and yarn function
function remove_npm_pnpm_yarn() {
    echo
    echo "${COLOR_BLUE}Removing npm, pnpm and yarn${COLOR_RESET}"
    echo
    set +e
    set -x
    npm cache clean --force
    # Remove pnpm
    npm uninstall -g pnpm
    # Remove yarn
    npm uninstall -g yarn
    # Remove npm
    apt-get remove -y npm
    echo "${COLOR_BLUE}npm, pnpm and yarn removed successfully${COLOR_RESET}"
    set +x
    set -e
}

common::get_colors

remove_npm_pnpm_yarn
