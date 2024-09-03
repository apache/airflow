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

function create_prod_venv() {
    echo
    echo "${COLOR_BLUE}Removing ${HOME}/.local and re-creating it as virtual environment.${COLOR_RESET}"
    rm -rf ~/.local
    python -m venv ~/.local
    echo "${COLOR_BLUE}The ${HOME}/.local virtualenv created.${COLOR_RESET}"
}

common::get_colors
common::get_packaging_tool
common::show_packaging_tool_version_and_location
create_prod_venv
common::install_packaging_tools
