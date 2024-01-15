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

function install_pipx_tools() {
    echo
    echo "${COLOR_BLUE}Installing pipx tools${COLOR_RESET}"
    echo
    # Make sure PIPX is installed in latest version
    pip install --root-user-action ignore  --upgrade "pipx>=1.2.1"
    if [[ $(uname -m) != "aarch64" ]]; then
        # Do not install mssql-cli for ARM
        # Install all the tools we need available in command line but without impacting the current environment
        pipx install mssql-cli

        # Unfortunately mssql-cli installed by `pipx` does not work out of the box because it uses
        # its own execution bash script which is not compliant with the auto-activation of
        # pipx venvs - we need to manually patch Python executable in the script to fix it: ¯\_(ツ)_/¯
        sed "s/python /\/root\/\.local\/pipx\/venvs\/mssql-cli\/bin\/python /" -i /root/.local/bin/mssql-cli
    fi
}

common::get_colors

install_pipx_tools
