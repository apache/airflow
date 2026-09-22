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

set -euo pipefail

COLOR_BLUE=$'\e[34m'
COLOR_RESET=$'\e[0m'

echo "${COLOR_BLUE}Disk space before cleanup${COLOR_RESET}"
df -H

echo "${COLOR_BLUE}Freeing up disk space${COLOR_RESET}"

# These directories are independent; cap concurrent deletions to limit disk contention.
cleanup_targets=(
    /usr/share/dotnet/
    /usr/local/graalvm/
    /usr/local/.ghcup/
    /usr/local/share/powershell
    /usr/local/share/chromium
    /usr/local/share/boost
    /usr/local/lib/android
    /opt/hostedtoolcache
    /opt/ghc
)
printf '%s\0' "${cleanup_targets[@]}" | xargs -0 -r -n 1 -P 4 sudo rm -rf --

sudo apt-get clean

echo "${COLOR_BLUE}Disk space after cleanup${COLOR_RESET}"
df -H
