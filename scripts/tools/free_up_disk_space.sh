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

COLOR_BLUE=$'\e[34m'
COLOR_RESET=$'\e[0m'

echo "${COLOR_BLUE}Disk space before cleanup${COLOR_RESET}"
df -H

echo "${COLOR_BLUE}Freeing up disk space${COLOR_RESET}"
sudo rm -rf /usr/share/dotnet/
sudo rm -rf /usr/local/graalvm/
sudo rm -rf /usr/local/.ghcup/
sudo rm -rf /usr/local/share/powershell
sudo rm -rf /usr/local/share/chromium
sudo rm -rf /usr/local/share/boost
sudo rm -rf /usr/local/lib/android
sudo rm -rf /opt/hostedtoolcache
sudo rm -rf /opt/ghc
sudo apt-get clean
echo "${COLOR_BLUE}Disk space after cleanup${COLOR_RESET}"
df -H
