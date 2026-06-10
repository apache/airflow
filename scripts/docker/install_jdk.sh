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
# shellcheck shell=bash
# shellcheck source=scripts/docker/common.sh
. "$( dirname "${BASH_SOURCE[0]}" )/common.sh"

set -euo pipefail

common::get_colors

# Keep these in sync:
# - jvmTarget, languageVersion, and sourceCompatibility in java-sdk/build.gradle.kts
# - TEMURIN_VERSION in scripts/docker/install_jdk.sh
# - JAVA_VERSION in .github/workflows/ci-amd.yml and .github/workflows/ci-arm.yml
# - java-version in .github/workflows/codeql-analysis.yml
readonly TEMURIN_VERSION="11"

# Fast path: skip if the right JDK is already on PATH (e.g. repeated container starts
# where the container image was not rebuilt between runs).
if java -version 2>&1 | grep -q "version \"${TEMURIN_VERSION}"; then
    echo
    echo "${COLOR_BLUE}Eclipse Temurin JDK ${TEMURIN_VERSION} is already installed, skipping.${COLOR_RESET}"
    echo
    exit 0
fi

echo
echo "${COLOR_BLUE}Installing Eclipse Temurin JDK ${TEMURIN_VERSION} from the Adoptium apt repository...${COLOR_RESET}"
echo

# Add the Adoptium apt repository (https://adoptium.net/installation/linux/).
# The Breeze CI image runs as the 'airflow' user which has passwordless sudo.
sudo apt-get update -qq
sudo apt-get install -y --no-install-recommends wget gnupg apt-transport-https ca-certificates

sudo mkdir -p /etc/apt/keyrings
wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public \
    | sudo tee /etc/apt/keyrings/adoptium.asc > /dev/null

# Derive the Debian codename from /etc/os-release (bookworm, focal, etc.).
# shellcheck source=/dev/null
DISTRO_CODENAME=$(. /etc/os-release; echo "${VERSION_CODENAME}")
echo "deb [signed-by=/etc/apt/keyrings/adoptium.asc] \
https://packages.adoptium.net/artifactory/deb ${DISTRO_CODENAME} main" \
    | sudo tee /etc/apt/sources.list.d/adoptium.list > /dev/null

sudo apt-get update -qq
sudo apt-get install -y --no-install-recommends "temurin-${TEMURIN_VERSION}-jdk"
sudo apt-get clean
sudo rm -rf /var/lib/apt/lists/*

echo
echo "${COLOR_GREEN}Eclipse Temurin JDK ${TEMURIN_VERSION} installed successfully.${COLOR_RESET}"
java -version
echo
