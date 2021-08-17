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

INSTALL_DIR="/files/opt/java"
BIN_PATH="/files/bin/java"

if [[ $# != "0" && ${1} == "--reinstall" ]]; then
    rm -rf "${INSTALL_DIR}"
    rm -f "${BIN_PATH}"
fi

hash -r

if command -v java; then
    echo 'The "java" command found. Installation not needed. Run with --reinstall to reinstall'
    exit 1
fi

DOWNLOAD_URL='https://download.java.net/openjdk/jdk8u41/ri/openjdk-8u41-b04-linux-x64-14_jan_2020.tar.gz'

if [[ -e ${INSTALL_DIR} ]]; then
    echo "The install directory (${INSTALL_DIR}) already exists. This may mean java is already installed."
    echo "Run with --reinstall to reinstall."
    exit 1
fi

TMP_DIR="$(mktemp -d)"
# shellcheck disable=SC2064
trap "rm -rf ${TMP_DIR}" EXIT

mkdir -p "${INSTALL_DIR}"

echo "Downloading from ${DOWNLOAD_URL}"
curl -# --fail "${DOWNLOAD_URL}" --output "${TMP_DIR}/openjdk.tar.gz"

echo "Extracting archive"
tar xzf "${TMP_DIR}/openjdk.tar.gz" -C "${INSTALL_DIR}" --strip-components=1

echo 'Symlinking executables files to /files/bin'
mkdir -p "/files/bin/"
while IPS='' read -r line; do
    BIN_NAME="$(basename "${line}")"
    ln -s "${line}" "/files/bin/${BIN_NAME}"
done < <(find "${INSTALL_DIR}/bin/" -type f)

# Sanity check
if ! command -v java > /dev/null; then
    echo 'Installation failed. The command "java" was not found.'
    exit 1
fi

echo 'Installation complete.'
