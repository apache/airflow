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

INSTALL_DIR="/files/opt/aws"
BIN_PATH="/files/bin/aws"

if [[ $# != "0" && ${1} == "--reinstall" ]]; then
    rm -rf "${INSTALL_DIR}"
    rm -f "${BIN_PATH}"
fi

hash -r

if command -v aws; then
    echo 'The "aws" command found. Installation not needed. Run with --reinstall to reinstall'
    echo "Run with --reinstall to reinstall."
    exit 1
fi

DOWNLOAD_URL="https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip"

if [[ -e ${INSTALL_DIR} ]]; then
    echo "The install directory (${INSTALL_DIR}) already exists. This may mean AWS CLI is already installed."
    echo "Please delete this directory to start the installation."
    exit 1
fi

TMP_DIR="$(mktemp -d)"
# shellcheck disable=SC2064
trap "rm -rf ${TMP_DIR}" EXIT

mkdir -p "${INSTALL_DIR}"
echo "Downloading from ${DOWNLOAD_URL}"
curl -# --fail "${DOWNLOAD_URL}" --output "${TMP_DIR}/awscliv2.zip"
echo "Extracting archive"
pushd "${TMP_DIR}" && unzip "${TMP_DIR}/awscliv2.zip" && cd aws && \
    "./install" \
    --update \
    --install-dir ${INSTALL_DIR} \
    --bin-dir "/files/bin/" && \
    popd

# Sanity check
if ! command -v aws > /dev/null; then
    echo 'Installation failed. The command "aws" was not found.'
    exit 1
fi

echo 'Installation complete.'
