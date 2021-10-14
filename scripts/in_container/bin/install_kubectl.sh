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

BIN_PATH="/files/bin/kubectl"

if [[ $# != "0" && ${1} == "--reinstall" ]]; then
    rm -f "${BIN_PATH}"
fi

hash -r

if command -v kubectl; then
    echo 'The "kubectl" command found. Installation not needed. Run with --reinstall to reinstall'
    exit 1
fi

KUBECTL_VERSION="$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)"
DOWNLOAD_URL="https://storage.googleapis.com/kubernetes-release/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl"

if [[ -e ${BIN_PATH} ]]; then
    echo "The binary file (${BIN_PATH}) already exists. This may mean kubectl is already installed."
    echo "Run with --reinstall to reinstall."
    exit 1
fi

mkdir -p "/files/bin/"
echo "Downloading from ${DOWNLOAD_URL}"
curl -# --fail "${DOWNLOAD_URL}" --output "${BIN_PATH}"
chmod +x "${BIN_PATH}"

# Coherence check
if ! command -v kubectl > /dev/null; then
    echo 'Installation failed. The command "kubectl" was not found.'
    exit 1
fi

echo 'Installation complete.'
