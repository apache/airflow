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

if command -v terraform; then
    echo 'The "terraform" command found. Installation not needed.'
    exit 1
fi

TERRAFORM_VERSION="0.14.4"
TERRAFORM_BASE_URL="https://releases.hashicorp.com/terraform"
TERRAFORM_ZIP="terraform_${TERRAFORM_VERSION}_$(uname | tr '[:upper:]' '[:lower:]')_amd64.zip"
DOWNLOAD_URL="${TERRAFORM_BASE_URL}/${TERRAFORM_VERSION}/${TERRAFORM_ZIP}"
TMP_DIR="$(mktemp -d)"

# shellcheck disable=SC2064
trap "rm -rf ${TMP_DIR}" EXIT

mkdir -p "/files/bin/"
echo "Downloading from ${DOWNLOAD_URL}"
curl -# --fail "${DOWNLOAD_URL}" --output "${TMP_DIR}/terraform.zip"
echo "Extracting archive"
unzip "${TMP_DIR}/terraform.zip" -d /files/bin

# Sanity check
if ! command -v terraform > /dev/null; then
    echo 'Installation failed. The command "terraform" was not found.'
    exit 1
fi

echo 'Installation complete.'
