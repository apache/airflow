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

if command -v imgcat; then
    echo 'The "imgcat" command found. Installation not needed.'
    exit 1
fi

DOWNLOAD_URL="https://iterm2.com/utilities/imgcat"

mkdir -p "/files/bin/"
echo "Downloading from ${DOWNLOAD_URL}"
curl -# --fail "${DOWNLOAD_URL}" --output "/files/bin/imgcat"
chmod +x "/files/bin/imgcat"

# Sanity check
if ! command -v imgcat > /dev/null; then
    echo 'Installation failed. The command "imgcat" was not found.'
    exit 1
fi

echo 'Installation complete.'
