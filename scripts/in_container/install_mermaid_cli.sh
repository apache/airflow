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

# Installs mermaid-cli (mmdc) and the Chromium it drives into the running CI image, so the docs
# build can draw ``.. mermaid::`` diagrams into SVG files (see devel-common/src/sphinx_exts/mermaid_svg.py).
#
# The docs build calls this only when a package it builds contains a mermaid diagram. Together the
# two take about 1 GB on disk, which would otherwise be in the CI image that every CI job loads.
set -euo pipefail

MERMAID_CLI_VERSION="11.17.0"

# Debian's chromium is used instead of the browser puppeteer would download. fonts-liberation gives
# mermaid Liberation Sans, which has Arial's metrics, to measure its labels with.
apt-get update -qq
apt-get install -y -qq --no-install-recommends chromium fonts-liberation > /dev/null
rm -rf /var/lib/apt/lists/*

# The same supply-chain rules ts-sdk applies through pnpm: no dependency lifecycle scripts, and no
# transitive dependency published less than 14 days ago.
npm install --global --ignore-scripts --no-fund --no-audit --loglevel=error \
    --before "$(date -u -d '14 days ago' +%Y-%m-%dT%H:%M:%SZ)" \
    "@mermaid-js/mermaid-cli@${MERMAID_CLI_VERSION}"

echo "Installed $(mmdc --version | head -1) and $(chromium --version)"
