#!/usr/bin/env bash
# Copyright 2020 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
# This software is provided as-is,
# without warranty or representation for any use or purpose.
# Your use of it is subject to your agreement with Google.
set -eao pipefail

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/../" && pwd )"
readonly ROOT_DIR

echo "${ROOT_DIR}"
cd "${ROOT_DIR}"

# In Cloud build there is no .git directory because it gets the sources
# via GCS cache. In order to make pre-commit work,
# we need to initialize the .git folder and add all files to the local repo
# so that pre-commit picks them up. Pre-commit uses git ls-files to find files to act on
# But if you run it locally, you are likely to have the .git folder already
if [[ ! -d .git ]]; then
    git init
    git add .
fi

pre-commit run --all-files --show-diff-on-failure --color always
