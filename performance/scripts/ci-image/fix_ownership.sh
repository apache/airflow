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

# In linux, running the script via local cloud build might change ownership of some files
# To root (because cloud build runs run in the container as root)
# This might cause problems with permissions when building images
# shellcheck source=scripts/ci-image/_functions.sh
source "$( dirname "${BASH_SOURCE[0]}" )/_functions.sh"

ci-image::initialize

docker run -v "$(pwd):/workspace" -e HOST_USER_ID="$(id -u)" "${CI_IMAGE}" \
    /bin/bash -c 'find /workspace -user root -print0 | sudo xargs -0 chown --no-dereference "${HOST_USER_ID}"'
