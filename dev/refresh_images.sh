#!/bin/bash
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
rm -rf docker-context-files/*.whl
rm -rf docker-context-files/*.tgz
export ANSWER="yes"
export CI="true"
export GITHUB_TOKEN=""

breeze self-upgrade --force

breeze build-image \
     --build-multiple-images \
     --prepare-buildx-cache \
     --force-build \
     --platform linux/amd64,linux/arm64 \
     --verbose

rm -fv ./dist/* ./docker-context-files/*

breeze prepare-provider-packages \
    --package-list-file ./scripts/ci/installed_providers.txt \
    --package-format wheel \
    --version-suffix-for-pypi dev0

breeze prepare-airflow-package --package-format wheel --version-suffix-for-pypi dev0

mv -v ./dist/*.whl ./docker-context-files

breeze build-prod-image \
     --build-multiple-images \
     --airflow-is-in-context \
     --install-packages-from-context \
     --prepare-buildx-cache \
     --disable-airflow-repo-cache \
     --platform linux/amd64,linux/arm64 \
     --verbose
