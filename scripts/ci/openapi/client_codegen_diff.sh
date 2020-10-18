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
# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$(dirname "${BASH_SOURCE[0]}")/../libraries/_script_init.sh"

set -e
target_remote="origin"
if [ "${CI_TARGET_REPO}" != "${CI_SOURCE_REPO}" ]; then
    target_remote="target"
    git remote add target "https://github.com/${CI_TARGET_REPO}"
    git fetch target "${CI_TARGET_BRANCH}" --depth=1
fi

echo "Diffing openapi spec against ${target_remote}/${CI_TARGET_BRANCH}..."

SPEC_FILE="airflow/api_connexion/openapi/v1.yaml"
readonly SPEC_FILE

GO_CLIENT_PATH="clients/go/airflow"
readonly GO_CLIENT_PATH

GO_TARGET_CLIENT_PATH="clients/go_target_branch/airflow"
readonly GO_TARGET_CLIENT_PATH


if ! git diff --name-only "${target_remote}/${CI_TARGET_BRANCH}" HEAD | grep "${SPEC_FILE}\|clients/gen"; then
    echo "No openapi spec change detected, going to skip client code gen validation."
    exit 0
fi

echo "OpenAPI spec change detected. comparing codegen diff..."

# generate client for current patch
mkdir -p "${GO_CLIENT_PATH}"

./clients/gen/go.sh "${SPEC_FILE}" "${GO_CLIENT_PATH}"
# generate client for target patch
mkdir -p "${GO_TARGET_CLIENT_PATH}"

git reset --hard "${target_remote}/${CI_TARGET_BRANCH}"
./clients/gen/go.sh "${SPEC_FILE}" "${GO_TARGET_CLIENT_PATH}"

diff -u "${GO_TARGET_CLIENT_PATH}" "${GO_CLIENT_PATH}" || true
