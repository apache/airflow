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
set -eu

# HEAD^1 says the "first" parent. For PR merge commits, or main commits, this is the "right" commit.
#
# In this example, 9c532b6 is the PR commit (HEAD^2), 4840892 is the head GitHub checks-out for us, and db121f7 is the
# "merge target" (HEAD^1) -- i.e. mainline
#
# *   4840892 (HEAD, pull/11906/merge) Merge 9c532b6a2c56cd5d4c2a80ecbed60f9dfd1f5fe6 into db121f726b3c7a37aca1ea05eb4714f884456005 [Ephraim Anierobi]
# |\
# | * 9c532b6 (grafted) make taskinstances pid and duration nullable [EphraimBuddy]
# * db121f7 (grafted) Add truncate table (before copy) option to S3ToRedshiftOperator (#9246) [JavierLopezT]

previous_mainline_commit="$(git rev-parse --short HEAD^1)"

echo "Diffing openapi spec against ${previous_mainline_commit}..."

SPEC_FILE="airflow/api_connexion/openapi/v1.yaml"
readonly SPEC_FILE

GO_CLIENT_PATH="clients/go/airflow"
readonly GO_CLIENT_PATH

GO_TARGET_CLIENT_PATH="clients/go_target_branch/airflow"
readonly GO_TARGET_CLIENT_PATH

# generate client for current patch
mkdir -p "${GO_CLIENT_PATH}"

./clients/gen/go.sh "${SPEC_FILE}" "${GO_CLIENT_PATH}"
# generate client for target patch
mkdir -p "${GO_TARGET_CLIENT_PATH}"

git checkout "${previous_mainline_commit}" -- "$SPEC_FILE"
./clients/gen/go.sh "${SPEC_FILE}" "${GO_TARGET_CLIENT_PATH}"

diff -u "${GO_TARGET_CLIENT_PATH}" "${GO_CLIENT_PATH}" || true
