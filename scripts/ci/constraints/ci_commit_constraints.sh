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
cd constraints || exit 1
git config --local user.email "dev@airflow.apache.org"
git config --local user.name "Automated GitHub Actions commit"
git diff --color --exit-code --ignore-matching-lines="^#.*" || \
git commit --all --message "Updating constraints. Github run id:${GITHUB_RUN_ID}

This update in constraints is automatically committed by the CI 'constraints-push' step based on
'${GITHUB_REF}' in the '${GITHUB_REPOSITORY}' repository with commit sha ${GITHUB_SHA}.

The action that build those constraints can be found at https://github.com/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}/

The image tag used for that build was: ${IMAGE_TAG}. You can enter Breeze environment
with this image by running 'breeze shell --image-tag ${IMAGE_TAG}'

All tests passed in this build so we determined we can push the updated constraints.

See https://github.com/apache/airflow/blob/main/README.md#installing-from-pypi for details.
"
