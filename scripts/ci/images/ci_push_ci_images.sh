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
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"

# Pushes Ci images with tags to registry in GitHub
function push_ci_image_with_tag_to_github() {
    start_end::group_start "Push CI image"
    docker_v tag "${AIRFLOW_CI_IMAGE}" "${AIRFLOW_CI_IMAGE}:${GITHUB_REGISTRY_PUSH_IMAGE_TAG}"
    push_pull_remove_images::push_image_with_retries "${AIRFLOW_CI_IMAGE}:${GITHUB_REGISTRY_PUSH_IMAGE_TAG}"
    start_end::group_end
}

build_images::prepare_ci_build

build_images::login_to_docker_registry

push_ci_image_with_tag_to_github
