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

# Pulls CI image that has already been built
function pull_ci_image_on_ci() {
    build_images::prepare_ci_build
    start_end::group_start "Pull CI image ${AIRFLOW_CI_IMAGE}"
    build_images::clean_build_cache
    md5sum::calculate_md5sum_for_all_files
    local image_name_with_tag="${AIRFLOW_CI_IMAGE}:${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
    push_pull_remove_images::wait_for_image "${image_name_with_tag}"
    docker_v tag  "${image_name_with_tag}" "${AIRFLOW_CI_IMAGE}"
    md5sum::update_all_md5_with_group
    start_end::group_end
}

pull_ci_image_on_ci
