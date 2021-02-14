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

function pull_prod_image() {
    local image_name_with_tag="${GITHUB_REGISTRY_AIRFLOW_PROD_IMAGE}:${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
    start_end::group_start "Pulling the ${image_name_with_tag} image and tagging with ${AIRFLOW_PROD_IMAGE}"

    push_pull_remove_images::pull_image_github_dockerhub "${AIRFLOW_PROD_IMAGE}" "${image_name_with_tag}"
    start_end::group_end
}

build_images::prepare_prod_build

pull_prod_image

verify_image::verify_prod_image "${AIRFLOW_PROD_IMAGE}"
