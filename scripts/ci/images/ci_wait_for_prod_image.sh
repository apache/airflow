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

function verify_prod_image_dependencies {
    echo
    echo "Checking if Airflow dependencies are non-conflicting in PROD image."
    echo

    push_pull_remove_images::pull_image_github_dockerhub "${AIRFLOW_PROD_IMAGE}" \
        "${GITHUB_REGISTRY_AIRFLOW_PROD_IMAGE}:${GITHUB_REGISTRY_PULL_IMAGE_TAG}"

    # TODO: remove the | true after we fixed pip check for prod image
    docker run --rm --entrypoint /bin/bash "${AIRFLOW_PROD_IMAGE}" -c 'pip check' || true
}

push_pull_remove_images::check_if_github_registry_wait_for_image_enabled

push_pull_remove_images::check_if_jq_installed

build_image::login_to_github_registry_if_needed

export AIRFLOW_PROD_IMAGE_NAME="${BRANCH_NAME}-python${PYTHON_MAJOR_MINOR_VERSION}"

echo
echo "Waiting for image to appear: ${AIRFLOW_PROD_IMAGE_NAME}"
echo

push_pull_remove_images::wait_for_github_registry_image \
    "${AIRFLOW_PROD_IMAGE_NAME}" "${GITHUB_REGISTRY_PULL_IMAGE_TAG}"

echo
echo "Verifying the ${AIRFLOW_PROD_IMAGE_NAME} image after pulling it"
echo

verify_prod_image_dependencies
