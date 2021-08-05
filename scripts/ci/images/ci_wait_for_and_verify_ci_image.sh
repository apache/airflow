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

if [[ $1 == "" ]]; then
  >&2 echo "Requires python MAJOR/MINOR version as first parameter"
  exit 1
fi

export PYTHON_MAJOR_MINOR_VERSION=$1
shift


# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"

image_name_with_tag="${AIRFLOW_CI_IMAGE}:${GITHUB_REGISTRY_PULL_IMAGE_TAG}"

# Remove me on 15th of August 2021 after all users had chance to rebase
legacy_ci_image="ghcr.io/${GITHUB_REPOSITORY}-${BRANCH_NAME}-python${PYTHON_MAJOR_MINOR_VERSION}-ci-v2:${GITHUB_REGISTRY_PULL_IMAGE_TAG}"

function pull_ci_image() {
    start_end::group_start "Pulling image: ${IMAGE_AVAILABLE}"
    push_pull_remove_images::pull_image_if_not_present_or_forced "${IMAGE_AVAILABLE}"
    # Remove me on 15th of August 2021 after all users had chance to rebase
    if [[ ${IMAGE_AVAILABLE} != "${image_name_with_tag}" ]]; then
        verbosity::print_info "Tagging the legacy ${IMAGE_AVAILABLE} with ${image_name_with_tag}"
        docker tag "${IMAGE_AVAILABLE}" "${image_name_with_tag}"
    fi
    start_end::group_end
}

start_end::group_start "Configure Docker Registry"
build_images::configure_docker_registry
start_end::group_end

start_end::group_start "Waiting for ${image_name_with_tag}"
# Remove me on 15th of August 2021 after all users had chance to rebase
push_pull_remove_images::wait_for_image "${image_name_with_tag}" "${legacy_ci_image}"
build_images::prepare_ci_build
start_end::group_end

pull_ci_image

if [[ ${VERIFY_IMAGE=} != "false" ]]; then
    verify_image::verify_ci_image "${image_name_with_tag}"
fi
