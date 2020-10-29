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
export AIRFLOW_SOURCES="${AIRFLOW_SOURCES:=$( cd "$( dirname "${BASH_SOURCE[0]}" )/../../.." && pwd )}"
echo
echo "Airflow sources: ${AIRFLOW_SOURCES}"
echo

if [[ ${USE_GITHUB_REGISTRY} != "true" ||  ${GITHUB_REGISTRY_WAIT_FOR_IMAGE} != "true" ]]; then
    echo
    echo "This script should not be called"
    echo "It need both USE_GITHUB_REGISTRY and GITHUB_REGISTRY_WAIT_FOR_IMAGE to true!"
    echo
    echo "USE_GITHUB_REGISTRY = ${USE_GITHUB_REGISTRY}"
    echo "GITHUB_REGISTRY_WAIT_FOR_IMAGE =${GITHUB_REGISTRY_WAIT_FOR_IMAGE}"
    echo
    exit 1
fi

echo
echo "Waiting for all images to appear: ${CURRENT_PYTHON_MAJOR_MINOR_VERSIONS_AS_STRING}"
echo

echo
echo "Check if jq is installed"
echo
command -v jq >/dev/null || (echo "ERROR! You must have 'jq' tool installed!" && exit 1)

echo
echo "The jq version $(jq --version)"
echo

# shellcheck source=scripts/ci/libraries/_all_libs.sh
source "${AIRFLOW_SOURCES}/scripts/ci/libraries/_all_libs.sh"

initialization::initialize_common_environment

for PYTHON_MAJOR_MINOR_VERSION in ${CURRENT_PYTHON_MAJOR_MINOR_VERSIONS_AS_STRING}
do
    export AIRFLOW_CI_IMAGE_NAME="${BRANCH_NAME}-python${PYTHON_MAJOR_MINOR_VERSION}-ci"
    push_pull_remove_images::wait_for_github_registry_image "${AIRFLOW_CI_IMAGE_NAME}" "${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
done
