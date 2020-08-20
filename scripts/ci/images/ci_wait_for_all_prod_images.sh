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

# This is hook build used by DockerHub. We are also using it
# on CI to potentially rebuild (and refresh layers that
# are not cached) Docker images that are used to run CI jobs
# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"



echo
echo "Waiting for all images to appear: ${CURRENT_PYTHON_MAJOR_MINOR_VERSIONS[*]}"
echo

echo
echo "Check if jq is installed"
echo
command -v jq

jq --version
for PYTHON_MAJOR_MINOR_VERSION in "${CURRENT_PYTHON_MAJOR_MINOR_VERSIONS[@]}"
do
    export AIRFLOW_PROD_IMAGE_NAME="${BRANCH_NAME}-python${PYTHON_MAJOR_MINOR_VERSION}"
    export AIRFLOW_PROD_BUILD_IMAGE_NAME="${BRANCH_NAME}-python${PYTHON_MAJOR_MINOR_VERSION}-build"
    wait_for_github_registry_image "${AIRFLOW_PROD_IMAGE_NAME}" "${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
    wait_for_github_registry_image "${AIRFLOW_PROD_BUILD_IMAGE_NAME}" "${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
done
