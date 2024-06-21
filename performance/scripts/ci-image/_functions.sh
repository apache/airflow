#!/usr/bin/env bash
# Copyright 2020 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
# This software is provided as-is,
# without warranty or representation for any use or purpose.
# Your use of it is subject to your agreement with Google.
set -eao pipefail

# Initializes the script
function ci-image::initialize() {
    set -euo pipefail

    ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/../../" && pwd )"
    readonly ROOT_DIR

    # shellcheck source=scripts/_common_values.sh
    source "${ROOT_DIR}/scripts/_common_values.sh"

    CI_IMAGE="${CONTAINER_REGISTRY_URL}/airflow-gepard-ci-build-image:latest"
    readonly CI_IMAGE

    cd "${ROOT_DIR}"
}

# Builds the image
function ci-image::build_image(){
    if [[ "$(docker images -q "${CI_IMAGE}" 2> /dev/null)" == "" ]]; then
        echo "Pulling ${CI_IMAGE}"
        docker pull "${CI_IMAGE}" || true
    fi

    docker build \
      . \
      --cache-from="${CI_IMAGE}" \
      --file=Dockerfile \
      --tag "${CI_IMAGE}"
}


# Pushes the image
function ci-image::push_image() {
    docker push "${CI_IMAGE}"
}
