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

set -xeuo pipefail

MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# shellcheck source=scripts/ci/_utils.sh
. "${MY_DIR}/_utils.sh"

basic_sanity_checks

script_start

export AIRFLOW_CONTAINER_FORCE_PULL_IMAGES="true"

# Cleanup docker installation. It should be empty in CI but let's not risk
docker system prune --all --force

if [[ ${TRAVIS_JOB_NAME} == "Tests"* ]]; then
    rebuild_image_if_needed_for_tests
elif [[ ${TRAVIS_JOB_NAME} == "Check license headers"  ]]; then
    rebuild_image_if_needed_for_checklicence
else
    rebuild_image_if_needed_for_static_checks
fi

KUBERNETES_VERSION=${KUBERNETES_VERSION:=""}
# Required for K8s v1.10.x. See
# https://github.com/kubernetes/kubernetes/issues/61058#issuecomment-372764783
if [[ "${KUBERNETES_VERSION}" == "" ]]; then
    sudo mount --make-shared /
    sudo service docker restart
fi

pip install pre-commit yamllint

script_end
