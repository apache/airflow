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

#
# Fixes ownership for files created inside container (files owned by root will be owned by host user)
#
# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"

if [[ ${OSTYPE} == "darwin"* ]]; then
    # No need to fix ownership on MacOS - the filesystem there takes care about ownership mapping
    exit
fi

declare -a EXTRA_DOCKER_FLAGS

sanity_checks::sanitize_mounted_files

read -r -a EXTRA_DOCKER_FLAGS <<<"$(local_mounts::convert_local_mounts_to_docker_params)"

if docker image inspect "${AIRFLOW_CI_IMAGE_WITH_TAG}" >/dev/null 2>&1; then
    docker_v run --entrypoint /bin/bash "${EXTRA_DOCKER_FLAGS[@]}" \
        --rm \
        --env-file "${AIRFLOW_SOURCES}/scripts/ci/docker-compose/_docker.env" \
        "${AIRFLOW_CI_IMAGE_WITH_TAG}" \
        -c /opt/airflow/scripts/in_container/run_fix_ownership.sh || true
else
    echo "Skip fixing ownership as seems that you do not have the ${AIRFLOW_CI_IMAGE_WITH_TAG} image yet"
fi
