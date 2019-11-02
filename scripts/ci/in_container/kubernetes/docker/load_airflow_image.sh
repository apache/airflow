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
set -euo pipefail
MY_DIR=$(cd "$(dirname "$0")" && pwd)

AIRFLOW_SOURCES=$(cd "${MY_DIR}/../../../../../" || exit 1 ; pwd)
export AIRFLOW_SOURCES

# shellcheck source=scripts/ci/in_container/_in_container_utils.sh
. "${MY_DIR}/../../_in_container_utils.sh"

assert_in_container

in_container_script_start

export AIRFLOW_CI_SAVED_IMAGE_DIR=/opt/airflow/.docker_image_dir
export AIRFLOW_CI_SAVED_IMAGE_ID_FILE=/opt/airflow/.docker_image.sha256
export CLUSTER_NAME="airflow-${KUBERNETES_VERSION}"

DOCKER_IMAGE_ID=$(docker inspect --format='{{index .Id}}' "${AIRFLOW_CI_IMAGE}" 2>/dev/null || true)
SAVED_IMAGE_ID=$(cat "${AIRFLOW_CI_SAVED_IMAGE_ID_FILE}")
if [[ "${SAVED_IMAGE_ID}" == "${DOCKER_IMAGE_ID}" ]]; then
    echo
    echo "Skip Loading the ${AIRFLOW_CI_IMAGE} as it has not changed."
    echo
else
    echo
    echo "Loading the ${AIRFLOW_CI_IMAGE} from ${AIRFLOW_CI_SAVED_IMAGE_DIR} to docker"
    echo
    tar -cC "${AIRFLOW_CI_SAVED_IMAGE_DIR}" . | docker image load
    echo
    echo "Loading the ${AIRFLOW_CI_IMAGE} to cluster ${CLUSTER_NAME} from docker"
    echo
    kind load docker-image --name "${CLUSTER_NAME}" "${AIRFLOW_CI_IMAGE}"
    echo
    echo "Loaded the ${AIRFLOW_CI_SAVED_IMAGE_DIR} dir to ${AIRFLOW_CI_IMAGE} in docker and cluster ${CLUSTER_NAME}"
    echo
fi

in_container_script_end
