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

export MOUNTED_TAR_IMAGE=/opt/airflow/.docker_image.tar
export CLUSTER_NAME="airflow-${KUBERNETES_VERSION}"

echo
echo "Loading the ${AIRFLOW_CI_KUBERNETES_IMAGE} from ${MOUNTED_TAR_IMAGE}"
echo

docker image load -i ${MOUNTED_TAR_IMAGE}

echo
echo "Loading the ${AIRFLOW_CI_KUBERNETES_IMAGE} to cluster ${CLUSTER_NAME}"
echo

kind load docker-image --name "${CLUSTER_NAME}" "${AIRFLOW_CI_KUBERNETES_IMAGE}"

echo
echo "Loaded the ${MOUNTED_TAR_IMAGE} file to ${AIRFLOW_CI_KUBERNETES_IMAGE} in docker and in cluster ${CLUSTER_NAME}"
echo

in_container_script_end
