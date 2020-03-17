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
MY_DIR=$(cd "$(dirname "$0")" && pwd)
AIRFLOW_SOURCES=$(cd "${MY_DIR}/../../../../../" || exit 1 ; pwd)
export AIRFLOW_SOURCES

# We keep _utils here because we are not in the in_container directory
# shellcheck source=scripts/ci/in_container/_in_container_utils.sh
. "${MY_DIR}/../../_in_container_utils.sh"

export OUTPUT_LOG=${AIRFLOW_SOURCES}/logs/rebuild_airflow_image.log

assert_in_container

in_container_script_start

cd "${AIRFLOW_SOURCES}" || exit 1

# Required to rebuild images from inside container
mkdir -pv scripts/docker/
cp /entrypoint.sh scripts/docker/

echo
echo "Building image from ${AIRFLOW_CI_IMAGE} with latest sources"
echo
start_output_heartbeat "Rebuilding Kubernetes image" 3
docker build \
    --build-arg PYTHON_BASE_IMAGE="${PYTHON_BASE_IMAGE}" \
    --build-arg AIRFLOW_VERSION="${AIRFLOW_VERSION}" \
    --build-arg AIRFLOW_EXTRAS="${AIRFLOW_EXTRAS}" \
    --build-arg AIRFLOW_BRANCH="${AIRFLOW_BRANCH}" \
    --build-arg AIRFLOW_CONTAINER_CI_OPTIMISED_BUILD="${AIRFLOW_CONTAINER_CI_OPTIMISED_BUILD}" \
    --build-arg HOME="${HOME}" \
    --cache-from "${AIRFLOW_CI_IMAGE}" \
    --tag="${AIRFLOW_CI_IMAGE}" \
    --target="main" \
    -f Dockerfile . >> "${OUTPUT_LOG}"
echo
echo "Adding kubernetes-specific scripts to basic CI image."
echo "Building ${AIRFLOW_KUBERNETES_IMAGE} from ${AIRFLOW_CI_IMAGE}"
echo
docker build \
    --build-arg AIRFLOW_CI_IMAGE="${AIRFLOW_CI_IMAGE}" \
    --cache-from "${AIRFLOW_CI_IMAGE}" \
    --tag="${AIRFLOW_KUBERNETES_IMAGE}" \
    -f- . >> "${OUTPUT_LOG}" <<EOF
ARG AIRFLOW_CI_IMAGE
FROM ${AIRFLOW_CI_IMAGE}

COPY scripts/ci/in_container/kubernetes/docker/airflow-test-env-init.sh /tmp/airflow-test-env-init.sh
COPY scripts/ci/in_container/kubernetes/docker/bootstrap.sh /bootstrap.sh

RUN chmod +x /bootstrap.sh

COPY airflow/ ${AIRFLOW_SOURCES}/airflow/

ENTRYPOINT ["/bootstrap.sh"]
EOF
stop_output_heartbeat

start_output_heartbeat "Loading image to Kind cluster" 10
echo
echo "Loading the ${AIRFLOW_KUBERNETES_IMAGE} to cluster ${CLUSTER_NAME} from docker"
echo
kind load docker-image --name "${CLUSTER_NAME}" "${AIRFLOW_KUBERNETES_IMAGE}"
echo
echo "Loaded the ${AIRFLOW_KUBERNETES_IMAGE} to cluster ${CLUSTER_NAME}"
echo

echo
echo "Stopping output heartbeat"
echo

stop_output_heartbeat

in_container_script_end
