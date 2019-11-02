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

export CLUSTER_NAME="airflow-${KUBERNETES_VERSION}"

cd "${AIRFLOW_SOURCES}"

if [[ "$(docker images -q "${AIRFLOW_CI_IMAGE}" 2> /dev/null)" == "" ]]; then
    echo
    echo "Pulling base kubernetes image from ${AIRFLOW_CI_IMAGE}"
    echo
    docker pull "${AIRFLOW_CI_IMAGE}"
else
    echo
    echo "Not pulling base kubernetes image - it's already pulled"
    echo
fi

# Copy entrypoing so that it is in the same place in Docker as in souurces
cp "/entrypoint.sh" "${AIRFLOW_SOURCES}/entrypoint.sh"

echo
echo "(Re)building kubernetes image from ${AIRFLOW_CI_IMAGE} with latest sources"
echo
docker build \
    --cache-from "${AIRFLOW_CI_IMAGE}" \
    --tag="${AIRFLOW_CI_IMAGE}" \
    --target="main" \
    -f Dockerfile .
echo
echo "Adding kubernetes-specific scripts. Building ${AIRFLOW_CI_KUBERNETES_IMAGE} from ${AIRFLOW_CI_IMAGE}"
echo
docker build \
    --build-arg AIRFLOW_CI_IMAGE="${AIRFLOW_CI_IMAGE}" \
    --cache-from "${AIRFLOW_CI_IMAGE}" \
    --tag="${AIRFLOW_CI_KUBERNETES_IMAGE}" \
    -f- . <<EOF
ARG AIRFLOW_CI_IMAGE
FROM ${AIRFLOW_CI_IMAGE}

COPY scripts/ci/in_container/kubernetes/docker/airflow-test-env-init.sh /tmp/airflow-test-env-init.sh

COPY scripts/ci/in_container/kubernetes/docker/bootstrap.sh /bootstrap.sh

RUN chmod +x /bootstrap.sh

COPY airflow/ ${AIRFLOW_SOURCES}/airflow/

ENTRYPOINT ["/bootstrap.sh"]
EOF

echo
echo "Loading the ${AIRFLOW_CI_IMAGE} to cluster ${CLUSTER_NAME} from docker"
echo
kind load docker-image --name "${CLUSTER_NAME}" "${AIRFLOW_CI_KUBERNETES_IMAGE}"
echo
echo "Loaded the ${AIRFLOW_CI_IMAGE} in cluster ${CLUSTER_NAME}"
echo

in_container_script_end
