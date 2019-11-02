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

AIRFLOW_SOURCES=$(cd "${MY_DIR}/../../../../" || exit 1 ; pwd)
export AIRFLOW_SOURCES

export RECREATE_KUBERNETES_CLUSTER=${RECREATE_KUBERNETES_CLUSTER:="true"}

# shellcheck source=scripts/ci/in_container/_in_container_utils.sh
. "${MY_DIR}/../_in_container_utils.sh"

assert_in_container

in_container_script_start

# just in case it's not set
export KUBERNETES_VERSION=${KUBERNETES_VERSION:="v1.15.3"}

echo
echo "Kubernetes version = ${KUBERNETES_VERSION}"
echo

export CLUSTER_NAME="airflow-${KUBERNETES_VERSION}"

function create_cluster {
    kind create cluster --name "${CLUSTER_NAME}" \
       --config "${MY_DIR}/kind-cluster-conf.yaml" --image "kindest/node:${KUBERNETES_VERSION}"
    echo
    echo "Created cluster ${CLUSTER_NAME}"
    echo
}

function delete_cluster {
    kind delete cluster --name "${CLUSTER_NAME}"
    echo
    echo "Deleted cluster ${CLUSTER_NAME}"
    echo
}

ALL_CLUSTERS=$(kind get clusters)
if [[ ${ALL_CLUSTERS} == *"${CLUSTER_NAME}"* ]]; then
    echo
    echo "Cluster ${CLUSTER_NAME} is already created"
    echo
    if [[ ${RECREATE_KUBERNETES_CLUSTER} == "true" ]]; then
        echo
        echo "Recreating cluster"
        echo
        delete_cluster
        create_cluster
    else
        echo
        echo "Reusing previously created cluster"
        echo
    fi
else
    echo
    echo "Creating cluster"
    echo
    create_cluster
fi

cp "$(kind get kubeconfig-path --name "${CLUSTER_NAME}")" ~/.kube/config
kubectl config set "clusters.${CLUSTER_NAME}.server" https://docker:19090
kubectl cluster-info

kubectl get nodes
echo
echo "Showing storageClass"
echo
kubectl get storageclass
echo
echo "Showing kube-system pods"
echo
kubectl get -n kube-system pods
echo
"${MY_DIR}/docker/load_image.sh"

echo
echo "Airflow environment on kubernetes is good to go!"
echo

in_container_script_end
