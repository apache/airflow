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

AIRFLOW_SOURCES=$(
    cd "${MY_DIR}/../../../../" || exit 1
    pwd
)
export AIRFLOW_SOURCES

export RECREATE_KIND_CLUSTER=${RECREATE_KIND_CLUSTER:="true"}

# We keep _utils here because we are not in the in_container directory
# shellcheck source=scripts/ci/in_container/_in_container_utils.sh
. "${MY_DIR}/../_in_container_utils.sh"

assert_in_container

in_container_script_start

# just in case it's not set
export KUBERNETES_VERSION=${KUBERNETES_VERSION:="v1.15.3"}

echo
echo "Kubernetes version = ${KUBERNETES_VERSION}"
echo

function create_cluster() {
    if [[ "${TRAVIS:="false"}" == "true" ]]; then
        # Travis CI does not handle the nice output of Kind well, so we need to capture it
        # And display only if kind fails to start
        start_output_heartbeat "Creating kubernetes cluster" 10
        set +e
        if ! OUTPUT=$(kind create cluster \
                                    --name "${CLUSTER_NAME}" \
                                    --config "${MY_DIR}/kind-cluster-conf.yaml" \
                                    --image "kindest/node:${KUBERNETES_VERSION}" 2>&1); then
            echo "${OUTPUT}"
        fi
        stop_output_heartbeat
    else
        kind create cluster \
            --name "${CLUSTER_NAME}" \
            --config "${MY_DIR}/kind-cluster-conf.yaml" \
            --image "kindest/node:${KUBERNETES_VERSION}"
    fi
    echo
    echo "Created cluster ${CLUSTER_NAME}"
    echo

    echo
    echo "Connecting Kubernetes' worker and ccontrol plane to the network used by Breeze"
    echo
    docker network connect docker-compose_default "${CLUSTER_NAME}-control-plane"
    docker network connect docker-compose_default "${CLUSTER_NAME}-worker"
    echo
    echo "Connected Kubernetes' worker and ccontrol plane to the network used by Breeze"
    echo


    echo
    echo "Replacing cluster host address with https://${CLUSTER_NAME}-control-plane:6443"
    echo
    kubectl config set "clusters.kind-${CLUSTER_NAME}.server" "https://${CLUSTER_NAME}-control-plane:6443"

    echo
    echo "Replaced cluster host address with https://${CLUSTER_NAME}-control-plane:6443"
    echo

    echo
    echo "Patching CoreDNS to avoid loop and to use 8.8.8.8 DNS as forward address."
    echo
    echo "============================================================================"
    echo "      Original coredns configmap:"
    echo "============================================================================"
    kubectl get configmaps --namespace=kube-system coredns -o yaml
    kubectl get configmaps --namespace=kube-system coredns -o yaml | \
        sed 's/forward \. .*$/forward . 8.8.8.8/' | kubectl apply -f -

    echo
    echo "============================================================================"
    echo "      Updated coredns configmap with new forward directive:"
    echo "============================================================================"
    kubectl get configmaps --namespace=kube-system coredns -o yaml


    echo
    echo "Restarting CoreDNS"
    echo
    kubectl scale deployment --namespace=kube-system coredns --replicas=0
    kubectl scale deployment --namespace=kube-system coredns --replicas=2
    echo
    echo "Restarted CoreDNS"
    echo
}

function delete_cluster() {
    kind delete cluster --name "${CLUSTER_NAME}"
    echo
    echo "Deleted cluster ${CLUSTER_NAME}"
    echo
    rm -rf "${HOME}/.kube/*"
}

ALL_CLUSTERS=$(kind get clusters || true)
if [[ ${ALL_CLUSTERS} == *"${CLUSTER_NAME}"* ]]; then
    echo
    echo "Cluster ${CLUSTER_NAME} is already created"
    echo
    if [[ ${KIND_CLUSTER_OPERATION} == "start" ]]; then
        echo
        echo "Reusing previously created cluster"
        echo
    elif [[ ${KIND_CLUSTER_OPERATION} == "recreate" ]]; then
        echo
        echo "Recreating cluster"
        echo
        delete_cluster
        create_cluster
    elif [[ ${KIND_CLUSTER_OPERATION} == "stop" ]]; then
        echo
        echo "Deleting cluster"
        echo
        delete_cluster
        exit
    else
        echo
        echo "Wrong cluster operation: ${KIND_CLUSTER_OPERATION}. Should be one of start/stop/recreate"
        echo
        exit 1
    fi
else
    if [[ ${KIND_CLUSTER_OPERATION} == "start" ]]; then
        echo
        echo "Creating cluster"
        echo
        create_cluster
    elif [[ ${KIND_CLUSTER_OPERATION} == "restart" ]]; then
        echo
        echo "Cluster ${CLUSTER_NAME} does not exist. Creating rather than recreating"
        echo
        echo "Creating cluster"
        echo
        create_cluster
    elif [[ ${KIND_CLUSTER_OPERATION} == "stop" ]]; then
        echo
        echo "Cluster ${CLUSTER_NAME} does not exist. It should exist for stop operation"
        echo
        exit 1
    else
        echo
        echo "Wrong cluster operation: ${KIND_CLUSTER_OPERATION}. Should be one of start/stop/recreate"
        echo
        exit 1
    fi
fi

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
echo "Airflow environment on kubernetes is good to go!"
echo

in_container_script_end
