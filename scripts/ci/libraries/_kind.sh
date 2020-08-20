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


function initialize_kind_variables(){
    # Name of the KinD cluster to connect to
    export KIND_CLUSTER_NAME=${KIND_CLUSTER_NAME:="airflow-python-${PYTHON_MAJOR_MINOR_VERSION}-${KUBERNETES_VERSION}"}
    # Name of the KinD cluster to connect to when referred to via kubectl
    export KUBECTL_CLUSTER_NAME=kind-${KIND_CLUSTER_NAME}
}

function dump_kind_logs() {
    echo "###########################################################################################"
    echo "                   Dumping logs from KIND"
    echo "###########################################################################################"

    echo "EXIT_CODE is ${EXIT_CODE:=}"

    local DUMP_DIR_NAME DUMP_DIR
    DUMP_DIR_NAME=kind_logs_$(date "+%Y-%m-%d")_${CI_BUILD_ID:="default"}_${CI_JOB_ID:="default"}
    DUMP_DIR="/tmp/${DUMP_DIR_NAME}"
    kind --name "${KIND_CLUSTER_NAME}" export logs "${DUMP_DIR}"
}

function make_sure_kubernetes_tools_are_installed() {
    SYSTEM=$(uname -s| tr '[:upper:]' '[:lower:]')
    KIND_VERSION=${KIND_VERSION:=${DEFAULT_KIND_VERSION}}
    KIND_URL="https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VERSION}/kind-${SYSTEM}-amd64"
    KIND_PATH="${BUILD_CACHE_DIR}/bin/kind"
    HELM_VERSION=${HELM_VERSION:=${DEFAULT_HELM_VERSION}}
    HELM_URL="https://get.helm.sh/helm-${HELM_VERSION}-${SYSTEM}-amd64.tar.gz"
    HELM_PATH="${BUILD_CACHE_DIR}/bin/helm"
    KUBECTL_VERSION=${KUBERNETES_VERSION:=${DEFAULT_KUBERNETES_VERSION}}
    KUBECTL_URL="https://storage.googleapis.com/kubernetes-release/release/${KUBECTL_VERSION}/bin/${SYSTEM}/amd64/kubectl"
    KUBECTL_PATH="${BUILD_CACHE_DIR}/bin/kubectl"
    mkdir -pv "${BUILD_CACHE_DIR}/bin"
    if [[ -f "${KIND_PATH}" ]]; then
        DOWNLOADED_KIND_VERSION=v"$(${KIND_PATH} --version | awk '{ print $3 }')"
        echo "Currently downloaded kind version = ${DOWNLOADED_KIND_VERSION}"
    fi
    if [[ ! -f "${KIND_PATH}"  || ${DOWNLOADED_KIND_VERSION} != "${KIND_VERSION}" ]]; then
        echo
        echo "Downloading Kind version ${KIND_VERSION}"
        curl --fail --location "${KIND_URL}" --output "${KIND_PATH}"
        chmod a+x "${KIND_PATH}"
    else
        echo "Kind version ok"
        echo
    fi
    if [[ -f "${KUBECTL_PATH}" ]]; then
        DOWNLOADED_KUBECTL_VERSION="$(${KUBECTL_PATH} version --client=true --short | awk '{ print $3 }')"
        echo "Currently downloaded kubectl version = ${DOWNLOADED_KUBECTL_VERSION}"
    fi
    if [[ ! -f "${KUBECTL_PATH}" || ${DOWNLOADED_KUBECTL_VERSION} != "${KUBECTL_VERSION}" ]]; then
        echo
        echo "Downloading Kubectl version ${KUBECTL_VERSION}"
        curl --fail --location "${KUBECTL_URL}" --output "${KUBECTL_PATH}"
        chmod a+x "${KUBECTL_PATH}"
    else
        echo "Kubectl version ok"
        echo
    fi
    if [[ -f "${HELM_PATH}" ]]; then
        DOWNLOADED_HELM_VERSION="$(${HELM_PATH} version --template '{{.Version}}')"
        echo "Currently downloaded helm version = ${DOWNLOADED_HELM_VERSION}"
    fi
    if [[ ! -f "${HELM_PATH}" || ${DOWNLOADED_HELM_VERSION} != "${HELM_VERSION}" ]]; then
        echo
        echo "Downloading Helm version ${HELM_VERSION}"
        curl     --location "${HELM_URL}" |
            tar -xvz -O "${SYSTEM}-amd64/helm" >"${HELM_PATH}"
        chmod a+x "${HELM_PATH}"
    else
        echo "Helm version ok"
        echo
    fi
    PATH=${PATH}:${BUILD_CACHE_DIR}/bin
}

function create_cluster() {
    if [[ "${TRAVIS:="false"}" == "true" ]]; then
        # Travis CI does not handle the nice output of Kind well, so we need to capture it
        # And display only if kind fails to start
        start_output_heartbeat "Creating kubernetes cluster" 10
        set +e
        if ! OUTPUT=$(kind create cluster \
                        --name "${KIND_CLUSTER_NAME}" \
                        --config "${AIRFLOW_SOURCES}/scripts/ci/kubernetes/kind-cluster-conf.yaml" \
                        --image "kindest/node:${KUBERNETES_VERSION}" 2>&1); then
            echo "${OUTPUT}"
        fi
        stop_output_heartbeat
    else
        kind create cluster \
            --name "${KIND_CLUSTER_NAME}" \
            --config "${AIRFLOW_SOURCES}/scripts/ci/kubernetes/kind-cluster-conf.yaml" \
            --image "kindest/node:${KUBERNETES_VERSION}"
    fi
    echo
    echo "Created cluster ${KIND_CLUSTER_NAME}"
    echo
}

function delete_cluster() {
    kind delete cluster --name "${KIND_CLUSTER_NAME}"
    echo
    echo "Deleted cluster ${KIND_CLUSTER_NAME}"
    echo
    rm -rf "${HOME}/.kube/*"
}

function perform_kind_cluster_operation() {
    ALLOWED_KIND_OPERATIONS="[ start restart stop deploy test shell recreate ]"

    set +u
    if [[ -z "${1}" ]]; then
        echo >&2
        echo >&2 "Operation must be provided as first parameter. One of: ${ALLOWED_KIND_OPERATIONS}"
        echo >&2
        exit 1
    fi
    set -u
    OPERATION="${1}"
    ALL_CLUSTERS=$(kind get clusters || true)

    echo
    echo "Kubernetes mode: ${KUBERNETES_MODE}"
    echo

    if [[ ${OPERATION} == "status" ]]; then
        if [[ ${ALL_CLUSTERS} == *"${KIND_CLUSTER_NAME}"* ]]; then
            echo
            echo "Cluster name: ${KIND_CLUSTER_NAME}"
            echo
            kind get nodes --name "${KIND_CLUSTER_NAME}"
            echo
            exit
        else
            echo
            echo "Cluster ${KIND_CLUSTER_NAME} is not running"
            echo
            exit
        fi
    fi
    if [[ ${ALL_CLUSTERS} == *"${KIND_CLUSTER_NAME}"* ]]; then
        if [[ ${OPERATION} == "start" ]]; then
            echo
            echo "Cluster ${KIND_CLUSTER_NAME} is already created"
            echo "Reusing previously created cluster"
            echo
        elif [[ ${OPERATION} == "restart" ]]; then
            echo
            echo "Recreating cluster"
            echo
            delete_cluster
            create_cluster
        elif [[ ${OPERATION} == "stop" ]]; then
            echo
            echo "Deleting cluster"
            echo
            delete_cluster
            exit
        elif [[ ${OPERATION} == "deploy" ]]; then
            echo
            echo "Deploying Airflow to KinD"
            echo
            get_environment_for_builds_on_ci
            make_sure_kubernetes_tools_are_installed
            initialize_kind_variables
            prepare_prod_build
            build_prod_images
            build_image_for_kubernetes_tests
            load_image_to_kind_cluster
            deploy_airflow_with_helm
            forward_port_to_kind_webserver
            deploy_test_kubernetes_resources
        elif [[ ${OPERATION} == "test" ]]; then
            echo
            echo "Testing with KinD"
            echo
            "${AIRFLOW_SOURCES}/scripts/ci/kubernetes/ci_run_kubernetes_tests.sh"
        elif [[ ${OPERATION} == "shell" ]]; then
            echo
            echo "Entering an interactive shell for kubernetes testing"
            echo
            "${AIRFLOW_SOURCES}/scripts/ci/kubernetes/ci_run_kubernetes_tests.sh" "-i"
        else
            echo >&2
            echo >&2 "Wrong cluster operation: ${OPERATION}. Should be one of: ${ALLOWED_KIND_OPERATIONS}"
            echo >&2
            exit 1
        fi
    else
        if [[ ${OPERATION} == "start" ]]; then
            echo
            echo "Creating cluster"
            echo
            create_cluster
        elif [[ ${OPERATION} == "recreate" ]]; then
            echo
            echo "Cluster ${KIND_CLUSTER_NAME} does not exist. Creating rather than recreating"
            echo "Creating cluster"
            echo
            create_cluster
        elif [[ ${OPERATION} == "stop" || ${OPERATION} == "deploy" || \
                ${OPERATION} == "test" || ${OPERATION} == "shell" ]]; then
            echo >&2
            echo >&2 "Cluster ${KIND_CLUSTER_NAME} does not exist. It should exist for ${OPERATION} operation"
            echo >&2
            exit 1
        else
            echo >&2
            echo >&2 "Wrong cluster operation: ${OPERATION}. Should be one of ${ALLOWED_KIND_OPERATIONS}"
            echo >&2
            exit 1
        fi
    fi
}

function check_cluster_ready_for_airflow() {
    kubectl cluster-info --cluster "${KUBECTL_CLUSTER_NAME}"
    kubectl get nodes --cluster "${KUBECTL_CLUSTER_NAME}"
    echo
    echo "Showing storageClass"
    echo
    kubectl get storageclass --cluster "${KUBECTL_CLUSTER_NAME}"
    echo
    echo "Showing kube-system pods"
    echo
    kubectl get -n kube-system pods --cluster "${KUBECTL_CLUSTER_NAME}"
    echo
    echo "Airflow environment on kubernetes is good to go!"
    echo
    kubectl create namespace test-namespace --cluster "${KUBECTL_CLUSTER_NAME}"
}


function build_image_for_kubernetes_tests() {
    cd "${AIRFLOW_SOURCES}" || exit 1
    docker build --tag "${AIRFLOW_PROD_IMAGE_KUBERNETES}" . -f - <<EOF
FROM ${AIRFLOW_PROD_IMAGE}

USER root

COPY --chown=airflow:root airflow/example_dags/ \${AIRFLOW_HOME}/dags/

USER airflow

EOF
    echo "The ${AIRFLOW_PROD_IMAGE_KUBERNETES} is prepared for test kubernetes deployment."
}

function load_image_to_kind_cluster() {
    echo
    echo "Loading ${AIRFLOW_PROD_IMAGE_KUBERNETES} to ${KIND_CLUSTER_NAME}"
    echo
    kind load docker-image --name "${KIND_CLUSTER_NAME}" "${AIRFLOW_PROD_IMAGE_KUBERNETES}"
}

function forward_port_to_kind_webserver() {
    num_tries=0
    set +e
    while ! curl http://localhost:8080/health -s | grep -q healthy; do
        if [[ ${num_tries} == 6 ]]; then
            echo >&2
            echo >&2 "ERROR! Could not setup a forward port to Airflow's webserver after ${num_tries}! Exiting."
            echo >&2
            exit 1
        fi
        echo
        echo "Trying to establish port forwarding to 'airflow webserver'"
        echo
        kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow >/dev/null &
        sleep 10
        num_tries=$(( num_tries + 1))
    done
    echo "Connection to 'airflow webserver' established"
    set -e
}

function deploy_airflow_with_helm() {
    echo
    echo "Deploying Airflow with Helm"
    echo
    echo "Deleting namespace ${HELM_AIRFLOW_NAMESPACE}"
    kubectl delete namespace "${HELM_AIRFLOW_NAMESPACE}" >/dev/null 2>&1 || true
    kubectl delete namespace "test-namespace" >/dev/null 2>&1 || true
    kubectl create namespace "${HELM_AIRFLOW_NAMESPACE}"
    kubectl create namespace "test-namespace"
    pushd "${AIRFLOW_SOURCES}/chart" || exit 1
    helm repo add stable https://kubernetes-charts.storage.googleapis.com
    helm dep update
    helm install airflow . --namespace "${HELM_AIRFLOW_NAMESPACE}" \
        --set "defaultAirflowRepository=${DOCKERHUB_USER}/${DOCKERHUB_REPO}" \
        --set "images.airflow.repository=${DOCKERHUB_USER}/${DOCKERHUB_REPO}" \
        --set "images.airflow.tag=${AIRFLOW_PROD_BASE_TAG}-kubernetes" -v 1 \
        --set "defaultAirflowTag=${AIRFLOW_PROD_BASE_TAG}-kubernetes" -v 1 \
        --set "config.api.auth_backend=airflow.api.auth.backend.default"
    echo
    popd || exit 1
}


function deploy_test_kubernetes_resources() {
    echo
    echo "Deploying Custom kubernetes resources"
    echo
    kubectl apply -f "scripts/ci/kubernetes/volumes.yaml" --namespace default
}


function dump_kubernetes_logs() {
    POD=$(kubectl get pods -o go-template --template '{{range .items}}{{.metadata.name}}{{"\n"}}{{end}}' \
        --cluster "${KUBECTL_CLUSTER_NAME}" | grep airflow | head -1)
    echo "------- pod description -------"
    kubectl describe pod "${POD}" --cluster "${KUBECTL_CLUSTER_NAME}"
    echo "------- airflow pod logs -------"
    kubectl logs "${POD}" --all-containers=true || true
    echo "--------------"
}
