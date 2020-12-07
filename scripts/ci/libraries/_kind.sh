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

function kind::get_kind_cluster_name() {
    # Name of the KinD cluster to connect to
    export KIND_CLUSTER_NAME=${KIND_CLUSTER_NAME:="airflow-python-${PYTHON_MAJOR_MINOR_VERSION}-${KUBERNETES_VERSION}"}
    # Name of the KinD cluster to connect to when referred to via kubectl
    export KUBECTL_CLUSTER_NAME=kind-${KIND_CLUSTER_NAME}
    export KUBECONFIG="${BUILD_CACHE_DIR}/.kube/config"
    mkdir -pv "${BUILD_CACHE_DIR}/.kube/"
    touch "${KUBECONFIG}"
}

function kind::dump_kind_logs() {
    echo "###########################################################################################"
    echo "                   Dumping logs from KIND"
    echo "###########################################################################################"

    echo "EXIT_CODE is ${EXIT_CODE:=}"

    local DUMP_DIR_NAME DUMP_DIR
    DUMP_DIR_NAME=kind_logs_$(date "+%Y-%m-%d")_${CI_BUILD_ID}_${CI_JOB_ID}
    DUMP_DIR="/tmp/${DUMP_DIR_NAME}"
    kind --name "${KIND_CLUSTER_NAME}" export logs "${DUMP_DIR}"
}

function kind::make_sure_kubernetes_tools_are_installed() {
    SYSTEM=$(uname -s | tr '[:upper:]' '[:lower:]')

    KIND_URL="https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VERSION}/kind-${SYSTEM}-amd64"
    mkdir -pv "${BUILD_CACHE_DIR}/bin"
    if [[ -f "${KIND_BINARY_PATH}" ]]; then
        DOWNLOADED_KIND_VERSION=v"$(${KIND_BINARY_PATH} --version | awk '{ print $3 }')"
        echo "Currently downloaded kind version = ${DOWNLOADED_KIND_VERSION}"
    fi
    if [[ ! -f "${KIND_BINARY_PATH}" || ${DOWNLOADED_KIND_VERSION} != "${KIND_VERSION}" ]]; then
        echo
        echo "Downloading Kind version ${KIND_VERSION}"
        repeats::run_with_retry 4 \
            "curl --fail --location '${KIND_URL}' --output '${KIND_BINARY_PATH}'"
        chmod a+x "${KIND_BINARY_PATH}"
    else
        echo "Kind version ok"
        echo
    fi

    KUBECTL_URL="https://storage.googleapis.com/kubernetes-release/release/${KUBECTL_VERSION}/bin/${SYSTEM}/amd64/kubectl"
    if [[ -f "${KUBECTL_BINARY_PATH}" ]]; then
        DOWNLOADED_KUBECTL_VERSION="$(${KUBECTL_BINARY_PATH} version --client=true --short | awk '{ print $3 }')"
        echo "Currently downloaded kubectl version = ${DOWNLOADED_KUBECTL_VERSION}"
    fi
    if [[ ! -f "${KUBECTL_BINARY_PATH}" || ${DOWNLOADED_KUBECTL_VERSION} != "${KUBECTL_VERSION}" ]]; then
        echo
        echo "Downloading Kubectl version ${KUBECTL_VERSION}"
        repeats::run_with_retry 4 \
            "curl --fail --location '${KUBECTL_URL}' --output '${KUBECTL_BINARY_PATH}'"
        chmod a+x "${KUBECTL_BINARY_PATH}"
    else
        echo "Kubectl version ok"
        echo
    fi

    HELM_URL="https://get.helm.sh/helm-${HELM_VERSION}-${SYSTEM}-amd64.tar.gz"
    if [[ -f "${HELM_BINARY_PATH}" ]]; then
        DOWNLOADED_HELM_VERSION="$(${HELM_BINARY_PATH} version --template '{{.Version}}')"
        echo "Currently downloaded helm version = ${DOWNLOADED_HELM_VERSION}"
    fi
    if [[ ! -f "${HELM_BINARY_PATH}" || ${DOWNLOADED_HELM_VERSION} != "${HELM_VERSION}" ]]; then
        echo
        echo "Downloading Helm version ${HELM_VERSION}"
        repeats::run_with_retry 4 \
            "curl --location '${HELM_URL}' | tar -xvz -O '${SYSTEM}-amd64/helm' >'${HELM_BINARY_PATH}'"
        chmod a+x "${HELM_BINARY_PATH}"
    else
        echo "Helm version ok"
        echo
    fi
    PATH=${PATH}:${BUILD_CACHE_DIR}/bin
}

function kind::create_cluster() {
    kind create cluster \
        --name "${KIND_CLUSTER_NAME}" \
        --config "${AIRFLOW_SOURCES}/scripts/ci/kubernetes/kind-cluster-conf.yaml" \
        --image "kindest/node:${KUBERNETES_VERSION}"
    echo
    echo "Created cluster ${KIND_CLUSTER_NAME}"
    echo
}

function kind::delete_cluster() {
    kind delete cluster --name "${KIND_CLUSTER_NAME}"
    echo
    echo "Deleted cluster ${KIND_CLUSTER_NAME}"
    echo
    rm -rf "${HOME}/.kube/*"
}

function kind::set_current_context() {
    kubectl config set-context --current --namespace=airflow
}

function kind::perform_kind_cluster_operation() {
    ALLOWED_KIND_OPERATIONS="[ start restart stop deploy test shell recreate k9s]"
    set +u
    if [[ -z "${1=}" ]]; then
        echo
        echo  "${COLOR_RED_ERROR} Operation must be provided as first parameter. One of: ${ALLOWED_KIND_OPERATIONS}  ${COLOR_RESET}"
        echo
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
            kind::check_cluster_ready_for_airflow
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
            kind::delete_cluster
            kind::create_cluster
            kind::set_current_context
        elif [[ ${OPERATION} == "stop" ]]; then
            echo
            echo "Deleting cluster"
            echo
            kind::delete_cluster
            exit
        elif [[ ${OPERATION} == "deploy" ]]; then
            echo
            echo "Deploying Airflow to KinD"
            echo
            kind::build_image_for_kubernetes_tests
            kind::load_image_to_kind_cluster
            kind::deploy_airflow_with_helm
            kind::deploy_test_kubernetes_resources
            kind::wait_for_webserver_healthy
        elif [[ ${OPERATION} == "test" ]]; then
            echo
            echo "Testing with KinD"
            echo
            kind::set_current_context
            "${AIRFLOW_SOURCES}/scripts/ci/kubernetes/ci_run_kubernetes_tests.sh"
        elif [[ ${OPERATION} == "shell" ]]; then
            echo
            echo "Entering an interactive shell for kubernetes testing"
            echo
            kind::set_current_context
            "${AIRFLOW_SOURCES}/scripts/ci/kubernetes/ci_run_kubernetes_tests.sh" "-i"
        elif [[ ${OPERATION} == "k9s" ]]; then
            echo
            echo "Starting k9s CLI"
            echo
            export TERM=xterm-256color
            export EDITOR=vim
            export K9S_EDITOR=vim
            kind::set_current_context
            exec docker run --rm -it --network host \
                -e COLUMNS="$(tput cols)" -e LINES="$(tput lines)" \
                -e EDITOR -e K9S_EDITOR \
                -v "${KUBECONFIG}:/root/.kube/config" quay.io/derailed/k9s
        else
            echo
            echo  "${COLOR_RED_ERROR} Wrong cluster operation: ${OPERATION}. Should be one of: ${ALLOWED_KIND_OPERATIONS}  ${COLOR_RESET}"
            echo
            exit 1
        fi
    else
        if [[ ${OPERATION} == "start" ]]; then
            echo
            echo "Creating cluster"
            echo
            kind::create_cluster
        elif [[ ${OPERATION} == "recreate" ]]; then
            echo
            echo "Cluster ${KIND_CLUSTER_NAME} does not exist. Creating rather than recreating"
            echo "Creating cluster"
            echo
            kind::create_cluster
        elif [[ ${OPERATION} == "stop" || ${OPERATION} == "deploy" || ${OPERATION} == "test" || ${OPERATION} == "shell" ]]; then
            echo
            echo  "${COLOR_RED_ERROR} Cluster ${KIND_CLUSTER_NAME} does not exist. It should exist for ${OPERATION} operation  ${COLOR_RESET}"
            echo
            exit 1
        else
            echo
            echo  "${COLOR_RED_ERROR} Wrong cluster operation: ${OPERATION}. Should be one of ${ALLOWED_KIND_OPERATIONS}  ${COLOR_RESET}"
            echo
            exit 1
        fi
    fi
}

function kind::check_cluster_ready_for_airflow() {
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

function kind::build_image_for_kubernetes_tests() {
    cd "${AIRFLOW_SOURCES}" || exit 1
    docker build --tag "${AIRFLOW_PROD_IMAGE_KUBERNETES}" . -f - <<EOF
FROM ${AIRFLOW_PROD_IMAGE}

USER root

COPY --chown=airflow:root airflow/example_dags/ \${AIRFLOW_HOME}/dags/

COPY --chown=airflow:root airflow/kubernetes_executor_templates/ \${AIRFLOW_HOME}/pod_templates/

USER airflow

EOF
    echo "The ${AIRFLOW_PROD_IMAGE_KUBERNETES} is prepared for test kubernetes deployment."
}

function kind::load_image_to_kind_cluster() {
    echo
    echo "Loading ${AIRFLOW_PROD_IMAGE_KUBERNETES} to ${KIND_CLUSTER_NAME}"
    echo
    kind load docker-image --name "${KIND_CLUSTER_NAME}" "${AIRFLOW_PROD_IMAGE_KUBERNETES}"
}

MAX_NUM_TRIES_FOR_HEALTH_CHECK=12
readonly MAX_NUM_TRIES_FOR_HEALTH_CHECK

SLEEP_TIME_FOR_HEALTH_CHECK=10
readonly SLEEP_TIME_FOR_HEALTH_CHECK

FORWARDED_PORT_NUMBER=8080
readonly FORWARDED_PORT_NUMBER


function kind::wait_for_webserver_healthy() {
    num_tries=0
    set +e
    sleep "${SLEEP_TIME_FOR_HEALTH_CHECK}"
    while ! curl "http://localhost:${FORWARDED_PORT_NUMBER}/health" -s | grep -q healthy; do
        echo
        echo "Sleeping ${SLEEP_TIME_FOR_HEALTH_CHECK} while waiting for webserver being ready"
        echo
        sleep "${SLEEP_TIME_FOR_HEALTH_CHECK}"
        num_tries=$((num_tries + 1))
        if [[ ${num_tries} == "${MAX_NUM_TRIES_FOR_HEALTH_CHECK}" ]]; then
            echo
            echo  "${COLOR_RED_ERROR} Timeout while waiting for the webserver health check  ${COLOR_RESET}"
            echo
        fi
    done
    echo
    echo "Connection to 'airflow webserver' established on port ${FORWARDED_PORT_NUMBER}"
    echo
    initialization::ga_env CLUSTER_FORWARDED_PORT "${FORWARDED_PORT_NUMBER}"
    export CLUSTER_FORWARDED_PORT="${FORWARDED_PORT_NUMBER}"
    set -e
}

function kind::deploy_airflow_with_helm() {
    echo
    echo "Deploying Airflow with Helm"
    echo
    echo "Deleting namespace ${HELM_AIRFLOW_NAMESPACE}"
    kubectl delete namespace "${HELM_AIRFLOW_NAMESPACE}" >/dev/null 2>&1 || true
    kubectl delete namespace "test-namespace" >/dev/null 2>&1 || true
    kubectl create namespace "${HELM_AIRFLOW_NAMESPACE}"
    kubectl create namespace "test-namespace"
    pushd "${AIRFLOW_SOURCES}/chart" || exit 1
    helm repo add stable https://charts.helm.sh/stable/
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

function kind::deploy_test_kubernetes_resources() {
    echo
    echo "Deploying Custom kubernetes resources"
    echo
    kubectl apply -f "scripts/ci/kubernetes/volumes.yaml" --namespace default
    kubectl apply -f "scripts/ci/kubernetes/nodeport.yaml" --namespace airflow
}

function kind::dump_kubernetes_logs() {
    POD=$(kubectl get pods -o go-template --template '{{range .items}}{{.metadata.name}}{{"\n"}}{{end}}' \
        --cluster "${KUBECTL_CLUSTER_NAME}" | grep airflow | head -1)
    echo "------- pod description -------"
    kubectl describe pod "${POD}" --cluster "${KUBECTL_CLUSTER_NAME}"
    echo "------- airflow pod logs -------"
    kubectl logs "${POD}" --all-containers=true || true
    echo "--------------"
}
