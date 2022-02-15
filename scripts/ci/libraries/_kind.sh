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
    readonly KIND_CLUSTER_NAME
    # Name of the KinD cluster to connect to when referred to via kubectl
    export KUBECTL_CLUSTER_NAME=kind-${KIND_CLUSTER_NAME}
    readonly KUBECTL_CLUSTER_NAME
    export KUBECONFIG="${BUILD_CACHE_DIR}/${KIND_CLUSTER_NAME}/.kube/config"
    readonly KUBECONFIG
    mkdir -pv "${BUILD_CACHE_DIR}/${KIND_CLUSTER_NAME}/.kube/"
    touch "${KUBECONFIG}"
}

function kind::dump_kind_logs() {
    verbosity::print_info "Dumping logs from KinD"
    local dump_dir_name dump_dir
    dump_dir_name=kind_logs_$(date "+%Y-%m-%d")_${CI_BUILD_ID}_${CI_JOB_ID}
    dump_dir="/tmp/${dump_dir_name}"
    kind --name "${KIND_CLUSTER_NAME}" export logs "${dump_dir}"
}

function kind::make_sure_kubernetes_tools_are_installed() {
    local system
    system=$(uname -s | tr '[:upper:]' '[:lower:]')

    local kind_url="https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VERSION}/kind-${system}-amd64"
    mkdir -pv "${BUILD_CACHE_DIR}/kubernetes-bin/${KUBERNETES_VERSION}"
    if [[ -f "${KIND_BINARY_PATH}" ]]; then
        local downloaded_kind_version
        downloaded_kind_version=v"$(${KIND_BINARY_PATH} --version | awk '{ print $3 }')"
        echo "Currently downloaded kind version = ${downloaded_kind_version}"
    fi
    if [[ ! -f "${KIND_BINARY_PATH}" || ${downloaded_kind_version} != "${KIND_VERSION}" ]]; then
        echo
        echo "Downloading Kind version ${KIND_VERSION}"
        repeats::run_with_retry 4 \
            "curl --connect-timeout 60  --max-time 180 --fail --location '${kind_url}' --output '${KIND_BINARY_PATH}'"
        chmod a+x "${KIND_BINARY_PATH}"
    else
        echo "Kind version ok"
        echo
    fi

    local kubectl_url="https://storage.googleapis.com/kubernetes-release/release/${KUBECTL_VERSION}/bin/${system}/amd64/kubectl"
    if [[ -f "${KUBECTL_BINARY_PATH}" ]]; then
        local downloaded_kubectl_version
        downloaded_kubectl_version="$(${KUBECTL_BINARY_PATH} version --client=true --short | awk '{ print $3 }')"
        echo "Currently downloaded kubectl version = ${downloaded_kubectl_version}"
    fi
    if [[ ! -f "${KUBECTL_BINARY_PATH}" || ${downloaded_kubectl_version} != "${KUBECTL_VERSION}" ]]; then
        echo
        echo "Downloading Kubectl version ${KUBECTL_VERSION}"
        repeats::run_with_retry 4 \
            "curl --connect-timeout 60 --max-time 180 --fail --location '${kubectl_url}' --output '${KUBECTL_BINARY_PATH}'"
        chmod a+x "${KUBECTL_BINARY_PATH}"
    else
        echo "Kubectl version ok"
        echo
    fi

    local helm_url="https://get.helm.sh/helm-${HELM_VERSION}-${system}-amd64.tar.gz"
    if [[ -f "${HELM_BINARY_PATH}" ]]; then
        local downloaded_helm_version
        downloaded_helm_version="$(${HELM_BINARY_PATH} version --template '{{.Version}}')"
        echo "Currently downloaded helm version = ${downloaded_helm_version}"
    fi
    if [[ ! -f "${HELM_BINARY_PATH}" || ${downloaded_helm_version} != "${HELM_VERSION}" ]]; then
        echo
        echo "Downloading Helm version ${HELM_VERSION}"
        repeats::run_with_retry 4 \
            "curl --connect-timeout 60  --max-time 180 --location '${helm_url}' | tar -xvz -O '${system}-amd64/helm' >'${HELM_BINARY_PATH}'"
        chmod a+x "${HELM_BINARY_PATH}"
    else
        echo "Helm version ok"
        echo
    fi
    PATH=${PATH}:${BUILD_CACHE_DIR}/kubernetes-bin/${KUBERNETES_VERSION}
}

function kind::create_cluster() {
        sed "s/{{FORWARDED_PORT_NUMBER}}/${FORWARDED_PORT_NUMBER}/" < \
            "${AIRFLOW_SOURCES}/scripts/ci/kubernetes/kind-cluster-conf.yaml" | \
        sed "s/{{API_SERVER_PORT}}/${API_SERVER_PORT}/" | \
        kind create cluster \
            --name "${KIND_CLUSTER_NAME}" \
            --config - \
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
    rm -rf "${BUILD_CACHE_DIR}/${KIND_CLUSTER_NAME}/.kube/"
}

function kind::set_current_context() {
    kubectl config set-context --current --namespace=airflow
}

function kind::perform_kind_cluster_operation() {
    ALLOWED_KIND_OPERATIONS="[ start restart stop deploy test shell recreate k9s]"
    readonly ALLOWED_KIND_OPERATIONS
    set +u
    if [[ -z "${1=}" ]]; then
        echo
        echo  "${COLOR_RED}ERROR: Operation must be provided as first parameter. One of: ${ALLOWED_KIND_OPERATIONS}  ${COLOR_RESET}"
        echo
        exit 1
    fi

    set -u
    local operation="${1}"
    local all_clusters
    all_clusters=$(kind get clusters || true)

    echo
    echo "Kubernetes mode: ${KUBERNETES_MODE}"
    echo

    if [[ ${operation} == "status" ]]; then
        if [[ ${all_clusters} == *"${KIND_CLUSTER_NAME}"* ]]; then
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
    if [[ ${all_clusters} == *"${KIND_CLUSTER_NAME}"* ]]; then
        if [[ ${operation} == "start" ]]; then
            echo
            echo "Cluster ${KIND_CLUSTER_NAME} is already created"
            echo "Reusing previously created cluster"
            echo
        elif [[ ${operation} == "restart" ]]; then
            echo
            echo "Recreating cluster"
            echo
            kind::delete_cluster
            kind::create_cluster
            kind::set_current_context
        elif [[ ${operation} == "stop" ]]; then
            echo
            echo "Deleting cluster"
            echo
            kind::delete_cluster
            exit
        elif [[ ${operation} == "deploy" ]]; then
            echo
            echo "Deploying Airflow to KinD"
            echo
            kind::build_image_for_kubernetes_tests
            kind::load_image_to_kind_cluster
            kind::deploy_airflow_with_helm
            kind::deploy_test_kubernetes_resources
            kind::wait_for_webserver_healthy
        elif [[ ${operation} == "test" ]]; then
            echo
            echo "Testing with KinD"
            echo
            kind::set_current_context
            "${AIRFLOW_SOURCES}/scripts/ci/kubernetes/ci_run_kubernetes_tests.sh"
        elif [[ ${operation} == "shell" ]]; then
            echo
            echo "Entering an interactive shell for kubernetes testing"
            echo
            kind::set_current_context
            "${AIRFLOW_SOURCES}/scripts/ci/kubernetes/ci_run_kubernetes_tests.sh" "-i"
        elif [[ ${operation} == "k9s" ]]; then
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
            echo  "${COLOR_RED}ERROR: Wrong cluster operation: ${operation}. Should be one of: ${ALLOWED_KIND_OPERATIONS}  ${COLOR_RESET}"
            echo
            exit 1
        fi
    else
        if [[ ${operation} == "start" ]]; then
            echo
            echo "Creating cluster"
            echo
            kind::create_cluster
        elif [[ ${operation} == "recreate" ]]; then
            echo
            echo "Cluster ${KIND_CLUSTER_NAME} does not exist. Creating rather than recreating"
            echo "Creating cluster"
            echo
            kind::create_cluster
        elif [[ ${operation} == "stop" || ${operation} == "deploy" || ${operation} == "test" || ${operation} == "shell" ]]; then
            echo
            echo  "${COLOR_RED}ERROR: Cluster ${KIND_CLUSTER_NAME} does not exist. It should exist for ${operation} operation  ${COLOR_RESET}"
            echo
            exit 1
        else
            echo
            echo  "${COLOR_RED}ERROR: Wrong cluster operation: ${operation}. Should be one of ${ALLOWED_KIND_OPERATIONS}  ${COLOR_RESET}"
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
    local image_tag="latest"
    if [[ -n ${GITHUB_REGISTRY_PULL_IMAGE_TAG=} ]]; then
        image_tag="${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
    fi
    echo "Building ${AIRFLOW_IMAGE_KUBERNETES}:latest from ${AIRFLOW_PROD_IMAGE}:${image_tag}"
    docker_v build --tag "${AIRFLOW_IMAGE_KUBERNETES}:latest" . -f - <<EOF
FROM ${AIRFLOW_PROD_IMAGE}:${image_tag}

COPY airflow/example_dags/ \${AIRFLOW_HOME}/dags/

COPY airflow/kubernetes_executor_templates/ \${AIRFLOW_HOME}/pod_templates/

ENV GUNICORN_CMD_ARGS='--preload' AIRFLOW__WEBSERVER__WORKER_REFRESH_INTERVAL=0

EOF
    echo "The ${AIRFLOW_IMAGE_KUBERNETES}:${image_tag} is prepared for test kubernetes deployment."
}

function kind::load_image_to_kind_cluster() {
    kind load docker-image --name "${KIND_CLUSTER_NAME}" "${AIRFLOW_IMAGE_KUBERNETES}:latest"
}

MAX_NUM_TRIES_FOR_HEALTH_CHECK=12
readonly MAX_NUM_TRIES_FOR_HEALTH_CHECK

SLEEP_TIME_FOR_HEALTH_CHECK=10
readonly SLEEP_TIME_FOR_HEALTH_CHECK

function kind::wait_for_webserver_healthy() {
    num_tries=0
    set +e
    sleep "${SLEEP_TIME_FOR_HEALTH_CHECK}"
    while ! curl --connect-timeout 60  --max-time 60 "http://localhost:${FORWARDED_PORT_NUMBER}/health" -s | grep -q healthy; do
        echo
        echo "Sleeping ${SLEEP_TIME_FOR_HEALTH_CHECK} while waiting for webserver being ready"
        echo
        sleep "${SLEEP_TIME_FOR_HEALTH_CHECK}"
        num_tries=$((num_tries + 1))
        if [[ ${num_tries} == "${MAX_NUM_TRIES_FOR_HEALTH_CHECK}" ]]; then
            echo
            echo  "${COLOR_RED}ERROR: Timeout while waiting for the webserver health check  ${COLOR_RESET}"
            echo
            return 1
        fi
    done
    echo
    echo "Connection to 'airflow webserver' established on port ${FORWARDED_PORT_NUMBER}"
    echo
    set -e
}

function kind::deploy_airflow_with_helm() {
    echo "Deleting namespace ${HELM_AIRFLOW_NAMESPACE}"
    kubectl delete namespace "${HELM_AIRFLOW_NAMESPACE}" >/dev/null 2>&1 || true
    kubectl delete namespace "test-namespace" >/dev/null 2>&1 || true
    kubectl create namespace "${HELM_AIRFLOW_NAMESPACE}"
    kubectl create namespace "test-namespace"

    # If on CI, "pass-through" the current docker credentials from the host to be default image pull-secrets in the namespace
    if [[ ${CI:=} == "true" ]]; then
      local regcred
      regcred=$(jq -sRn '
        .apiVersion="v1" |
        .kind = "Secret" |
        .type = "kubernetes.io/dockerconfigjson" |
        .metadata.name="regcred" |
        .data[".dockerconfigjson"] = @base64 "\(inputs)"
      ' ~/.docker/config.json)
      kubectl -n test-namespace apply -f - <<<"$regcred"
      kubectl -n test-namespace patch serviceaccount default -p '{"imagePullSecrets": [{"name": "regcred"}]}'

      kubectl -n "${HELM_AIRFLOW_NAMESPACE}" apply -f - <<<"$regcred"
      kubectl -n "${HELM_AIRFLOW_NAMESPACE}" patch serviceaccount default -p '{"imagePullSecrets": [{"name": "regcred"}]}'
    fi

    local chartdir
    chartdir=$(mktemp -d)
    traps::add_trap "rm -rf ${chartdir}" EXIT INT HUP TERM
    # Copy chart to temporary directory to allow chart deployment in parallel
    # Otherwise helm deployment will fail on renaming charts to tmpcharts
    cp -r "${AIRFLOW_SOURCES}/chart" "${chartdir}"

    pushd "${chartdir}/chart" >/dev/null 2>&1 || exit 1
    helm repo add stable https://charts.helm.sh/stable/
    helm dep update
    helm install airflow . \
        --timeout 10m0s \
        --namespace "${HELM_AIRFLOW_NAMESPACE}" \
        --set "defaultAirflowRepository=${AIRFLOW_IMAGE_KUBERNETES}" \
        --set "images.airflow.repository=${AIRFLOW_IMAGE_KUBERNETES}" \
        --set "images.airflow.tag=latest" -v 1 \
        --set "defaultAirflowTag=latest" -v 1 \
        --set "config.api.auth_backend=airflow.api.auth.backend.basic_auth" \
        --set "config.logging.logging_level=DEBUG" \
        --set "executor=${EXECUTOR}"
    echo
    popd > /dev/null 2>&1|| exit 1
}

function kind::deploy_test_kubernetes_resources() {
    verbosity::print_info "Deploying Custom kubernetes resources"
    kubectl apply -f "scripts/ci/kubernetes/volumes.yaml" --namespace default
    kubectl apply -f "scripts/ci/kubernetes/nodeport.yaml" --namespace airflow
}

function kind::upgrade_airflow_with_helm() {
    local mode=$1
    echo
    echo "Upgrading airflow with ${mode}"
    echo
    local chartdir
    chartdir=$(mktemp -d)
    traps::add_trap "rm -rf ${chartdir}" EXIT INT HUP TERM
    # Copy chart to temporary directory to allow chart deployment in parallel
    # Otherwise helm deployment will fail on renaming charts to tmpcharts
    cp -r "${AIRFLOW_SOURCES}/chart" "${chartdir}"

    pushd "${chartdir}/chart" >/dev/null 2>&1 || exit 1
    helm repo add stable https://charts.helm.sh/stable/
    helm dep update
    helm upgrade airflow . --namespace "${HELM_AIRFLOW_NAMESPACE}" \
        --set "defaultAirflowRepository=${AIRFLOW_IMAGE_KUBERNETES}" \
        --set "images.airflow.repository=${AIRFLOW_IMAGE_KUBERNETES}" \
        --set "images.airflow.tag=latest" -v 1 \
        --set "defaultAirflowTag=latest" -v 1 \
        --set "config.api.auth_backend=airflow.api.auth.backend.basic_auth" \
        --set "config.logging.logging_level=DEBUG" \
        --set "executor=${mode}"

    echo
    popd > /dev/null 2>&1|| exit 1
}
