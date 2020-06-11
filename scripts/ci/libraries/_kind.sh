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

function check_kind_and_kubectl_are_installed() {
    SYSTEM=$(uname -s| tr '[:upper:]' '[:lower:]')
    KIND_VERSION="v0.7.0"
    KIND_URL="https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VERSION}/kind-${SYSTEM}-amd64"
    KIND_PATH="${BUILD_CACHE_DIR}/bin/kind"
    KUBECTL_VERSION="v1.15.3"
    KUBECTL_URL="https://storage.googleapis.com/kubernetes-release/release/${KUBECTL_VERSION}/bin/${SYSTEM}/amd64/kubectl"
    KUBECTL_PATH="${BUILD_CACHE_DIR}/bin/kubectl"
    mkdir -pv "${BUILD_CACHE_DIR}/bin"
    if [[ ! -f "${KIND_PATH}" ]]; then
        echo
        echo "Downloading Kind version ${KIND_VERSION}"
        echo
        curl --fail --location "${KIND_URL}" --output "${KIND_PATH}"
        chmod +x "${KIND_PATH}"
    fi
    if [[ ! -f "${KUBECTL_PATH}" ]]; then
        echo
        echo "Downloading Kubectl version ${KUBECTL_VERSION}"
        echo
        curl --fail --location "${KUBECTL_URL}" --output "${KUBECTL_PATH}"
        chmod +x "${KUBECTL_PATH}"
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

    echo
    echo "Patching CoreDNS to avoid loop and to use 8.8.8.8 DNS as forward address."
    echo
    echo "============================================================================"
    echo "      Original coredns configmap:"
    echo "============================================================================"
    kubectl --cluster "${KUBECTL_CLUSTER_NAME}" get configmaps --namespace=kube-system coredns -o yaml
    kubectl --cluster "${KUBECTL_CLUSTER_NAME}" get configmaps --namespace=kube-system coredns -o yaml | \
        sed 's/forward \. .*$/forward . 8.8.8.8/' | kubectl --cluster "${KUBECTL_CLUSTER_NAME}" apply -f -

    echo
    echo "============================================================================"
    echo "      Updated coredns configmap with new forward directive:"
    echo "============================================================================"
    kubectl --cluster "${KUBECTL_CLUSTER_NAME}" get configmaps --namespace=kube-system coredns -o yaml


    echo
    echo "Restarting CoreDNS"
    echo
    kubectl --cluster "${KUBECTL_CLUSTER_NAME}" scale deployment --namespace=kube-system coredns --replicas=0
    kubectl --cluster "${KUBECTL_CLUSTER_NAME}" scale deployment --namespace=kube-system coredns --replicas=2
    echo
    echo "Restarted CoreDNS"
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
            get_ci_environment
            check_kind_and_kubectl_are_installed
            build_kubernetes_image
            load_image_to_kind_cluster
            prepare_kubernetes_app_variables
            prepare_kubernetes_resources
            apply_kubernetes_resources
            wait_for_airflow_pods_up_and_running
            wait_for_airflow_webserver_up_and_running
        elif [[ ${OPERATION} == "test" ]]; then
            echo
            echo "Testing with kind to KinD"
            echo
            "${AIRFLOW_SOURCES}/scripts/ci/ci_run_kubernetes_tests.sh"
        else
            echo
            echo "Wrong cluster operation: ${OPERATION}. Should be one of:"
            echo "${FORMATTED_KIND_OPERATIONS}"
            echo
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
        elif [[ ${OPERATION} == "stop" || ${OEPRATON} == "deploy" || ${OPERATION} == "test" ]]; then
            echo
            echo "Cluster ${KIND_CLUSTER_NAME} does not exist. It should exist for ${OPERATION} operation"
            echo
            exit 1
        else
            echo
            echo "Wrong cluster operation: ${OPERATION}. Should be one of:"
            echo "${FORMATTED_KIND_OPERATIONS}"
            echo
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


function build_kubernetes_image() {
    cd "${AIRFLOW_SOURCES}" || exit 1
    prepare_prod_build
    if [[ $(docker images -q "${AIRFLOW_PROD_IMAGE}") == "" ||
            ${FORCE_BUILD_IMAGES:="false"} == "true" ]]; then
        build_prod_image
    else
        echo
        echo "Skip rebuilding prod image. Use --force-build-images to rebuild prod image."
        echo
    fi
    echo
    echo "Adding kubernetes-specific scripts to prod image."
    echo "Building ${AIRFLOW_KUBERNETES_IMAGE} from ${AIRFLOW_PROD_IMAGE} with latest sources."
    echo
    docker build \
        --build-arg AIRFLOW_PROD_IMAGE="${AIRFLOW_PROD_IMAGE}" \
        --cache-from "${AIRFLOW_PROD_IMAGE}" \
        --tag="${AIRFLOW_KUBERNETES_IMAGE}" \
        -f- . << 'EOF'
    ARG AIRFLOW_PROD_IMAGE
    FROM ${AIRFLOW_PROD_IMAGE}

    ARG AIRFLOW_SOURCES=/home/airflow/airflow_sources/
    ENV AIRFLOW_SOURCES=${AIRFLOW_SOURCES}

    USER root

    COPY --chown=airflow:airflow . ${AIRFLOW_SOURCES}

    COPY scripts/ci/kubernetes/docker/airflow-test-env-init-db.sh /tmp/airflow-test-env-init-db.sh
    COPY scripts/ci/kubernetes/docker/airflow-test-env-init-dags.sh /tmp/airflow-test-env-init-dags.sh
    COPY scripts/ci/kubernetes/docker/bootstrap.sh /bootstrap.sh

    RUN chmod +x /bootstrap.sh


    USER airflow


    ENTRYPOINT ["/bootstrap.sh"]
EOF

    echo "The ${AIRFLOW_KUBERNETES_IMAGE} is prepared for deployment."
}

function load_image_to_kind_cluster() {
    echo
    echo "Loading ${AIRFLOW_KUBERNETES_IMAGE} to ${KIND_CLUSTER_NAME}"
    echo
    kind load docker-image --name "${KIND_CLUSTER_NAME}" "${AIRFLOW_KUBERNETES_IMAGE}"
}

function prepare_kubernetes_app_variables() {
    echo
    echo "Preparing kubernetes variables"
    echo
    KUBERNETES_APP_DIR="${AIRFLOW_SOURCES}/scripts/ci/kubernetes/app"
    TEMPLATE_DIRNAME="${KUBERNETES_APP_DIR}/templates"
    BUILD_DIRNAME="${KUBERNETES_APP_DIR}/build"

    # shellcheck source=common/_image_variables.sh
    . "${AIRFLOW_SOURCES}/common/_image_variables.sh"

    # Source branch will be set in DockerHub
    SOURCE_BRANCH=${SOURCE_BRANCH:=${DEFAULT_BRANCH}}
    BRANCH_NAME=${BRANCH_NAME:=${SOURCE_BRANCH}}

    if [[ ! -d "${BUILD_DIRNAME}" ]]; then
        mkdir -p "${BUILD_DIRNAME}"
    fi

    rm -f "${BUILD_DIRNAME}"/*
    rm -f "${BUILD_DIRNAME}"/*

    if [[ "${KUBERNETES_MODE}" == "image" ]]; then
        INIT_DAGS_VOLUME_NAME=airflow-dags
        POD_AIRFLOW_DAGS_VOLUME_NAME=airflow-dags
        CONFIGMAP_DAGS_FOLDER=/opt/airflow/dags
        CONFIGMAP_GIT_DAGS_FOLDER_MOUNT_POINT=
        CONFIGMAP_DAGS_VOLUME_CLAIM=airflow-dags
    else
        INIT_DAGS_VOLUME_NAME=airflow-dags-fake
        POD_AIRFLOW_DAGS_VOLUME_NAME=airflow-dags-git
        CONFIGMAP_DAGS_FOLDER=/opt/airflow/dags/repo/airflow/example_dags
        CONFIGMAP_GIT_DAGS_FOLDER_MOUNT_POINT=/opt/airflow/dags
        CONFIGMAP_DAGS_VOLUME_CLAIM=
    fi


    CONFIGMAP_GIT_REPO=${CI_SOURCE_REPO}
    CONFIGMAP_BRANCH=${CI_SOURCE_BRANCH}
}

function prepare_kubernetes_resources() {
    echo
    echo "Preparing kubernetes resources"
    echo
    if [[ "${KUBERNETES_MODE}" == "image" ]]; then
        sed -e "s/{{INIT_GIT_SYNC}}//g" \
            "${TEMPLATE_DIRNAME}/airflow.template.yaml" >"${BUILD_DIRNAME}/airflow.yaml"
    else
        sed -e "/{{INIT_GIT_SYNC}}/{r ${TEMPLATE_DIRNAME}/init_git_sync.template.yaml" -e 'd}' \
            "${TEMPLATE_DIRNAME}/airflow.template.yaml" >"${BUILD_DIRNAME}/airflow.yaml"
    fi
    sed -i "s|{{AIRFLOW_KUBERNETES_IMAGE}}|${AIRFLOW_KUBERNETES_IMAGE}|g" "${BUILD_DIRNAME}/airflow.yaml"

    sed -i "s|{{CONFIGMAP_GIT_REPO}}|${CONFIGMAP_GIT_REPO}|g" "${BUILD_DIRNAME}/airflow.yaml"
    sed -i "s|{{CONFIGMAP_BRANCH}}|${CONFIGMAP_BRANCH}|g" "${BUILD_DIRNAME}/airflow.yaml"
    sed -i "s|{{INIT_DAGS_VOLUME_NAME}}|${INIT_DAGS_VOLUME_NAME}|g" "${BUILD_DIRNAME}/airflow.yaml"
    sed -i "s|{{POD_AIRFLOW_DAGS_VOLUME_NAME}}|${POD_AIRFLOW_DAGS_VOLUME_NAME}|g" \
        "${BUILD_DIRNAME}/airflow.yaml"

    sed "s|{{CONFIGMAP_DAGS_FOLDER}}|${CONFIGMAP_DAGS_FOLDER}|g" \
        "${TEMPLATE_DIRNAME}/configmaps.template.yaml" >"${BUILD_DIRNAME}/configmaps.yaml"
    sed -i "s|{{CONFIGMAP_GIT_REPO}}|${CONFIGMAP_GIT_REPO}|g" "${BUILD_DIRNAME}/configmaps.yaml"
    sed -i "s|{{CONFIGMAP_BRANCH}}|${CONFIGMAP_BRANCH}|g" "${BUILD_DIRNAME}/configmaps.yaml"
    sed -i "s|{{CONFIGMAP_GIT_DAGS_FOLDER_MOUNT_POINT}}|${CONFIGMAP_GIT_DAGS_FOLDER_MOUNT_POINT}|g" \
        "${BUILD_DIRNAME}/configmaps.yaml"
    sed -i "s|{{CONFIGMAP_DAGS_VOLUME_CLAIM}}|${CONFIGMAP_DAGS_VOLUME_CLAIM}|g" \
        "${BUILD_DIRNAME}/configmaps.yaml"
    sed -i "s|{{AIRFLOW_KUBERNETES_IMAGE_NAME}}|${AIRFLOW_KUBERNETES_IMAGE_NAME}|g" \
        "${BUILD_DIRNAME}/configmaps.yaml"
    sed -i "s|{{AIRFLOW_KUBERNETES_IMAGE_TAG}}|${AIRFLOW_KUBERNETES_IMAGE_TAG}|g" \
        "${BUILD_DIRNAME}/configmaps.yaml"
}

function apply_kubernetes_resources() {
    echo
    echo "Apply kubernetes resources."
    echo


    kubectl delete -f "${KUBERNETES_APP_DIR}/postgres.yaml" --cluster "${KUBECTL_CLUSTER_NAME}" \
        2>&1 | grep -v "NotFound" || true
    kubectl delete -f "${BUILD_DIRNAME}/airflow.yaml" --cluster "${KUBECTL_CLUSTER_NAME}" \
        2>&1 | grep -v "NotFound" || true
    kubectl delete -f "${KUBERNETES_APP_DIR}/secrets.yaml" --cluster "${KUBECTL_CLUSTER_NAME}" \
        2>&1 | grep -v "NotFound" || true

    set -e

    kubectl apply -f "${KUBERNETES_APP_DIR}/secrets.yaml" --cluster "${KUBECTL_CLUSTER_NAME}"
    kubectl apply -f "${BUILD_DIRNAME}/configmaps.yaml" --cluster "${KUBECTL_CLUSTER_NAME}"
    kubectl apply -f "${KUBERNETES_APP_DIR}/postgres.yaml" --cluster "${KUBECTL_CLUSTER_NAME}"
    kubectl apply -f "${KUBERNETES_APP_DIR}/volumes.yaml" --cluster "${KUBECTL_CLUSTER_NAME}"
    kubectl apply -f "${BUILD_DIRNAME}/airflow.yaml" --cluster "${KUBECTL_CLUSTER_NAME}"
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

function wait_for_airflow_pods_up_and_running() {
    set +o pipefail
    # wait for up to 10 minutes for everything to be deployed
    PODS_ARE_READY="0"
    for i in {1..150}; do
        echo "------- Running kubectl get pods: $i -------"
        PODS=$(kubectl get pods --cluster "${KUBECTL_CLUSTER_NAME}" | awk 'NR>1 {print $0}')
        echo "$PODS"
        NUM_AIRFLOW_READY=$(echo "${PODS}" | grep airflow | awk '{print $2}' | grep -cE '([0-9])\/(\1)' \
            | xargs)
        NUM_POSTGRES_READY=$(echo "${PODS}" | grep postgres | awk '{print $2}' | grep -cE '([0-9])\/(\1)' \
            | xargs)
        if [[ "${NUM_AIRFLOW_READY}" == "1" && "${NUM_POSTGRES_READY}" == "1" ]]; then
            PODS_ARE_READY="1"
            break
        fi
        sleep 4
    done
    POD=$(kubectl get pods -o go-template --template '{{range .items}}{{.metadata.name}}{{"\n"}}{{end}}' \
        --cluster "${KUBECTL_CLUSTER_NAME}" | grep airflow | head -1)

    if [[ "${PODS_ARE_READY}" == "1" ]]; then
        echo "PODS are ready."
        set -o pipefail
    else
        echo >&2 "PODS are not ready after waiting for a long time. Exiting..."
        dump_kubernetes_logs
        exit 1
    fi
}


function wait_for_airflow_webserver_up_and_running() {
    set +o pipefail
    # Wait until Airflow webserver is up
    KUBERNETES_HOST=localhost
    AIRFLOW_WEBSERVER_IS_READY="0"
    CONSECUTIVE_SUCCESS_CALLS=0
    for i in {1..30}; do
        echo "------- Wait until webserver is up: $i -------"
        PODS=$(kubectl get pods --cluster "${KUBECTL_CLUSTER_NAME}" | awk 'NR>1 {print $0}')
        echo "$PODS"
        HTTP_CODE=$(curl -LI "http://${KUBERNETES_HOST}:30809/health" -o /dev/null -w '%{http_code}\n' -sS) \
            || true
        if [[ "${HTTP_CODE}" == 200 ]]; then
            ((CONSECUTIVE_SUCCESS_CALLS += 1))
        else
            CONSECUTIVE_SUCCESS_CALLS="0"
        fi
        if [[ "${CONSECUTIVE_SUCCESS_CALLS}" == 3 ]]; then
            AIRFLOW_WEBSERVER_IS_READY="1"
            break
        fi
        sleep 10
    done
    set -o pipefail
    if [[ "${AIRFLOW_WEBSERVER_IS_READY}" == "1" ]]; then
        echo
        echo "Airflow webserver is ready."
        echo
    else
        echo >&2
        echo >&2 "Airflow webserver is not ready after waiting for a long time. Exiting..."
        echo >&2
        dump_kubernetes_logs
        exit 1
    fi
}
