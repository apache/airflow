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

AIRFLOW_SOURCES=$(
    cd "${MY_DIR}/../../../../../" || exit 1
    pwd
)
export AIRFLOW_SOURCES

# We keep _utils here because we are not in the in_container directory
# shellcheck source=scripts/ci/in_container/_in_container_utils.sh
. "${MY_DIR}/../../_in_container_utils.sh"

assert_in_container

in_container_script_start

function end_and_dump_logs() {
    dump_logs
    in_container_script_end
}

trap in_container_script_end EXIT

export TEMPLATE_DIRNAME="${MY_DIR}/templates"
export BUILD_DIRNAME="${MY_DIR}/build"

# shellcheck source=common/_image_variables.sh
. "${AIRFLOW_SOURCES}/common/_image_variables.sh"

# Source branch will be set in DockerHub
SOURCE_BRANCH=${SOURCE_BRANCH:=${DEFAULT_BRANCH}}
# if BRANCH_NAME is not set it will be set to either SOURCE_BRANCH (if overridden)
# or default branch for the sources
BRANCH_NAME=${BRANCH_NAME:=${SOURCE_BRANCH}}

if [[ ! -d "${BUILD_DIRNAME}" ]]; then
    mkdir -p "${BUILD_DIRNAME}"
fi

rm -f "${BUILD_DIRNAME}"/*

if [[ "${KUBERNETES_MODE}" == "persistent_mode" ]]; then
    INIT_DAGS_VOLUME_NAME=airflow-dags
    POD_AIRFLOW_DAGS_VOLUME_NAME=airflow-dags
    CONFIGMAP_DAGS_FOLDER=/root/airflow/dags
    CONFIGMAP_GIT_DAGS_FOLDER_MOUNT_POINT=
    CONFIGMAP_DAGS_VOLUME_CLAIM=airflow-dags
else
    INIT_DAGS_VOLUME_NAME=airflow-dags-fake
    POD_AIRFLOW_DAGS_VOLUME_NAME=airflow-dags-git
    CONFIGMAP_DAGS_FOLDER=/root/airflow/dags/repo/airflow/contrib/example_dags
    CONFIGMAP_GIT_DAGS_FOLDER_MOUNT_POINT=/root/airflow/dags
    CONFIGMAP_DAGS_VOLUME_CLAIM=
fi

if [[ ${TRAVIS_PULL_REQUEST:=} != "" && ${TRAVIS_PULL_REQUEST} != "false" ]]; then
    CONFIGMAP_GIT_REPO=${TRAVIS_PULL_REQUEST_SLUG}
    CONFIGMAP_BRANCH=${TRAVIS_PULL_REQUEST_BRANCH}
else
    CONFIGMAP_GIT_REPO=${TRAVIS_REPO_SLUG:-apache/airflow}
    CONFIGMAP_BRANCH=${TRAVIS_BRANCH:=master}
fi


if [[ "${KUBERNETES_MODE}" == "persistent_mode" ]]; then
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

cat "${BUILD_DIRNAME}/airflow.yaml"
cat "${BUILD_DIRNAME}/configmaps.yaml"

kubectl delete -f "${MY_DIR}/postgres.yaml" || true
kubectl delete -f "${BUILD_DIRNAME}/airflow.yaml" || true
kubectl delete -f "${MY_DIR}/secrets.yaml" || true

set -e

kubectl apply -f "${MY_DIR}/secrets.yaml"
kubectl apply -f "${BUILD_DIRNAME}/configmaps.yaml"
kubectl apply -f "${MY_DIR}/postgres.yaml"
kubectl apply -f "${MY_DIR}/volumes.yaml"
kubectl apply -f "${BUILD_DIRNAME}/airflow.yaml"

dump_logs() {
    POD=$(kubectl get pods -o go-template --template '{{range .items}}{{.metadata.name}}{{"\n"}}{{end}}' | grep airflow | head -1)
    echo "------- pod description -------"
    kubectl describe pod "${POD}"
    echo "------- webserver init container logs - init -------"
    kubectl logs "${POD}" -c init || true
    if [[ "${KUBERNETES_MODE}" != "persistent_mode" ]]; then
        echo "------- webserver init container logs - git-sync-clone -------"
        kubectl logs "${POD}" -c git-sync-clone || true
    fi
    echo "------- webserver logs -------"
    kubectl logs "${POD}" -c webserver || true
    echo "------- scheduler logs -------"
    kubectl logs "${POD}" -c scheduler || true
    echo "--------------"
}

set +x
set +o pipefail
# wait for up to 10 minutes for everything to be deployed
PODS_ARE_READY="0"
for i in {1..150}; do
    echo "------- Running kubectl get pods: $i -------"
    PODS=$(kubectl get pods | awk 'NR>1 {print $0}')
    echo "$PODS"
    NUM_AIRFLOW_READY=$(echo "${PODS}" | grep airflow | awk '{print $2}' | grep -cE '([0-9])\/(\1)' | xargs)
    NUM_POSTGRES_READY=$(echo "${PODS}" | grep postgres | awk '{print $2}' | grep -cE '([0-9])\/(\1)' | xargs)
    if [[ "${NUM_AIRFLOW_READY}" == "1" && "${NUM_POSTGRES_READY}" == "1" ]]; then
        PODS_ARE_READY="1"
        break
    fi
    sleep 4
done
POD=$(kubectl get pods -o go-template --template '{{range .items}}{{.metadata.name}}{{"\n"}}{{end}}' | grep airflow | head -1)

if [[ "${PODS_ARE_READY}" == "1" ]]; then
    echo "PODS are ready."
else
    echo >&2 "PODS are not ready after waiting for a long time. Exiting..."
    dump_logs
    exit 1
fi

# Wait until Airflow webserver is up
KUBERNETES_HOST=${CLUSTER_NAME}-worker
AIRFLOW_WEBSERVER_IS_READY="0"
CONSECUTIVE_SUCCESS_CALLS=0
for i in {1..30}; do
    echo "------- Wait until webserver is up: $i -------"
    PODS=$(kubectl get pods | awk 'NR>1 {print $0}')
    echo "$PODS"
    HTTP_CODE=$(curl -LI "http://${KUBERNETES_HOST}:30809/health" -o /dev/null -w '%{http_code}\n' -sS) || true
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
    echo "Airflow webserver is ready."
else
    echo >&2 "Airflow webserver is not ready after waiting for a long time. Exiting..."
    exit 1
fi
