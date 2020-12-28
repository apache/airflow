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
# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$(dirname "${BASH_SOURCE[0]}")/../libraries/_script_init.sh"

function run_command_in_image() {
    docker run --rm \
            -e COLUMNS=180 \
            --entrypoint /bin/bash "${AIRFLOW_PROD_IMAGE}" \
            -c "${@}"
}

FEATURES_OK="true"

function check_feature() {
    DESCRIPTION="${1}"
    COMMAND=${2}
    set +e
    echo -n "Feature: ${DESCRIPTION} "
    local output
    output=$(run_command_in_image "${COMMAND}" 2>&1)
    local res=$?
    if [[ ${res} == "0" ]]; then
        echo "${COLOR_GREEN}OK${COLOR_RESET}"
    else
        echo "${COLOR_RED}NOK${COLOR_RESET}"
        echo "${COLOR_BLUE}========================= OUTPUT start ============================${COLOR_RESET}"
        echo "${output}"
        echo "${COLOR_BLUE}========================= OUTPUT end   ===========================${COLOR_RESET}"
        FEATURES_OK="false"
    fi
    set -e
}

function verify_prod_image_has_airflow_and_providers() {
    start_end::group_start "Verify prod image: ${AIRFLOW_PROD_IMAGE}"
    echo
    echo "Checking if Providers are installed"
    echo

    all_providers_installed_in_image=$(run_command_in_image "airflow providers list --output table")

    echo
    echo "Installed providers:"
    echo
    echo "${all_providers_installed_in_image}"
    echo
    local error="false"
    for provider in "${INSTALLED_PROVIDERS[@]}"; do
        echo -n "Verifying if provider ${provider} installed: "
        if [[ ${all_providers_installed_in_image} == *"apache-airflow-providers-${provider//./-}"* ]]; then
            echo "${COLOR_GREEN}OK${COLOR_RESET}"
        else
            echo "${COLOR_RED}NOK${COLOR_RESET}"
            error="true"
        fi
    done
    if [[ ${error} == "true" ]]; then
        echo
        echo "${COLOR_RED_ERROR} Some expected providers are not installed!${COLOR_RESET}"
        echo
        exit 1
    else
        echo
        echo "${COLOR_GREEN_OK} All expected providers installed!${COLOR_RESET}"
        echo
    fi
    start_end::group_end
}

function verify_prod_image_dependencies() {
    start_end::group_start "Checking if Airflow dependencies are non-conflicting in ${AIRFLOW_PROD_IMAGE} image."

    set +e
    run_command_in_image 'pip check'
    local res=$?
    if [[ ${res} != "0" ]]; then
        echo "${COLOR_RED_ERROR} ^^^ Some dependencies are conflicting. See instructions below on how to deal with it.  ${COLOR_RESET}"
        echo
        build_images::inform_about_pip_check "--production "
        exit ${res}
    else
        echo
        echo "${COLOR_GREEN_OK} The ${AIRFLOW_PROD_IMAGE} image dependencies are consistent.  ${COLOR_RESET}"
        echo
    fi
    set -e
    start_end::group_end
}

GOOGLE_IMPORTS=(
    'OpenSSL'
    'google.ads'
    'googleapiclient'
    'google.auth'
    'google_auth_httplib2'
    'google.cloud.automl'
    'google.cloud.bigquery_datatransfer'
    'google.cloud.bigtable'
    'google.cloud.container'
    'google.cloud.datacatalog'
    'google.cloud.dataproc'
    'google.cloud.dlp'
    'google.cloud.kms'
    'google.cloud.language'
    'google.cloud.logging'
    'google.cloud.memcache'
    'google.cloud.monitoring'
    'google.cloud.oslogin'
    'google.cloud.pubsub'
    'google.cloud.redis'
    'google.cloud.secretmanager'
    'google.cloud.spanner'
    'google.cloud.speech'
    'google.cloud.storage'
    'google.cloud.tasks'
    'google.cloud.texttospeech'
    'google.cloud.translate'
    'google.cloud.videointelligence'
    'google.cloud.vision'
)

AZURE_IMPORTS=(
    'azure.batch'
    'azure.cosmos'
    'azure.datalake.store'
    'azure.identity'
    'azure.keyvault'
    'azure.kusto.data'
    'azure.mgmt.containerinstance'
    'azure.mgmt.datalake.store'
    'azure.mgmt.resource'
    'azure.storage'
)

function verify_production_image_features() {
    start_end::group_start "Verify prod image features: ${AIRFLOW_PROD_IMAGE}"

    check_feature "Import: async" "python -c 'import gevent, eventlet, greenlet'"
    check_feature "Import: amazon" "python -c 'import boto3, botocore, watchtower'"
    check_feature "Import: celery" "python -c 'import celery, flower, vine'"
    check_feature "Import: cncf.kubernetes" "python -c 'import kubernetes, cryptography'"
    check_feature "Import: docker" "python -c 'import docker'"
    check_feature "Import: dask" "python -c 'import cloudpickle, distributed'"
    check_feature "Import: elasticsearch" "python -c 'import elasticsearch,es.elastic, elasticsearch_dsl'"
    check_feature "Import: grpc" "python -c 'import grpc, google.auth, google_auth_httplib2'"
    check_feature "Import: hashicorp" "python -c 'import hvac'"
    for google_import in "${GOOGLE_IMPORTS[@]}"
    do
        check_feature "Import google: ${google_import}" "python -c 'import ${google_import}'"
    done
    for azure_import in "${AZURE_IMPORTS[@]}"
    do
        check_feature "Import azure: ${azure_import}" "python -c 'import ${azure_import}'"
    done
    check_feature "Import: mysql" "python -c 'import mysql'"
    check_feature "Import: postgres" "python -c 'import psycopg2'"
    check_feature "Import: redis" "python -c 'import redis'"
    check_feature "Import: sendgrid" "python -c 'import sendgrid'"
    check_feature "Import: sftp/ssh" "python -c 'import paramiko, pysftp, sshtunnel'"
    check_feature "Import: slack" "python -c 'import slack'"
    check_feature "Import: statsd" "python -c 'import statsd'"
    check_feature "Import: virtualenv" "python -c 'import virtualenv'"

    if [[ ${FEATURES_OK} == "true" ]]; then
        echo
        echo "${COLOR_GREEN_OK} The ${AIRFLOW_PROD_IMAGE} features are all OK.  ${COLOR_RESET}"
        echo
    else
        echo
        echo "${COLOR_RED_ERROR} Some features were not ok!${COLOR_RESET}"
        echo
        exit 1
    fi
    start_end::group_end
}


function pull_prod_image() {
    local image_name_with_tag="${GITHUB_REGISTRY_AIRFLOW_PROD_IMAGE}:${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
    start_end::group_start "Pulling the ${image_name_with_tag} image and tagging with ${AIRFLOW_PROD_IMAGE}"

    push_pull_remove_images::pull_image_github_dockerhub "${AIRFLOW_PROD_IMAGE}" "${image_name_with_tag}"
    start_end::group_end
}

build_images::prepare_prod_build

pull_prod_image

verify_prod_image_has_airflow_and_providers

verify_production_image_features

verify_prod_image_dependencies
