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
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"

function verify_prod_image_has_airflow_and_providers {
    echo
    echo "Airflow folders installed in the image:"
    echo

    AIRFLOW_INSTALLATION_LOCATION="/home/airflow/.local"

    docker run --rm --entrypoint /bin/bash "${AIRFLOW_PROD_IMAGE}" -c \
        'find '"${AIRFLOW_INSTALLATION_LOCATION}"'/lib/python*/site-packages/airflow/ -type d'

    EXPECTED_MIN_AIRFLOW_DIRS_COUNT="60"
    readonly EXPECTED_MIN_AIRFLOW_DIRS_COUNT

    COUNT_AIRFLOW_DIRS=$(docker run --rm --entrypoint /bin/bash "${AIRFLOW_PROD_IMAGE}" -c \
         'find '"${AIRFLOW_INSTALLATION_LOCATION}"'/lib/python*/site-packages/airflow/ -type d | grep -c -v "/airflow/providers"')

    echo
    echo "Number of airflow dirs: ${COUNT_AIRFLOW_DIRS}"
    echo

    if [[ "${COUNT_AIRFLOW_DIRS}" -lt "${EXPECTED_MIN_AIRFLOW_DIRS_COUNT}" ]]; then
        >&2 echo
        >&2 echo Number of airflow folders installed is less than ${EXPECTED_MIN_AIRFLOW_DIRS_COUNT}
        >&2 echo This is unexpected. Please investigate, looking at the output above!
        >&2 echo
        exit 1
    else
        echo
        echo -e " \e[32mOK. Airflow is installed.\e[0m"
        echo
    fi

    EXPECTED_MIN_PROVIDERS_DIRS_COUNT="240"
    readonly EXPECTED_MIN_PROVIDERS_DIRS_COUNT

    COUNT_AIRFLOW_PROVIDERS_DIRS=$(docker run --rm --entrypoint /bin/bash "${AIRFLOW_PROD_IMAGE}" -c \
         'find '"${AIRFLOW_INSTALLATION_LOCATION}"'/lib/python*/site-packages/airflow/providers -type d | grep -c "" | xargs')

    echo
    echo "Number of providers dirs: ${COUNT_AIRFLOW_PROVIDERS_DIRS}"
    echo

    if [ "${COUNT_AIRFLOW_PROVIDERS_DIRS}" -lt "${EXPECTED_MIN_PROVIDERS_DIRS_COUNT}" ]; then
        >&2 echo
        >&2 echo Number of providers folders installed is less than ${EXPECTED_MIN_PROVIDERS_DIRS_COUNT}
        >&2 echo This is unexpected. Please investigate, looking at the output above!
        >&2 echo
        exit 1
    else
        echo
        echo -e " \e[32mOK. Airflow Providers are installed.\e[0m"
        echo
    fi
}


function verify_prod_image_dependencies {

    echo
    echo "Checking if Airflow dependencies are non-conflicting in ${AIRFLOW_PROD_IMAGE} image."
    echo

    set +e
    docker run --rm --entrypoint /bin/bash "${AIRFLOW_PROD_IMAGE}" -c 'pip check'
    local res=$?
    if [[ ${res} != "0" ]]; then
        echo -e " \e[31mERROR: ^^^ Some dependencies are conflicting. See instructions below on how to deal with it.\e[0m"
        echo
        build_images::inform_about_pip_check "--production "
        # TODO(potiuk) - enable the comment once https://github.com/apache/airflow/pull/12188 is merged
        # exit ${res}
    else
        echo
        echo " \e[32mOK. The ${AIRFLOW_PROD_IMAGE} image dependencies are consistent.\e[0m"
        echo
    fi
    set -e

}

function pull_prod_image() {
    local image_name_with_tag="${GITHUB_REGISTRY_AIRFLOW_PROD_IMAGE}:${GITHUB_REGISTRY_PULL_IMAGE_TAG}"

    echo
    echo "Pulling the ${image_name_with_tag} image."
    echo

    push_pull_remove_images::pull_image_github_dockerhub "${AIRFLOW_PROD_IMAGE}" "${image_name_with_tag}"
}

build_images::prepare_prod_build


verify_prod_image_has_airflow_and_providers

verify_prod_image_dependencies
