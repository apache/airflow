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

export INSTALL_FROM_PYPI="false"
export INSTALL_FROM_DOCKER_CONTEXT_FILES="true"
export INSTALL_PROVIDERS_FROM_SOURCES="false"
export AIRFLOW_PRE_CACHED_PIP_PACKAGES="false"
export DOCKER_CACHE="local"
export VERBOSE="true"

export INSTALLED_PROVIDERS=(
    "amazon"
    "microsoft.azure"
    "celery"
    "elasticsearch"
    "google"
    "cncf.kubernetes"
    "mysql"
    "postgres"
    "redis"
    "slack"
    "ssh"
)
readonly INSTALLED_PROVIDERS

export INSTALLED_EXTRAS="async,amazon,celery,cncf.kubernetes,docker,dask,elasticsearch,ftp,grpc,hashicorp,http,google,microsoft.azure,mysql,postgres,redis,sendgrid,sftp,slack,ssh,statsd,virtualenv"
readonly INSTALLED_EXTRAS

# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"

# Builds or waits for the PROD image in the CI environment
# Depending on the "USE_GITHUB_REGISTRY" and "GITHUB_REGISTRY_WAIT_FOR_IMAGE" setting
function build_prod_images_on_ci() {
    build_images::prepare_prod_build

    if [[ ${USE_GITHUB_REGISTRY} == "true" && ${GITHUB_REGISTRY_WAIT_FOR_IMAGE} == "true" ]]; then

        # Tries to wait for the image indefinitely
        # skips further image checks - since we already have the target image

        build_images::wait_for_image_tag "${GITHUB_REGISTRY_AIRFLOW_PROD_IMAGE}" \
            ":${GITHUB_REGISTRY_PULL_IMAGE_TAG}" "${AIRFLOW_PROD_IMAGE}"

        build_images::wait_for_image_tag "${GITHUB_REGISTRY_AIRFLOW_PROD_BUILD_IMAGE}" \
            ":${GITHUB_REGISTRY_PULL_IMAGE_TAG}" "${AIRFLOW_PROD_BUILD_IMAGE}"
    else
        # Cleanup dist and docker-context-files folders
        mkdir -pv "${AIRFLOW_SOURCES}/dist"
        mkdir -pv "${AIRFLOW_SOURCES}/docker-context-files"
        rm -f "${AIRFLOW_SOURCES}/dist/"*.{whl,tar.gz}
        rm -f "${AIRFLOW_SOURCES}/docker-context-files/"*.{whl,tar.gz}

        pip_download_command="pip download -d /dist '.[${INSTALLED_EXTRAS}]' --constraint 'https://raw.githubusercontent.com/apache/airflow/${DEFAULT_CONSTRAINTS_BRANCH}/constraints-${PYTHON_MAJOR_MINOR_VERSION}.txt'"

        docker run --rm --entrypoint /bin/bash \
            "${EXTRA_DOCKER_FLAGS[@]}" \
            "${AIRFLOW_CI_IMAGE}" -c "${pip_download_command}"

        # Remove all downloaded apache airflow packages
        rm -f "${AIRFLOW_SOURCES}/dist/"apache_airflow*.whl
        rm -f "${AIRFLOW_SOURCES}/dist/"apache-airflow*.tar.gz

        # Remove all downloaded apache airflow packages
        mv -f "${AIRFLOW_SOURCES}/dist/"* "${AIRFLOW_SOURCES}/docker-context-files/"

        # Build necessary provider packages
        runs::run_prepare_provider_packages "${INSTALLED_PROVIDERS[@]}"

        mv "${AIRFLOW_SOURCES}/dist/"*.whl "${AIRFLOW_SOURCES}/docker-context-files/"

        # Build apache airflow packages
        build_airflow_packages::build_airflow_packages

        # Remove generated tar.gz packages
        rm -f "${AIRFLOW_SOURCES}/dist/"apache-airflow*.tar.gz

        # move the packages to docker-context-files folder
        mkdir -pv "${AIRFLOW_SOURCES}/docker-context-files"
        mv "${AIRFLOW_SOURCES}/dist/"* "${AIRFLOW_SOURCES}/docker-context-files/"
        build_images::build_prod_images
    fi


    # Disable force pulling forced above this is needed for the subsequent scripts so that
    # They do not try to pull/build images again
    unset FORCE_PULL_IMAGES
    unset FORCE_BUILD
}

build_prod_images_on_ci
