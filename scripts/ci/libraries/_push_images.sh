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

# Pushes Ci image and it's manifest to the registry. In case the image was taken from cache registry
# it is pushed to the cache, not to the main registry. Manifest is only pushed to the main registry
function push_ci_image() {
    if [[ ${CACHED_AIRFLOW_CI_IMAGE:=} != "" ]]; then
        verbose_docker tag "${AIRFLOW_CI_IMAGE}" "${CACHED_AIRFLOW_CI_IMAGE}"
        IMAGE_TO_PUSH="${CACHED_AIRFLOW_CI_IMAGE}"
    else
        IMAGE_TO_PUSH="${AIRFLOW_CI_IMAGE}"
    fi
    verbose_docker push "${IMAGE_TO_PUSH}"
    if [[ ${CACHED_AIRFLOW_CI_IMAGE} == "" ]]; then
        # Only push manifest image for builds that are not using CI cache
        verbose_docker tag "${AIRFLOW_CI_LOCAL_MANIFEST_IMAGE}" "${AIRFLOW_CI_REMOTE_MANIFEST_IMAGE}"
        verbose_docker push "${AIRFLOW_CI_REMOTE_MANIFEST_IMAGE}"
        if [[ -n ${DEFAULT_IMAGE:=""} ]]; then
            verbose_docker push "${DEFAULT_IMAGE}"
        fi
    fi
    if [[ ${CACHED_PYTHON_BASE_IMAGE} != "" ]]; then
        verbose_docker tag "${PYTHON_BASE_IMAGE}" "${CACHED_PYTHON_BASE_IMAGE}"
        verbose_docker push "${CACHED_PYTHON_BASE_IMAGE}"
    fi

}

# Pushes PROD image to the registry. In case the image was taken from cache registry
# it is also pushed to the cache, not to the main registry
function push_prod_images() {
    if [[ ${CACHED_AIRFLOW_PROD_IMAGE:=} != "" ]]; then
        verbose_docker tag "${AIRFLOW_PROD_IMAGE}" "${CACHED_AIRFLOW_PROD_IMAGE}"
        IMAGE_TO_PUSH="${CACHED_AIRFLOW_PROD_IMAGE}"
    else
        IMAGE_TO_PUSH="${AIRFLOW_PROD_IMAGE}"
    fi
    if [[ ${CACHED_AIRFLOW_PROD_BUILD_IMAGE:=} != "" ]]; then
        verbose_docker tag "${AIRFLOW_PROD_BUILD_IMAGE}" "${CACHED_AIRFLOW_PROD_BUILD_IMAGE}"
        IMAGE_TO_PUSH_BUILD="${CACHED_AIRFLOW_PROD_BUILD_IMAGE}"
    else
        IMAGE_TO_PUSH_BUILD="${AIRFLOW_PROD_BUILD_IMAGE}"
    fi
    verbose_docker push "${IMAGE_TO_PUSH}"
    verbose_docker push "${IMAGE_TO_PUSH_BUILD}"
    if [[ -n ${DEFAULT_IMAGE:=""} && ${CACHED_AIRFLOW_PROD_IMAGE} == "" ]]; then
        verbose_docker push "${DEFAULT_IMAGE}"
    fi

    # we do not need to push PYTHON base image here - they are already pushed in the CI push
}
