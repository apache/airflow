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
export MOUNT_LOCAL_SOURCES="false"

# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$(dirname "${BASH_SOURCE[0]}")/../../scripts/ci/libraries/_script_init.sh"

function enter_breeze_with_mapped_sources() {
    docker run -it "${EXTRA_DOCKER_FLAGS[@]}" \
        -v "${AIRFLOW_SOURCES}/dist:/dist:cached" \
        -v "${AIRFLOW_SOURCES}/setup.py:/airflow_sources/setup.py:cached" \
        -v "${AIRFLOW_SOURCES}/setup.cfg:/airflow_sources/setup.cfg:cached" \
        -v "${AIRFLOW_SOURCES}/airflow/__init__.py:/airflow_sources/airflow/__init__.py:cached" \
        -v "${AIRFLOW_SOURCES}/airflow/version.py:/airflow_sources/airflow/version.py:cached" \
        -v "${AIRFLOW_SOURCES}/empty:/opt/airflow/airflow:cached" \
        -v "${AIRFLOW_SOURCES}/scripts/in_container:/opt/airflow/scripts/in_container:cached" \
        -v "${AIRFLOW_SOURCES}/dev/import_all_classes.py:/opt/airflow/dev/import_all_classes.py:cached" \
        "${AIRFLOW_CI_IMAGE}"
}

build_images::prepare_ci_build

build_images::rebuild_ci_image_if_needed

enter_breeze_with_mapped_sources
