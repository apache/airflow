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
export PYTHON_MAJOR_MINOR_VERSION=${PYTHON_MAJOR_MINOR_VERSION:-3.6}

# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"

function run_test_package_import_all_classes() {
    docker run "${EXTRA_DOCKER_FLAGS[@]}" \
        --entrypoint "/usr/local/bin/dumb-init"  \
        -v "${AIRFLOW_SOURCES}/dist:/dist:cached" \
        -v "${AIRFLOW_SOURCES}/setup.py:/airflow_sources/setup.py:cached" \
        -v "${AIRFLOW_SOURCES}/setup.cfg:/airflow_sources/setup.cfg:cached" \
        -v "${AIRFLOW_SOURCES}/airflow/__init__.py:/airflow_sources/airflow/__init__.py:cached" \
        -v "${AIRFLOW_SOURCES}/airflow/version.py:/airflow_sources/airflow/version.py:cached" \
        -v "${AIRFLOW_SOURCES}/backport_packages/import_all_provider_classes.py:/import_all_provider_classes.py:cached" \
        "${AIRFLOW_CI_IMAGE}" \
        "--" "/opt/airflow/scripts/in_container/run_test_package_import_all_classes.sh"
}

get_environment_for_builds_on_ci

prepare_ci_build

rebuild_ci_image_if_needed

run_test_package_import_all_classes
