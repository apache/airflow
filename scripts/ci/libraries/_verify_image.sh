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

function verify_image::check_not_empty {
    image_size=$(docker inspect "${1}" -f '{{.Size}}')
    if [[ ${image_size} == "0" ]]; then
        echo "${COLOR_RED}The image ${1} is empty - which means it failed to build. See 'Build Image' step for details. Exiting!${COLOR_RESET}"
        exit 1
    fi
}

function verify_image::verify_prod_image {
    verify_image::check_not_empty "${1}"
    export DOCKER_IMAGE="${1}"
    python3 "${SCRIPTS_CI_DIR}/images/ci_run_docker_tests.py" "${AIRFLOW_SOURCES}/docker_tests/test_prod_image.py"
}

function verify_image::verify_ci_image {
    verify_image::check_not_empty "${1}"
    export DOCKER_IMAGE="${1}"
    python3 "${SCRIPTS_CI_DIR}/images/ci_run_docker_tests.py" "${AIRFLOW_SOURCES}/docker_tests/test_ci_image.py"
}
