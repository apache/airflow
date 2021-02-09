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

# Build airflow packages
function build_airflow_packages::build_airflow_packages() {
    start_end::group_start "Build airflow packages ${PACKAGE_FORMAT}"
    rm -rf -- *egg-info*
    rm -rf -- build

    pip install --upgrade "pip==${AIRFLOW_PIP_VERSION}" "wheel==${WHEEL_VERSION}"

    local packages=()

    if [[ ${PACKAGE_FORMAT} == "wheel" || ${PACKAGE_FORMAT} == "both" ]] ; then
        packages+=("bdist_wheel")
    fi
    if [[ ${PACKAGE_FORMAT} == "sdist" || ${PACKAGE_FORMAT} == "both" ]] ; then
        packages+=("sdist")
    fi

    # Prepare airflow's wheel
    PYTHONUNBUFFERED=1 python setup.py compile_assets "${packages[@]}"

    # clean-up
    rm -rf -- *egg-info*
    rm -rf -- build

    echo
    echo "Airflow package prepared: ${PACKAGE_FORMAT}"
    echo
    start_end::group_end
}
