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

if [ -z "${FILES_DIR+x}" ]; then
    export FILES_DIR="/files"
fi
if [ -z "${AIRFLOW_BREEZE_CONFIG_DIR+x}" ]; then
    export AIRFLOW_BREEZE_CONFIG_DIR="${FILES_DIR}/airflow-breeze-config"
fi

if [ -z "${INIT_SCRIPT_FILE=}" ]; then
    export INIT_SCRIPT_FILE="init.sh"
fi

if [[ -d "${AIRFLOW_BREEZE_CONFIG_DIR}" && \
    -f "${AIRFLOW_BREEZE_CONFIG_DIR}/${INIT_SCRIPT_FILE}" ]]; then

        pushd "${AIRFLOW_BREEZE_CONFIG_DIR}" >/dev/null 2>&1 || exit 1
        echo
        echo "Sourcing the initialization script from ${INIT_SCRIPT_FILE} in ${AIRFLOW_BREEZE_CONFIG_DIR}"
        echo
         # shellcheck disable=1090
        source "${INIT_SCRIPT_FILE}"
        popd >/dev/null 2>&1 || exit 1
else
    echo
    echo "You can add ${AIRFLOW_BREEZE_CONFIG_DIR} directory and place ${INIT_SCRIPT_FILE}"
    echo "In it to make breeze source an initialization script automatically for you"
    echo
fi
