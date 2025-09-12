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
set -euxo pipefail

cd "$( dirname "${BASH_SOURCE[0]}" )/../../"

PYTHON_ARG=""

PIP_VERSION="25.2"
UV_VERSION="0.8.17"
if [[ ${PYTHON_VERSION=} != "" ]]; then
    PYTHON_ARG="--python=$(which python"${PYTHON_VERSION}") "
fi

python -m pip install --upgrade "pip==${PIP_VERSION}"
python -m pip install "uv==${UV_VERSION}"
uv tool uninstall apache-airflow-breeze >/dev/null 2>&1 || true
# shellcheck disable=SC2086
uv tool install ${PYTHON_ARG} --force --editable ./dev/breeze/
echo '/home/runner/.local/bin' >> "${GITHUB_PATH}"
