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

set -euo pipefail

PRE_COMMIT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
AIRFLOW_SOURCES=$(cd "${PRE_COMMIT_DIR}/../../../" && pwd);
cd "${AIRFLOW_SOURCES}" || exit 1

PYTHONPATH="$(pwd)"
export PYTHONPATH
find airflow/providers -name '*.py' -print0 | \
    xargs -0 python3 tests/build_provider_packages_dependencies.py \
        --provider-dependencies-file "airflow/providers/dependencies.json" \
        --documentation-file CONTRIBUTING.rst
