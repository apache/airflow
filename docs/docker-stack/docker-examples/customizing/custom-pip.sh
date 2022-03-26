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

# This is an example docker build script. It is not intended for PRODUCTION use
set -euo pipefail
AIRFLOW_SOURCES="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../" && pwd)"
TEMP_DOCKER_DIR=$(mktemp -d)
pushd "${TEMP_DOCKER_DIR}"

cp "${AIRFLOW_SOURCES}/Dockerfile" "${TEMP_DOCKER_DIR}"
# [START build]
mkdir -p docker-context-files

cat <<EOF >./docker-context-files/pip.conf
[global]
verbose = 2
EOF

export DOCKER_BUILDKIT=1
docker build . \
    --build-arg DOCKER_CONTEXT_FILES=./docker-context-files \
    --tag "my-custom-pip-verbose-airflow:0.0.1"
docker run -it my-beautifulsoup4-airflow:0.0.1 python -c 'import bs4; import sys; sys.exit(0)' && \
    echo "Success! Beautifulsoup4 installed" && echo
# [END build]
docker rmi --force "my-custom-pip-verbose-airflow:0.0.1"
popd
rm -rf "${TEMP_DOCKER_DIR}"
