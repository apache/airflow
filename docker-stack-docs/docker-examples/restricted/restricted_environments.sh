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

# [START download]
mkdir -p docker-context-files
export AIRFLOW_VERSION="2.5.3"
rm docker-context-files/*.whl docker-context-files/*.tar.gz docker-context-files/*.txt || true

curl -Lo "docker-context-files/constraints-3.10.txt" \
    "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.10.txt"

echo
echo "Make sure you use the right python version here (should be same as in constraints)!"
echo
python --version

pip download --dest docker-context-files \
    --constraint docker-context-files/constraints-3.10.txt  \
    "apache-airflow[async,celery,elasticsearch,kubernetes,postgres,redis,ssh,statsd,virtualenv]==${AIRFLOW_VERSION}"
# [END download]

# [START build]
export DOCKER_BUILDKIT=1

docker build . \
    --pull \
    --build-arg BASE_IMAGE="debian:bookworm-slim" \
    --build-arg AIRFLOW_PYTHON_VERSION="3.12.12" \
    --build-arg AIRFLOW_INSTALLATION_METHOD="apache-airflow" \
    --build-arg AIRFLOW_VERSION="${AIRFLOW_VERSION}" \
    --build-arg INSTALL_MYSQL_CLIENT="false" \
    --build-arg INSTALL_MSSQL_CLIENT="false" \
    --build-arg INSTALL_POSTGRES_CLIENT="true" \
    --build-arg DOCKER_CONTEXT_FILES="docker-context-files" \
    --build-arg INSTALL_DISTRIBUTIONS_FROM_CONTEXT="true" \
    --build-arg AIRFLOW_CONSTRAINTS_LOCATION="/docker-context-files/constraints-3.10.txt" \
    --tag airflow-my-restricted-environment:0.0.1
# [END build]

docker rmi --force "airflow-my-restricted-environment"
popd
rm -rf "${TEMP_DOCKER_DIR}"
