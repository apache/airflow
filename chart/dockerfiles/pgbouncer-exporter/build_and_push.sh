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
DOCKERHUB_USER=${DOCKERHUB_USER:="apache"}
readonly DOCKERHUB_USER
DOCKERHUB_REPO=${DOCKERHUB_REPO:="airflow"}
readonly DOCKERHUB_REPO

PGBOUNCER_EXPORTER_VERSION="0.5.0"
readonly PGBOUNCER_EXPORTER_VERSION

AIRFLOW_PGBOUNCER_EXPORTER_VERSION="2020.09.05"
readonly AIRFLOW_PGBOUNCER_EXPORTER_VERSION

EXPECTED_GO_VERSION="1.15.1"
readonly EXPECTED_GO_VERSION

COMMIT_SHA=$(git rev-parse HEAD)
readonly COMMIT_SHA

cd "$( dirname "${BASH_SOURCE[0]}" )" || exit 1

current_go_version=$("go${EXPECTED_GO_VERSION}" version 2>/dev/null | awk '{ print $3 }' 2>/dev/null || true)

if [[ ${current_go_version} == "" ]]; then
  current_go_version=$(go version 2>/dev/null | awk '{ print $3 }' 2>/dev/null)
  GO_BIN=$(command -v go 2>/dev/null || true)
else
  GO_BIN=$(command -v go${EXPECTED_GO_VERSION} 2>/dev/null)
fi
readonly GO_BIN

if [[ ${current_go_version} == "" ]]; then
  echo "ERROR! You have no go installed"
  echo
  echo "Please install go${EXPECTED_GO_VERSION} to build the package"
  echo
  echo "You need to have golang installed. Follow https://golang.org/doc/install"
  echo
fi

if [[ ${current_go_version} != "go${EXPECTED_GO_VERSION}" ]]; then
  echo "ERROR! You have unexpected version of go in the path ${current_go_version}"
  echo
  echo "Make sure you have go${EXPECTED_GO_VERSION} installed:"
  echo
  echo "   go get golang.org/dl/go${EXPECTED_GO_VERSION}"
  echo
  echo "   go${EXPECTED_GO_VERSION} download"
  echo
  echo "You might need to add ${HOME}/go/bin to your PATH"
  echo
  exit 1
fi

PGBOUNCER_EXPORTER_DIR="$(pwd)/pgbouncer_exporter-${PGBOUNCER_EXPORTER_VERSION}"
readonly  PGBOUNCER_EXPORTER_DIR

# Needs to be set for alpine images to run net package of GO
export CGO_ENABLED=0
rm -rf "$(pwd)/pgbouncer_exporter*"
mkdir -pv "${PGBOUNCER_EXPORTER_DIR}"

curl -L "https://github.com/jbub/pgbouncer_exporter/archive/v${PGBOUNCER_EXPORTER_VERSION}.tar.gz" | tar -zx

cd "${PGBOUNCER_EXPORTER_DIR}"

"${GO_BIN}" get ./...
"${GO_BIN}" build

tag="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:airflow-pgbouncer-exporter-${AIRFLOW_PGBOUNCER_EXPORTER_VERSION}-${PGBOUNCER_EXPORTER_VERSION}"

docker build . \
    --pull \
    --build-arg "PGBOUNCER_EXPORTER_VERSION=${PGBOUNCER_EXPORTER_VERSION}" \
    --build-arg "AIRFLOW_PGBOUNCER_EXPORTER_VERSION=${AIRFLOW_PGBOUNCER_EXPORTER_VERSION}"\
    --build-arg "COMMIT_SHA=${COMMIT_SHA}" \
    --build-arg "GO_VERSION=${current_go_version}" \
    --tag "${tag}"

docker push "${tag}"
