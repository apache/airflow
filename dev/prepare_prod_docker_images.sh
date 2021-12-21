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
AIRFLOW_SOURCES_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
export AIRFLOW_SOURCES_DIR

set -e

CURRENT_PYTHON_MAJOR_MINOR_VERSIONS=("3.7" "3.8" "3.9" "3.6")

usage() {
    local cmdname
    cmdname="$(basename -- "$0")"

    cat << EOF
Usage: ${cmdname} <AIRFLOW_VERSION>

Prepares prod docker images for the version specified.

EOF
}

if [[ "$#" -ne 1 ]]; then
    >&2 echo "You must provide Airflow version."
    usage
    exit 1
fi

export INSTALL_AIRFLOW_VERSION="${1}"

for python_version in "${CURRENT_PYTHON_MAJOR_MINOR_VERSIONS[@]}"
do
  export PYTHON_MAJOR_MINOR_VERSION=${python_version}
  "${AIRFLOW_SOURCES_DIR}/scripts/ci/tools/build_dockerhub.sh"
done

if [[ ${INSTALL_AIRFLOW_VERSION} =~ .*rc.* ]]; then
    echo
    echo "Skipping tagging latest as this is an rc version"
    echo
    exit
fi

echo "Should we tag version ${1} with latest tag [y/N]"
read -r RESPONSE

if [[ ${RESPONSE} == 'n' || ${RESPONSE} = 'N' ]]; then
    echo
    echo "Skip tagging the image with latest tag."
    echo
    exit
fi

for python_version in "${CURRENT_PYTHON_MAJOR_MINOR_VERSIONS[@]}"
do
    docker tag "apache/airflow:${INSTALL_AIRFLOW_VERSION}-python${python_version}" \
        "apache/airflow:latest-python${python_version}"
    docker push "apache/airflow:latest-python${python_version}"
done

docker tag "apache/airflow:${INSTALL_AIRFLOW_VERSION}" "apache/airflow:latest"
docker push "apache/airflow:latest"
